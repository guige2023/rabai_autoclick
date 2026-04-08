"""API metrics and monitoring action module for RabAI AutoClick.

Provides:
- ApiMetricsAction: Collect and aggregate API metrics
- ApiHealthMonitorAction: Monitor API health
- ApiObserverAction: Observe API behavior
- ApiNotificationAction: Send notifications based on API events
"""

import time
import json
import threading
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AlertSeverity(str, Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ApiMetricsAction(BaseAction):
    """Collect and aggregate API metrics."""
    action_type = "api_metrics"
    display_name = "API指标收集"
    description = "API指标收集与聚合"

    def __init__(self):
        super().__init__()
        self._metrics: Dict[str, List[Dict]] = {}
        self._counters: Dict[str, float] = {}
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "record")
            metric_name = params.get("metric_name", "")

            if operation == "record":
                if not metric_name:
                    return ActionResult(success=False, message="metric_name required")

                if metric_name not in self._metrics:
                    self._metrics[metric_name] = []

                metric_type = params.get("type", "counter")
                value = params.get("value", 1)
                labels = params.get("labels", {})

                entry = {
                    "timestamp": time.time(),
                    "value": value,
                    "labels": labels,
                    "type": metric_type
                }

                self._metrics[metric_name].append(entry)
                if len(self._metrics[metric_name]) > 100000:
                    self._metrics[metric_name] = self._metrics[metric_name][-50000:]

                if metric_type == "counter":
                    self._counters[metric_name] = self._counters.get(metric_name, 0) + value
                elif metric_type == "gauge":
                    self._gauges[metric_name] = value
                elif metric_type == "histogram":
                    if metric_name not in self._histograms:
                        self._histograms[metric_name] = []
                    self._histograms[metric_name].append(value)
                    if len(self._histograms[metric_name]) > 10000:
                        self._histograms[metric_name] = self._histograms[metric_name][-5000:]

                return ActionResult(success=True, data={"recorded": metric_name, "value": value})

            elif operation == "query":
                if not metric_name:
                    return ActionResult(success=False, message="metric_name required")

                start_time = params.get("start_time", time.time() - 3600)
                end_time = params.get("end_time", time.time())

                entries = self._metrics.get(metric_name, [])
                filtered = [e for e in entries if start_time <= e["timestamp"] <= end_time]

                values = [e["value"] for e in filtered]
                count = len(values)
                if not values:
                    return ActionResult(success=True, data={"metric": metric_name, "count": 0, "values": []})

                total = sum(values)
                avg = total / count

                return ActionResult(
                    success=True,
                    data={
                        "metric": metric_name,
                        "count": count,
                        "sum": round(total, 4),
                        "avg": round(avg, 4),
                        "min": min(values),
                        "max": max(values)
                    }
                )

            elif operation == "aggregate":
                metric_names = params.get("metric_names", [])
                start_time = params.get("start_time", time.time() - 3600)
                end_time = params.get("end_time", time.time())
                agg_type = params.get("agg_type", "sum")

                results = {}
                for mname in metric_names:
                    entries = self._metrics.get(mname, [])
                    filtered = [e for e in entries if start_time <= e["timestamp"] <= end_time]
                    values = [e["value"] for e in filtered]
                    if not values:
                        results[mname] = 0
                    elif agg_type == "sum":
                        results[mname] = round(sum(values), 4)
                    elif agg_type == "avg":
                        results[mname] = round(sum(values) / len(values), 4)
                    elif agg_type == "count":
                        results[mname] = len(values)
                    elif agg_type == "max":
                        results[mname] = max(values)
                    elif agg_type == "min":
                        results[mname] = min(values)

                return ActionResult(success=True, data={"aggregation": results, "type": agg_type})

            elif operation == "rate":
                metric_name = params.get("metric_name", "")
                window_seconds = params.get("window_seconds", 60)

                entries = self._metrics.get(metric_name, [])
                now = time.time()
                cutoff = now - window_seconds
                recent = [e for e in entries if e["timestamp"] >= cutoff]

                rate = len(recent) / window_seconds if window_seconds > 0 else 0

                return ActionResult(
                    success=True,
                    data={"metric": metric_name, "rate": round(rate, 4), "window_seconds": window_seconds}
                )

            elif operation == "histogram":
                metric_name = params.get("metric_name", "")
                if metric_name not in self._histograms:
                    return ActionResult(success=False, message=f"No histogram for '{metric_name}'")

                values = sorted(self._histograms[metric_name])
                n = len(values)
                if n == 0:
                    return ActionResult(success=True, data={"metric": metric_name, "count": 0})

                percentiles = {}
                for p in [50, 75, 90, 95, 99]:
                    idx = int(n * p / 100)
                    percentiles[f"p{p}"] = round(values[min(idx, n - 1)], 4)

                return ActionResult(
                    success=True,
                    data={
                        "metric": metric_name,
                        "count": n,
                        "mean": round(sum(values) / n, 4),
                        "min": values[0],
                        "max": values[-1],
                        "percentiles": percentiles
                    }
                )

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={
                        "metrics": list(self._metrics.keys()),
                        "counters": list(self._counters.keys()),
                        "gauges": list(self._gauges.keys()),
                        "histograms": list(self._histograms.keys())
                    }
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Metrics error: {str(e)}")


class ApiHealthMonitorAction(BaseAction):
    """Monitor API health status."""
    action_type = "api_health_monitor"
    display_name = "API健康监控"
    description = "API健康状态监控"

    def __init__(self):
        super().__init__()
        self._services: Dict[str, Dict] = {}
        self._health_history: Dict[str, List[Dict]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "register")
            service_name = params.get("service_name", "")

            if operation == "register":
                if not service_name:
                    return ActionResult(success=False, message="service_name required")

                self._services[service_name] = {
                    "name": service_name,
                    "endpoint": params.get("endpoint", ""),
                    "check_interval": params.get("check_interval", 30),
                    "timeout": params.get("timeout", 5),
                    "failure_threshold": params.get("failure_threshold", 3),
                    "success_threshold": params.get("success_threshold", 2),
                    "enabled": params.get("enabled", True),
                    "created_at": time.time(),
                    "failure_count": 0,
                    "success_count": 0,
                    "current_status": "unknown"
                }
                self._health_history[service_name] = []

                return ActionResult(success=True, data={"service": service_name}, message=f"Service '{service_name}' registered")

            elif operation == "report":
                if not service_name:
                    return ActionResult(success=False, message="service_name required")

                if service_name not in self._services:
                    return ActionResult(success=False, message=f"Service '{service_name}' not registered")

                service = self._services[service_name]
                is_healthy = params.get("is_healthy", True)
                response_time = params.get("response_time", 0)
                details = params.get("details", {})

                health_entry = {
                    "timestamp": time.time(),
                    "healthy": is_healthy,
                    "response_time_ms": response_time,
                    "details": details
                }
                self._health_history[service_name].append(health_entry)
                if len(self._health_history[service_name]) > 1000:
                    self._health_history[service_name] = self._health_history[service_name][-500:]

                if is_healthy:
                    service["success_count"] += 1
                    service["failure_count"] = 0
                    if service["success_count"] >= service["success_threshold"]:
                        service["current_status"] = "healthy"
                else:
                    service["failure_count"] += 1
                    service["success_count"] = 0
                    if service["failure_count"] >= service["failure_threshold"]:
                        service["current_status"] = "unhealthy"

                return ActionResult(
                    success=True,
                    data={"service": service_name, "status": service["current_status"]},
                    message=f"Service '{service_name}': {service['current_status']}"
                )

            elif operation == "status":
                if service_name:
                    if service_name not in self._services:
                        return ActionResult(success=False, message=f"Service '{service_name}' not found")

                    service = self._services[service_name]
                    recent = self._health_history.get(service_name, [])[-10:]
                    avg_response = sum(e.get("response_time_ms", 0) for e in recent) / len(recent) if recent else 0

                    return ActionResult(
                        success=True,
                        data={
                            "service": service_name,
                            "status": service["current_status"],
                            "failure_count": service["failure_count"],
                            "success_count": service["success_count"],
                            "avg_response_time_ms": round(avg_response, 2),
                            "recent_health": recent
                        }
                    )

                all_status = {name: svc["current_status"] for name, svc in self._services.items()}
                healthy_count = sum(1 for s in all_status.values() if s == "healthy")
                return ActionResult(
                    success=True,
                    data={
                        "services": all_status,
                        "healthy_count": healthy_count,
                        "total_count": len(all_status)
                    }
                )

            elif operation == "history":
                if service_name not in self._health_history:
                    return ActionResult(success=False, message=f"No history for '{service_name}'")
                return ActionResult(
                    success=True,
                    data={"service": service_name, "history": self._health_history[service_name]}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Health monitor error: {str(e)}")


class ApiObserverAction(BaseAction):
    """Observe API behavior and track patterns."""
    action_type = "api_observer"
    display_name = "API行为观察"
    description = "API行为追踪"

    def __init__(self):
        super().__init__()
        self._observations: Dict[str, List[Dict]] = {}
        self._patterns: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "observe")
            api_name = params.get("api_name", "")

            if operation == "observe":
                if not api_name:
                    return ActionResult(success=False, message="api_name required")

                if api_name not in self._observations:
                    self._observations[api_name] = []

                observation = {
                    "timestamp": time.time(),
                    "endpoint": params.get("endpoint", ""),
                    "method": params.get("method", "GET"),
                    "status_code": params.get("status_code", 200),
                    "duration_ms": params.get("duration_ms", 0),
                    "request_size": params.get("request_size", 0),
                    "response_size": params.get("response_size", 0),
                    "error": params.get("error", None),
                    "user_agent": params.get("user_agent", ""),
                    "client_ip": params.get("client_ip", "")
                }

                self._observations[api_name].append(observation)
                if len(self._observations[api_name]) > 50000:
                    self._observations[api_name] = self._observations[api_name][-20000:]

                self._detect_patterns(api_name)

                return ActionResult(success=True, data={"observed": observation})

            elif operation == "report":
                if api_name not in self._observations:
                    return ActionResult(success=False, message=f"No observations for '{api_name}'")

                observations = self._observations[api_name]
                recent = observations[-1000:] if len(observations) > 1000 else observations

                status_codes = {}
                total_duration = 0
                error_count = 0
                methods = {}

                for obs in recent:
                    sc = obs.get("status_code", 200)
                    status_codes[sc] = status_codes.get(sc, 0) + 1
                    total_duration += obs.get("duration_ms", 0)
                    if obs.get("error"):
                        error_count += 1
                    method = obs.get("method", "GET")
                    methods[method] = methods.get(method, 0) + 1

                return ActionResult(
                    success=True,
                    data={
                        "api": api_name,
                        "total_requests": len(recent),
                        "avg_duration_ms": round(total_duration / len(recent), 2) if recent else 0,
                        "error_count": error_count,
                        "error_rate": round(error_count / len(recent), 4) if recent else 0,
                        "status_codes": status_codes,
                        "methods": methods
                    }
                )

            elif operation == "patterns":
                return ActionResult(
                    success=True,
                    data={"patterns": self._patterns}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Observer error: {str(e)}")

    def _detect_patterns(self, api_name: str):
        recent = self._observations.get(api_name, [])[-100:]
        if len(recent) < 10:
            return

        error_observations = [o for o in recent if o.get("error") or o.get("status_code", 200) >= 400]
        if len(error_observations) > 5:
            self._patterns[f"{api_name}_high_error_rate"] = {
                "api": api_name,
                "pattern": "high_error_rate",
                "error_count": len(error_observations),
                "sample": error_observations[-3:]
            }


class ApiNotificationAction(BaseAction):
    """Send notifications based on API events."""
    action_type = "api_notification"
    display_name = "API通知"
    description = "API事件通知"

    def __init__(self):
        super().__init__()
        self._notification_rules: Dict[str, Dict] = {}
        self._notifications: List[Dict] = []
        self._channels: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "send")
            rule_name = params.get("rule_name", "")

            if operation == "add_rule":
                if not rule_name:
                    return ActionResult(success=False, message="rule_name required")

                self._notification_rules[rule_name] = {
                    "name": rule_name,
                    "condition": params.get("condition", {}),
                    "channel": params.get("channel", "log"),
                    "template": params.get("template", "Alert: {message}"),
                    "enabled": params.get("enabled", True),
                    "cooldown_seconds": params.get("cooldown_seconds", 300),
                    "last_triggered": None,
                    "trigger_count": 0
                }
                return ActionResult(success=True, data={"rule": rule_name}, message=f"Rule '{rule_name}' added")

            elif operation == "add_channel":
                channel_name = params.get("channel_name", "")
                channel_type = params.get("channel_type", "log")
                self._channels[channel_name] = {
                    "name": channel_name,
                    "type": channel_type,
                    "config": params.get("config", {}),
                    "created_at": time.time()
                }
                return ActionResult(success=True, data={"channel": channel_name}, message=f"Channel '{channel_name}' configured")

            elif operation == "send":
                message = params.get("message", "")
                channel_name = params.get("channel", "log")
                severity = params.get("severity", AlertSeverity.INFO.value)

                notification = {
                    "id": len(self._notifications),
                    "timestamp": time.time(),
                    "message": message,
                    "channel": channel_name,
                    "severity": severity,
                    "metadata": params.get("metadata", {})
                }

                self._notifications.append(notification)
                if len(self._notifications) > 10000:
                    self._notifications = self._notifications[-5000:]

                return ActionResult(
                    success=True,
                    data={"notification_id": notification["id"], "channel": channel_name},
                    message=f"Notification sent: {severity} - {message[:50]}"
                )

            elif operation == "trigger":
                if not rule_name:
                    return ActionResult(success=False, message="rule_name required")

                if rule_name not in self._notification_rules:
                    return ActionResult(success=False, message=f"Rule '{rule_name}' not found")

                rule = self._notification_rules[rule_name]
                now = time.time()

                if rule["last_triggered"]:
                    elapsed = now - rule["last_triggered"]
                    if elapsed < rule["cooldown_seconds"]:
                        return ActionResult(
                            success=False,
                            data={"cooldown_remaining": round(rule["cooldown_seconds"] - elapsed, 1)},
                            message=f"Rule '{rule_name}' in cooldown"
                        )

                rule["last_triggered"] = now
                rule["trigger_count"] += 1

                message = rule["template"].format(**params.get("template_vars", {}))

                notification = {
                    "id": len(self._notifications),
                    "timestamp": now,
                    "message": message,
                    "channel": rule["channel"],
                    "severity": params.get("severity", AlertSeverity.WARNING.value),
                    "rule": rule_name
                }
                self._notifications.append(notification)

                return ActionResult(
                    success=True,
                    data={"notification_id": notification["id"], "rule": rule_name},
                    message=f"Rule '{rule_name}' triggered"
                )

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={
                        "rules": [r["name"] for r in self._notification_rules.values()],
                        "channels": list(self._channels.keys()),
                        "total_notifications": len(self._notifications)
                    }
                )

            elif operation == "recent":
                limit = params.get("limit", 20)
                return ActionResult(
                    success=True,
                    data={"notifications": self._notifications[-limit:]}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Notification error: {str(e)}")
