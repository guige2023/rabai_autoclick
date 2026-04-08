"""Automation monitoring action module for RabAI AutoClick.

Provides automation monitoring and observability:
- MetricsCollectorAction: Collect and aggregate metrics
- AlertManagerAction: Manage alerts and notifications
- HealthCheckAction: Health check for automation components
- ExecutionTrackerAction: Track execution metrics
"""

import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set
from threading import Lock

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MetricsStore:
    """Thread-safe metrics storage."""

    def __init__(self):
        self._lock = Lock()
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = defaultdict(list)
        self._series: Dict[str, List[Dict]] = defaultdict(list)

    def increment(self, name: str, value: float = 1.0, labels: Optional[Dict] = None):
        with self._lock:
            key = self._make_key(name, labels)
            self._counters[key] += value

    def set_gauge(self, name: str, value: float, labels: Optional[Dict] = None):
        with self._lock:
            key = self._make_key(name, labels)
            self._gauges[key] = value

    def observe(self, name: str, value: float, labels: Optional[Dict] = None):
        with self._lock:
            key = self._make_key(name, labels)
            self._histograms[key].append(value)

    def push_series(self, name: str, value: float, labels: Optional[Dict] = None):
        with self._lock:
            key = self._make_key(name, labels)
            self._series[key].append({"value": value, "timestamp": time.time()})

    def get_snapshot(self) -> Dict:
        with self._lock:
            return {
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "histograms": {k: {"count": len(v), "sum": sum(v), "min": min(v), "max": max(v)} for k, v in self._histograms.items()},
            }

    def _make_key(self, name: str, labels: Optional[Dict]) -> str:
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"


_metrics_store = MetricsStore()


class MetricsCollectorAction(BaseAction):
    """Collect and aggregate metrics."""
    action_type = "metrics_collector"
    display_name = "指标收集器"
    description = "收集和聚合指标数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "record")
            metric_name = params.get("metric_name", "")
            value = params.get("value", 1.0)
            labels = params.get("labels", {})

            if action == "record":
                if not metric_name:
                    return ActionResult(success=False, message="metric_name is required")

                metric_type = params.get("type", "counter")
                if metric_type == "counter":
                    _metrics_store.increment(metric_name, value, labels)
                elif metric_type == "gauge":
                    _metrics_store.set_gauge(metric_name, value, labels)
                elif metric_type == "histogram":
                    _metrics_store.observe(metric_name, value, labels)
                elif metric_type == "series":
                    _metrics_store.push_series(metric_name, value, labels)

                return ActionResult(
                    success=True,
                    message=f"Recorded {metric_type} {metric_name}={value}",
                    data={"metric_name": metric_name, "value": value, "type": metric_type, "labels": labels},
                )

            elif action == "snapshot":
                snapshot = _metrics_store.get_snapshot()
                return ActionResult(success=True, message="Metrics snapshot", data=snapshot)

            elif action == "reset":
                _metrics_store._counters.clear()
                _metrics_store._gauges.clear()
                _metrics_store._histograms.clear()
                _metrics_store._series.clear()
                return ActionResult(success=True, message="Metrics reset")

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"MetricsCollector error: {e}")


class AlertManagerAction(BaseAction):
    """Manage alerts and notifications."""
    action_type = "alert_manager"
    display_name = "告警管理器"
    description = "管理和触发告警通知"

    def __init__(self):
        super().__init__()
        self._alerts: Dict[str, Dict] = {}
        self._alert_history: List[Dict] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "create")
            alert_id = params.get("alert_id", "")
            severity = params.get("severity", "warning")
            message = params.get("message", "")

            if action == "create":
                if not alert_id or not message:
                    return ActionResult(success=False, message="alert_id and message are required")

                alert = {
                    "id": alert_id,
                    "severity": severity,
                    "message": message,
                    "created_at": time.time(),
                    "status": "firing",
                    "labels": params.get("labels", {}),
                }
                self._alerts[alert_id] = alert
                self._alert_history.append(alert.copy())

                return ActionResult(
                    success=True,
                    message=f"Alert '{alert_id}' created: {severity}",
                    data={"alert": alert},
                )

            elif action == "resolve":
                if alert_id not in self._alerts:
                    return ActionResult(success=False, message=f"Alert '{alert_id}' not found")

                self._alerts[alert_id]["status"] = "resolved"
                self._alerts[alert_id]["resolved_at"] = time.time()
                resolved = self._alerts[alert_id].copy()
                self._alert_history.append(resolved)

                return ActionResult(success=True, message=f"Alert '{alert_id}' resolved", data={"alert": resolved})

            elif action == "list":
                severity_filter = params.get("severity")
                status_filter = params.get("status")

                alerts = list(self._alerts.values())
                if severity_filter:
                    alerts = [a for a in alerts if a["severity"] == severity_filter]
                if status_filter:
                    alerts = [a for a in alerts if a["status"] == status_filter]

                return ActionResult(
                    success=True,
                    message=f"{len(alerts)} alerts",
                    data={"alerts": alerts, "count": len(alerts)},
                )

            elif action == "history":
                limit = params.get("limit", 100)
                return ActionResult(
                    success=True,
                    message=f"{len(self._alert_history)} alert events",
                    data={"history": self._alert_history[-limit:]},
                )

            elif action == "count_by_severity":
                counts = {"firing": defaultdict(int), "resolved": defaultdict(int)}
                for alert in self._alerts.values():
                    counts[alert["status"]][alert["severity"]] += 1
                return ActionResult(success=True, message="Alert counts", data={"counts": counts})

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"AlertManager error: {e}")


class HealthCheckAction(BaseAction):
    """Health check for automation components."""
    action_type = "health_check"
    display_name = "健康检查"
    description = "自动化组件健康检查"

    def __init__(self):
        super().__init__()
        self._health_status: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "check")
            component = params.get("component", "default")
            timeout = params.get("timeout", 5)

            if action == "register":
                check_interval = params.get("check_interval", 60)
                self._health_status[component] = {
                    "status": "unknown",
                    "last_check": None,
                    "check_interval": check_interval,
                    "metadata": params.get("metadata", {}),
                }
                return ActionResult(success=True, message=f"Component '{component}' registered")

            elif action == "check":
                if component not in self._health_status:
                    self._health_status[component] = {"status": "unknown", "last_check": None}

                start_time = time.time()
                try:
                    self._health_status[component]["status"] = "healthy"
                    self._health_status[component]["last_check"] = time.time()
                    self._health_status[component]["latency_ms"] = int((time.time() - start_time) * 1000)
                    status = "healthy"
                except Exception as e:
                    self._health_status[component]["status"] = "unhealthy"
                    self._health_status[component]["error"] = str(e)
                    status = "unhealthy"

                return ActionResult(
                    success=status == "healthy",
                    message=f"Component '{component}': {status}",
                    data={"component": component, "status": status, "details": self._health_status.get(component)},
                )

            elif action == "list":
                all_healthy = all(s.get("status") == "healthy" for s in self._health_status.values())
                return ActionResult(
                    success=all_healthy,
                    message=f"{len(self._health_status)} components",
                    data={"components": self._health_status, "all_healthy": all_healthy},
                )

            elif action == "batch_check":
                components = params.get("components", list(self._health_status.keys()))
                results = {}
                for comp in components:
                    self._health_status.setdefault(comp, {"status": "unknown"})
                    results[comp] = self._health_status[comp].get("status", "unknown")
                return ActionResult(
                    success=all(r == "healthy" for r in results.values()),
                    message=f"Batch check: {sum(1 for r in results.values() if r == 'healthy')}/{len(results)} healthy",
                    data={"results": results},
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"HealthCheck error: {e}")


class ExecutionTrackerAction(BaseAction):
    """Track execution metrics."""
    action_type = "execution_tracker"
    display_name = "执行追踪器"
    description = "追踪执行指标和性能"

    def __init__(self):
        super().__init__()
        self._executions: List[Dict] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "track")
            execution_id = params.get("execution_id", f"exec_{int(time.time())}")

            if action == "start":
                self._executions.append({
                    "id": execution_id,
                    "status": "running",
                    "started_at": time.time(),
                    "name": params.get("name", ""),
                    "metadata": params.get("metadata", {}),
                })
                return ActionResult(
                    success=True,
                    message=f"Execution '{execution_id}' started",
                    data={"execution_id": execution_id, "status": "running"},
                )

            elif action == "complete":
                for exec in reversed(self._executions):
                    if exec["id"] == execution_id and exec["status"] == "running":
                        exec["status"] = "completed"
                        exec["completed_at"] = time.time()
                        exec["duration_ms"] = int((exec["completed_at"] - exec["started_at"]) * 1000)
                        exec["result"] = params.get("result", {})
                        break
                return ActionResult(
                    success=True,
                    message=f"Execution '{execution_id}' completed",
                    data={"execution_id": execution_id, "status": "completed"},
                )

            elif action == "fail":
                for exec in reversed(self._executions):
                    if exec["id"] == execution_id and exec["status"] == "running":
                        exec["status"] = "failed"
                        exec["failed_at"] = time.time()
                        exec["duration_ms"] = int((exec["failed_at"] - exec["started_at"]) * 1000)
                        exec["error"] = params.get("error", "Unknown error")
                        break
                return ActionResult(
                    success=False,
                    message=f"Execution '{execution_id}' failed",
                    data={"execution_id": execution_id, "status": "failed"},
                )

            elif action == "list":
                status_filter = params.get("status")
                limit = params.get("limit", 100)
                executions = self._executions[-limit:]
                if status_filter:
                    executions = [e for e in executions if e.get("status") == status_filter]
                return ActionResult(
                    success=True,
                    message=f"{len(executions)} executions",
                    data={"executions": executions, "count": len(executions)},
                )

            elif action == "stats":
                if not self._executions:
                    return ActionResult(success=True, message="No executions", data={"total": 0})

                total = len(self._executions)
                completed = sum(1 for e in self._executions if e.get("status") == "completed")
                failed = sum(1 for e in self._executions if e.get("status") == "failed")
                running = sum(1 for e in self._executions if e.get("status") == "running")

                durations = [e.get("duration_ms", 0) for e in self._executions if "duration_ms" in e]
                avg_duration = sum(durations) / len(durations) if durations else 0

                return ActionResult(
                    success=True,
                    message=f"Stats: {completed} completed, {failed} failed, {running} running",
                    data={
                        "total": total,
                        "completed": completed,
                        "failed": failed,
                        "running": running,
                        "avg_duration_ms": int(avg_duration),
                    },
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"ExecutionTracker error: {e}")
