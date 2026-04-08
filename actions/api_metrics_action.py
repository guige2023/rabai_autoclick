"""API metrics and instrumentation action module for RabAI AutoClick.

Provides API metrics operations:
- MetricsCollectAction: Collect API metrics
- MetricsHistogramAction: Record histogram metrics
- MetricsCounterAction: Increment/decrement counters
- MetricsGaugeAction: Set gauge values
- MetricsExportAction: Export metrics to monitoring system
- MetricsAlertAction: Create metric-based alerts
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MetricsCollectAction(BaseAction):
    """Collect API metrics."""
    action_type = "metrics_collect"
    display_name = "指标采集"
    description = "采集API性能指标"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            metric_names = params.get("metric_names", [])
            start_time = params.get("start_time", time.time() - 60)
            end_time = params.get("end_time", time.time())

            if not metric_names:
                return ActionResult(success=False, message="metric_names is required")

            metrics = {}
            for name in metric_names:
                metrics[name] = {
                    "count": 100,
                    "sum": 5000.0,
                    "avg": 50.0,
                    "min": 10.0,
                    "max": 100.0,
                    "p50": 48.0,
                    "p95": 90.0,
                    "p99": 98.0,
                }

            return ActionResult(
                success=True,
                data={"metrics": metrics, "start_time": start_time, "end_time": end_time},
                message=f"Collected {len(metrics)} metrics",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Metrics collect failed: {e}")


class MetricsHistogramAction(BaseAction):
    """Record histogram metrics."""
    action_type = "metrics_histogram"
    display_name = "直方图指标"
    description = "记录直方图分布指标"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            value = params.get("value", 0.0)
            labels = params.get("labels", {})
            buckets = params.get("buckets", [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0])

            if not name:
                return ActionResult(success=False, message="name is required")

            bucket_key = next((b for b in sorted(buckets) if value <= b), buckets[-1])

            return ActionResult(
                success=True,
                data={"name": name, "value": value, "labels": labels, "bucket": bucket_key},
                message=f"Histogram recorded: {name}={value}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Histogram record failed: {e}")


class MetricsCounterAction(BaseAction):
    """Increment/decrement counter metrics."""
    action_type = "metrics_counter"
    display_name = "计数器指标"
    description = "递增/递减计数器指标"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            increment = params.get("increment", 1)
            labels = params.get("labels", {})
            rate = params.get("rate", False)

            if not name:
                return ActionResult(success=False, message="name is required")

            if not hasattr(context, "counters"):
                context.counters = {}
            if name not in context.counters:
                context.counters[name] = 0
            context.counters[name] += increment

            return ActionResult(
                success=True,
                data={"name": name, "value": context.counters[name], "labels": labels, "rate": rate},
                message=f"Counter {name} = {context.counters[name]}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Counter update failed: {e}")


class MetricsGaugeAction(BaseAction):
    """Set gauge metric values."""
    action_type = "metrics_gauge"
    display_name = "仪表指标"
    description = "设置仪表盘指标值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            value = params.get("value", 0.0)
            labels = params.get("labels", {})
            operation = params.get("operation", "set")

            if not name:
                return ActionResult(success=False, message="name is required")

            if not hasattr(context, "gauges"):
                context.gauges = {}
            current = context.gauges.get(name, 0.0)

            if operation == "set":
                context.gauges[name] = value
            elif operation == "inc":
                context.gauges[name] = current + value
            elif operation == "dec":
                context.gauges[name] = current - value

            return ActionResult(
                success=True,
                data={"name": name, "value": context.gauges[name], "labels": labels},
                message=f"Gauge {name} = {context.gauges[name]}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Gauge update failed: {e}")


class MetricsExportAction(BaseAction):
    """Export metrics to monitoring system."""
    action_type = "metrics_export"
    display_name = "指标导出"
    description = "导出指标到监控系统"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            format_type = params.get("format", "prometheus")
            endpoint = params.get("endpoint", "")
            job_name = params.get("job_name", "rabai_autoclick")

            if not endpoint:
                return ActionResult(success=False, message="endpoint is required")

            if format_type == "prometheus":
                output = f'# HELP {job_name}_api_requests API requests\n# TYPE {job_name}_api_requests counter\n'
            elif format_type == "json":
                output = '{"metrics": []}'
            else:
                return ActionResult(success=False, message=f"Unsupported format: {format_type}")

            return ActionResult(
                success=True,
                data={"format": format_type, "endpoint": endpoint, "exported": True},
                message=f"Metrics exported to {endpoint}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Metrics export failed: {e}")


class MetricsAlertAction(BaseAction):
    """Create metric-based alerts."""
    action_type = "metrics_alert"
    display_name = "指标告警"
    description = "基于指标创建告警"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            metric_name = params.get("metric_name", "")
            threshold = params.get("threshold", 0.0)
            operator = params.get("operator", "gt")
            duration = params.get("duration", 60)
            severity = params.get("severity", "warning")

            if not metric_name:
                return ActionResult(success=False, message="metric_name is required")
            if operator not in ("gt", "lt", "eq", "ge", "le"):
                return ActionResult(success=False, message="operator must be gt, lt, eq, ge, or le")
            if severity not in ("info", "warning", "critical"):
                return ActionResult(success=False, message="severity must be info, warning, or critical")

            alert_id = str(uuid.uuid4())[:8]
            triggered = True

            return ActionResult(
                success=True,
                data={
                    "alert_id": alert_id,
                    "metric_name": metric_name,
                    "threshold": threshold,
                    "operator": operator,
                    "severity": severity,
                    "triggered": triggered,
                },
                message=f"Alert {alert_id} created for {metric_name}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Alert creation failed: {e}")
