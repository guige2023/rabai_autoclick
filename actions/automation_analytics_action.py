"""Automation analytics action module for RabAI AutoClick.

Provides analytics and metrics operations:
- MetricsCollectorAction: Collect automation metrics
- MetricsAggregatorAction: Aggregate metrics data
- MetricsExporterAction: Export metrics to external systems
- MetricsAlertAction: Alert based on metrics thresholds
"""

import sys
import os
import time
import logging
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict
import threading

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult

logger = logging.getLogger(__name__)


@dataclass
class MetricPoint:
    """A single metric data point."""
    name: str
    value: float
    timestamp: datetime = field(default_factory=datetime.now)
    tags: Dict[str, str] = field(default_factory=dict)
    unit: str = ""


@dataclass
class MetricSummary:
    """Summary statistics for a metric."""
    name: str
    count: int
    sum: float
    min: float
    max: float
    avg: float
    p50: float
    p95: float
    p99: float


class MetricsStore:
    """In-memory metrics storage."""

    def __init__(self, retention_minutes: int = 60) -> None:
        self._metrics: Dict[str, List[MetricPoint]] = defaultdict(list)
        self._lock = threading.Lock()
        self._retention = timedelta(minutes=retention_minutes)

    def record(self, metric: MetricPoint) -> None:
        with self._lock:
            self._metrics[metric.name].append(metric)
            self._cleanup(metric.name)

    def _cleanup(self, name: str) -> None:
        cutoff = datetime.now() - self._retention
        self._metrics[name] = [
            m for m in self._metrics[name] if m.timestamp > cutoff
        ]

    def get(self, name: str, since: Optional[datetime] = None) -> List[MetricPoint]:
        with self._lock:
            metrics = self._metrics.get(name, [])
            if since:
                metrics = [m for m in metrics if m.timestamp >= since]
            return metrics

    def summarize(self, name: str, since: Optional[datetime] = None) -> Optional[MetricSummary]:
        points = self.get(name, since)
        if not points:
            return None

        values = sorted([p.value for p in points])
        count = len(values)
        total = sum(values)

        def percentile(data: List[float], p: float) -> float:
            if not data:
                return 0.0
            idx = int(len(data) * p)
            idx = min(idx, len(data) - 1)
            return data[idx]

        return MetricSummary(
            name=name,
            count=count,
            sum=total,
            min=values[0],
            max=values[-1],
            avg=total / count,
            p50=percentile(values, 0.50),
            p95=percentile(values, 0.95),
            p99=percentile(values, 0.99)
        )

    def list_metrics(self) -> List[str]:
        with self._lock:
            return list(self._metrics.keys())


_store = MetricsStore()
_alert_rules: Dict[str, Dict[str, Any]] = {}


class MetricsCollectorAction(BaseAction):
    """Collect automation metrics."""
    action_type = "automation_metrics_collector"
    display_name = "指标收集器"
    description = "收集自动化执行的指标数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        metric_name = params.get("name", "")
        value = params.get("value", 1.0)
        tags = params.get("tags", {})
        unit = params.get("unit", "")

        if not metric_name:
            return ActionResult(success=False, message="name参数是必需的")

        metric = MetricPoint(
            name=metric_name,
            value=float(value),
            tags=tags,
            unit=unit
        )
        _store.record(metric)

        return ActionResult(
            success=True,
            message=f"指标 {metric_name}={value}{unit} 已记录",
            data={"name": metric_name, "value": value, "timestamp": metric.timestamp.isoformat()}
        )


class MetricsAggregatorAction(BaseAction):
    """Aggregate metrics data."""
    action_type = "automation_metrics_aggregator"
    display_name = "指标聚合器"
    description = "聚合和分析指标数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        metric_name = params.get("name", "")
        since_minutes = params.get("since_minutes", 60)
        operation = params.get("operation", "summarize")

        if not metric_name:
            all_metrics = _store.list_metrics()
            return ActionResult(
                success=True,
                message=f"共 {len(all_metrics)} 个指标",
                data={"metrics": all_metrics}
            )

        since = datetime.now() - timedelta(minutes=since_minutes)

        if operation == "summarize":
            summary = _store.summarize(metric_name, since=since)
            if not summary:
                return ActionResult(success=False, message=f"指标 {metric_name} 无数据")

            return ActionResult(
                success=True,
                message=f"指标 {metric_name} 汇总",
                data={
                    "name": summary.name,
                    "count": summary.count,
                    "sum": round(summary.sum, 4),
                    "min": round(summary.min, 4),
                    "max": round(summary.max, 4),
                    "avg": round(summary.avg, 4),
                    "p50": round(summary.p50, 4),
                    "p95": round(summary.p95, 4),
                    "p99": round(summary.p99, 4)
                }
            )

        if operation == "get":
            points = _store.get(metric_name, since=since)
            return ActionResult(
                success=True,
                message=f"获取 {len(points)} 个数据点",
                data={
                    "points": [
                        {"value": p.value, "timestamp": p.timestamp.isoformat(), "tags": p.tags}
                        for p in points[-100:]
                    ]
                }
            )

        return ActionResult(success=False, message=f"未知操作: {operation}")


class MetricsExporterAction(BaseAction):
    """Export metrics to external systems."""
    action_type = "automation_metrics_exporter"
    display_name = "指标导出器"
    description = "将指标导出到外部系统"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        format_type = params.get("format", "json")
        metric_names = params.get("names", [])
        since_minutes = params.get("since_minutes", 60)
        since = datetime.now() - timedelta(minutes=since_minutes)

        if not metric_names:
            metric_names = _store.list_metrics()

        export_data = {}
        for name in metric_names:
            summary = _store.summarize(name, since=since)
            if summary:
                export_data[name] = {
                    "count": summary.count,
                    "sum": round(summary.sum, 4),
                    "min": round(summary.min, 4),
                    "max": round(summary.max, 4),
                    "avg": round(summary.avg, 4),
                    "p95": round(summary.p95, 4)
                }

        if format_type == "json":
            output = json.dumps(export_data, indent=2)
            return ActionResult(
                success=True,
                message=f"导出 {len(export_data)} 个指标 (JSON)",
                data={"format": "json", "data": export_data}
            )

        if format_type == "prometheus":
            lines = []
            for name, stats in export_data.items():
                safe_name = name.replace(".", "_").replace(" ", "_")
                lines.append(f"# TYPE {safe_name} gauge")
                for key, val in stats.items():
                    lines.append(f"{safe_name}_{key} {val}")
            output = "\n".join(lines)
            return ActionResult(
                success=True,
                message=f"导出 {len(export_data)} 个指标 (Prometheus)",
                data={"format": "prometheus", "data": output}
            )

        if format_type == "csv":
            lines = ["metric,count,sum,min,max,avg,p95"]
            for name, stats in export_data.items():
                lines.append(f"{name},{stats['count']},{stats['sum']},{stats['min']},{stats['max']},{stats['avg']},{stats['p95']}")
            output = "\n".join(lines)
            return ActionResult(
                success=True,
                message=f"导出 {len(export_data)} 个指标 (CSV)",
                data={"format": "csv", "data": output}
            )

        return ActionResult(success=False, message=f"未知格式: {format_type}")


class MetricsAlertAction(BaseAction):
    """Alert based on metrics thresholds."""
    action_type = "automation_metrics_alert"
    display_name = "指标告警"
    description = "基于指标阈值触发告警"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        operation = params.get("operation", "check")
        rule_name = params.get("rule_name", "")
        metric_name = params.get("metric_name", "")
        threshold = params.get("threshold", 0.0)
        comparison = params.get("comparison", "gt")
        severity = params.get("severity", "warning")

        global _alert_rules

        if operation == "create":
            if not rule_name or not metric_name:
                return ActionResult(success=False, message="rule_name和metric_name是必需的")

            _alert_rules[rule_name] = {
                "metric_name": metric_name,
                "threshold": threshold,
                "comparison": comparison,
                "severity": severity,
                "enabled": True
            }

            return ActionResult(
                success=True,
                message=f"告警规则 {rule_name} 已创建",
                data={"rule_name": rule_name}
            )

        if operation == "list":
            return ActionResult(
                success=True,
                message=f"共 {len(_alert_rules)} 条告警规则",
                data={"rules": list(_alert_rules.items())}
            )

        if operation == "check":
            triggered = []
            for name, rule in _alert_rules.items():
                if not rule.get("enabled", True):
                    continue
                summary = _store.summarize(rule["metric_name"])
                if not summary:
                    continue

                value = summary.avg
                thresh = rule["threshold"]
                comp = rule["comparison"]

                fired = False
                if comp == "gt" and value > thresh:
                    fired = True
                elif comp == "gte" and value >= thresh:
                    fired = True
                elif comp == "lt" and value < thresh:
                    fired = True
                elif comp == "lte" and value <= thresh:
                    fired = True
                elif comp == "eq" and value == thresh:
                    fired = True

                if fired:
                    triggered.append({
                        "rule": name,
                        "metric": rule["metric_name"],
                        "value": round(value, 4),
                        "threshold": thresh,
                        "comparison": comp,
                        "severity": rule["severity"]
                    })

            return ActionResult(
                success=True,
                message=f"检查完成，{len(triggered)} 个告警触发",
                data={"triggered": triggered, "count": len(triggered)}
            )

        if operation == "delete":
            if rule_name in _alert_rules:
                del _alert_rules[rule_name]
                return ActionResult(success=True, message=f"规则 {rule_name} 已删除")
            return ActionResult(success=False, message=f"规则 {rule_name} 不存在")

        return ActionResult(success=False, message=f"未知操作: {operation}")


import json
