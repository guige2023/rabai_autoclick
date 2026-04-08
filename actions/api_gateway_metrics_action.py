"""API Gateway Metrics Action Module.

Collects and aggregates API gateway metrics including
request counts, latency, error rates, and throughput.
"""

from __future__ import annotations

import sys
import os
import time
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class MetricPoint:
    """A single metric data point."""
    name: str
    value: float
    timestamp: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class AggregatedMetrics:
    """Aggregated metric values."""
    metric_name: str
    count: int = 0
    sum: float = 0.0
    min: float = 0.0
    max: float = 0.0
    avg: float = 0.0


class APIGatewayMetricsAction(BaseAction):
    """
    API Gateway metrics collection and aggregation.

    Collects request metrics, latency, error rates,
    and provides time-series aggregation.

    Example:
        metrics = APIGatewayMetricsAction()
        result = metrics.execute(ctx, {"action": "record", "metric": "request_count", "value": 1})
    """
    action_type = "api_gateway_metrics"
    display_name = "API网关指标"
    description = "API网关指标收集：请求数、延迟、错误率"

    def __init__(self) -> None:
        super().__init__()
        self._metrics: List[MetricPoint] = []
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = defaultdict(list)

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "record":
                return self._record_metric(params)
            elif action == "increment":
                return self._increment_counter(params)
            elif action == "set_gauge":
                return self._set_gauge(params)
            elif action == "observe":
                return self._observe_histogram(params)
            elif action == "query":
                return self._query_metrics(params)
            elif action == "get_summary":
                return self._get_summary(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Metrics error: {str(e)}")

    def _record_metric(self, params: Dict[str, Any]) -> ActionResult:
        metric = params.get("metric", "")
        value = params.get("value", 0.0)
        labels = params.get("labels", {})

        if not metric:
            return ActionResult(success=False, message="metric is required")

        point = MetricPoint(name=metric, value=value, timestamp=time.time(), labels=labels)
        self._metrics.append(point)

        return ActionResult(success=True, message=f"Recorded: {metric}={value}")

    def _increment_counter(self, params: Dict[str, Any]) -> ActionResult:
        counter = params.get("counter", "")
        value = params.get("value", 1.0)

        if not counter:
            return ActionResult(success=False, message="counter is required")

        self._counters[counter] += value

        return ActionResult(success=True, data={"counter": counter, "value": self._counters[counter]})

    def _set_gauge(self, params: Dict[str, Any]) -> ActionResult:
        gauge = params.get("gauge", "")
        value = params.get("value", 0.0)

        if not gauge:
            return ActionResult(success=False, message="gauge is required")

        self._gauges[gauge] = value

        return ActionResult(success=True, data={"gauge": gauge, "value": value})

    def _observe_histogram(self, params: Dict[str, Any]) -> ActionResult:
        histogram = params.get("histogram", "")
        value = params.get("value", 0.0)

        if not histogram:
            return ActionResult(success=False, message="histogram is required")

        self._histograms[histogram].append(value)

        return ActionResult(success=True, message=f"Observed: {histogram}={value}")

    def _query_metrics(self, params: Dict[str, Any]) -> ActionResult:
        metric_name = params.get("metric_name", "")
        start_time = params.get("start_time", time.time() - 3600)
        end_time = params.get("end_time", time.time())

        points = [p for p in self._metrics if p.name == metric_name and start_time <= p.timestamp <= end_time]

        return ActionResult(success=True, data={"count": len(points), "points": [{"value": p.value, "timestamp": p.timestamp} for p in points[:100]]})

    def _get_summary(self, params: Dict[str, Any]) -> ActionResult:
        counters = dict(self._counters)
        gauges = dict(self._gauges)

        histogram_summary = {}
        for name, values in self._histograms.items():
            if values:
                histogram_summary[name] = {"count": len(values), "avg": sum(values) / len(values), "min": min(values), "max": max(values)}

        return ActionResult(success=True, data={"counters": counters, "gauges": gauges, "histograms": histogram_summary})
