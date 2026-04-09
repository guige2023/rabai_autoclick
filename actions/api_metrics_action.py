"""API Metrics Action Module.

Provides API metrics collection, analysis, and reporting with support
for latency tracking, error rates, throughput, and custom metrics.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Types of metrics."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"
    RATE = "rate"


class Aggregation(Enum):
    """Aggregation methods."""
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    PERCENTILE = "percentile"
    P50 = "p50"
    P90 = "p90"
    P95 = "p95"
    P99 = "p99"


@dataclass
class MetricPoint:
    """A single metric data point."""
    timestamp: float
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class MetricSeries:
    """A time series of metric data."""
    name: str
    metric_type: MetricType
    unit: str = ""
    points: List[MetricPoint] = field(default_factory=list)

    def add(self, value: float, timestamp: Optional[float] = None,
            labels: Optional[Dict[str, str]] = None) -> None:
        """Add a data point."""
        self.points.append(MetricPoint(
            timestamp=timestamp or time.time(),
            value=value,
            labels=labels or {},
        ))

    def get_percentile(self, p: float) -> float:
        """Calculate percentile of values."""
        if not self.points:
            return 0.0
        sorted_values = sorted(pt.value for pt in self.points)
        idx = int((p / 100.0) * len(sorted_values))
        return sorted_values[min(idx, len(sorted_values) - 1)]

    def get_stats(self) -> Dict[str, float]:
        """Get statistical summary."""
        if not self.points:
            return {"count": 0, "sum": 0, "avg": 0, "min": 0, "max": 0}
        values = [pt.value for pt in self.points]
        return {
            "count": len(values),
            "sum": sum(values),
            "avg": sum(values) / len(values),
            "min": min(values),
            "max": max(values),
            "p50": self.get_percentile(50),
            "p90": self.get_percentile(90),
            "p95": self.get_percentile(95),
            "p99": self.get_percentile(99),
        }


@dataclass
class APIMetricsConfig:
    """Configuration for API metrics."""
    window_size_seconds: float = 60.0
    retention_seconds: float = 3600.0
    percentiles: List[float] = field(default_factory=lambda: [50, 90, 95, 99])
    enable_histograms: bool = True


class InMemoryMetricsStore:
    """Thread-safe in-memory metrics storage."""

    def __init__(self, config: Optional[APIMetricsConfig] = None):
        self.config = config or APIMetricsConfig()
        self._metrics: Dict[str, MetricSeries] = {}
        self._lock = threading.RLock()
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}

    def increment_counter(self, name: str, value: float = 1.0,
                         labels: Optional[Dict[str, str]] = None) -> None:
        """Increment a counter metric."""
        key = self._make_key(name, labels)
        with self._lock:
            self._counters[key] += value
            if key not in self._metrics:
                self._metrics[key] = MetricSeries(
                    name=name, metric_type=MetricType.COUNTER, labels=labels or {}
                )

    def set_gauge(self, name: str, value: float,
                  labels: Optional[Dict[str, str]] = None) -> None:
        """Set a gauge metric."""
        key = self._make_key(name, labels)
        with self._lock:
            self._gauges[key] = value
            if key not in self._metrics:
                self._metrics[key] = MetricSeries(
                    name=name, metric_type=MetricType.GAUGE, labels=labels or {}
                )
            self._metrics[key].add(value, labels=labels)

    def record_histogram(self, name: str, value: float,
                         labels: Optional[Dict[str, str]] = None) -> None:
        """Record a histogram value."""
        key = self._make_key(name, labels)
        with self._lock:
            if key not in self._metrics:
                self._metrics[key] = MetricSeries(
                    name=name, metric_type=MetricType.HISTOGRAM, labels=labels or {}
                )
            self._metrics[key].add(value, labels=labels)

    def record_timer(self, name: str, duration_ms: float,
                     labels: Optional[Dict[str, str]] = None) -> None:
        """Record a timer duration."""
        self.record_histogram(name, duration_ms, labels)

    def get_metric(self, name: str, labels: Optional[Dict[str, str]] = None) -> Optional[MetricSeries]:
        """Get a metric by name."""
        key = self._make_key(name, labels)
        with self._lock:
            return self._metrics.get(key)

    def get_all_metrics(self) -> Dict[str, MetricSeries]:
        """Get all metrics."""
        with self._lock:
            return dict(self._metrics)

    def get_counter_value(self, name: str, labels: Optional[Dict[str, str]] = None) -> float:
        """Get current counter value."""
        key = self._make_key(name, labels)
        with self._lock:
            return self._counters.get(key, 0.0)

    def get_gauge_value(self, name: str, labels: Optional[Dict[str, str]] = None) -> Optional[float]:
        """Get current gauge value."""
        key = self._make_key(name, labels)
        with self._lock:
            return self._gauges.get(key)

    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self._metrics.clear()
            self._counters.clear()
            self._gauges.clear()

    def _make_key(self, name: str, labels: Optional[Dict[str, str]]) -> str:
        """Create a unique key for a metric with labels."""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def cleanup_old_data(self) -> int:
        """Remove data older than retention period."""
        cutoff = time.time() - self.config.retention_seconds
        removed = 0
        with self._lock:
            for series in self._metrics.values():
                original_count = len(series.points)
                series.points = [p for p in series.points if p.timestamp > cutoff]
                removed += original_count - len(series.points)
        return removed


class APIMetricsAction(BaseAction):
    """API Metrics Action for collecting and analyzing API metrics.

    Supports counters, gauges, histograms, timers with percentile
    calculations and time-window aggregation.

    Examples:
        >>> action = APIMetricsAction()
        >>> result = action.execute(ctx, {
        ...     "command": "record",
        ...     "metric_type": "histogram",
        ...     "name": "api_request_duration_ms",
        ...     "value": 150.5
        ... })
    """

    action_type = "api_metrics"
    display_name = "API指标"
    description = "API指标收集分析：延迟/错误率/吞吐量/自定义指标"

    _store: Optional[InMemoryMetricsStore] = None
    _store_lock = threading.Lock()

    def __init__(self):
        super().__init__()
        with self._store_lock:
            if APIMetricsAction._store is None:
                APIMetricsAction._store = InMemoryMetricsStore()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute metrics command.

        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'record', 'get', 'increment', 'set', 'list', 'stats', 'reset'
                - metric_type: 'counter', 'gauge', 'histogram', 'timer'
                - name: Metric name
                - value: Metric value
                - labels: Labels/dimensions for the metric
                - window: Time window for aggregation (seconds)

        Returns:
            ActionResult with metrics data.
        """
        command = params.get("command", "record")

        try:
            if command == "record":
                return self._record_metric(params)
            elif command == "increment":
                return self._increment_counter(params)
            elif command == "set":
                return self._set_gauge(params)
            elif command == "get":
                return self._get_metric(params)
            elif command == "list":
                return self._list_metrics(params)
            elif command == "stats":
                return self._get_stats(params)
            elif command == "reset":
                return self._reset_metrics(params)
            elif command == "query":
                return self._query_metrics(params)
            else:
                return ActionResult(success=False, message=f"Unknown command: {command}")

        except Exception as e:
            logger.exception("Metrics command failed")
            return ActionResult(success=False, message=f"Metrics error: {str(e)}")

    def _record_metric(self, params: Dict[str, Any]) -> ActionResult:
        """Record a metric value."""
        metric_type_str = params.get("metric_type", "histogram")
        name = params.get("name", "unnamed_metric")
        value = params.get("value", 0.0)
        labels = params.get("labels", {})

        try:
            metric_type = MetricType(metric_type_str)
        except ValueError:
            return ActionResult(success=False, message=f"Invalid metric type: {metric_type_str}")

        with self._store_lock:
            if metric_type == MetricType.COUNTER:
                self._store.increment_counter(name, value, labels)
            elif metric_type == MetricType.GAUGE:
                self._store.set_gauge(name, value, labels)
            elif metric_type in (MetricType.HISTOGRAM, MetricType.TIMER):
                self._store.record_histogram(name, value, labels)

        return ActionResult(
            success=True,
            message=f"Recorded {metric_type.value} metric: {name}",
            data={"name": name, "metric_type": metric_type.value, "value": value}
        )

    def _increment_counter(self, params: Dict[str, Any]) -> ActionResult:
        """Increment a counter metric."""
        name = params.get("name")
        if not name:
            return ActionResult(success=False, message="name required")

        value = params.get("value", 1.0)
        labels = params.get("labels", {})

        self._store.increment_counter(name, value, labels)
        current = self._store.get_counter_value(name, labels)

        return ActionResult(
            success=True,
            message=f"Incremented counter: {name}",
            data={"name": name, "value": value, "total": current}
        )

    def _set_gauge(self, params: Dict[str, Any]) -> ActionResult:
        """Set a gauge metric."""
        name = params.get("name")
        if not name:
            return ActionResult(success=False, message="name required")

        value = params.get("value", 0.0)
        labels = params.get("labels", {})

        self._store.set_gauge(name, value, labels)

        return ActionResult(
            success=True,
            message=f"Set gauge: {name}",
            data={"name": name, "value": value}
        )

    def _get_metric(self, params: Dict[str, Any]) -> ActionResult:
        """Get a specific metric."""
        name = params.get("name")
        if not name:
            return ActionResult(success=False, message="name required")

        labels = params.get("labels")
        metric = self._store.get_metric(name, labels)

        if metric is None:
            return ActionResult(success=False, message=f"Metric not found: {name}")

        return ActionResult(
            success=True,
            message=f"Retrieved metric: {name}",
            data={
                "name": name,
                "metric_type": metric.metric_type.value,
                "points": len(metric.points),
                "stats": metric.get_stats(),
            }
        )

    def _list_metrics(self, params: Dict[str, Any]) -> ActionResult:
        """List all metrics."""
        all_metrics = self._store.get_all_metrics()
        metric_list = []

        for key, series in all_metrics.items():
            metric_list.append({
                "name": series.name,
                "metric_type": series.metric_type.value,
                "points": len(series.points),
                "labels": series.labels,
                "stats": series.get_stats(),
            })

        return ActionResult(
            success=True,
            message=f"Listed {len(metric_list)} metrics",
            data={"metrics": metric_list, "total": len(metric_list)}
        )

    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get aggregated stats for metrics."""
        name = params.get("name")
        window = params.get("window", 60.0)  # seconds

        labels = params.get("labels")

        if name:
            metric = self._store.get_metric(name, labels)
            if metric is None:
                return ActionResult(success=False, message=f"Metric not found: {name}")

            # Filter by window
            cutoff = time.time() - window
            window_points = [p for p in metric.points if p.timestamp > cutoff]

            if not window_points:
                return ActionResult(
                    success=True,
                    message=f"No data in window for: {name}",
                    data={"name": name, "window": window, "points": 0}
                )

            values = [p.value for p in window_points]
            return ActionResult(
                success=True,
                message=f"Stats for: {name}",
                data={
                    "name": name,
                    "window": window,
                    "points": len(values),
                    "sum": sum(values),
                    "avg": sum(values) / len(values),
                    "min": min(values),
                    "max": max(values),
                    "p50": metric.get_percentile(50),
                    "p90": metric.get_percentile(90),
                    "p95": metric.get_percentile(95),
                    "p99": metric.get_percentile(99),
                }
            )
        else:
            # Overall stats
            all_metrics = self._store.get_all_metrics()
            return ActionResult(
                success=True,
                message="Overall stats",
                data={"total_metrics": len(all_metrics)}
            )

    def _reset_metrics(self, params: Dict[str, Any]) -> ActionResult:
        """Reset all metrics."""
        self._store.reset()
        return ActionResult(success=True, message="Metrics reset")

    def _query_metrics(self, params: Dict[str, Any]) -> ActionResult:
        """Query metrics with filtering and aggregation."""
        pattern = params.get("pattern", "*")
        aggregation = params.get("aggregation", "avg")
        window = params.get("window", 60.0)

        all_metrics = self._store.get_all_metrics()
        results = []

        for key, series in all_metrics.items():
            if pattern != "*" and pattern not in key:
                continue

            cutoff = time.time() - window
            window_points = [p for p in series.points if p.timestamp > cutoff]

            if not window_points:
                continue

            values = [p.value for p in window_points]
            agg_value = 0.0

            if aggregation == "sum":
                agg_value = sum(values)
            elif aggregation == "avg":
                agg_value = sum(values) / len(values)
            elif aggregation == "min":
                agg_value = min(values)
            elif aggregation == "max":
                agg_value = max(values)
            elif aggregation == "count":
                agg_value = len(values)
            elif aggregation.startswith("p"):
                p = float(aggregation[1:])
                agg_value = series.get_percentile(p)

            results.append({
                "name": series.name,
                "labels": series.labels,
                "aggregation": aggregation,
                "window": window,
                "value": agg_value,
                "points": len(values),
            })

        return ActionResult(
            success=True,
            message=f"Query returned {len(results)} results",
            data={"results": results, "total": len(results)}
        )

    def record_api_call(self, endpoint: str, method: str, status_code: int,
                        duration_ms: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Convenience method to record an API call metrics."""
        call_labels = dict(labels or {}, endpoint=endpoint, method=method)

        self._store.increment_counter("api_requests_total", 1.0, call_labels)
        self._store.record_histogram("api_request_duration_ms", duration_ms, call_labels)

        if status_code >= 500:
            self._store.increment_counter("api_errors_total", 1.0, call_labels)
        elif status_code >= 400:
            self._store.increment_counter("api_client_errors_total", 1.0, call_labels)

    def get_required_params(self) -> List[str]:
        return ["command"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "metric_type": "histogram",
            "name": None,
            "value": 0.0,
            "labels": {},
            "window": 60.0,
            "aggregation": "avg",
            "pattern": "*",
        }
