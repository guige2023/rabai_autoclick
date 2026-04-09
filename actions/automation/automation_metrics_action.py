"""Metrics collection and reporting for automation workflows.

Provides comprehensive metrics tracking including execution times,
success/failure rates, resource usage, and custom business metrics.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import math
import json


class MetricType(Enum):
    """Type of metric."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"
    SUMMARY = "summary"
    RATE = "rate"


@dataclass
class MetricValue:
    """A single metric value with metadata."""
    name: str
    value: float
    metric_type: MetricType
    timestamp: float = field(default_factory=time.time)
    labels: Dict[str, str] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)


@dataclass
class AggregatedMetric:
    """Aggregated metric statistics."""
    name: str
    count: int = 0
    sum: float = 0.0
    min_val: float = float("inf")
    max_val: float = float("-inf")
    mean: float = 0.0
    p50: float = 0.0
    p90: float = 0.0
    p95: float = 0.0
    p99: float = 0.0
    std_dev: float = 0.0
    metric_type: MetricType = MetricType.GAUGE
    labels: Dict[str, str] = field(default_factory=dict)
    last_updated: float = field(default_factory=time.time)


class MetricsStore:
    """Thread-safe metrics storage with aggregation."""

    def __init__(self, max_values_per_metric: int = 10000):
        self._values: Dict[str, List[MetricValue]] = defaultdict(list)
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = defaultdict(float)
        self._lock = threading.RLock()
        self._max_values = max_values_per_metric
        self._aggregation_window = 300

    def record(
        self,
        name: str,
        value: float,
        metric_type: MetricType = MetricType.GAUGE,
        labels: Optional[Dict[str, str]] = None,
        tags: Optional[List[str]] = None,
    ) -> None:
        """Record a metric value."""
        with self._lock:
            metric_value = MetricValue(
                name=name,
                value=value,
                metric_type=metric_type,
                labels=labels or {},
                tags=tags or [],
            )

            if metric_type == MetricType.COUNTER:
                key = self._make_key(name, labels)
                self._counters[key] += value
            elif metric_type == MetricType.GAUGE:
                key = self._make_key(name, labels)
                self._gauges[key] = value
            else:
                key = self._make_key(name, labels)
                values_list = self._values[key]
                values_list.append(metric_value)
                if len(values_list) > self._max_values:
                    values_list[:] = values_list[-self._max_values:]

    def _make_key(self, name: str, labels: Optional[Dict[str, str]]) -> str:
        """Create a unique key from name and labels."""
        if not labels:
            return name
        sorted_labels = sorted(labels.items())
        label_str = ",".join(f"{k}={v}" for k, v in sorted_labels)
        return f"{name}{{{label_str}}}"

    def get_counter(self, name: str, labels: Optional[Dict[str, str]] = None) -> float:
        """Get current counter value."""
        key = self._make_key(name, labels)
        with self._lock:
            return self._counters.get(key, 0.0)

    def get_gauge(self, name: str, labels: Optional[Dict[str, str]] = None) -> float:
        """Get current gauge value."""
        key = self._make_key(name, labels)
        with self._lock:
            return self._gauges.get(key, 0.0)

    def get_histogram_stats(
        self,
        name: str,
        labels: Optional[Dict[str, str]] = None,
    ) -> Optional[AggregatedMetric]:
        """Get aggregated histogram statistics."""
        key = self._make_key(name, labels)
        with self._lock:
            values_list = self._values.get(key, [])
            if not values_list:
                return None

            raw_values = [v.value for v in values_list]
            raw_values.sort()

            n = len(raw_values)
            total = sum(raw_values)

            std_dev = 0.0
            if n > 1:
                mean = total / n
                variance = sum((x - mean) ** 2 for x in raw_values) / n
                std_dev = math.sqrt(variance)

            def percentile(p: float) -> float:
                if not raw_values:
                    return 0.0
                idx = int(math.ceil(n * p / 100.0)) - 1
                idx = max(0, min(idx, n - 1))
                return raw_values[idx]

            return AggregatedMetric(
                name=name,
                count=n,
                sum=total,
                min_val=raw_values[0],
                max_val=raw_values[-1],
                mean=total / n if n > 0 else 0.0,
                p50=percentile(50),
                p90=percentile(90),
                p95=percentile(95),
                p99=percentile(99),
                std_dev=std_dev,
                metric_type=MetricType.HISTOGRAM,
                labels=labels or {},
            )

    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all current metric values."""
        with self._lock:
            result = {
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "histograms": {},
                "timestamp": datetime.now().isoformat(),
            }

            for key, values in self._values.items():
                if values:
                    stats = self.get_histogram_stats(
                        values[0].name, values[0].labels
                    )
                    if stats:
                        result["histograms"][key] = {
                            "count": stats.count,
                            "mean": stats.mean,
                            "min": stats.min_val,
                            "max": stats.max_val,
                            "p50": stats.p50,
                            "p90": stats.p90,
                            "p95": stats.p95,
                            "p99": stats.p99,
                            "std_dev": stats.std_dev,
                        }

            return result

    def reset(self, name: Optional[str] = None, labels: Optional[Dict[str, str]] = None) -> int:
        """Reset metrics. Returns count of reset metrics."""
        key = self._make_key(name or "", labels) if name else None
        with self._lock:
            count = 0
            if name is None:
                count = len(self._counters) + len(self._gauges) + len(self._values)
                self._counters.clear()
                self._gauges.clear()
                self._values.clear()
            elif key in self._counters:
                del self._counters[key]
                count += 1
            elif key in self._gauges:
                del self._gauges[key]
                count += 1
            elif key in self._values:
                del self._values[key]
                count += 1
            return count


class Timer:
    """Context manager for timing operations."""

    def __init__(self, metrics_store: MetricsStore, name: str, labels: Optional[Dict[str, str]] = None):
        self._store = metrics_store
        self._name = name
        self._labels = labels or {}
        self._start_time: Optional[float] = None
        self._duration: Optional[float] = None

    def __enter__(self) -> "Timer":
        self._start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._start_time is not None:
            self._duration = time.time() - self._start_time
            self._store.record(
                self._name,
                self._duration,
                MetricType.HISTOGRAM,
                self._labels,
            )

    def get_duration(self) -> Optional[float]:
        """Get the recorded duration."""
        return self._duration


class AutomationMetricsAction:
    """Action providing metrics collection for automation workflows."""

    def __init__(self, store: Optional[MetricsStore] = None):
        self._store = store or MetricsStore()

    def counter(
        self,
        name: str,
        value: float = 1.0,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """Increment a counter metric."""
        self._store.record(name, value, MetricType.COUNTER, labels)

    def gauge(
        self,
        name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """Set a gauge metric."""
        self._store.record(name, value, MetricType.GAUGE, labels)

    def histogram(
        self,
        name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """Record a histogram value."""
        self._store.record(name, value, MetricType.HISTOGRAM, labels)

    def timer(
        self,
        name: str,
        labels: Optional[Dict[str, str]] = None,
    ) -> Timer:
        """Create a timer context manager."""
        return Timer(self._store, name, labels)

    def rate(
        self,
        name: str,
        window_seconds: float = 60.0,
        labels: Optional[Dict[str, str]] = None,
    ) -> float:
        """Calculate rate (value per second) over a window."""
        stats = self._store.get_histogram_stats(name, labels)
        if not stats or not window_seconds:
            return 0.0
        return stats.sum / window_seconds

    def execute(
        self,
        context: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute an action with metrics collection.

        Required params:
            operation: callable - The operation to execute
            metric_name: str - Base name for metrics

        Optional params:
            track_duration: bool - Whether to track execution time (default True)
            track_success: bool - Whether to track success/failure (default True)
            labels: dict - Labels to attach to all metrics
        """
        operation = params.get("operation")
        metric_name = params.get("metric_name", "automation_execution")
        track_duration = params.get("track_duration", True)
        track_success = params.get("track_success", True)
        labels = params.get("labels", {})

        if not callable(operation):
            raise ValueError("operation must be a callable")

        result = None
        error = None
        duration = None

        start_time = time.time()

        if track_duration:
            self._store.record(
                f"{metric_name}_total",
                1.0,
                MetricType.COUNTER,
                labels,
            )

        try:
            result = operation(context=context, params=params)

            if track_success:
                self._store.record(
                    f"{metric_name}_success",
                    1.0,
                    MetricType.COUNTER,
                    labels,
                )

            return {
                "result": result,
                "duration": time.time() - start_time,
                "metrics_recorded": True,
            }

        except Exception as e:
            error = str(e)
            duration = time.time() - start_time

            if track_success:
                self._store.record(
                    f"{metric_name}_failure",
                    1.0,
                    MetricType.COUNTER,
                    {**labels, "error_type": type(e).__name__},
                )

            return {
                "error": error,
                "duration": duration,
                "metrics_recorded": True,
            }

    def get_metrics(self) -> Dict[str, Any]:
        """Get all collected metrics."""
        return self._store.get_all_metrics()

    def get_metric_summary(
        self,
        name: str,
        labels: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Get summary statistics for a specific metric."""
        stats = self._store.get_histogram_stats(name, labels)
        counter_val = self._store.get_counter(name, labels)
        gauge_val = self._store.get_gauge(name, labels)

        result = {
            "name": name,
            "labels": labels or {},
            "counter": counter_val,
            "gauge": gauge_val,
        }

        if stats:
            result["histogram"] = {
                "count": stats.count,
                "mean": stats.mean,
                "min": stats.min_val,
                "max": stats.max_val,
                "p50": stats.p50,
                "p90": stats.p90,
                "p95": stats.p95,
                "p99": stats.p99,
                "std_dev": stats.std_dev,
            }

        return result

    def reset(self, name: Optional[str] = None) -> int:
        """Reset metrics."""
        return self._store.reset(name)
