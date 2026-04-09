"""
API metrics collector for monitoring and observability.

This module provides comprehensive metrics collection for API operations
including latency histograms, counters, gauges, and alerting.

Author: RabAiBot
License: MIT
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple
from collections import defaultdict
import threading
import math

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Types of metrics."""
    COUNTER = auto()
    GAUGE = auto()
    HISTOGRAM = auto()
    SUMMARY = auto()


@dataclass
class TimeSeriesPoint:
    """A single time series data point."""
    timestamp: float
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class MetricValue:
    """Container for a metric value with metadata."""
    name: str
    metric_type: MetricType
    value: Any
    labels: Dict[str, str] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    unit: str = ""


class Histogram:
    """Histogram for tracking distributions."""

    def __init__(self, buckets: Optional[List[float]] = None):
        if buckets is None:
            buckets = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        self.buckets = [0.0] + sorted(buckets)
        self._counts = [0] * len(self.buckets)
        self._sum = 0.0
        self._count = 0
        self._min = float("inf")
        self._max = float("-inf")
        self._lock = threading.Lock()

    def observe(self, value: float) -> None:
        """Record an observation."""
        with self._lock:
            self._sum += value
            self._count += 1
            self._min = min(self._min, value)
            self._max = max(self._max, value)

            for i, bucket in enumerate(self.buckets):
                if value <= bucket:
                    self._counts[i] += 1

    @property
    def count(self) -> int:
        return self._count

    @property
    def sum(self) -> float:
        return self._sum

    @property
    def mean(self) -> float:
        return self._sum / self._count if self._count > 0 else 0.0

    @property
    def min(self) -> float:
        return self._min if self._count > 0 else 0.0

    @property
    def max(self) -> float:
        return self._max if self._count > 0 else 0.0

    def percentile(self, p: float) -> float:
        """Calculate percentile (0-100)."""
        if self._count == 0:
            return 0.0

        sorted_values = []
        for i in range(1, len(self.buckets)):
            sorted_values.extend([self.buckets[i]] * (self._counts[i] - self._counts[i-1]))

        if not sorted_values:
            return 0.0

        sorted_values.sort()
        idx = int(math.ceil(p / 100.0 * len(sorted_values))) - 1
        idx = max(0, min(idx, len(sorted_values) - 1))
        return sorted_values[idx]

    def get_percentiles(self, ps: List[float] = None) -> Dict[str, float]:
        """Get multiple percentiles."""
        if ps is None:
            ps = [50, 90, 95, 99]
        return {f"p{int(p)}": self.percentile(p) for p in ps}

    def get_bucket_counts(self) -> Dict[str, int]:
        """Get counts for each bucket."""
        result = {}
        cumulative = 0
        for i, bucket in enumerate(self.buckets):
            cumulative += self._counts[i]
            result[f"le_{bucket}"] = cumulative
        return result

    def reset(self) -> None:
        """Reset the histogram."""
        with self._lock:
            self._counts = [0] * len(self.buckets)
            self._sum = 0.0
            self._count = 0
            self._min = float("inf")
            self._max = float("-inf")


class MetricsCollector:
    """
    Comprehensive API metrics collector.

    Features:
    - Counters, gauges, histograms, summaries
    - Time series storage with retention
    - Label support for multi-dimensional metrics
    - Alerting thresholds
    - Export to Prometheus format
    - Thread-safe operations

    Example:
        >>> collector = MetricsCollector()
        >>> collector.increment("api_requests", labels={"endpoint": "/users"})
        >>> collector.observe("api_latency", 0.123, labels={"endpoint": "/users"})
        >>> collector.set("active_connections", 42)
        >>> stats = collector.get_stats()
    """

    def __init__(
        self,
        retention_seconds: float = 3600.0,
        collection_interval: float = 10.0,
    ):
        """
        Initialize the metrics collector.

        Args:
            retention_seconds: How long to retain time series data
            collection_interval: Interval for aggregation tasks
        """
        self.retention_seconds = retention_seconds
        self.collection_interval = collection_interval

        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, Histogram] = defaultdict(Histogram)
        self._summaries: Dict[str, List[float]] = defaultdict(list)
        self._labels: Dict[str, Dict[str, str]] = {}
        self._time_series: Dict[str, List[TimeSeriesPoint]] = defaultdict(list)
        self._alerts: Dict[str, Callable[[], bool]] = {}
        self._alert_states: Dict[str, bool] = {}
        self._alert_callbacks: Dict[str, List[Callable[[str, float], None]]] = defaultdict(list)
        self._lock = threading.RLock()
        self._cleanup_task: Optional[asyncio.Task] = None
        self._running = False

        self._counters_total: Dict[str, float] = defaultdict(float)
        self._gauges_initial: Dict[str, float] = {}

        logger.info(f"MetricsCollector initialized (retention={retention_seconds}s)")

    def counter(self, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        """
        Increment a counter metric.

        Args:
            name: Metric name
            value: Value to add
            labels: Optional labels
        """
        with self._lock:
            key = self._make_key(name, labels)
            self._counters[key] += value
            self._counters_total[name] += value
            self._labels[key] = labels or {}
            self._record_time_series(name, self._counters[key], labels)
            logger.debug(f"Counter {name}: +{value} = {self._counters[key]}")

    def increment(self, name: str, labels: Optional[Dict[str, str]] = None) -> None:
        """Increment a counter by 1."""
        self.counter(name, 1.0, labels)

    def gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """
        Set a gauge metric.

        Args:
            name: Metric name
            value: Gauge value
            labels: Optional labels
        """
        with self._lock:
            key = self._make_key(name, labels)
            self._gauges[key] = value
            self._labels[key] = labels or {}
            if name not in self._gauges_initial:
                self._gauges_initial[name] = value
            self._record_time_series(name, value, labels)
            self._check_alerts(name, value, labels)
            logger.debug(f"Gauge {name}: {value}")

    def observe(
        self,
        name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Record an observation for histogram or summary.

        Args:
            name: Metric name
            value: Observed value
            labels: Optional labels
        """
        with self._lock:
            key = self._make_key(name, labels)
            self._histograms[key].observe(value)
            self._labels[key] = labels or {}
            self._record_time_series(name, value, labels)
            logger.debug(f"Histogram {name}: {value}")

    def summary(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Record a value for summary."""
        with self._lock:
            key = self._make_key(name, labels)
            self._summaries[key].append(value)
            self._labels[key] = labels or {}
            if len(self._summaries[key]) > 1000:
                self._summaries[key] = self._summaries[key][-500:]

    def _make_key(self, name: str, labels: Optional[Dict[str, str]]) -> str:
        """Create a unique key from name and labels."""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def _record_time_series(
        self,
        name: str,
        value: float,
        labels: Optional[Dict[str, str]],
    ) -> None:
        """Record a time series point."""
        point = TimeSeriesPoint(
            timestamp=time.time(),
            value=value,
            labels=labels or {},
        )
        self._time_series[name].append(point)

    def set_alert(
        self,
        name: str,
        condition_fn: Callable[[float], bool],
        callback: Optional[Callable[[str, float], None]] = None,
    ) -> None:
        """
        Set an alert condition.

        Args:
            name: Metric name
            condition_fn: Function that returns True when alert should fire
            callback: Optional callback when alert fires
        """
        with self._lock:
            self._alerts[name] = condition_fn
            if callback:
                self._alert_callbacks[name].append(callback)
            logger.info(f"Alert set for metric: {name}")

    def _check_alerts(self, name: str, value: float, labels: Optional[Dict[str, str]]) -> None:
        """Check if alert conditions are met."""
        key = self._make_key(name, labels)
        if key not in self._alerts:
            return

        should_fire = self._alerts[key](value)
        was_firing = self._alert_states.get(key, False)

        if should_fire and not was_firing:
            self._alert_states[key] = True
            logger.warning(f"ALERT: {key} = {value}")
            for callback in self._alert_callbacks[key]:
                try:
                    callback(key, value)
                except Exception as e:
                    logger.error(f"Alert callback failed: {e}")
        elif not should_fire and was_firing:
            self._alert_states[key] = False
            logger.info(f"ALERT CLEARED: {key}")

    def get_stats(self) -> Dict[str, Any]:
        """Get all metrics statistics."""
        with self._lock:
            result = {
                "timestamp": time.time(),
                "counters": {
                    k: {"value": v, "labels": self._labels.get(k, {})}
                    for k, v in self._counters.items()
                },
                "gauges": {
                    k: {"value": v, "labels": self._labels.get(k, {})}
                    for k, v in self._gauges.items()
                },
                "histograms": {},
                "totals": dict(self._counters_total),
            }

            for key, hist in self._histograms.items():
                result["histograms"][key] = {
                    "count": hist.count,
                    "sum": hist.sum,
                    "mean": hist.mean,
                    "min": hist.min,
                    "max": hist.max,
                    "percentiles": hist.get_percentiles(),
                    "labels": self._labels.get(key, {}),
                }

            return result

    def get_histogram_stats(self, name: str, labels: Optional[Dict[str, str]] = None) -> Dict[str, float]:
        """Get histogram statistics for a metric."""
        key = self._make_key(name, labels)
        with self._lock:
            if key not in self._histograms:
                return {}
            hist = self._histograms[key]
            return {
                "count": hist.count,
                "sum": hist.sum,
                "mean": hist.mean,
                "min": hist.min,
                "max": hist.max,
                **hist.get_percentiles(),
            }

    def export_prometheus(self) -> str:
        """Export metrics in Prometheus text format."""
        lines = []
        timestamp = int(time.time() * 1000)

        with self._lock:
            for key, value in self._counters.items():
                labels = self._labels.get(key, {})
                label_str = self._format_labels(labels)
                lines.append(f"{key}{label_str} {value} {timestamp}")

            for key, value in self._gauges.items():
                labels = self._labels.get(key, {})
                label_str = self._format_labels(labels)
                lines.append(f"{key}{label_str} {value} {timestamp}")

            for key, hist in self._histograms.items():
                labels = self._labels.get(key, {})
                label_str = self._format_labels(labels)

                lines.append(f"{key}_count{label_str} {hist.count} {timestamp}")
                lines.append(f"{key}_sum{label_str} {hist.sum} {timestamp}")

                for bucket, count in hist.get_bucket_counts().items():
                    bucket_labels = {**labels, "le": bucket.replace("le_", "")}
                    bucket_label_str = self._format_labels(bucket_labels)
                    lines.append(f"{key}_bucket{bucket_label_str} {count} {timestamp}")

        return "\n".join(lines)

    def _format_labels(self, labels: Dict[str, str]) -> str:
        """Format labels for Prometheus output."""
        if not labels:
            return ""
        return "{" + ",".join(f'{k}="{v}"' for k, v in labels.items()) + "}"

    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._histograms.clear()
            self._summaries.clear()
            self._time_series.clear()
            self._labels.clear()
            self._counters_total.clear()
            self._gauges_initial.clear()
            logger.info("MetricsCollector reset")

    def cleanup_old_data(self) -> int:
        """Remove data older than retention period."""
        cutoff = time.time() - self.retention_seconds
        removed = 0

        with self._lock:
            for name in list(self._time_series.keys()):
                series = self._time_series[name]
                self._time_series[name] = [p for p in series if p.timestamp > cutoff]
                removed += len(series) - len(self._time_series[name])

        logger.debug(f"Cleaned up {removed} old time series points")
        return removed


class LatencyTracker:
    """Helper for tracking operation latencies."""

    def __init__(self, collector: MetricsCollector, metric_name: str):
        self.collector = collector
        self.metric_name = metric_name
        self._start_time: Optional[float] = None

    def __enter__(self) -> "LatencyTracker":
        self._start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        duration = time.perf_counter() - self._start_time
        self.collector.observe(self.metric_name, duration)
        return False

    async def __aenter__(self) -> "LatencyTracker":
        self._start_time = time.perf_counter()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        duration = time.perf_counter() - self._start_time
        self.collector.observe(self.metric_name, duration)
        return False
