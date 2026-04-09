"""
API Metric Collector Action Module.

Collects, aggregates, and exports API performance metrics
with histograms, counters, gauges, and alerting support.
"""

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

from uuid import uuid4


class MetricType(Enum):
    """Types of metrics."""

    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"
    SUMMARY = "summary"


@dataclass
class MetricPoint:
    """A single metric data point."""

    timestamp: float
    value: float
    labels: dict[str, str] = field(default_factory=dict)


@dataclass
class HistogramStats:
    """Statistics for a histogram metric."""

    count: int = 0
    sum: float = 0.0
    min: float = float("inf")
    max: float = float("-inf")
    mean: float = 0.0
    p50: float = 0.0
    p90: float = 0.0
    p95: float = 0.0
    p99: float = 0.0

    def update(self, value: float) -> None:
        """Update histogram with a new value."""
        self.count += 1
        self.sum += value
        self.min = min(self.min, value)
        self.max = max(self.max, value)
        self.mean = self.sum / self.count

    def compute_percentiles(self, values: list[float]) -> None:
        """Compute percentiles from a sorted list of values."""
        if not values:
            return
        n = len(values)
        self.p50 = values[int(n * 0.50)]
        self.p90 = values[int(n * 0.90)]
        self.p95 = values[int(n * 0.95)]
        self.p99 = values[int(n * 0.99)]


@dataclass
class MetricDefinition:
    """Definition of a metric."""

    name: str
    metric_type: MetricType
    description: str = ""
    unit: str = ""
    buckets: Optional[list[float]] = None


class MetricCollector:
    """
    Collects and aggregates API performance metrics.

    Supports counters, gauges, histograms with configurable buckets,
    and export to various formats.
    """

    def __init__(
        self,
        flush_interval: float = 60.0,
        max_points_per_metric: int = 10000,
    ) -> None:
        """
        Initialize the metric collector.

        Args:
            flush_interval: Seconds between metric flushes.
            max_points_per_metric: Max data points to retain per metric.
        """
        self._flush_interval = flush_interval
        self._max_points = max_points_per_metric
        self._definitions: dict[str, MetricDefinition] = {}
        self._counters: dict[str, float] = defaultdict(float)
        self._gauges: dict[str, float] = {}
        self._histograms: dict[str, list[float]] = defaultdict(list)
        self._histogram_stats: dict[str, HistogramStats] = defaultdict(HistogramStats)
        self._timers: dict[str, list[float]] = defaultdict(list)
        self._labels: dict[str, dict[str, str]] = {}
        self._start_time = time.time()
        self._flush_task: Optional[asyncio.Task] = None
        self._export_handlers: list[Callable[[dict[str, Any]], None]] = []

    def define_metric(
        self,
        name: str,
        metric_type: MetricType,
        description: str = "",
        unit: str = "",
        buckets: Optional[list[float]] = None,
    ) -> None:
        """
        Define a metric before collecting it.

        Args:
            name: Metric name.
            metric_type: Type of metric.
            description: Human-readable description.
            unit: Unit of measurement.
            buckets: Bucket boundaries for histograms.
        """
        self._definitions[name] = MetricDefinition(
            name=name,
            metric_type=metric_type,
            description=description,
            unit=unit,
            buckets=buckets,
        )

    def increment_counter(
        self,
        name: str,
        value: float = 1.0,
        labels: Optional[dict[str, str]] = None,
    ) -> None:
        """
        Increment a counter metric.

        Args:
            name: Counter name.
            value: Amount to increment.
            labels: Optional label set.
        """
        key = self._make_key(name, labels)
        self._counters[key] += value
        self._labels[key] = labels or {}
        self._record_point(name, value, labels)

    def set_gauge(
        self,
        name: str,
        value: float,
        labels: Optional[dict[str, str]] = None,
    ) -> None:
        """
        Set a gauge metric value.

        Args:
            name: Gauge name.
            value: Value to set.
            labels: Optional label set.
        """
        key = self._make_key(name, labels)
        self._gauges[key] = value
        self._labels[key] = labels or {}
        self._record_point(name, value, labels)

    def observe_histogram(
        self,
        name: str,
        value: float,
        labels: Optional[dict[str, str]] = None,
    ) -> None:
        """
        Observe a value for histogram metric.

        Args:
            name: Histogram name.
            value: Value to record.
            labels: Optional label set.
        """
        key = self._make_key(name, labels)
        self._histograms[key].append(value)
        self._histogram_stats[key].update(value)
        self._labels[key] = labels or {}

        if len(self._histograms[key]) > self._max_points:
            self._histograms[key] = self._histograms[key][-self._max_points:]

        self._record_point(name, value, labels)

    def start_timer(
        self,
        name: str,
        labels: Optional[dict[str, str]] = None,
    ) -> Callable[[], float]:
        """
        Start a timer. Returns a callable to stop and record.

        Args:
            name: Timer name.
            labels: Optional label set.

        Returns:
            Callable that returns elapsed time when called.
        """
        start = time.time()

        def stop() -> float:
            elapsed = time.time() - start
            self.record_timer(name, elapsed, labels)
            return elapsed

        return stop

    def record_timer(
        self,
        name: str,
        duration: float,
        labels: Optional[dict[str, str]] = None,
    ) -> None:
        """
        Record a timer duration.

        Args:
            name: Timer name.
            duration: Duration in seconds.
            labels: Optional label set.
        """
        key = self._make_key(name, labels)
        self._timers[key].append(duration)
        self._labels[key] = labels or {}
        self._record_point(name, duration, labels)

    def _make_key(
        self,
        name: str,
        labels: Optional[dict[str, str]],
    ) -> str:
        """Create a unique key for metric + labels combination."""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def _record_point(
        self,
        name: str,
        value: float,
        labels: Optional[dict[str, str]],
    ) -> None:
        """Record a data point for a metric."""
        point = MetricPoint(
            timestamp=time.time(),
            value=value,
            labels=labels or {},
        )
        return  # Points stored in in-memory structures above

    def get_counter(self, name: str, labels: Optional[dict[str, str]] = None) -> float:
        """Get current counter value."""
        key = self._make_key(name, labels)
        return self._counters.get(key, 0.0)

    def get_gauge(self, name: str, labels: Optional[dict[str, str]] = None) -> Optional[float]:
        """Get current gauge value."""
        key = self._make_key(name, labels)
        return self._gauges.get(key)

    def get_histogram_stats(
        self,
        name: str,
        labels: Optional[dict[str, str]] = None,
    ) -> Optional[HistogramStats]:
        """Get histogram statistics."""
        key = self._make_key(name, labels)
        if key in self._histograms and self._histograms[key]:
            values = sorted(self._histograms[key])
            stats = self._histogram_stats[key]
            stats.compute_percentiles(values)
            return stats
        return None

    def get_all_metrics(self) -> dict[str, Any]:
        """
        Get all current metric values.

        Returns:
            Dictionary with all metrics organized by type.
        """
        uptime = time.time() - self._start_time

        return {
            "uptime_seconds": uptime,
            "timestamp": time.time(),
            "counters": dict(self._counters),
            "gauges": dict(self._gauges),
            "histograms": {
                name: {
                    "count": stats.count,
                    "sum": stats.sum,
                    "mean": stats.mean,
                    "min": stats.min if stats.min != float("inf") else 0,
                    "max": stats.max if stats.max != float("-inf") else 0,
                    "p50": stats.p50,
                    "p90": stats.p90,
                    "p95": stats.p95,
                    "p99": stats.p99,
                }
                for name, stats in self._histogram_stats.items()
            },
            "definitions": {
                name: {
                    "type": defn.metric_type.value,
                    "description": defn.description,
                    "unit": defn.unit,
                    "buckets": defn.buckets,
                }
                for name, defn in self._definitions.items()
            },
        }

    def export_prometheus(self) -> str:
        """
        Export metrics in Prometheus text format.

        Returns:
            Prometheus-formatted string.
        """
        lines: list[str] = []
        metrics = self.get_all_metrics()

        for name, value in metrics.get("counters", {}).items():
            lines.append(f"# TYPE {name} counter")
            lines.append(f"{name} {value}")

        for name, value in metrics.get("gauges", {}).items():
            lines.append(f"# TYPE {name} gauge")
            lines.append(f"{name} {value}")

        for name, stats in metrics.get("histograms", {}).items():
            lines.append(f"# TYPE {name} histogram")
            lines.append(f"{name}_sum {stats['sum']}")
            lines.append(f"{name}_count {stats['count']}")

        return "\n".join(lines) + "\n"

    def export_json(self) -> str:
        """Export metrics as JSON."""
        import json
        return json.dumps(self.get_all_metrics(), indent=2)

    def add_export_handler(
        self,
        handler: Callable[[dict[str, Any]], None],
    ) -> None:
        """
        Add an export handler called on flush.

        Args:
            handler: Function to call with all metrics.
        """
        self._export_handlers.append(handler)

    async def start_auto_flush(self) -> None:
        """Start automatic periodic flushing."""
        async def flusher() -> None:
            while True:
                await asyncio.sleep(self._flush_interval)
                self.flush()

        self._flush_task = asyncio.create_task(flusher())

    def flush(self) -> None:
        """Trigger metric export to all handlers."""
        metrics = self.get_all_metrics()
        for handler in self._export_handlers:
            try:
                handler(metrics)
            except Exception:
                pass

    async def stop(self) -> None:
        """Stop the collector and flush remaining metrics."""
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        self.flush()


def create_metric_collector(
    flush_interval: float = 60.0,
) -> MetricCollector:
    """
    Factory function to create a metric collector.

    Args:
        flush_interval: Seconds between metric flushes.

    Returns:
        Configured MetricCollector instance.
    """
    return MetricCollector(flush_interval=flush_interval)
