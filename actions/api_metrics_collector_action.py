"""
API Metrics Collector Action Module.

Collects and aggregates API metrics.
"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class MetricPoint:
    """A single metric data point."""
    timestamp: float
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class AggregatedMetric:
    """Aggregated metric statistics."""
    name: str
    count: int
    sum: float
    min: float
    max: float
    avg: float
    p50: float
    p90: float
    p95: float
    p99: float


class ApiMetricsCollectorAction:
    """
    Collect and aggregate API metrics.

    Supports counters, gauges, histograms, and percentiles.
    """

    def __init__(
        self,
        retention_seconds: float = 300.0,
        max_points_per_metric: int = 10000,
    ) -> None:
        self.retention_seconds = retention_seconds
        self.max_points_per_metric = max_points_per_metric

        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=max_points_per_metric)
        )
        self._rate_counters: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=1000)
        )

    def increment_counter(
        self,
        name: str,
        value: float = 1.0,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Increment a counter metric.

        Args:
            name: Metric name
            value: Value to add
            labels: Optional labels
        """
        key = self._make_key(name, labels)
        self._counters[key] += value

        self._rate_counters[key].append({
            "timestamp": time.time(),
            "value": value,
        })

    def set_gauge(
        self,
        name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Set a gauge metric.

        Args:
            name: Metric name
            value: Gauge value
            labels: Optional labels
        """
        key = self._make_key(name, labels)
        self._gauges[key] = value

    def observe_histogram(
        self,
        name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Observe a histogram value.

        Args:
            name: Metric name
            value: Observed value
            labels: Optional labels
        """
        key = self._make_key(name, labels)
        self._histograms[key].append(MetricPoint(
            timestamp=time.time(),
            value=value,
            labels=labels or {},
        ))

    def record_latency(
        self,
        endpoint: str,
        latency_ms: float,
        status_code: int,
    ) -> None:
        """
        Record API latency.

        Args:
            endpoint: API endpoint
            latency_ms: Latency in milliseconds
            status_code: HTTP status code
        """
        self.increment_counter(
            "api_requests_total",
            labels={"endpoint": endpoint, "status": str(status_code)},
        )

        self.observe_histogram(
            "api_latency_ms",
            latency_ms,
            labels={"endpoint": endpoint},
        )

    def _make_key(
        self,
        name: str,
        labels: Optional[Dict[str, str]],
    ) -> str:
        """Create metric key from name and labels."""
        if not labels:
            return name

        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def get_counter(
        self,
        name: str,
        labels: Optional[Dict[str, str]] = None,
    ) -> float:
        """Get counter value."""
        key = self._make_key(name, labels)
        return self._counters.get(key, 0.0)

    def get_gauge(
        self,
        name: str,
        labels: Optional[Dict[str, str]] = None,
    ) -> Optional[float]:
        """Get gauge value."""
        key = self._make_key(name, labels)
        return self._gauges.get(key)

    def get_histogram_stats(
        self,
        name: str,
        labels: Optional[Dict[str, str]] = None,
    ) -> Optional[AggregatedMetric]:
        """Get histogram statistics."""
        key = self._make_key(name, labels)
        points = self._histograms.get(key)

        if not points:
            return None

        values = [p.value for p in points]
        values.sort()

        n = len(values)

        return AggregatedMetric(
            name=name,
            count=n,
            sum=sum(values),
            min=values[0],
            max=values[-1],
            avg=sum(values) / n,
            p50=values[int(n * 0.50)],
            p90=values[int(n * 0.90)] if n > 1 else values[0],
            p95=values[int(n * 0.95)] if n > 1 else values[0],
            p99=values[int(n * 0.99)] if n > 1 else values[0],
        )

    def get_rate(
        self,
        name: str,
        window_seconds: float = 60.0,
        labels: Optional[Dict[str, str]] = None,
    ) -> float:
        """
        Calculate rate per second over window.

        Args:
            name: Metric name
            window_seconds: Time window
            labels: Optional labels

        Returns:
            Rate per second
        """
        key = self._make_key(name, labels)
        points = self._rate_counters.get(key, deque())

        if not points:
            return 0.0

        cutoff = time.time() - window_seconds
        recent = [p for p in points if p["timestamp"] >= cutoff]

        if not recent:
            return 0.0

        total = sum(p["value"] for p in recent)
        duration = recent[-1]["timestamp"] - recent[0]["timestamp"]

        if duration <= 0:
            return 0.0

        return total / duration

    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all metrics."""
        return {
            "counters": dict(self._counters),
            "gauges": dict(self._gauges),
            "histograms": {
                name: len(points)
                for name, points in self._histograms.items()
            },
        }

    def cleanup_old_points(self) -> int:
        """Remove points outside retention window."""
        cutoff = time.time() - self.retention_seconds
        removed = 0

        for name, points in self._histograms.items():
            while points and points[0].timestamp < cutoff:
                points.popleft()
                removed += 1

        for name, points in self._rate_counters.items():
            while points and points[0]["timestamp"] < cutoff:
                points.popleft()
                removed += 1

        return removed

    def reset(self, name: Optional[str] = None) -> None:
        """Reset metrics."""
        if name:
            self._counters.pop(name, None)
            self._gauges.pop(name, None)
            self._histograms.pop(name, None)
            self._rate_counters.pop(name, None)
        else:
            self._counters.clear()
            self._gauges.clear()
            self._histograms.clear()
            self._rate_counters.clear()
