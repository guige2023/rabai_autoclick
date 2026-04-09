"""API Metrics Collector.

This module provides metrics collection for API monitoring:
- Counter, gauge, histogram metrics
- Time series storage
- Percentile calculation
- Metric aggregation

Example:
    >>> from actions.api_metrics_collector_action import MetricsCollector
    >>> collector = MetricsCollector()
    >>> collector.increment("requests_total", tags={"method": "GET"})
    >>> collector.record("request_duration_ms", 45.2, tags={"endpoint": "/api/users"})
"""

from __future__ import annotations

import time
import logging
import threading
import math
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class MetricPoint:
    """A single metric data point."""
    timestamp: float
    value: float
    tags: dict[str, str]


class MetricsCollector:
    """Collects and stores API metrics."""

    def __init__(
        self,
        retention_seconds: int = 3600,
        max_points_per_metric: int = 10000,
    ) -> None:
        """Initialize the metrics collector.

        Args:
            retention_seconds: How long to retain metric points.
            max_points_per_metric: Max points stored per metric.
        """
        self._metrics: dict[str, list[MetricPoint]] = defaultdict(list)
        self._retention_seconds = retention_seconds
        self._max_points = max_points_per_metric
        self._lock = threading.RLock()
        self._counters: dict[str, float] = defaultdict(float)
        self._gauges: dict[str, float] = {}
        self._tag_keys: set[str] = set()

    def increment(
        self,
        name: str,
        value: float = 1.0,
        tags: Optional[dict[str, str]] = None,
    ) -> None:
        """Increment a counter metric.

        Args:
            name: Metric name.
            value: Value to add.
            tags: Metric tags.
        """
        key = self._make_key(name, tags)
        with self._lock:
            self._counters[key] += value
            self._record_point(name, value, tags)

    def decrement(
        self,
        name: str,
        value: float = 1.0,
        tags: Optional[dict[str, str]] = None,
    ) -> None:
        """Decrement a counter metric.

        Args:
            name: Metric name.
            value: Value to subtract.
            tags: Metric tags.
        """
        self.increment(name, -value, tags)

    def record(
        self,
        name: str,
        value: float,
        tags: Optional[dict[str, str]] = None,
    ) -> None:
        """Record a histogram/summary value.

        Args:
            name: Metric name.
            value: Value to record.
            tags: Metric tags.
        """
        with self._lock:
            self._record_point(name, value, tags)

    def set_gauge(
        self,
        name: str,
        value: float,
        tags: Optional[dict[str, str]] = None,
    ) -> None:
        """Set a gauge metric value.

        Args:
            name: Metric name.
            value: Gauge value.
            tags: Metric tags.
        """
        key = self._make_key(name, tags)
        with self._lock:
            self._gauges[key] = value

    def _record_point(
        self,
        name: str,
        value: float,
        tags: Optional[dict[str, str]],
    ) -> None:
        """Record a data point."""
        now = time.time()
        cutoff = now - self._retention_seconds

        if tags:
            for k in tags:
                self._tag_keys.add(k)

        point = MetricPoint(timestamp=now, value=value, tags=tags or {})
        self._metrics[name].append(point)

        cutoff_idx = 0
        for i, p in enumerate(self._metrics[name]):
            if p.timestamp >= cutoff:
                break
            cutoff_idx = i + 1

        if cutoff_idx > 0:
            self._metrics[name] = self._metrics[name][cutoff_idx:]

        if len(self._metrics[name]) > self._max_points:
            self._metrics[name] = self._metrics[name][-self._max_points:]

    def get_counter(self, name: str, tags: Optional[dict[str, str]] = None) -> float:
        """Get current counter value.

        Args:
            name: Metric name.
            tags: Metric tags.

        Returns:
            Counter value.
        """
        key = self._make_key(name, tags)
        with self._lock:
            return self._counters.get(key, 0.0)

    def get_gauge(self, name: str, tags: Optional[dict[str, str]] = None) -> Optional[float]:
        """Get current gauge value.

        Args:
            name: Metric name.
            tags: Metric tags.

        Returns:
            Gauge value or None.
        """
        key = self._make_key(name, tags)
        with self._lock:
            return self._gauges.get(key)

    def get_histogram_stats(
        self,
        name: str,
        tags: Optional[dict[str, str]] = None,
    ) -> dict[str, float]:
        """Get histogram statistics for a metric.

        Args:
            name: Metric name.
            tags: Metric tags.

        Returns:
            Dict with min, max, mean, p50, p90, p95, p99.
        """
        with self._lock:
            points = self._metrics.get(name, [])

        values = [p.value for p in points if not p.tags or p.tags == (tags or {})]

        if not values:
            return {}

        values_sorted = sorted(values)
        n = len(values_sorted)

        return {
            "count": n,
            "min": values_sorted[0],
            "max": values_sorted[-1],
            "mean": sum(values_sorted) / n,
            "p50": values_sorted[int(n * 0.5)],
            "p90": values_sorted[int(n * 0.9)],
            "p95": values_sorted[int(n * 0.95)],
            "p99": values_sorted[int(n * 0.99)] if n >= 100 else values_sorted[-1],
        }

    def get_all_metrics(self) -> dict[str, Any]:
        """Get all metrics snapshot.

        Returns:
            Dict with counters, gauges, and metric names.
        """
        with self._lock:
            return {
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "metric_names": list(self._metrics.keys()),
            }

    def list_metrics(self) -> list[str]:
        """List all registered metric names."""
        with self._lock:
            return list(self._metrics.keys())

    def clear_metric(self, name: str) -> None:
        """Clear all data for a metric.

        Args:
            name: Metric name.
        """
        with self._lock:
            self._metrics.pop(name, None)
            logger.info("Cleared metric: %s", name)

    def _make_key(self, name: str, tags: Optional[dict[str, str]]) -> str:
        """Create a unique key for a metric with tags."""
        if not tags:
            return name
        tag_str = ",".join(f"{k}={v}" for k, v in sorted(tags.items()))
        return f"{name}{{{tag_str}}}"
