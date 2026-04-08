"""API Metrics Action Module.

Provides API metrics collection with counters,
histograms, and gauges for monitoring.
"""
from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Metric type."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


@dataclass
class MetricValue:
    """Metric value."""
    name: str
    metric_type: MetricType
    value: float
    timestamp: float
    labels: Dict[str, str] = field(default_factory=dict)


class APIMetricsAction:
    """API metrics collector.

    Example:
        metrics = APIMetricsAction()

        metrics.increment("requests", labels={"method": "GET"})
        metrics.gauge("active_connections", 10)

        with metrics.timer("request_duration"):
            await api_call()

        snapshot = metrics.snapshot()
    """

    def __init__(self) -> None:
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = defaultdict(list)
        self._labels: Dict[str, Dict[str, str]] = {}
        self._timers: Dict[str, float] = {}

    def increment(
        self,
        name: str,
        value: float = 1.0,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """Increment counter metric.

        Args:
            name: Metric name
            value: Value to add
            labels: Optional labels
        """
        key = self._make_key(name, labels)
        self._counters[key] += value

        if labels:
            self._labels[key] = labels

    def decrement(
        self,
        name: str,
        value: float = 1.0,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """Decrement counter metric.

        Args:
            name: Metric name
            value: Value to subtract
            labels: Optional labels
        """
        self.increment(name, -value, labels)

    def gauge(
        self,
        name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """Set gauge metric.

        Args:
            name: Metric name
            value: Gauge value
            labels: Optional labels
        """
        key = self._make_key(name, labels)
        self._gauges[key] = value

        if labels:
            self._labels[key] = labels

    def observe(
        self,
        name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """Observe value for histogram.

        Args:
            name: Metric name
            value: Observed value
            labels: Optional labels
        """
        key = self._make_key(name, labels)
        self._histograms[key].append(value)

        if labels:
            self._labels[key] = labels

    def timer(
        self,
        name: str,
        labels: Optional[Dict[str, str]] = None,
    ) -> "TimerContext":
        """Start timer context.

        Returns:
            TimerContext for timing
        """
        return TimerContext(self, name, labels)

    def start_timer(self, name: str, labels: Optional[Dict[str, str]] = None) -> None:
        """Start named timer."""
        key = self._make_key(name, labels)
        self._timers[key] = time.time()

    def stop_timer(self, name: str, labels: Optional[Dict[str, str]] = None) -> float:
        """Stop named timer and record duration.

        Returns:
            Elapsed time in seconds
        """
        key = self._make_key(name, labels)
        if key not in self._timers:
            return 0.0

        elapsed = time.time() - self._timers[key]
        self.observe(name, elapsed, labels)
        del self._timers[key]

        return elapsed

    def snapshot(self) -> Dict[str, Any]:
        """Get metrics snapshot.

        Returns:
            All current metric values
        """
        return {
            "counters": dict(self._counters),
            "gauges": dict(self._gauges),
            "histograms": {
                k: self._compute_stats(v)
                for k, v in self._histograms.items()
            },
            "timestamp": time.time(),
        }

    def _compute_stats(self, values: List[float]) -> Dict[str, float]:
        """Compute histogram statistics."""
        if not values:
            return {}

        sorted_values = sorted(values)
        n = len(sorted_values)

        return {
            "count": n,
            "sum": sum(values),
            "min": min(values),
            "max": max(values),
            "mean": sum(values) / n,
            "p50": sorted_values[n // 2],
            "p95": sorted_values[int(n * 0.95)],
            "p99": sorted_values[int(n * 0.99)],
        }

    def _make_key(
        self,
        name: str,
        labels: Optional[Dict[str, str]],
    ) -> str:
        """Make metric key from name and labels."""
        if not labels:
            return name

        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def reset(self) -> None:
        """Reset all metrics."""
        self._counters.clear()
        self._gauges.clear()
        self._histograms.clear()
        self._labels.clear()
        self._timers.clear()


class TimerContext:
    """Timer context manager."""

    def __init__(
        self,
        metrics: APIMetricsAction,
        name: str,
        labels: Optional[Dict[str, str]],
    ) -> None:
        self.metrics = metrics
        self.name = name
        self.labels = labels
        self.start_time: Optional[float] = None

    def __enter__(self) -> "TimerContext":
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.start_time is not None:
            elapsed = time.time() - self.start_time
            self.metrics.observe(self.name, elapsed, self.labels)
