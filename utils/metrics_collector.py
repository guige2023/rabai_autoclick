"""
Metrics Collector Utility

Collects and aggregates performance metrics for automation operations.
Provides real-time statistics and historical analysis.

Example:
    >>> collector = MetricsCollector()
    >>> collector.increment("clicks")
    >>> collector.record("page_load_time", 1.23)
    >>> stats = collector.get_stats()
    >>> print(stats["clicks"]["count"])
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Optional
import math


@dataclass
class MetricStats:
    """Statistics for a single metric."""
    count: int = 0
    sum: float = 0.0
    min_val: float = float("inf")
    max_val: float = float("-inf")
    mean: float = 0.0
    variance: float = 0.0
    values: list[float] = field(default_factory=list)

    def add(self, value: float) -> None:
        """Add a value to the metric."""
        self.count += 1
        self.sum += value
        self.values.append(value)

        if value < self.min_val:
            self.min_val = value
        if value > self.max_val:
            self.max_val = value

        # Welford's online algorithm for variance
        if self.count == 1:
            self.mean = value
            self.variance = 0.0
        else:
            old_mean = self.mean
            self.mean += (value - old_mean) / self.count
            self.variance += (value - old_mean) * (value - self.mean)

    @property
    def stddev(self) -> float:
        """Standard deviation."""
        return math.sqrt(self.variance) if self.variance > 0 else 0.0

    def percentile(self, p: float) -> float:
        """Calculate percentile (0-100)."""
        if not self.values:
            return 0.0
        sorted_vals = sorted(self.values)
        idx = int(len(sorted_vals) * p / 100.0)
        idx = min(idx, len(sorted_vals) - 1)
        return sorted_vals[idx]


class MetricsCollector:
    """
    Thread-safe metrics collection and aggregation.

    Supports counters, gauges, and timing metrics.
    """

    def __init__(self, max_values_per_metric: int = 1000) -> None:
        self.max_values_per_metric = max_values_per_metric
        self._metrics: dict[str, MetricStats] = {}
        self._counters: dict[str, int] = defaultdict(int)
        self._gauges: dict[str, float] = {}
        self._lock = threading.RLock()
        self._start_time = time.time()

    def increment(self, name: str, value: int = 1) -> None:
        """Increment a counter metric."""
        with self._lock:
            self._counters[name] += value

    def decrement(self, name: str, value: int = 1) -> None:
        """Decrement a counter metric."""
        with self._lock:
            self._counters[name] -= value

    def record(self, name: str, value: float) -> None:
        """Record a value for a metric."""
        with self._lock:
            if name not in self._metrics:
                self._metrics[name] = MetricStats()
            self._metrics[name].add(value)

            # Limit stored values
            if len(self._metrics[name].values) > self.max_values_per_metric:
                self._metrics[name].values = self._metrics[name].values[-self.max_values_per_metric:]

    def gauge(self, name: str, value: float) -> None:
        """Set a gauge value (point-in-time metric)."""
        with self._lock:
            self._gauges[name] = value

    def get_counter(self, name: str) -> int:
        """Get current counter value."""
        with self._lock:
            return self._counters.get(name, 0)

    def get_gauge(self, name: str) -> Optional[float]:
        """Get current gauge value."""
        with self._lock:
            return self._gauges.get(name)

    def get_stats(self, name: str) -> Optional[dict[str, Any]]:
        """Get statistics for a recorded metric."""
        with self._lock:
            metric = self._metrics.get(name)
            if metric is None:
                return None

            return {
                "count": metric.count,
                "sum": metric.sum,
                "min": metric.min_val if metric.count > 0 else 0,
                "max": metric.max_val if metric.count > 0 else 0,
                "mean": metric.mean,
                "stddev": metric.stddev,
                "p50": metric.percentile(50),
                "p90": metric.percentile(90),
                "p95": metric.percentile(95),
                "p99": metric.percentile(99),
            }

    def get_all_stats(self) -> dict[str, Any]:
        """Get all collected metrics."""
        with self._lock:
            result = {
                "uptime": time.time() - self._start_time,
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "metrics": {},
            }

            for name, stats in self._metrics.items():
                result["metrics"][name] = self.get_stats(name)

            return result

    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self._metrics.clear()
            self._counters.clear()
            self._gauges.clear()
            self._start_time = time.time()

    def timer(self, name: str) -> "_TimerContext":
        """Context manager for timing operations."""
        return _TimerContext(self, name)

    def export_prometheus(self) -> str:
        """Export metrics in Prometheus text format."""
        lines: list[str] = []
        with self._lock:
            for name, value in self._counters.items():
                safe_name = name.replace(".", "_").replace("-", "_")
                lines.append(f"# TYPE {safe_name} counter")
                lines.append(f"{safe_name} {value}")

            for name, value in self._gauges.items():
                safe_name = name.replace(".", "_").replace("-", "_")
                lines.append(f"# TYPE {safe_name} gauge")
                lines.append(f"{safe_name} {value}")

            for name, stats in self._metrics.items():
                safe_name = name.replace(".", "_").replace("-", "_")
                lines.append(f"# TYPE {safe_name} summary")
                lines.append(f"{safe_name}_count {stats.count}")
                lines.append(f"{safe_name}_sum {stats.sum}")
                lines.append(f"{safe_name}_mean {stats.mean}")

        return "\n".join(lines) + "\n"


class _TimerContext:
    """Context manager for timing operations."""

    def __init__(self, collector: MetricsCollector, name: str) -> None:
        self.collector = collector
        self.name = name
        self._start_time: Optional[float] = None

    def __enter__(self) -> "_TimerContext":
        self._start_time = time.time()
        return self

    def __exit__(self, *args: Any) -> None:
        if self._start_time is not None:
            duration = time.time() - self._start_time
            self.collector.record(self.name, duration)
