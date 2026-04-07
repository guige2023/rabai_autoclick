"""Metrics collector utilities: counter, gauge, histogram, summary with percentile support."""

from __future__ import annotations

import math
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "Metric",
    "Counter",
    "Gauge",
    "Histogram",
    "Summary",
    "MetricsCollector",
]


@dataclass
class Metric:
    """Base metric class."""

    name: str
    labels: dict[str, str] = field(default_factory=dict)


class Counter(Metric):
    """Monotonically increasing counter metric."""

    def __init__(self, name: str, labels: dict[str, str] | None = None) -> None:
        super().__init__(name, labels or {})
        self._value = 0.0
        self._lock = threading.Lock()

    def inc(self, amount: float = 1.0) -> None:
        with self._lock:
            self._value += amount

    @property
    def value(self) -> float:
        with self._lock:
            return self._value

    def reset(self) -> None:
        with self._lock:
            self._value = 0.0


class Gauge(Metric):
    """Gauge metric that can go up or down."""

    def __init__(self, name: str, labels: dict[str, str] | None = None) -> None:
        super().__init__(name, labels or {})
        self._value = 0.0
        self._lock = threading.Lock()

    def set(self, value: float) -> None:
        with self._lock:
            self._value = value

    def inc(self, amount: float = 1.0) -> None:
        with self._lock:
            self._value += amount

    def dec(self, amount: float = 1.0) -> None:
        with self._lock:
            self._value -= amount

    @property
    def value(self) -> float:
        with self._lock:
            return self._value


class Histogram(Metric):
    """Histogram metric that accumulates observations."""

    def __init__(
        self,
        name: str,
        buckets: list[float] | None = None,
        labels: dict[str, str] | None = None,
    ) -> None:
        super().__init__(name, labels or {})
        self._buckets = buckets or [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        self._counts: dict[float, int] = defaultdict(int)
        self._sum = 0.0
        self._total_count = 0
        self._lock = threading.Lock()

    def observe(self, value: float) -> None:
        with self._lock:
            self._sum += value
            self._total_count += 1
            for bound in self._buckets:
                if value <= bound:
                    self._counts[bound] += 1

    @property
    def count(self) -> int:
        with self._lock:
            return self._total_count

    @property
    def sum(self) -> float:
        with self._lock:
            return self._sum

    def percentile(self, p: float) -> float:
        """Calculate percentile (0-100)."""
        with self._lock:
            if self._total_count == 0:
                return 0.0
            target = (p / 100.0) * self._total_count
            cumulative = 0
            for bound in sorted(self._buckets):
                cumulative += self._counts[bound]
                if cumulative >= target:
                    return bound
            return self._buckets[-1]


class Summary(Metric):
    """Summary metric with windowed quantile estimation."""

    def __init__(
        self,
        name: str,
        window_size: int = 1000,
        labels: dict[str, str] | None = None,
    ) -> None:
        super().__init__(name, labels or {})
        self._window_size = window_size
        self._values: list[float] = []
        self._lock = threading.Lock()

    def observe(self, value: float) -> None:
        with self._lock:
            self._values.append(value)
            if len(self._values) > self._window_size:
                self._values = self._values[-self._window_size:]

    def quantile(self, q: float) -> float:
        """Calculate quantile (0-1)."""
        with self._lock:
            if not self._values:
                return 0.0
            sorted_values = sorted(self._values)
            idx = int(q * len(sorted_values))
            return sorted_values[min(idx, len(sorted_values) - 1)]


class MetricsCollector:
    """Collects and exports metrics."""

    def __init__(self) -> None:
        self._counters: dict[str, Counter] = {}
        self._gauges: dict[str, Gauge] = {}
        self._histograms: dict[str, Histogram] = {}
        self._summaries: dict[str, Summary] = {}
        self._lock = threading.Lock()

    def counter(self, name: str, labels: dict[str, str] | None = None) -> Counter:
        key = self._make_key(name, labels)
        with self._lock:
            if key not in self._counters:
                self._counters[key] = Counter(name, labels)
            return self._counters[key]

    def gauge(self, name: str, labels: dict[str, str] | None = None) -> Gauge:
        key = self._make_key(name, labels)
        with self._lock:
            if key not in self._gauges:
                self._gauges[key] = Gauge(name, labels)
            return self._gauges[key]

    def histogram(
        self,
        name: str,
        labels: dict[str, str] | None = None,
        buckets: list[float] | None = None,
    ) -> Histogram:
        key = self._make_key(name, labels)
        with self._lock:
            if key not in self._histograms:
                self._histograms[key] = Histogram(name, buckets, labels)
            return self._histograms[key]

    def summary(
        self,
        name: str,
        labels: dict[str, str] | None = None,
        window_size: int = 1000,
    ) -> Summary:
        key = self._make_key(name, labels)
        with self._lock:
            if key not in self._summaries:
                self._summaries[key] = Summary(name, window_size, labels)
            return self._summaries[key]

    def export(self) -> dict[str, Any]:
        """Export all metrics."""
        with self._lock:
            return {
                "timestamp": time.time(),
                "counters": {
                    k: {"value": c.value, "labels": c.labels}
                    for k, c in self._counters.items()
                },
                "gauges": {
                    k: {"value": g.value, "labels": g.labels}
                    for k, g in self._gauges.items()
                },
                "histograms": {
                    k: {
                        "count": h.count,
                        "sum": h.sum,
                        "p50": h.percentile(50),
                        "p95": h.percentile(95),
                        "p99": h.percentile(99),
                        "labels": h.labels,
                    }
                    for k, h in self._histograms.items()
                },
            }

    @staticmethod
    def _make_key(name: str, labels: dict[str, str] | None) -> str:
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"
