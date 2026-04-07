"""Metrics collection utilities: counters, gauges, histograms, and export."""

from __future__ import annotations

import math
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "Counter",
    "Gauge",
    "Histogram",
    "Timer",
    "MetricsRegistry",
    "MetricsExporter",
]


@dataclass
class MetricPoint:
    """A single metric data point."""
    name: str
    value: float
    labels: dict[str, str]
    timestamp: float
    metric_type: str


class Counter:
    """Monotonically increasing counter."""

    def __init__(self, name: str, labels: dict[str, str] | None = None) -> None:
        self.name = name
        self._labels = labels or {}
        self._value = 0.0
        self._lock = threading.Lock()

    def inc(self, amount: float = 1.0) -> None:
        with self._lock:
            self._value += amount

    def value(self) -> float:
        return self._value

    def snapshot(self, timestamp: float) -> MetricPoint:
        return MetricPoint(self.name, self._value, self._labels.copy(), timestamp, "counter")


class Gauge:
    """Point-in-time value gauge."""

    def __init__(self, name: str, labels: dict[str, str] | None = None) -> None:
        self.name = name
        self._labels = labels or {}
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

    def value(self) -> float:
        return self._value

    def snapshot(self, timestamp: float) -> MetricPoint:
        return MetricPoint(self.name, self._value, self._labels.copy(), timestamp, "gauge")


class Histogram:
    """Histogram for tracking distributions."""

    def __init__(
        self,
        name: str,
        buckets: tuple[float, ...] | None = None,
        labels: dict[str, str] | None = None,
    ) -> None:
        self.name = name
        self.buckets = buckets or (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
        self._labels = labels or {}
        self._sum = 0.0
        self._count = 0
        self._bucket_counts: dict[float, int] = defaultdict(int)
        self._min = math.inf
        self._max = -math.inf
        self._lock = threading.Lock()

    def observe(self, value: float) -> None:
        with self._lock:
            self._sum += value
            self._count += 1
            self._min = min(self._min, value)
            self._max = max(self._max, value)
            for bucket in self.buckets:
                if value <= bucket:
                    self._bucket_counts[bucket] += 1

    def mean(self) -> float:
        with self._lock:
            if self._count == 0:
                return 0.0
            return self._sum / self._count

    def snapshot(self, timestamp: float) -> list[MetricPoint]:
        with self._lock:
            points = []
            for bucket in self.buckets:
                p = MetricPoint(
                    f"{self.name}_bucket",
                    float(self._bucket_counts[bucket]),
                    {**self._labels, "le": str(bucket)},
                    timestamp,
                    "histogram",
                )
                points.append(p)
            points.append(MetricPoint(
                f"{self.name}_bucket",
                float(self._count),
                {**self._labels, "le": "+Inf"},
                timestamp,
                "histogram",
            ))
            points.append(MetricPoint(f"{self.name}_sum", self._sum, self._labels.copy(), timestamp, "histogram"))
            points.append(MetricPoint(f"{self.name}_count", float(self._count), self._labels.copy(), timestamp, "histogram"))
            return points


class Timer:
    """Context manager for timing operations."""

    def __init__(self, histogram: Histogram) -> None:
        self._histogram = histogram
        self._start: float | None = None

    def __enter__(self) -> "Timer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, *args: Any) -> None:
        if self._start is not None:
            elapsed = time.perf_counter() - self._start
            self._histogram.observe(elapsed)


class MetricsRegistry:
    """Central metrics registry with export support."""

    def __init__(self) -> None:
        self._counters: dict[str, Counter] = {}
        self._gauges: dict[str, Gauge] = {}
        self._histograms: dict[str, Histogram] = {}
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
        buckets: tuple[float, ...] | None = None,
        labels: dict[str, str] | None = None,
    ) -> Histogram:
        key = self._make_key(name, labels)
        with self._lock:
            if key not in self._histograms:
                self._histograms[key] = Histogram(name, buckets, labels)
            return self._histograms[key]

    def timer(self, name: str, labels: dict[str, str] | None = None) -> Timer:
        h = self.histogram(f"{name}_seconds", labels=labels)
        return Timer(h)

    def _make_key(self, name: str, labels: dict[str, str] | None) -> str:
        if not labels:
            return name
        return f"{name}:{','.join(f'{k}={v}' for k,v in sorted(labels.items()))}"

    def collect(self) -> list[MetricPoint]:
        """Collect all metrics snapshots."""
        timestamp = time.time()
        points: list[MetricPoint] = []
        with self._lock:
            for c in self._counters.values():
                points.append(c.snapshot(timestamp))
            for g in self._gauges.values():
                points.append(g.snapshot(timestamp))
            for h in self._histograms.values():
                points.extend(h.snapshot(timestamp))
        return points

    def export_prometheus(self) -> str:
        """Export metrics in Prometheus text format."""
        lines: list[str] = []
        for p in self.collect():
            label_str = ""
            if p.labels:
                label_str = "{" + ",".join(f'{k}="{v}"' for k, v in p.labels.items()) + "}"
            lines.append(f"{p.name}{label_str} {p.value}")
        return "\n".join(lines) + "\n"

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._histograms.clear()
