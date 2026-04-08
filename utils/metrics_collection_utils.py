"""Metrics collection and reporting utilities for automation performance tracking.

Provides counters, gauges, histograms, and timers for
measuring automation action performance, with export
to various backends (statsd, prometheus, JSON).

Example:
    >>> from utils.metrics_collection_utils import Counter, Histogram, Timer
    >>> Counter('clicks').inc()
    >>> with Timer('action_duration'):
    ...     perform_action()
    >>> metrics = get_current_metrics()
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

__all__ = [
    "Counter",
    "Gauge",
    "Histogram",
    "Timer",
    "Meter",
    "MetricsRegistry",
    "get_current_metrics",
    "export_prometheus",
    "export_json",
]


@dataclass
class MetricSnapshot:
    """A point-in-time snapshot of metrics."""

    timestamp: float
    counters: dict
    gauges: dict
    histograms: dict


class Counter:
    """A monotonically increasing counter.

    Thread-safe.
    """

    def __init__(self, name: str, registry: Optional["MetricsRegistry"] = None):
        self.name = name
        self._value: int = 0
        self._lock = threading.Lock()
        if registry:
            registry.register(self)

    def inc(self, delta: int = 1) -> None:
        with self._lock:
            self._value += delta

    def dec(self, delta: int = 1) -> None:
        with self._lock:
            self._value -= delta

    def value(self) -> int:
        with self._lock:
            return self._value

    def reset(self) -> None:
        with self._lock:
            self._value = 0

    def snapshot(self) -> int:
        return self.value()


class Gauge:
    """A value that can go up and down."""

    def __init__(self, name: str, registry: Optional["MetricsRegistry"] = None):
        self.name = name
        self._value: float = 0.0
        self._lock = threading.Lock()
        if registry:
            registry.register(self)

    def set(self, value: float) -> None:
        with self._lock:
            self._value = value

    def inc(self, delta: float = 1.0) -> None:
        with self._lock:
            self._value += delta

    def dec(self, delta: float = 1.0) -> None:
        with self._lock:
            self._value -= delta

    def value(self) -> float:
        with self._lock:
            return self._value

    def snapshot(self) -> float:
        return self.value()


class Histogram:
    """A histogram for measuring distributions.

    Tracks count, sum, min, max, and percentiles.
    """

    def __init__(
        self,
        name: str,
        registry: Optional["MetricsRegistry"] = None,
        bins: int = 100,
    ):
        self.name = name
        self._values: list[float] = []
        self._lock = threading.Lock()
        self._bins = bins
        if registry:
            registry.register(self)

    def observe(self, value: float) -> None:
        with self._lock:
            self._values.append(value)
            if len(self._values) > self._bins * 10:
                self._values = self._values[-self._bins :]

    def snapshot(self) -> dict:
        with self._lock:
            if not self._values:
                return {
                    "count": 0,
                    "sum": 0.0,
                    "min": 0.0,
                    "max": 0.0,
                    "mean": 0.0,
                    "p50": 0.0,
                    "p95": 0.0,
                    "p99": 0.0,
                }

            sorted_vals = sorted(self._values)
            n = len(sorted_vals)
            return {
                "count": n,
                "sum": sum(sorted_vals),
                "min": sorted_vals[0],
                "max": sorted_vals[-1],
                "mean": sum(sorted_vals) / n,
                "p50": sorted_vals[int(n * 0.5)],
                "p95": sorted_vals[int(n * 0.95)] if n >= 20 else sorted_vals[-1],
                "p99": sorted_vals[int(n * 0.99)] if n >= 100 else sorted_vals[-1],
            }


class Meter:
    """A meter for measuring request rates."""

    def __init__(self, name: str, registry: Optional["MetricsRegistry"] = None):
        self.name = name
        self._count: int = 0
        self._lock = threading.Lock()
        self._start_time = time.time()
        if registry:
            registry.register(self)

    def mark(self, n: int = 1) -> None:
        with self._lock:
            self._count += n

    def rate(self) -> float:
        elapsed = time.time() - self._start_time
        if elapsed == 0:
            return 0.0
        return self._count / elapsed

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "count": self._count,
                "rate": self.rate(),
            }


class Timer:
    """A timer that measures elapsed time.

    Can be used as a context manager or with start/stop.

    Example:
        >>> with Timer('my_operation'):
        ...     do_work()
    """

    def __init__(self, name: str, histogram: Optional[Histogram] = None):
        self.name = name
        self.histogram = histogram
        self._start: Optional[float] = None
        self._elapsed: float = 0.0
        self._lock = threading.Lock()

    def start(self) -> "Timer":
        self._start = time.perf_counter()
        return self

    def stop(self) -> float:
        if self._start is None:
            return 0.0
        with self._lock:
            self._elapsed = time.perf_counter() - self._start
        if self.histogram:
            self.histogram.observe(self._elapsed)
        return self._elapsed

    def __enter__(self) -> "Timer":
        self.start()
        return self

    def __exit__(self, *args) -> None:
        self.stop()

    @property
    def elapsed(self) -> float:
        if self._start is None:
            return self._elapsed
        with self._lock:
            return time.perf_counter() - self._start


class MetricsRegistry:
    """Global metrics registry.

    Example:
        >>> registry = MetricsRegistry()
        >>> counter = Counter('requests', registry=registry)
        >>> gauge = Gauge('queue_size', registry=registry)
        >>> snapshot = registry.snapshot()
    """

    def __init__(self):
        self._metrics: dict[str, object] = {}
        self._lock = threading.Lock()

    def register(self, metric: object) -> None:
        name = getattr(metric, "name", str(id(metric)))
        with self._lock:
            self._metrics[name] = metric

    def get(self, name: str) -> Optional[object]:
        with self._lock:
            return self._metrics.get(name)

    def snapshot(self) -> MetricSnapshot:
        with self._lock:
            counters = {
                name: m.snapshot()
                for name, m in self._metrics.items()
                if isinstance(m, Counter)
            }
            gauges = {
                name: m.snapshot()
                for name, m in self._metrics.items()
                if isinstance(m, Gauge)
            }
            histograms = {
                name: m.snapshot()
                for name, m in self._metrics.items()
                if isinstance(m, Histogram)
            }

        return MetricSnapshot(
            timestamp=time.time(),
            counters=counters,
            gauges=gauges,
            histograms=histograms,
        )


# Global default registry
_default_registry = MetricsRegistry()


def get_current_metrics() -> MetricSnapshot:
    """Get a snapshot of all metrics from the default registry."""
    return _default_registry.snapshot()


def export_prometheus() -> str:
    """Export metrics in Prometheus text format.

    Returns:
        Prometheus-formatted metrics string.
    """
    snapshot = get_current_metrics()
    lines = []

    for name, value in snapshot.counters.items():
        lines.append(f"# TYPE {name} counter")
        lines.append(f"{name} {value}")

    for name, value in snapshot.gauges.items():
        lines.append(f"# TYPE {name} gauge")
        lines.append(f"{name} {value}")

    for name, data in snapshot.histograms.items():
        lines.append(f"# TYPE {name} histogram")
        for suffix, val in data.items():
            if suffix != "count":
                lines.append(f"{name}_{suffix} {val}")

    return "\n".join(lines) + "\n"


def export_json() -> str:
    """Export metrics as JSON.

    Returns:
        JSON-formatted metrics string.
    """
    import json

    snapshot = get_current_metrics()
    return json.dumps(
        {
            "timestamp": snapshot.timestamp,
            "counters": snapshot.counters,
            "gauges": snapshot.gauges,
            "histograms": snapshot.histograms,
        },
        indent=2,
    )
