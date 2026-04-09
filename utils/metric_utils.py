"""Metrics collection and reporting utilities.

Provides counters, gauges, histograms, and
timers for performance monitoring.
"""

import time
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class MetricPoint:
    """Single metric measurement."""
    value: float
    timestamp: float = field(default_factory=time.time)
    labels: Dict[str, str] = field(default_factory=dict)


class Counter:
    """Increment-only metric counter.

    Example:
        counter = Counter("requests_total", labels={"method": "GET"})
        counter.inc()
        counter.inc(5)
    """

    def __init__(
        self,
        name: str,
        initial_value: float = 0.0,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        self.name = name
        self._labels = labels or {}
        self._value = initial_value
        self._lock = threading.Lock()

    def inc(self, amount: float = 1.0) -> None:
        """Increment counter."""
        with self._lock:
            self._value += amount

    def get(self) -> float:
        """Get current value."""
        return self._value

    def reset(self) -> None:
        """Reset counter to zero."""
        with self._lock:
            self._value = 0.0


class Gauge:
    """Value that can go up or down.

    Example:
        gauge = Gauge("memory_usage_bytes")
        gauge.set(1024 * 1024)
        gauge.inc(100)
        gauge.dec(50)
    """

    def __init__(
        self,
        name: str,
        initial_value: float = 0.0,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        self.name = name
        self._labels = labels or {}
        self._value = initial_value
        self._lock = threading.Lock()

    def set(self, value: float) -> None:
        """Set absolute value."""
        with self._lock:
            self._value = value

    def inc(self, amount: float = 1.0) -> None:
        """Increment gauge."""
        with self._lock:
            self._value += amount

    def dec(self, amount: float = 1.0) -> None:
        """Decrement gauge."""
        with self._lock:
            self._value -= amount

    def get(self) -> float:
        """Get current value."""
        return self._value


class Histogram:
    """Distribution of values.

    Example:
        hist = Histogram("request_duration_seconds", buckets=[0.01, 0.05, 0.1, 0.5, 1.0])
        hist.observe(0.12)
    """

    DEFAULT_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]

    def __init__(
        self,
        name: str,
        buckets: Optional[List[float]] = None,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        self.name = name
        self.buckets = buckets or self.DEFAULT_BUCKETS
        self._labels = labels or {}
        self._counts: Dict[float, int] = defaultdict(int)
        self._sum = 0.0
        self._count = 0
        self._lock = threading.Lock()

    def observe(self, value: float) -> None:
        """Record an observation."""
        with self._lock:
            self._sum += value
            self._count += 1
            for bound in self.buckets:
                if value <= bound:
                    self._counts[bound] += 1

    def get_stats(self) -> Dict[str, float]:
        """Get histogram statistics."""
        with self._lock:
            if self._count == 0:
                return {"count": 0, "sum": 0, "avg": 0, "min": 0, "max": 0}
            return {
                "count": self._count,
                "sum": self._sum,
                "avg": self._sum / self._count,
                "min": min(k for k, v in self._counts.items() if v > 0),
                "max": max(self.buckets[:self._count]),
            }

    def get_bucket_counts(self) -> Dict[float, int]:
        """Get counts per bucket."""
        return dict(self._counts)


class Timer:
    """Context manager for timing operations.

    Example:
        timer = Timer("operation_seconds")
        with timer:
            do_work()
        print(timer.elapsed)
    """

    def __init__(
        self,
        name: str,
        histogram: Optional[Histogram] = None,
    ) -> None:
        self.name = name
        self.histogram = histogram
        self._start: Optional[float] = None
        self._end: Optional[float] = None

    def __enter__(self) -> "Timer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, *args: Any) -> None:
        self._end = time.perf_counter()
        if self.histogram:
            self.histogram.observe(self.elapsed)

    @property
    def elapsed(self) -> float:
        """Get elapsed time in seconds."""
        if self._start is None:
            return 0.0
        end = self._end if self._end else time.perf_counter()
        return end - self._start


class MetricsRegistry:
    """Central metrics registry.

    Example:
        registry = MetricsRegistry()
        counter = registry.counter("requests_total")
        gauge = registry.gauge("active_connections")
        registry.report()
    """

    def __init__(self) -> None:
        self._counters: Dict[str, Counter] = {}
        self._gauges: Dict[str, Gauge] = {}
        self._histograms: Dict[str, Histogram] = {}
        self._lock = threading.Lock()

    def counter(
        self,
        name: str,
        labels: Optional[Dict[str, str]] = None,
    ) -> Counter:
        """Get or create counter."""
        with self._lock:
            key = self._make_key(name, labels)
            if key not in self._counters:
                self._counters[key] = Counter(name, labels=labels)
            return self._counters[key]

    def gauge(
        self,
        name: str,
        labels: Optional[Dict[str, str]] = None,
    ) -> Gauge:
        """Get or create gauge."""
        with self._lock:
            key = self._make_key(name, labels)
            if key not in self._gauges:
                self._gauges[key] = Gauge(name, labels=labels)
            return self._gauges[key]

    def histogram(
        self,
        name: str,
        buckets: Optional[List[float]] = None,
        labels: Optional[Dict[str, str]] = None,
    ) -> Histogram:
        """Get or create histogram."""
        with self._lock:
            key = self._make_key(name, labels)
            if key not in self._histograms:
                self._histograms[key] = Histogram(name, buckets=buckets, labels=labels)
            return self._histograms[key]

    def timer(self, name: str) -> Timer:
        """Create timer for measuring durations."""
        return Timer(name, self.histogram(name))

    def _make_key(self, name: str, labels: Optional[Dict[str, str]]) -> str:
        """Create unique key for metric."""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def report(self) -> Dict[str, Any]:
        """Generate metrics report."""
        with self._lock:
            report: Dict[str, Any] = {
                "counters": {},
                "gauges": {},
                "histograms": {},
            }

            for key, counter in self._counters.items():
                report["counters"][key] = counter.get()

            for key, gauge in self._gauges.items():
                report["gauges"][key] = gauge.get()

            for key, hist in self._histograms.items():
                report["histograms"][key] = hist.get_stats()

            return report


_global_registry = MetricsRegistry()


def get_registry() -> MetricsRegistry:
    """Get global metrics registry."""
    return _global_registry


def counter(name: str, **labels: str) -> Counter:
    """Get counter from global registry."""
    return _global_registry.counter(name, labels=labels or None)


def gauge(name: str, **labels: str) -> Gauge:
    """Get gauge from global registry."""
    return _global_registry.gauge(name, labels=labels or None)


def histogram(name: str, **labels: str) -> Histogram:
    """Get histogram from global registry."""
    return _global_registry.histogram(name, labels=labels or None)


def timer(name: str) -> Timer:
    """Create timer from global registry."""
    return _global_registry.timer(name)
