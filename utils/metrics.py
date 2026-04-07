"""Metrics collection utilities for RabAI AutoClick.

Provides:
- MetricsCollector: Collect and aggregate metrics
- Counter: Count occurrences
- Gauge: Track current values
- Histogram: Track value distributions
- Timer: Track durations
"""

import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TypeVar


T = TypeVar("T")


@dataclass
class MetricPoint:
    """Single metric data point."""
    timestamp: float
    value: float
    labels: Dict[str, str]


class Counter:
    """Thread-safe counter metric.

    Usage:
        counter = Counter("requests")
        counter.inc()
        counter.inc(5)
        print(counter.value)  # 6
    """

    def __init__(self, name: str, initial_value: float = 0, labels: Optional[Dict[str, str]] = None) -> None:
        self.name = name
        self._value = initial_value
        self._labels = labels or {}
        self._lock = threading.Lock()

    def inc(self, amount: float = 1) -> None:
        """Increment counter by amount."""
        with self._lock:
            self._value += amount

    def dec(self, amount: float = 1) -> None:
        """Decrement counter by amount."""
        with self._lock:
            self._value -= amount

    def set(self, value: float) -> None:
        """Set counter to specific value."""
        with self._lock:
            self._value = value

    @property
    def value(self) -> float:
        """Get current counter value."""
        with self._lock:
            return self._value

    def reset(self) -> None:
        """Reset counter to zero."""
        with self._lock:
            self._value = 0


class Gauge:
    """Thread-safe gauge metric for current values.

    Usage:
        gauge = Gauge("memory_usage")
        gauge.set(1024)
        gauge.inc(10)
        gauge.dec(5)
    """

    def __init__(self, name: str, initial_value: float = 0, labels: Optional[Dict[str, str]] = None) -> None:
        self.name = name
        self._value = initial_value
        self._labels = labels or {}
        self._lock = threading.Lock()

    def set(self, value: float) -> None:
        """Set gauge to specific value."""
        with self._lock:
            self._value = value

    def inc(self, amount: float = 1) -> None:
        """Increment gauge by amount."""
        with self._lock:
            self._value += amount

    def dec(self, amount: float = 1) -> None:
        """Decrement gauge by amount."""
        with self._lock:
            self._value -= amount

    @property
    def value(self) -> float:
        """Get current gauge value."""
        with self._lock:
            return self._value

    def reset(self) -> None:
        """Reset gauge to zero."""
        with self._lock:
            self._value = 0


class Histogram:
    """Thread-safe histogram metric for value distributions.

    Usage:
        hist = Histogram("request_duration", buckets=[0.1, 0.5, 1.0, 5.0])
        hist.observe(0.3)
        hist.observe(0.8)
        print(hist.mean)  # Average value
    """

    def __init__(
        self,
        name: str,
        buckets: Optional[List[float]] = None,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        self.name = name
        self.buckets = buckets or [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        self._labels = labels or {}
        self._values: List[float] = []
        self._count = 0
        self._sum = 0.0
        self._min = float('inf')
        self._max = float('-inf')
        self._lock = threading.Lock()

    def observe(self, value: float) -> None:
        """Record an observation."""
        with self._lock:
            self._values.append(value)
            self._count += 1
            self._sum += value
            self._min = min(self._min, value)
            self._max = max(self._max, value)

    @property
    def count(self) -> int:
        """Get total observation count."""
        with self._lock:
            return self._count

    @property
    def sum(self) -> float:
        """Get sum of all observations."""
        with self._lock:
            return self._sum

    @property
    def mean(self) -> float:
        """Get mean of observations."""
        with self._lock:
            if self._count == 0:
                return 0.0
            return self._sum / self._count

    @property
    def min(self) -> float:
        """Get minimum observation."""
        with self._lock:
            return self._min if self._count > 0 else 0.0

    @property
    def max(self) -> float:
        """Get maximum observation."""
        with self._lock:
            return self._max if self._count > 0 else 0.0

    def bucket_counts(self) -> Dict[float, int]:
        """Get count per bucket."""
        with self._lock:
            counts = {b: 0 for b in self.buckets}
            for v in self._values:
                for bucket in self.buckets:
                    if v <= bucket:
                        counts[bucket] += 1
            return counts

    def reset(self) -> None:
        """Reset all observations."""
        with self._lock:
            self._values.clear()
            self._count = 0
            self._sum = 0.0
            self._min = float('inf')
            self._max = float('-inf')


class Timer:
    """Thread-safe timer metric for tracking durations.

    Usage:
        timer = Timer("request_duration")
        with timer.time():
            do_something()
        print(timer.mean)  # Average duration
    """

    def __init__(
        self,
        name: str,
        histogram: Optional[Histogram] = None,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        self.name = name
        self.histogram = histogram or Histogram(name)
        self._labels = labels or {}
        self._start_time: Optional[float] = None
        self._lock = threading.Lock()

    def time(self) -> '_TimerContext':
        """Start timing. Use as context manager."""
        return _TimerContext(self)

    def observe(self, duration: float) -> None:
        """Record a duration observation."""
        self.histogram.observe(duration)

    @property
    def count(self) -> int:
        """Get total timing count."""
        return self.histogram.count

    @property
    def mean(self) -> float:
        """Get mean duration."""
        return self.histogram.mean


class _TimerContext:
    """Context manager for timing."""
    _timer: Timer
    _duration: Optional[float] = None

    def __init__(self, timer: Timer) -> None:
        self._timer = timer

    def __enter__(self) -> '_TimerContext':
        self._start = time.perf_counter()
        return self

    def __exit__(self, *args: Any) -> None:
        self._duration = time.perf_counter() - self._start
        self._timer.observe(self._duration)

    @property
    def duration(self) -> float:
        """Get recorded duration."""
        return self._duration or 0.0


class MetricsCollector:
    """Central metrics collection and aggregation.

    Usage:
        collector = MetricsCollector()
        collector.counter("requests").inc()
        collector.gauge("memory", 1024)
        collector.histogram("latency").observe(0.5)
    """

    def __init__(self) -> None:
        self._counters: Dict[str, Counter] = {}
        self._gauges: Dict[str, Gauge] = {}
        self._histograms: Dict[str, Histogram] = {}
        self._timers: Dict[str, Timer] = {}
        self._lock = threading.Lock()

    def counter(self, name: str, labels: Optional[Dict[str, str]] = None) -> Counter:
        """Get or create a counter."""
        key = self._make_key(name, labels)
        with self._lock:
            if key not in self._counters:
                self._counters[key] = Counter(name, labels=labels)
            return self._counters[key]

    def gauge(self, name: str, value: Optional[float] = None, labels: Optional[Dict[str, str]] = None) -> Gauge:
        """Get or create a gauge, optionally setting its value."""
        key = self._make_key(name, labels)
        with self._lock:
            if key not in self._gauges:
                self._gauges[key] = Gauge(name, labels=labels)
            if value is not None:
                self._gauges[key].set(value)
            return self._gauges[key]

    def histogram(self, name: str, labels: Optional[Dict[str, str]] = None) -> Histogram:
        """Get or create a histogram."""
        key = self._make_key(name, labels)
        with self._lock:
            if key not in self._histograms:
                self._histograms[key] = Histogram(name, labels=labels)
            return self._histograms[key]

    def timer(self, name: str, labels: Optional[Dict[str, str]] = None) -> Timer:
        """Get or create a timer."""
        key = self._make_key(name, labels)
        with self._lock:
            if key not in self._timers:
                self._timers[key] = Timer(name, labels=labels)
            return self._timers[key]

    def timed(self, name: str, labels: Optional[Dict[str, str]] = None) -> Callable[[Callable[..., T]], Callable[..., T]]:
        """Decorator to time function execution."""
        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            timer = self.timer(name, labels)
            def wrapper(*args: Any, **kwargs: Any) -> T:
                with timer.time():
                    return func(*args, **kwargs)
            return wrapper
        return decorator

    def _make_key(self, name: str, labels: Optional[Dict[str, str]]) -> str:
        """Create unique key for metric with labels."""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all current metrics."""
        with self._lock:
            return {
                "counters": {k: c.value for k, c in self._counters.items()},
                "gauges": {k: g.value for k, g in self._gauges.items()},
                "histograms": {
                    k: {"count": h.count, "sum": h.sum, "mean": h.mean}
                    for k, h in self._histograms.items()
                },
            }

    def reset_all(self) -> None:
        """Reset all metrics."""
        with self._lock:
            for c in self._counters.values():
                c.reset()
            for g in self._gauges.values():
                g.reset()
            for h in self._histograms.values():
                h.reset()


# Global metrics collector
metrics = MetricsCollector()