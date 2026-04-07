"""
Metrics Collection Utilities

Provides comprehensive metrics collection with counters, gauges,
histograms, timers, and aggregation capabilities.
"""

from __future__ import annotations

import copy
import time
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class MetricType(Enum):
    """Type of metric."""
    COUNTER = auto()
    GAUGE = auto()
    HISTOGRAM = auto()
    TIMER = auto()
    METER = auto()


@dataclass
class MetricValue:
    """A single metric value with metadata."""
    value: float
    timestamp: float = field(default_factory=time.time)
    labels: dict[str, str] = field(default_factory=dict)


@dataclass
class Counter:
    """Simple counter metric."""
    name: str
    value: float = 0.0
    labels: dict[str, str] = field(default_factory=dict)

    def increment(self, amount: float = 1.0) -> None:
        """Increment the counter."""
        self.value += amount

    def decrement(self, amount: float = 1.0) -> None:
        """Decrement the counter."""
        self.value -= amount

    def reset(self) -> None:
        """Reset the counter."""
        self.value = 0.0


@dataclass
class Gauge:
    """Gauge metric that can go up and down."""
    name: str
    value: float = 0.0
    labels: dict[str, str] = field(default_factory=dict)

    def set(self, value: float) -> None:
        """Set the gauge value."""
        self.value = value

    def increment(self, amount: float = 1.0) -> None:
        """Increment the gauge."""
        self.value += amount

    def decrement(self, amount: float = 1.0) -> None:
        """Decrement the gauge."""
        self.value -= amount


@dataclass
class Histogram:
    """Histogram metric for distribution data."""
    name: str
    values: list[float] = field(default_factory=list)
    labels: dict[str, str] = field(default_factory=dict)
    min_value: float = float("inf")
    max_value: float = float("-inf")

    def observe(self, value: float) -> None:
        """Record an observation."""
        self.values.append(value)
        self.min_value = min(self.min_value, value)
        self.max_value = max(self.max_value, value)

    def count(self) -> int:
        """Number of observations."""
        return len(self.values)

    def sum(self) -> float:
        """Sum of all observations."""
        return sum(self.values)

    def mean(self) -> float:
        """Mean of observations."""
        return self.sum() / self.count() if self.values else 0.0

    def percentiles(self, *percentiles: float) -> dict[float, float]:
        """Calculate percentiles."""
        if not self.values:
            return {}

        sorted_values = sorted(self.values)
        result = {}

        for p in percentiles:
            if p < 0 or p > 100:
                continue
            idx = int(len(sorted_values) * p / 100)
            idx = min(idx, len(sorted_values) - 1)
            result[p] = sorted_values[idx]

        return result


@dataclass
class Timer:
    """Timer metric for measuring durations."""
    name: str
    durations: list[float] = field(default_factory=list)
    labels: dict[str, str] = field(default_factory=dict)

    def start(self) -> float:
        """Start timing. Returns start time."""
        return time.time()

    def stop(self, start_time: float) -> float:
        """Stop timing and record duration."""
        duration = (time.time() - start_time) * 1000  # Convert to ms
        self.durations.append(duration)
        return duration

    def time(self, func: Callable, *args: Any, **kwargs: Any) -> tuple[float, Any]:
        """Time a function execution."""
        start = time.time()
        result = func(*args, **kwargs)
        duration = (time.time() - start) * 1000
        self.durations.append(duration)
        return duration, result

    @contextmanager
    def measure(self):
        """Context manager for timing."""
        start = time.time()
        try:
            yield
        finally:
            self.durations.append((time.time() - start) * 1000)

    def count(self) -> int:
        """Number of timed events."""
        return len(self.durations)

    def mean(self) -> float:
        """Mean duration."""
        return sum(self.durations) / len(self.durations) if self.durations else 0.0

    def total(self) -> float:
        """Total duration."""
        return sum(self.durations)


from contextlib import contextmanager


class MetricsRegistry:
    """
    Central registry for all metrics.
    """

    def __init__(self, name: str = "default"):
        self.name = name
        self._counters: dict[str, Counter] = {}
        self._gauges: dict[str, Gauge] = {}
        self._histograms: dict[str, Histogram] = {}
        self._timers: dict[str, Timer] = {}
        self._lock = threading.RLock()

    def counter(self, name: str, labels: dict[str, str] | None = None) -> Counter:
        """Get or create a counter."""
        with self._lock:
            key = self._make_key(name, labels)
            if key not in self._counters:
                self._counters[key] = Counter(name=name, labels=labels or {})
            return self._counters[key]

    def gauge(self, name: str, labels: dict[str, str] | None = None) -> Gauge:
        """Get or create a gauge."""
        with self._lock:
            key = self._make_key(name, labels)
            if key not in self._gauges:
                self._gauges[key] = Gauge(name=name, labels=labels or {})
            return self._gauges[key]

    def histogram(self, name: str, labels: dict[str, str] | None = None) -> Histogram:
        """Get or create a histogram."""
        with self._lock:
            key = self._make_key(name, labels)
            if key not in self._histograms:
                self._histograms[key] = Histogram(name=name, labels=labels or {})
            return self._histograms[key]

    def timer(self, name: str, labels: dict[str, str] | None = None) -> Timer:
        """Get or create a timer."""
        with self._lock:
            key = self._make_key(name, labels)
            if key not in self._timers:
                self._timers[key] = Timer(name=name, labels=labels or {})
            return self._timers[key]

    def _make_key(self, name: str, labels: dict[str, str] | None) -> str:
        """Create a unique key for a metric."""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def get_all_metrics(self) -> dict[str, Any]:
        """Get all metrics as a dictionary."""
        with self._lock:
            return {
                "counters": {k: vars(v) for k, v in self._counters.items()},
                "gauges": {k: vars(v) for k, v in self._gauges.items()},
                "histograms": {k: {**vars(v), "percentiles": v.percentiles(50, 90, 95, 99)}
                               for k, v in self._histograms.items()},
                "timers": {k: {**vars(v), "mean_ms": v.mean(), "total_ms": v.total()}
                           for k, v in self._timers.items()},
            }

    def reset_all(self) -> None:
        """Reset all metrics."""
        with self._lock:
            for counter in self._counters.values():
                counter.reset()
            self._histograms.clear()
            self._timers.clear()

    def list_metric_names(self) -> list[str]:
        """List all metric names."""
        with self._lock:
            return (
                list(self._counters.keys()) +
                list(self._gauges.keys()) +
                list(self._histograms.keys()) +
                list(self._timers.keys())
            )


# Global default registry
_default_registry: MetricsRegistry | None = None


def get_registry(name: str = "default") -> MetricsRegistry:
    """Get or create a named registry."""
    global _default_registry
    if _default_registry is None:
        _default_registry = MetricsRegistry(name)
    return _default_registry
