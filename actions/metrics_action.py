"""metrics_action module for rabai_autoclick.

Provides metrics collection and aggregation: counters, gauges,
histograms, timers, meters, and metric reporting.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional

__all__ = [
    "MetricType",
    "Counter",
    "Gauge",
    "Histogram",
    "Timer",
    "Meter",
    "MetricsRegistry",
    "Snapshot",
    "MetricsSnapshot",
    "report_metrics",
    "get_metrics",
]


class MetricType(Enum):
    """Types of metrics."""
    COUNTER = auto()
    GAUGE = auto()
    HISTOGRAM = auto()
    TIMER = auto()
    METER = auto()


@dataclass
class Snapshot:
    """Point-in-time snapshot of a metric."""
    timestamp: float
    value: float
    count: int = 1


@dataclass
class MetricsSnapshot:
    """Complete snapshot of all metrics."""
    timestamp: float
    metrics: Dict[str, Dict[str, Any]]


class Counter:
    """Monotonically increasing counter."""

    def __init__(self, name: str, tags: Optional[Dict[str, str]] = None) -> None:
        self.name = name
        self.tags = tags or {}
        self._value: int = 0
        self._lock = threading.Lock()

    def inc(self, delta: int = 1) -> None:
        """Increment counter by delta."""
        with self._lock:
            self._value += delta

    def dec(self, delta: int = 1) -> None:
        """Decrement counter by delta."""
        with self._lock:
            self._value -= delta

    def get(self) -> int:
        """Get current value."""
        with self._lock:
            return self._value

    def reset(self) -> None:
        """Reset counter to zero."""
        with self._lock:
            self._value = 0

    def snapshot(self) -> Snapshot:
        """Get point-in-time snapshot."""
        with self._lock:
            return Snapshot(timestamp=time.time(), value=float(self._value), count=1)


class Gauge:
    """Point-in-time value gauge."""

    def __init__(self, name: str, tags: Optional[Dict[str, str]] = None) -> None:
        self.name = name
        self.tags = tags or {}
        self._value: float = 0.0
        self._lock = threading.Lock()

    def set(self, value: float) -> None:
        """Set gauge to value."""
        with self._lock:
            self._value = float(value)

    def get(self) -> float:
        """Get current value."""
        with self._lock:
            return self._value

    def inc(self, delta: float = 1.0) -> None:
        """Increment by delta."""
        with self._lock:
            self._value += delta

    def dec(self, delta: float = 1.0) -> None:
        """Decrement by delta."""
        with self._lock:
            self._value -= delta

    def snapshot(self) -> Snapshot:
        """Get point-in-time snapshot."""
        with self._lock:
            return Snapshot(timestamp=time.time(), value=self._value, count=1)


class Histogram:
    """Statistical distribution of values."""

    def __init__(
        self,
        name: str,
        tags: Optional[Dict[str, str]] = None,
        reservoir_size: int = 1028,
    ) -> None:
        self.name = name
        self.tags = tags or {}
        self.reservoir_size = reservoir_size
        self._values: deque = deque(maxlen=reservoir_size)
        self._count: int = 0
        self._sum: float = 0.0
        self._min: float = float("inf")
        self._max: float = float("-inf")
        self._lock = threading.Lock()

    def update(self, value: float) -> None:
        """Record a value."""
        with self._lock:
            self._values.append(value)
            self._count += 1
            self._sum += value
            self._min = min(self._min, value)
            self._max = max(self._max, value)

    def get_stats(self) -> Dict[str, float]:
        """Get histogram statistics."""
        with self._lock:
            if self._count == 0:
                return {"count": 0, "sum": 0, "mean": 0, "min": 0, "max": 0, "p50": 0, "p95": 0, "p99": 0}
            sorted_vals = sorted(self._values)
            n = len(sorted_vals)
            return {
                "count": self._count,
                "sum": self._sum,
                "mean": self._sum / self._count,
                "min": self._min,
                "max": self._max,
                "p50": sorted_vals[n // 2],
                "p95": sorted_vals[int(n * 0.95)],
                "p99": sorted_vals[int(n * 0.99)],
            }

    def snapshot(self) -> Snapshot:
        """Get point-in-time snapshot."""
        stats = self.get_stats()
        return Snapshot(timestamp=time.time(), value=stats["mean"], count=self._count)


class Timer:
    """Stopwatch-style timer for measuring durations."""

    def __init__(self, name: str, tags: Optional[Dict[str, str]] = None) -> None:
        self.name = name
        self.tags = tags or {}
        self._histogram = Histogram(name, tags)
        self._start_time: Optional[float] = None
        self._lock = threading.Lock()

    def start(self) -> "Timer":
        """Start the timer."""
        self._start_time = time.perf_counter()
        return self

    def stop(self) -> float:
        """Stop timer and record duration.

        Returns:
            Duration in seconds.
        """
        with self._lock:
            if self._start_time is None:
                return 0.0
            duration = time.perf_counter() - self._start_time
            self._histogram.update(duration)
            self._start_time = None
            return duration

    def __enter__(self) -> "Timer":
        self.start()
        return self

    def __exit__(self, *args: Any) -> None:
        self.stop()

    def record(self, duration: float) -> None:
        """Record a duration directly."""
        self._histogram.update(duration)

    def get_stats(self) -> Dict[str, float]:
        """Get timer statistics (same as histogram)."""
        return self._histogram.get_stats()

    def snapshot(self) -> Snapshot:
        """Get point-in-time snapshot."""
        return self._histogram.snapshot()


class Meter:
    """Rate meter tracking throughput."""

    def __init__(
        self,
        name: str,
        tags: Optional[Dict[str, str]] = None,
        window_seconds: float = 60.0,
    ) -> None:
        self.name = name
        self.tags = tags or {}
        self.window_seconds = window_seconds
        self._timestamps: deque = deque()
        self._count: int = 0
        self._lock = threading.Lock()

    def mark(self, n: int = 1) -> None:
        """Mark n events."""
        with self._lock:
            now = time.time()
            for _ in range(n):
                self._timestamps.append(now)
            self._count += n
            self._cleanup(now)

    def _cleanup(self, now: float) -> None:
        """Remove old timestamps outside the window."""
        cutoff = now - self.window_seconds
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()

    def get_count(self) -> int:
        """Get total count."""
        with self._lock:
            return self._count

    def get_rate(self) -> float:
        """Get events per second in window."""
        with self._lock:
            self._cleanup(time.time())
            if not self._timestamps:
                return 0.0
            elapsed = time.time() - self._timestamps[0]
            if elapsed <= 0:
                return 0.0
            return len(self._timestamps) / elapsed

    def snapshot(self) -> Snapshot:
        """Get point-in-time snapshot."""
        rate = self.get_rate()
        return Snapshot(timestamp=time.time(), value=rate, count=self._count)


class MetricsRegistry:
    """Central registry for all metrics."""

    def __init__(self) -> None:
        self._counters: Dict[str, Counter] = {}
        self._gauges: Dict[str, Gauge] = {}
        self._histograms: Dict[str, Histogram] = {}
        self._timers: Dict[str, Timer] = {}
        self._meters: Dict[str, Meter] = {}
        self._lock = threading.Lock()

    def counter(self, name: str, tags: Optional[Dict[str, str]] = None) -> Counter:
        """Get or create a counter."""
        with self._lock:
            key = self._make_key(name, tags)
            if key not in self._counters:
                self._counters[key] = Counter(name, tags)
            return self._counters[key]

    def gauge(self, name: str, tags: Optional[Dict[str, str]] = None) -> Gauge:
        """Get or create a gauge."""
        with self._lock:
            key = self._make_key(name, tags)
            if key not in self._gauges:
                self._gauges[key] = Gauge(name, tags)
            return self._gauges[key]

    def histogram(self, name: str, tags: Optional[Dict[str, str]] = None) -> Histogram:
        """Get or create a histogram."""
        with self._lock:
            key = self._make_key(name, tags)
            if key not in self._histograms:
                self._histograms[key] = Histogram(name, tags)
            return self._histograms[key]

    def timer(self, name: str, tags: Optional[Dict[str, str]] = None) -> Timer:
        """Get or create a timer."""
        with self._lock:
            key = self._make_key(name, tags)
            if key not in self._timers:
                self._timers[key] = Timer(name, tags)
            return self._timers[key]

    def meter(self, name: str, tags: Optional[Dict[str, str]] = None) -> Meter:
        """Get or create a meter."""
        with self._lock:
            key = self._make_key(name, tags)
            if key not in self._meters:
                self._meters[key] = Meter(name, tags)
            return self._meters[key]

    def _make_key(self, name: str, tags: Optional[Dict[str, str]]) -> str:
        """Create unique key for metric."""
        if not tags:
            return name
        tag_str = ",".join(f"{k}={v}" for k, v in sorted(tags.items()))
        return f"{name}{{{tag_str}}}"

    def snapshot(self) -> MetricsSnapshot:
        """Get snapshot of all metrics."""
        with self._lock:
            metrics: Dict[str, Dict[str, Any]] = {}
            for key, c in self._counters.items():
                s = c.snapshot()
                metrics[key] = {"type": "counter", "value": s.value, "count": c.get()}
            for key, g in self._gauges.items():
                s = g.snapshot()
                metrics[key] = {"type": "gauge", "value": s.value}
            for key, h in self._histograms.items():
                stats = h.get_stats()
                metrics[key] = {"type": "histogram", **stats}
            for key, t in self._timers.items():
                stats = t.get_stats()
                metrics[key] = {"type": "timer", **stats}
            for key, m in self._meters.items():
                s = m.snapshot()
                metrics[key] = {"type": "meter", "value": s.value, "count": m.get_count()}
            return MetricsSnapshot(timestamp=time.time(), metrics=metrics)


_global_registry: Optional[MetricsRegistry] = None


def get_registry() -> MetricsRegistry:
    """Get global metrics registry."""
    global _global_registry
    if _global_registry is None:
        _global_registry = MetricsRegistry()
    return _global_registry


def report_metrics() -> MetricsSnapshot:
    """Get snapshot of all global metrics."""
    return get_registry().snapshot()


def get_metrics() -> Dict[str, Any]:
    """Get metrics as dictionary."""
    snapshot = report_metrics()
    return snapshot.metrics
