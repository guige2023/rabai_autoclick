"""Counter and metric utilities for RabAI AutoClick.

Provides:
- Thread-safe counters
- Rate counters
- Metric collectors
- Percentile tracking
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
)


@dataclass
class Counter:
    """Thread-safe counter with increment and get operations."""

    value: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    def increment(self, amount: int = 1) -> int:
        """Increment counter and return new value."""
        with self._lock:
            self.value += amount
            return self.value

    def decrement(self, amount: int = 1) -> int:
        """Decrement counter and return new value."""
        with self._lock:
            self.value -= amount
            return self.value

    def get(self) -> int:
        """Get current value."""
        with self._lock:
            return self.value

    def reset(self) -> None:
        """Reset counter to zero."""
        with self._lock:
            self.value = 0


class MultiCounter:
    """Multiple named counters.

    Example:
        counters = MultiCounter()
        counters.inc("requests")
        counters.inc("requests")
        counters.inc("errors")
        counters.get("requests")  # 2
        counters.get("errors")     # 1
    """

    def __init__(self) -> None:
        self._counters: Dict[str, Counter] = defaultdict(Counter)
        self._lock = threading.Lock()

    def inc(self, name: str, amount: int = 1) -> int:
        return self._counters[name].increment(amount)

    def dec(self, name: str, amount: int = 1) -> int:
        return self._counters[name].decrement(amount)

    def get(self, name: str) -> int:
        return self._counters[name].get()

    def reset(self, name: Optional[str] = None) -> None:
        if name:
            self._counters[name].reset()
        else:
            for counter in self._counters.values():
                counter.reset()

    def total(self) -> int:
        return sum(c.get() for c in self._counters.values())

    def keys(self) -> List[str]:
        return list(self._counters.keys())


class RateCounter:
    """Counter that tracks rate of events.

    Example:
        rate = RateCounter(window_seconds=60)
        rate.inc()
        rate.inc()
        rate.rate()  # Events per second
    """

    def __init__(self, window_seconds: float = 60.0) -> None:
        self._window_seconds = window_seconds
        self._events: deque = deque()
        self._lock = threading.Lock()

    def inc(self, amount: int = 1) -> None:
        now = time.time()
        with self._lock:
            for _ in range(amount):
                self._events.append(now)
            self._cleanup(now)

    def rate(self) -> float:
        """Get events per second."""
        now = time.time()
        with self._lock:
            self._cleanup(now)
            if not self._events:
                return 0.0
            duration = now - self._events[0] if self._events else 0
            if duration == 0:
                return 0.0
            return len(self._events) / duration

    def count(self) -> int:
        """Get count of events in window."""
        now = time.time()
        with self._lock:
            self._cleanup(now)
            return len(self._events)

    def _cleanup(self, now: float) -> None:
        cutoff = now - self._window_seconds
        while self._events and self._events[0] < cutoff:
            self._events.popleft()

    def reset(self) -> None:
        with self._lock:
            self._events.clear()


@dataclass
class PercentileTracker:
    """Tracks percentiles over a sliding window.

    Example:
        tracker = PercentileTracker(window_size=1000)
        for value in data:
            tracker.add(value)
        tracker.percentile(50)   # Median
        tracker.percentile(95)   # 95th percentile
        tracker.percentile(99)   # 99th percentile
    """

    window_size: int
    _values: deque = field(default_factory=lambda: deque(maxlen=10000))
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    def add(self, value: float) -> None:
        with self._lock:
            self._values.append(value)
            if len(self._values) > self.window_size:
                trim = len(self._values) - self.window_size
                for _ in range(trim):
                    self._values.popleft()

    def percentile(self, p: float) -> Optional[float]:
        """Get percentile value.

        Args:
            p: Percentile (0-100).

        Returns:
            Value at percentile or None if no data.
        """
        with self._lock:
            if not self._values:
                return None
            sorted_values = sorted(self._values)
            index = int(len(sorted_values) * p / 100)
            index = min(index, len(sorted_values) - 1)
            return sorted_values[index]

    def mean(self) -> Optional[float]:
        with self._lock:
            if not self._values:
                return None
            return sum(self._values) / len(self._values)

    def median(self) -> Optional[float]:
        return self.percentile(50)

    def reset(self) -> None:
        with self._lock:
            self._values.clear()


class MetricsCollector:
    """Collects and aggregates metrics.

    Example:
        metrics = MetricsCollector()
        metrics.increment("requests", tags={"method": "GET", "status": "200"})
        metrics.gauge("memory_usage", 1024)
        metrics.histogram("request_duration", 0.15)
    """

    def __init__(self) -> None:
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, PercentileTracker] = {}
        self._lock = threading.Lock()

    def increment(self, name: str, value: float = 1.0, tags: Optional[dict] = None) -> None:
        key = self._make_key(name, tags)
        with self._lock:
            self._counters[key] += value

    def decrement(self, name: str, value: float = 1.0, tags: Optional[dict] = None) -> None:
        key = self._make_key(name, tags)
        with self._lock:
            self._counters[key] -= value

    def gauge(self, name: str, value: float, tags: Optional[dict] = None) -> None:
        key = self._make_key(name, tags)
        with self._lock:
            self._gauges[key] = value

    def histogram(
        self,
        name: str,
        value: float,
        tags: Optional[dict] = None,
        window_size: int = 1000,
    ) -> None:
        key = self._make_key(name, tags)
        with self._lock:
            if key not in self._histograms:
                self._histograms[key] = PercentileTracker(window_size=window_size)
            self._histograms[key].add(value)

    def get_counter(self, name: str, tags: Optional[dict] = None) -> float:
        key = self._make_key(name, tags)
        with self._lock:
            return self._counters.get(key, 0.0)

    def get_gauge(self, name: str, tags: Optional[dict] = None) -> Optional[float]:
        key = self._make_key(name, tags)
        with self._lock:
            return self._gauges.get(key)

    def get_histogram_stats(self, name: str, tags: Optional[dict] = None) -> Optional[dict]:
        key = self._make_key(name, tags)
        with self._lock:
            if key not in self._histograms:
                return None
            tracker = self._histograms[key]
            return {
                "count": len(tracker._values),
                "mean": tracker.mean(),
                "median": tracker.median(),
                "p95": tracker.percentile(95),
                "p99": tracker.percentile(99),
            }

    def _make_key(self, name: str, tags: Optional[dict]) -> str:
        if not tags:
            return name
        tag_str = ",".join(f"{k}={v}" for k, v in sorted(tags.items()))
        return f"{name}[{tag_str}]"

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._histograms.clear()

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "histograms": {
                    k: {
                        "count": len(v._values),
                        "mean": v.mean(),
                        "median": v.median(),
                        "p95": v.percentile(95),
                    }
                    for k, v in self._histograms.items()
                },
            }
