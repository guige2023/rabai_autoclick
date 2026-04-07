"""Counter and tally utilities for RabAI AutoClick.

Provides:
- Counter management
- Tally tracking
- Statistics collection
"""

import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class CounterStats:
    """Counter statistics."""
    name: str
    value: int
    min_value: int
    max_value: int
    avg_value: float
    total_increments: int


class Counter:
    """Thread-safe counter."""

    def __init__(self, name: str, initial_value: int = 0) -> None:
        """Initialize counter.

        Args:
            name: Counter name.
            initial_value: Starting value.
        """
        self._name = name
        self._value = initial_value
        self._min_value = initial_value
        self._max_value = initial_value
        self._total_increments = 0
        self._lock = threading.Lock()

    @property
    def name(self) -> str:
        """Get counter name."""
        return self._name

    @property
    def value(self) -> int:
        """Get current value."""
        with self._lock:
            return self._value

    def increment(self, amount: int = 1) -> int:
        """Increment counter.

        Args:
            amount: Amount to increment.

        Returns:
            New value.
        """
        with self._lock:
            self._value += amount
            self._total_increments += 1
            if self._value > self._max_value:
                self._max_value = self._value
            return self._value

    def decrement(self, amount: int = 1) -> int:
        """Decrement counter.

        Args:
            amount: Amount to decrement.

        Returns:
            New value.
        """
        with self._lock:
            self._value -= amount
            if self._value < self._min_value:
                self._min_value = self._value
            return self._value

    def reset(self, value: int = 0) -> None:
        """Reset counter.

        Args:
            value: Value to reset to.
        """
        with self._lock:
            self._value = value
            self._min_value = value
            self._max_value = value
            self._total_increments = 0

    def get_stats(self) -> CounterStats:
        """Get counter statistics.

        Returns:
            Counter stats.
        """
        with self._lock:
            avg = self._value
            if self._total_increments > 0:
                avg = self._value / self._total_increments

            return CounterStats(
                name=self._name,
                value=self._value,
                min_value=self._min_value,
                max_value=self._max_value,
                avg_value=avg,
                total_increments=self._total_increments,
            )


class Tally:
    """Track tallies for different categories."""

    def __init__(self, name: str) -> None:
        """Initialize tally.

        Args:
            name: Tally name.
        """
        self._name = name
        self._tallies: Dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

    @property
    def name(self) -> str:
        """Get tally name."""
        return self._name

    def increment(self, category: str, amount: int = 1) -> int:
        """Increment category tally.

        Args:
            category: Category name.
            amount: Amount to add.

        Returns:
            New tally value.
        """
        with self._lock:
            self._tallies[category] += amount
            return self._tallies[category]

    def decrement(self, category: str, amount: int = 1) -> int:
        """Decrement category tally.

        Args:
            category: Category name.
            amount: Amount to subtract.

        Returns:
            New tally value.
        """
        with self._lock:
            self._tallies[category] -= amount
            return self._tallies[category]

    def get(self, category: str) -> int:
        """Get category tally.

        Args:
            category: Category name.

        Returns:
            Tally value.
        """
        with self._lock:
            return self._tallies.get(category, 0)

    def set(self, category: str, value: int) -> None:
        """Set category tally.

        Args:
            category: Category name.
            value: Value to set.
        """
        with self._lock:
            self._tallies[category] = value

    def reset(self, category: Optional[str] = None) -> None:
        """Reset tallies.

        Args:
            category: Category to reset (None = all).
        """
        with self._lock:
            if category:
                self._tallies[category] = 0
            else:
                self._tallies.clear()

    def total(self) -> int:
        """Get total of all tallies.

        Returns:
            Sum of all tallies.
        """
        with self._lock:
            return sum(self._tallies.values())

    def categories(self) -> List[str]:
        """Get all categories.

        Returns:
            List of category names.
        """
        with self._lock:
            return list(self._tallies.keys())

    def items(self) -> Dict[str, int]:
        """Get all tallies.

        Returns:
            Dict of category to value.
        """
        with self._lock:
            return dict(self._tallies)


class RateCounter:
    """Counter that tracks rate of events."""

    def __init__(self, name: str, window_size: float = 60.0) -> None:
        """Initialize rate counter.

        Args:
            name: Counter name.
            window_size: Time window in seconds.
        """
        self._name = name
        self._window_size = window_size
        self._events: List[tuple] = []
        self._lock = threading.Lock()

    @property
    def name(self) -> str:
        """Get counter name."""
        return self._name

    def record(self, count: int = 1) -> None:
        """Record events.

        Args:
            count: Number of events.
        """
        now = time.time()
        with self._lock:
            self._events.append((now, count))
            self._cleanup(now)

    def rate(self) -> float:
        """Get events per second in window.

        Returns:
            Events per second.
        """
        now = time.time()
        with self._lock:
            self._cleanup(now)
            if not self._events:
                return 0.0

            total = sum(count for _, count in self._events)
            duration = now - self._events[0][0] if self._events else 1
            return total / max(duration, 1)

    def total(self) -> int:
        """Get total events in window.

        Returns:
            Total events.
        """
        now = time.time()
        with self._lock:
            self._cleanup(now)
            return sum(count for _, count in self._events)

    def _cleanup(self, now: float) -> None:
        """Remove old events."""
        cutoff = now - self._window_size
        self._events = [(t, c) for t, c in self._events if t > cutoff]


class StatisticsCollector:
    """Collect various statistics."""

    def __init__(self, name: str) -> None:
        """Initialize collector.

        Args:
            name: Collector name.
        """
        self._name = name
        self._counters: Dict[str, Counter] = {}
        self._tallies: Dict[str, Tally] = {}
        self._rate_counters: Dict[str, RateCounter] = {}
        self._lock = threading.Lock()

    def counter(self, name: str, initial: int = 0) -> Counter:
        """Get or create counter.

        Args:
            name: Counter name.
            initial: Initial value.

        Returns:
            Counter instance.
        """
        with self._lock:
            if name not in self._counters:
                self._counters[name] = Counter(name, initial)
            return self._counters[name]

    def tally(self, name: str) -> Tally:
        """Get or create tally.

        Args:
            name: Tally name.

        Returns:
            Tally instance.
        """
        with self._lock:
            if name not in self._tallies:
                self._tallies[name] = Tally(name)
            return self._tallies[name]

    def rate_counter(self, name: str, window: float = 60.0) -> RateCounter:
        """Get or create rate counter.

        Args:
            name: Counter name.
            window: Time window.

        Returns:
            RateCounter instance.
        """
        with self._lock:
            if name not in self._rate_counters:
                self._rate_counters[name] = RateCounter(name, window)
            return self._rate_counters[name]

    def get_stats(self) -> Dict[str, Any]:
        """Get all statistics.

        Returns:
            Dict of statistics.
        """
        with self._lock:
            stats = {
                "counters": {
                    name: c.get_stats().__dict__
                    for name, c in self._counters.items()
                },
                "tallies": {
                    name: t.items()
                    for name, t in self._tallies.items()
                },
                "rates": {
                    name: {"rate": r.rate(), "total": r.total()}
                    for name, r in self._rate_counters.items()
                },
            }
            return stats


# Global statistics collector
_global_collector = StatisticsCollector("global")


def get_collector() -> StatisticsCollector:
    """Get global statistics collector.

    Returns:
        Global collector.
    """
    return _global_collector


def counter(name: str, initial: int = 0) -> Counter:
    """Get global counter.

    Args:
        name: Counter name.
        initial: Initial value.

    Returns:
        Counter instance.
    """
    return _global_collector.counter(name, initial)


def tally(name: str) -> Tally:
    """Get global tally.

    Args:
        name: Tally name.

    Returns:
        Tally instance.
    """
    return _global_collector.tally(name)


def rate_counter(name: str, window: float = 60.0) -> RateCounter:
    """Get global rate counter.

    Args:
        name: Counter name.
        window: Time window.

    Returns:
        RateCounter instance.
    """
    return _global_collector.rate_counter(name, window)
