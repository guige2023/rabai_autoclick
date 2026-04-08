"""
Counter metric action for tracking monotonically increasing values.

This module provides actions for recording and analyzing counter metrics,
supporting rate calculations, increment/decrement operations, and reset handling.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union


@dataclass
class CounterSnapshot:
    """A point-in-time snapshot of a counter."""
    name: str
    value: int
    increment_total: int
    decrement_total: int
    reset_count: int
    timestamp: datetime
    rate_1m: float = 0.0
    rate_5m: float = 0.0
    rate_15m: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert snapshot to dictionary."""
        return {
            "name": self.name,
            "value": self.value,
            "increment_total": self.increment_total,
            "decrement_total": self.decrement_total,
            "reset_count": self.reset_count,
            "timestamp": self.timestamp.isoformat(),
            "rate_1m": self.rate_1m,
            "rate_5m": self.rate_5m,
            "rate_15m": self.rate_15m,
        }


@dataclass
class CounterConfig:
    """Configuration for counter metrics."""
    initial_value: int = 0
    min_value: Optional[int] = None
    max_value: Optional[int] = None
    wrap: bool = False
    rate_windows: Tuple[int, int, int] = (60, 300, 900)
    history_size: int = 1000
    allow_negative: bool = False


class Counter:
    """
    Counter metric for tracking monotonically increasing values.

    Supports increments, decrements, rate calculations, and history tracking.
    """

    def __init__(
        self,
        name: str,
        config: Optional[CounterConfig] = None,
    ):
        """
        Initialize a counter.

        Args:
            name: Name of the counter metric.
            config: Optional counter configuration.
        """
        self.name = name
        self.config = config or CounterConfig()
        self._value = self.config.initial_value
        self._increment_total = 0
        self._decrement_total = 0
        self._reset_count = 0
        self._last_value = self.config.initial_value
        self._history: deque = deque(maxlen=self.config.history_size)
        self._rate_history: deque = deque(maxlen=self.config.history_size)
        self._lock = threading.RLock()

    def increment(self, amount: int = 1) -> Tuple[int, int]:
        """
        Increment the counter by a given amount.

        Args:
            amount: Amount to increment by.

        Returns:
            Tuple of (new_value, delta).
        """
        with self._lock:
            if amount < 0:
                return self.decrement(-amount)

            old_value = self._value
            self._value = self._apply_constraints(self._value + amount)
            self._increment_total += amount
            delta = self._value - old_value

            self._history.append({
                "type": "increment",
                "amount": amount,
                "value": self._value,
                "timestamp": datetime.now(),
            })

            self._update_rate(delta)

            return self._value, delta

    def decrement(self, amount: int = 1) -> Tuple[int, int]:
        """
        Decrement the counter by a given amount.

        Args:
            amount: Amount to decrement by.

        Returns:
            Tuple of (new_value, delta).
        """
        with self._lock:
            if amount < 0:
                return self.increment(-amount)

            if not self.config.allow_negative and self._value < amount:
                self._value = 0
                delta = -self._decrement_total
                self._decrement_total += self._value - old_value if 'old_value' in dir() else 0
                return self._value, delta

            old_value = self._value
            self._value = self._apply_constraints(self._value - amount)
            self._decrement_total += amount
            delta = self._value - old_value

            self._history.append({
                "type": "decrement",
                "amount": amount,
                "value": self._value,
                "timestamp": datetime.now(),
            })

            self._update_rate(delta)

            return self._value, delta

    def get(self) -> int:
        """Get the current value of the counter."""
        with self._lock:
            return self._value

    def get_snapshot(self) -> CounterSnapshot:
        """
        Get a point-in-time snapshot of the counter.

        Returns:
            CounterSnapshot with current statistics.
        """
        with self._lock:
            return CounterSnapshot(
                name=self.name,
                value=self._value,
                increment_total=self._increment_total,
                decrement_total=self._decrement_total,
                reset_count=self._reset_count,
                timestamp=datetime.now(),
                rate_1m=self._calculate_rate(60),
                rate_5m=self._calculate_rate(300),
                rate_15m=self._calculate_rate(900),
            )

    def reset(self) -> int:
        """
        Reset the counter to its initial value.

        Returns:
            The value before reset.
        """
        with self._lock:
            old_value = self._value
            self._value = self.config.initial_value
            self._reset_count += 1

            self._history.append({
                "type": "reset",
                "previous_value": old_value,
                "timestamp": datetime.now(),
            })

            return old_value

    def get_rate(self, window_seconds: float = 60.0) -> float:
        """
        Calculate the rate of change over a time window.

        Args:
            window_seconds: Time window in seconds.

        Returns:
            Rate of change (units per second).
        """
        with self._lock:
            return self._calculate_rate(window_seconds)

    def _calculate_rate(self, window_seconds: float) -> float:
        """Calculate rate from rate history."""
        if not self._rate_history:
            return 0.0

        cutoff = time.time() - window_seconds
        recent = [
            entry for entry in self._rate_history
            if entry["timestamp"] >= cutoff
        ]

        if not recent:
            return 0.0

        total_delta = sum(entry["delta"] for entry in recent)
        time_span = recent[-1]["timestamp"] - recent[0]["timestamp"]

        if time_span == 0:
            return 0.0

        return total_delta / time_span

    def _update_rate(self, delta: int) -> None:
        """Update rate tracking history."""
        self._rate_history.append({
            "delta": delta,
            "timestamp": time.time(),
        })

    def _apply_constraints(self, value: int) -> int:
        """Apply value constraints (min, max, wrap)."""
        if self.config.wrap and self.config.min_value is not None and self.config.max_value is not None:
            range_size = self.config.max_value - self.config.min_value + 1
            if range_size > 0:
                value = self.config.min_value + ((value - self.config.min_value) % range_size)
        else:
            if self.config.max_value is not None:
                value = min(value, self.config.max_value)
            if self.config.min_value is not None:
                if not self.config.allow_negative and value < self.config.min_value:
                    value = self.config.min_value

        return value

    def get_history(
        self,
        limit: Optional[int] = None,
        since: Optional[datetime] = None,
        event_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get the history of counter changes.

        Args:
            limit: Maximum number of entries to return.
            since: Only return entries since this time.
            event_type: Filter by event type (increment, decrement, reset).

        Returns:
            List of history entries.
        """
        with self._lock:
            history = list(self._history)

            if since:
                history = [
                    h for h in history
                    if h["timestamp"] >= since
                ]

            if event_type:
                history = [
                    h for h in history
                    if h.get("type") == event_type
                ]

            if limit:
                history = history[-limit:]

            return history


class CounterRegistry:
    """Thread-safe registry of counter metrics."""

    def __init__(self):
        """Initialize the counter registry."""
        self._counters: Dict[str, Counter] = {}
        self._lock = threading.RLock()

    def get_or_create(
        self,
        name: str,
        config: Optional[CounterConfig] = None,
    ) -> Counter:
        """Get an existing counter or create a new one."""
        with self._lock:
            if name not in self._counters:
                self._counters[name] = Counter(name, config)
            return self._counters[name]

    def get(self, name: str) -> Optional[Counter]:
        """Get a counter by name."""
        with self._lock:
            return self._counters.get(name)

    def list_counters(self) -> List[str]:
        """List all counter names."""
        with self._lock:
            return list(self._counters.keys())

    def increment(self, name: str, amount: int = 1) -> Tuple[int, int]:
        """Increment a counter by name."""
        counter = self.get_or_create(name)
        return counter.increment(amount)

    def decrement(self, name: str, amount: int = 1) -> Tuple[int, int]:
        """Decrement a counter by name."""
        counter = self.get_or_create(name)
        return counter.decrement(amount)

    def reset_all(self) -> None:
        """Reset all counters in the registry."""
        with self._lock:
            for c in self._counters.values():
                c.reset()

    def get_snapshots(self) -> List[CounterSnapshot]:
        """Get snapshots of all counters."""
        with self._lock:
            return [c.get_snapshot() for c in self._counters.values()]


_default_registry = CounterRegistry()


def counter_action(
    name: str,
    operation: str = "increment",
    amount: int = 1,
    initial_value: int = 0,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Action function for counter metric operations.

    Args:
        name: Name of the counter.
        operation: Operation to perform (increment, decrement, get, reset).
        amount: Amount for increment/decrement.
        initial_value: Initial value for new counters.
        min_value: Minimum value constraint.
        max_value: Maximum value constraint.

    Returns:
        Dictionary with counter result.
    """
    config = CounterConfig(
        initial_value=initial_value,
        min_value=min_value,
        max_value=max_value,
    )

    registry = CounterRegistry()
    counter = registry.get_or_create(name, config)

    if operation == "increment":
        new_value, delta = counter.increment(amount)
        return counter.get_snapshot().to_dict()

    elif operation == "decrement":
        new_value, delta = counter.decrement(amount)
        return counter.get_snapshot().to_dict()

    elif operation == "get":
        return counter.get_snapshot().to_dict()

    elif operation == "reset":
        old_value = counter.reset()
        return {
            "name": name,
            "previous_value": old_value,
            "reset_count": counter._reset_count,
            "timestamp": datetime.now().isoformat(),
        }

    else:
        raise ValueError(f"Unknown operation: {operation}")


def get_counter_value(name: str) -> Optional[int]:
    """Get the current value of a counter."""
    registry = CounterRegistry()
    counter = registry.get(name)
    if counter:
        return counter.get()
    return None


def increment_counter(name: str, amount: int = 1) -> Dict[str, Any]:
    """Increment a counter."""
    registry = CounterRegistry()
    counter = registry.get_or_create(name)
    counter.increment(amount)
    return counter.get_snapshot().to_dict()
