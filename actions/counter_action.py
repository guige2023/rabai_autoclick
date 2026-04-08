"""Counter action module for RabAI AutoClick.

Provides counter and metrics utilities:
- Counter: Thread-safe counter
- RateCounter: Track rates
- MetricsCounter: Aggregate metrics
"""

from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
import threading
import time
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class CounterSnapshot:
    """Counter snapshot."""
    name: str
    value: int
    timestamp: float
    delta: int = 0


class Counter:
    """Thread-safe counter."""

    def __init__(self, name: str, initial_value: int = 0):
        self.name = name
        self._value = initial_value
        self._lock = threading.RLock()
        self._history: List[CounterSnapshot] = []

    def increment(self, amount: int = 1) -> int:
        """Increment counter."""
        with self._lock:
            old_value = self._value
            self._value += amount
            self._record_snapshot(old_value)
            return self._value

    def decrement(self, amount: int = 1) -> int:
        """Decrement counter."""
        with self._lock:
            old_value = self._value
            self._value -= amount
            self._record_snapshot(old_value)
            return self._value

    def get(self) -> int:
        """Get current value."""
        with self._lock:
            return self._value

    def reset(self) -> int:
        """Reset counter."""
        with self._lock:
            old_value = self._value
            self._value = 0
            self._record_snapshot(old_value)
            return old_value

    def set(self, value: int) -> int:
        """Set counter value."""
        with self._lock:
            old_value = self._value
            self._value = value
            self._record_snapshot(old_value)
            return self._value

    def _record_snapshot(self, old_value: int) -> None:
        """Record a snapshot."""
        self._history.append(CounterSnapshot(
            name=self.name,
            value=self._value,
            timestamp=time.time(),
            delta=self._value - old_value,
        ))

    def get_history(self, limit: int = 100) -> List[Dict]:
        """Get history."""
        with self._lock:
            snapshots = self._history[-limit:]
            return [
                {"value": s.value, "delta": s.delta, "timestamp": s.timestamp}
                for s in snapshots
            ]


class RateCounter:
    """Rate tracking counter."""

    def __init__(self, name: str, window_size: float = 60.0):
        self.name = name
        self.window_size = window_size
        self._events: List[float] = []
        self._lock = threading.RLock()

    def increment(self, amount: int = 1) -> None:
        """Record events."""
        with self._lock:
            now = time.time()
            self._events.append(now)
            self._cleanup(now)

    def get_rate(self) -> float:
        """Get events per second."""
        with self._lock:
            self._cleanup(time.time())
            if not self._events:
                return 0.0
            return len(self._events) / self.window_size

    def get_count(self) -> int:
        """Get total events in window."""
        with self._lock:
            self._cleanup(time.time())
            return len(self._events)

    def _cleanup(self, now: float) -> None:
        """Clean up old events."""
        cutoff = now - self.window_size
        self._events = [e for e in self._events if e > cutoff]


class MetricsCounter:
    """Aggregated metrics counter."""

    def __init__(self, name: str):
        self.name = name
        self._counters: Dict[str, Counter] = {}
        self._rate_counters: Dict[str, RateCounter] = {}
        self._lock = threading.RLock()

    def counter(self, name: str, initial: int = 0) -> Counter:
        """Get or create a counter."""
        with self._lock:
            if name not in self._counters:
                self._counters[name] = Counter(name, initial)
            return self._counters[name]

    def rate(self, name: str, window: float = 60.0) -> RateCounter:
        """Get or create a rate counter."""
        with self._lock:
            if name not in self._rate_counters:
                self._rate_counters[name] = RateCounter(name, window)
            return self._rate_counters[name]

    def increment(self, counter_name: str, amount: int = 1) -> int:
        """Increment a counter."""
        return self.counter(counter_name).increment(amount)

    def get(self, counter_name: str) -> int:
        """Get counter value."""
        return self.counter(counter_name).get()

    def get_all_counters(self) -> Dict[str, int]:
        """Get all counter values."""
        with self._lock:
            return {name: c.get() for name, c in self._counters.items()}

    def get_all_rates(self) -> Dict[str, float]:
        """Get all rate values."""
        with self._lock:
            return {name: r.get_rate() for name, r in self._rate_counters.items()}

    def reset(self) -> None:
        """Reset all counters."""
        with self._lock:
            for c in self._counters.values():
                c.reset()


class CounterAction(BaseAction):
    """Counter action."""
    action_type = "counter"
    display_name = "计数器"
    description = "线程安全计数器"

    def __init__(self):
        super().__init__()
        self._metrics = MetricsCounter("global")

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "increment")

            if operation == "increment":
                return self._increment(params)
            elif operation == "decrement":
                return self._decrement(params)
            elif operation == "get":
                return self._get(params)
            elif operation == "reset":
                return self._reset(params)
            elif operation == "rate":
                return self._rate(params)
            elif operation == "list":
                return self._list()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Counter error: {str(e)}")

    def _increment(self, params: Dict[str, Any]) -> ActionResult:
        """Increment counter."""
        name = params.get("name", "default")
        amount = params.get("amount", 1)

        value = self._metrics.increment(name, amount)

        return ActionResult(success=True, message=f"Incremented: {name}={value}", data={"name": name, "value": value})

    def _decrement(self, params: Dict[str, Any]) -> ActionResult:
        """Decrement counter."""
        name = params.get("name", "default")
        amount = params.get("amount", 1)

        counter = self._metrics.counter(name)
        value = counter.decrement(amount)

        return ActionResult(success=True, message=f"Decremented: {name}={value}", data={"name": name, "value": value})

    def _get(self, params: Dict[str, Any]) -> ActionResult:
        """Get counter value."""
        name = params.get("name", "default")

        value = self._metrics.get(name)

        return ActionResult(success=True, message=f"{name}={value}", data={"name": name, "value": value})

    def _reset(self, params: Dict[str, Any]) -> ActionResult:
        """Reset counter."""
        name = params.get("name", "default")

        counter = self._metrics.counter(name)
        old_value = counter.reset()

        return ActionResult(success=True, message=f"Reset: {name}={old_value}", data={"name": name, "old_value": old_value})

    def _rate(self, params: Dict[str, Any]) -> ActionResult:
        """Get rate."""
        name = params.get("name", "default")
        window = params.get("window", 60.0)

        rate_counter = self._metrics.rate(name, window)
        rate_counter.increment()
        rate = rate_counter.get_rate()
        count = rate_counter.get_count()

        return ActionResult(success=True, message=f"{name}: {rate:.2f}/s ({count} events)", data={"name": name, "rate": rate, "count": count})

    def _list(self) -> ActionResult:
        """List all counters."""
        counters = self._metrics.get_all_counters()
        rates = self._metrics.get_all_rates()

        return ActionResult(
            success=True,
            message=f"{len(counters)} counters, {len(rates)} rates",
            data={"counters": counters, "rates": rates},
        )
