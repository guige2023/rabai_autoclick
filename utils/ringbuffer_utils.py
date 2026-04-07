"""
Ring Buffer Utilities

Provides efficient fixed-size circular buffer implementations
for streaming data and last-N tracking.
"""

from __future__ import annotations

import copy
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class RingBuffer(Generic[T]):
    """
    Thread-safe ring buffer with fixed capacity.

    Once full, new items overwrite the oldest items.
    """

    def __init__(self, capacity: int):
        if capacity <= 0:
            raise ValueError("Capacity must be positive")

        self._capacity = capacity
        self._buffer: list[T | None] = [None] * capacity
        self._head = 0  # Next write position
        self._tail = 0  # Oldest valid item position
        self._size = 0
        self._lock = threading.RLock()

    def append(self, item: T) -> None:
        """Add an item to the buffer."""
        with self._lock:
            self._buffer[self._head] = item
            self._head = (self._head + 1) % self._capacity

            if self._size < self._capacity:
                self._size += 1
            else:
                # Buffer is full, move tail forward
                self._tail = (self._tail + 1) % self._capacity

    def get(self, index: int) -> T | None:
        """Get item at index (0 = oldest, -1 = newest)."""
        with self._lock:
            if index < 0:
                index = self._size + index

            if index < 0 or index >= self._size:
                return None

            actual_index = (self._tail + index) % self._capacity
            return self._buffer[actual_index]

    def __getitem__(self, index: int) -> T | None:
        """Get item at index."""
        return self.get(index)

    def __iter__(self) -> Any:
        """Iterate over items in order (oldest to newest)."""
        with self._lock:
            result = []
            for i in range(self._size):
                idx = (self._tail + i) % self._capacity
                result.append(self._buffer[idx])
            return iter(result)

    def to_list(self) -> list[T]:
        """Convert buffer to list (oldest to newest)."""
        return list(self)

    @property
    def size(self) -> int:
        """Current number of items in buffer."""
        with self._lock:
            return self._size

    @property
    def capacity(self) -> int:
        """Maximum capacity of buffer."""
        return self._capacity

    @property
    def is_full(self) -> bool:
        """Check if buffer is full."""
        with self._lock:
            return self._size == self._capacity

    @property
    def is_empty(self) -> bool:
        """Check if buffer is empty."""
        with self._lock:
            return self._size == 0

    def first(self) -> T | None:
        """Get the oldest item."""
        with self._lock:
            if self._size == 0:
                return None
            return self._buffer[self._tail]

    def last(self) -> T | None:
        """Get the newest item."""
        with self._lock:
            if self._size == 0:
                return None
            last_index = (self._head - 1) % self._capacity
            return self._buffer[last_index]

    def clear(self) -> None:
        """Clear all items from buffer."""
        with self._lock:
            self._buffer = [None] * self._capacity
            self._head = 0
            self._tail = 0
            self._size = 0

    def apply(self, func: Callable[[T], Any]) -> None:
        """Apply a function to all items."""
        with self._lock:
            for i in range(self._size):
                idx = (self._tail + i) % self._capacity
                self._buffer[idx] = func(self._buffer[idx])  # type: ignore


class BlockingRingBuffer(RingBuffer[T]):
    """
    Ring buffer with blocking put and get operations.
    """

    def __init__(self, capacity: int):
        super().__init__(capacity)
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)

    def append(self, item: T, block: bool = True, timeout: float | None = None) -> bool:
        """
        Add an item to the buffer with optional blocking.

        Returns:
            True if item was added, False if timeout occurred.
        """
        with self._not_full:
            if self._size == self._capacity:
                if not block:
                    return False

                if timeout is not None:
                    end_time = time.time() + timeout
                    while self._size == self._capacity:
                        remaining = end_time - time.time()
                        if remaining <= 0:
                            return False
                        self._not_full.wait(remaining)
                else:
                    while self._size == self._capacity:
                        self._not_full.wait()

            super().append(item)
            self._not_empty.notify()
            return True

    def get(self, block: bool = True, timeout: float | None = None) -> T | None:
        """
        Get an item from the buffer with optional blocking.

        Returns:
            The item, or None if timeout occurred.
        """
        with self._not_empty:
            if self._size == 0:
                if not block:
                    return None

                if timeout is not None:
                    end_time = time.time() + timeout
                    while self._size == 0:
                        remaining = end_time - time.time()
                        if remaining <= 0:
                            return None
                        self._not_empty.wait(remaining)
                else:
                    while self._size == 0:
                        self._not_empty.wait()

            item = super().first()
            super().clear()
            super().append(item)  # Move head back

            # Actually remove the item
            self._head = (self._head - 1) % self._capacity
            self._size -= 1
            self._tail = (self._tail + 1) % self._capacity if self._size > 0 else self._head

            self._not_full.notify()
            return item

    def clear(self) -> None:
        """Clear all items and notify waiting threads."""
        with self._lock:
            super().clear()
            self._not_full.notify_all()


import time


class StatisticsRingBuffer(RingBuffer[T]):
    """
    Ring buffer that maintains running statistics.
    """

    def __init__(self, capacity: int):
        super().__init__(capacity)
        self._sum = 0.0
        self._sum_sq = 0.0

    def append(self, item: T) -> None:
        """Add an item and update statistics."""
        with self._lock:
            # If full, subtract oldest value
            if self._size == self._capacity:
                old = super().last()
                if isinstance(old, (int, float)):
                    self._sum -= old
                    self._sum_sq -= old * old

            super().append(item)

            if isinstance(item, (int, float)):
                self._sum += item
                self._sum_sq += item * item

    @property
    def mean(self) -> float | None:
        """Calculate mean of all values."""
        with self._lock:
            if self._size == 0:
                return None

            if isinstance(self.last(), (int, float)):
                return self._sum / self._size
            return None

    @property
    def variance(self) -> float | None:
        """Calculate variance of all values."""
        with self._lock:
            if self._size == 0:
                return None

            if isinstance(self.last(), (int, float)):
                mean = self._sum / self._size
                return (self._sum_sq / self._size) - (mean * mean)
            return None

    @property
    def std_dev(self) -> float | None:
        """Calculate standard deviation."""
        import math
        var = self.variance
        return math.sqrt(var) if var is not None else None

    def percentile(self, p: float) -> float | None:
        """Calculate percentile (0-100)."""
        with self._lock:
            if self._size == 0:
                return None

            if isinstance(self.last(), (int, float)):
                sorted_items = sorted(self.to_list())  # type: ignore
                idx = int(len(sorted_items) * p / 100)
                idx = min(idx, len(sorted_items) - 1)
                return sorted_items[idx]
            return None


@dataclass
class SlidingWindowStats:
    """Statistics for a sliding window."""
    count: int = 0
    sum: float = 0.0
    min: float | None = None
    max: float | None = None
    mean: float | None = None


class SlidingWindowRingBuffer(RingBuffer[float]):
    """
    Ring buffer optimized for sliding window statistics.
    """

    def __init__(self, capacity: int):
        super().__init__(capacity)
        self._stats = SlidingWindowStats()

    def append(self, item: float) -> None:
        """Add an item and update sliding window statistics."""
        with self._lock:
            # If full, subtract oldest value from stats
            if self._size == self._capacity:
                old = super().last()
                self._stats.sum -= old
                self._stats.count -= 1
                if old == self._stats.min:
                    self._stats.min = None
                if old == self._stats.max:
                    self._stats.max = None

            super().append(item)
            self._update_stats(item)

    def _update_stats(self, item: float) -> None:
        """Update running statistics."""
        self._stats.count = self._size
        self._stats.sum += item

        if self._stats.min is None or item < self._stats.min:
            self._stats.min = item
        if self._stats.max is None or item > self._stats.max:

        if self._stats.count > 0:
            self._stats.mean = self._stats.sum / self._stats.count

    @property
    def stats(self) -> SlidingWindowStats:
        """Get current sliding window statistics."""
        return copy.copy(self._stats)
