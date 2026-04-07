"""ringbuf_action module for rabai_autoclick.

Provides ring buffer (circular buffer) implementations: fixed-size
ring buffer, blocking ring buffer, and timestamped ring buffer.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Generic, Iterable, Iterator, List, Optional, TypeVar

__all__ = [
    "RingBuffer",
    "BlockingRingBuffer",
    "TimestampedRingBuffer",
    " TimestampedItem",
    "RingBufferFull",
    "RingBufferEmpty",
]


T = TypeVar("T")


class RingBufferFull(Exception):
    """Raised when ring buffer is full."""
    pass


class RingBufferEmpty(Exception):
    """Raised when ring buffer is empty."""
    pass


class RingBuffer(Generic[T]):
    """Fixed-size circular buffer (ring buffer)."""

    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("Capacity must be positive")
        self.capacity = capacity
        self._buffer: List[Optional[T]] = [None] * capacity
        self._head = 0
        self._tail = 0
        self._size = 0
        self._lock = threading.Lock()

    def push(self, item: T, overwrite: bool = False) -> None:
        """Add item to buffer.

        Args:
            item: Item to add.
            overwrite: If True, overwrite oldest item when full.
                      If False, raise RingBufferFull.
        """
        with self._lock:
            if self._size >= self.capacity:
                if overwrite:
                    self._tail = (self._tail + 1) % self.capacity
                    self._size -= 1
                else:
                    raise RingBufferFull(f"Ring buffer is full (capacity={self.capacity})")
            self._buffer[self._head] = item
            self._head = (self._head + 1) % self.capacity
            self._size += 1

    def pop(self) -> T:
        """Remove and return oldest item.

        Returns:
            Oldest item.

        Raises:
            RingBufferEmpty: If buffer is empty.
        """
        with self._lock:
            if self._size == 0:
                raise RingBufferEmpty("Ring buffer is empty")
            item = self._buffer[self._tail]
            self._buffer[self._tail] = None
            self._tail = (self._tail + 1) % self.capacity
            self._size -= 1
            return item

    def peek(self) -> T:
        """Return oldest item without removing.

        Raises:
            RingBufferEmpty: If buffer is empty.
        """
        with self._lock:
            if self._size == 0:
                raise RingBufferEmpty("Ring buffer is empty")
            return self._buffer[self._tail]

    def get(self, index: int) -> T:
        """Get item at index (0 = oldest)."""
        with self._lock:
            if index < 0 or index >= self._size:
                raise IndexError(f"Index {index} out of range (size={self._size})")
            actual_index = (self._tail + index) % self.capacity
            return self._buffer[actual_index]

    def __len__(self) -> int:
        return self._size

    def __bool__(self) -> bool:
        return self._size > 0

    def is_full(self) -> bool:
        return self._size >= self.capacity

    def is_empty(self) -> bool:
        return self._size == 0

    def clear(self) -> None:
        """Clear all items from buffer."""
        with self._lock:
            self._buffer = [None] * self.capacity
            self._head = 0
            self._tail = 0
            self._size = 0

    def to_list(self) -> List[T]:
        """Return buffer contents as list (oldest first)."""
        with self._lock:
            result = []
            for i in range(self._size):
                idx = (self._tail + i) % self.capacity
                result.append(self._buffer[idx])
            return result


class BlockingRingBuffer(Generic[T]):
    """Thread-safe blocking ring buffer."""

    def __init__(self, capacity: int) -> None:
        self._buffer = RingBuffer[T](capacity)
        self._lock = self._buffer._lock
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)

    def push(self, item: T, timeout: Optional[float] = None, overwrite: bool = False) -> bool:
        """Add item to buffer, blocking if full.

        Args:
            item: Item to add.
            timeout: Max seconds to wait (None = infinite).
            overwrite: Overwrite oldest if full.

        Returns:
            True if added, False on timeout.
        """
        deadline = None if timeout is None else time.monotonic() + timeout

        with self._not_full:
            while self._buffer.is_full():
                if timeout is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    if not self._not_full.wait(timeout=remaining):
                        return False
                else:
                    self._not_full.wait()
        with self._lock:
            self._buffer.push(item, overwrite=overwrite)
            self._not_empty.notify()
            return True

    def pop(self, timeout: Optional[float] = None) -> Optional[T]:
        """Remove and return oldest item, blocking if empty.

        Args:
            timeout: Max seconds to wait.

        Returns:
            Item or None on timeout.
        """
        deadline = None if timeout is None else time.monotonic() + timeout

        with self._not_empty:
            while self._buffer.is_empty():
                if timeout is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return None
                    if not self._not_empty.wait(timeout=remaining):
                        return None
                else:
                    self._not_empty.wait()
        with self._lock:
            item = self._buffer.pop()
            self._not_full.notify()
            return item

    def peek(self, timeout: Optional[float] = None) -> Optional[T]:
        """Peek at oldest item without removing."""
        deadline = None if timeout is None else time.monotonic() + timeout

        with self._not_empty:
            while self._buffer.is_empty():
                if timeout is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return None
                    if not self._not_empty.wait(timeout=remaining):
                        return None
                else:
                    self._not_empty.wait()
        with self._lock:
            return self._buffer.peek()

    def __len__(self) -> int:
        return len(self._buffer)

    def __bool__(self) -> bool:
        return bool(self._buffer)

    def is_full(self) -> bool:
        return self._buffer.is_full()

    def is_empty(self) -> bool:
        return self._buffer.is_empty()


@dataclass
class TimestampedItem(Generic[T]):
    """Item with timestamp."""
    value: T
    timestamp: float = field(default_factory=time.time)


class TimestampedRingBuffer(Generic[T]):
    """Ring buffer that stores items with timestamps."""

    def __init__(self, capacity: int, ttl_seconds: Optional[float] = None) -> None:
        self.capacity = capacity
        self.ttl_seconds = ttl_seconds
        self._buffer: deque = deque(maxlen=capacity)

    def push(self, item: T, timestamp: Optional[float] = None) -> None:
        """Add item with current or specified timestamp."""
        ts = timestamp if timestamp is not None else time.time()
        self._buffer.append(TimestampedItem(value=item, timestamp=ts))

    def pop(self) -> Optional[T]:
        """Remove and return oldest item."""
        if not self._buffer:
            return None
        return self._buffer.popleft().value

    def peek(self) -> Optional[T]:
        """Return oldest item without removing."""
        if not self._buffer:
            return None
        return self._buffer[0].value

    def get_oldest_timestamp(self) -> Optional[float]:
        """Get timestamp of oldest item."""
        if not self._buffer:
            return None
        return self._buffer[0].timestamp

    def get_newest_timestamp(self) -> Optional[float]:
        """Get timestamp of newest item."""
        if not self._buffer:
            return None
        return self._buffer[-1].timestamp

    def get_range(
        self,
        start_time: float,
        end_time: float,
    ) -> List[T]:
        """Get items within time range."""
        result = []
        for item in self._buffer:
            if start_time <= item.timestamp <= end_time:
                result.append(item.value)
        return result

    def cleanup_expired(self) -> int:
        """Remove expired items.

        Returns:
            Number of items removed.
        """
        if self.ttl_seconds is None:
            return 0
        cutoff = time.time() - self.ttl_seconds
        removed = 0
        while self._buffer and self._buffer[0].timestamp < cutoff:
            self._buffer.popleft()
            removed += 1
        return removed

    def __len__(self) -> int:
        return len(self._buffer)

    def __bool__(self) -> bool:
        return len(self._buffer) > 0

    def is_full(self) -> bool:
        return len(self._buffer) >= self.capacity

    def is_empty(self) -> bool:
        return len(self._buffer) == 0

    def to_list(self) -> List[T]:
        """Return all values as list (oldest first)."""
        return [item.value for item in self._buffer]
