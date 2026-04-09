"""Ring buffer and circular data structure utilities.

Provides efficient fixed-size buffers for streaming data,
circular arrays, and FIFO/LIFO data handling.
"""

from __future__ import annotations

from typing import (
    TypeVar, Generic, List, Optional, Iterator, Callable, Any
)
from collections import deque
from dataclasses import dataclass
import math


T = TypeVar('T')


class RingBuffer(Generic[T]):
    """Fixed-size circular buffer.

    New items overwrite the oldest items when buffer is full.

    Example:
        buffer = RingBuffer[str](max_size=3)
        buffer.push("a")
        buffer.push("b")
        buffer.push("c")
        buffer.push("d")  # "a" is overwritten
        print(buffer.to_list())  # ['b', 'c', 'd']
    """

    def __init__(self, max_size: int) -> None:
        if max_size < 1:
            raise ValueError("max_size must be at least 1")
        self._max_size = max_size
        self._buffer: List[Optional[T]] = [None] * max_size
        self._head: int = 0  # Next write position
        self._size: int = 0

    def push(self, item: T) -> None:
        """Add item to buffer (overwrites oldest if full)."""
        self._buffer[self._head] = item
        self._head = (self._head + 1) % self._max_size
        if self._size < self._max_size:
            self._size += 1

    def pop(self) -> Optional[T]:
        """Remove and return oldest item (FIFO)."""
        if self._size == 0:
            return None
        tail = (self._head - self._size + self._max_size) % self._max_size
        item = self._buffer[tail]
        self._buffer[tail] = None
        self._size -= 1
        return item

    def pop_last(self) -> Optional[T]:
        """Remove and return newest item (LIFO)."""
        if self._size == 0:
            return None
        self._head = (self._head - 1 + self._max_size) % self._max_size
        item = self._buffer[self._head]
        self._buffer[self._head] = None
        self._size -= 1
        return item

    def peek(self) -> Optional[T]:
        """Get oldest item without removing it."""
        if self._size == 0:
            return None
        tail = (self._head - self._size + self._max_size) % self._max_size
        return self._buffer[tail]

    def peek_last(self) -> Optional[T]:
        """Get newest item without removing it."""
        if self._size == 0:
            return None
        last = (self._head - 1 + self._max_size) % self._max_size
        return self._buffer[last]

    def to_list(self) -> List[T]:
        """Get buffer contents as list (oldest to newest)."""
        if self._size == 0:
            return []
        result = []
        tail = (self._head - self._size + self._max_size) % self._max_size
        for _ in range(self._size):
            result.append(self._buffer[tail])
            tail = (tail + 1) % self._max_size
        return result

    def clear(self) -> None:
        """Clear all items from buffer."""
        self._buffer = [None] * self._max_size
        self._head = 0
        self._size = 0

    @property
    def size(self) -> int:
        """Current number of items in buffer."""
        return self._size

    @property
    def max_size(self) -> int:
        """Maximum buffer capacity."""
        return self._max_size

    @property
    def is_full(self) -> bool:
        """Check if buffer is at capacity."""
        return self._size == self._max_size

    @property
    def is_empty(self) -> bool:
        """Check if buffer is empty."""
        return self._size == 0

    def __len__(self) -> int:
        return self._size

    def __repr__(self) -> str:
        return f"RingBuffer(size={self._size}, max_size={self._max_size})"


@dataclass
class TimestampedBuffer(Generic[T]):
    """Buffer that stores items with timestamps.

    Useful for time-series data with automatic expiry.
    """
    max_size: int
    ttl_seconds: float = 60.0

    def __post_init__(self) -> None:
        self._buffer: RingBuffer[Tuple[float, T]] = RingBuffer(max_size=self.max_size)
        self._timestamps: RingBuffer[float] = RingBuffer(max_size=self.max_size)

    def push(self, item: T, timestamp: Optional[float] = None) -> None:
        """Add item with timestamp (defaults to current time)."""
        import time
        ts = timestamp if timestamp is not None else time.time()
        self._buffer.push((ts, item))
        self._timestamps.push(ts)

    def get_valid(self, current_time: Optional[float] = None) -> List[T]:
        """Get all items that haven't expired."""
        import time
        now = current_time if current_time is not None else time.time()
        expired_cutoff = now - self.ttl_seconds
        valid = []
        for ts, item in self._buffer.to_list():
            if ts >= expired_cutoff:
                valid.append(item)
        return valid

    def prune(self, current_time: Optional[float] = None) -> int:
        """Remove expired items, return count removed."""
        import time
        now = current_time if current_time is not None else time.time()
        count = 0
        while not self._buffer.is_empty:
            oldest_ts, _ = self._buffer.peek()
            if oldest_ts < now - self.ttl_seconds:
                self._buffer.pop()
                self._timestamps.pop()
                count += 1
            else:
                break
        return count


class SlidingWindow(Generic[T]):
    """Sliding window for computing running statistics.

    Example:
        window = SlidingWindow[int](size=5)
        for val in [1, 2, 3, 4, 5, 6]:
            window.add(val)
            print(window.mean())  # Running average
    """

    def __init__(self, size: int) -> None:
        if size < 1:
            raise ValueError("Window size must be at least 1")
        self._size = size
        self._buffer: deque[T] = deque(maxlen=size)

    def add(self, item: T) -> None:
        """Add item to window."""
        self._buffer.append(item)

    @property
    def values(self) -> List[T]:
        """Get current window values."""
        return list(self._buffer)

    @property
    def is_full(self) -> bool:
        return len(self._buffer) >= self._size

    @property
    def is_empty(self) -> bool:
        return len(self._buffer) == 0

    def mean(self) -> Optional[float]:
        """Compute mean of window values."""
        if not self._buffer:
            return None
        return sum(self._buffer) / len(self._buffer)

    def sum(self) -> Optional[float]:
        """Compute sum of window values."""
        if not self._buffer:
            return None
        return sum(self._buffer)  # type: ignore

    def min(self) -> Optional[T]:
        """Get minimum value in window."""
        if not self._buffer:
            return None
        return min(self._buffer)

    def max(self) -> Optional[T]:
        """Get maximum value in window."""
        if not self._buffer:
            return None
        return max(self._buffer)

    def std(self) -> Optional[float]:
        """Compute standard deviation of window values."""
        if len(self._buffer) < 2:
            return None
        m = self.mean()
        if m is None:
            return None
        variance = sum((x - m) ** 2 for x in self._buffer) / len(self._buffer)
        return math.sqrt(variance)

    def median(self) -> Optional[float]:
        """Compute median of window values."""
        if not self._buffer:
            return None
        sorted_vals = sorted(self._buffer)
        n = len(sorted_vals)
        mid = n // 2
        if n % 2 == 1:
            return float(sorted_vals[mid])
        return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0

    def __len__(self) -> int:
        return len(self._buffer)


class MovingStatistics(Generic[T]):
    """Running statistics over a sliding window.

    Computes mean, variance, stddev, min, max, etc.
    """

    def __init__(self, window_size: int) -> None:
        self._window = SlidingWindow[T](size=window_size)
        self._sum: float = 0.0
        self._sum_sq: float = 0.0

    def add(self, value: T) -> None:
        """Add a value to the statistics."""
        if not isinstance(value, (int, float)):
            raise TypeError("MovingStatistics requires numeric values")
        if self._window.is_full:
            old = self._window.values[0]
            if isinstance(old, (int, float)):
                self._sum -= old
                self._sum_sq -= old * old
        self._window.add(value)
        self._sum += value  # type: ignore
        self._sum_sq += value * value  # type: ignore

    def mean(self) -> Optional[float]:
        """Running mean."""
        if self._window.is_empty:
            return None
        return self._sum / len(self._window)

    def variance(self) -> Optional[float]:
        """Running variance."""
        if len(self._window) < 2:
            return None
        m = self.mean()
        if m is None:
            return None
        return (self._sum_sq - self._sum * m) / len(self._window)

    def stddev(self) -> Optional[float]:
        """Running standard deviation."""
        var = self.variance()
        if var is None:
            return None
        return math.sqrt(var)

    def min(self) -> Optional[T]:
        return self._window.min()

    def max(self) -> Optional[T]:
        return self._window.max()

    def count(self) -> int:
        return len(self._window)


class CircularDeque(Generic[T]):
    """Simple circular deque implementation.

    Double-ended queue with O(1) operations at both ends.
    """

    def __init__(self, max_size: Optional[int] = None) -> None:
        self._max_size = max_size
        self._buffer: deque[T] = deque(maxlen=max_size)

    def append_left(self, item: T) -> None:
        """Add item to left end."""
        if self._max_size and len(self._buffer) >= self._max_size:
            self._buffer.pop()
        self._buffer.appendleft(item)

    def append_right(self, item: T) -> None:
        """Add item to right end."""
        self._buffer.append(item)

    def pop_left(self) -> Optional[T]:
        """Remove and return leftmost item."""
        try:
            return self._buffer.popleft()
        except IndexError:
            return None

    def pop_right(self) -> Optional[T]:
        """Remove and return rightmost item."""
        try:
            return self._buffer.pop()
        except IndexError:
            return None

    def clear(self) -> None:
        """Clear all items."""
        self._buffer.clear()

    @property
    def is_empty(self) -> bool:
        return len(self._buffer) == 0

    @property
    def is_full(self) -> bool:
        return self._max_size is not None and len(self._buffer) >= self._max_size

    def to_list(self) -> List[T]:
        """Get contents as list."""
        return list(self._buffer)

    def __len__(self) -> int:
        return len(self._buffer)

    def __repr__(self) -> str:
        return f"CircularDeque({list(self._buffer)})"
