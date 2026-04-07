"""Window utilities for RabAI AutoClick.

Provides:
- Window statistics
- Window aggregation
- Tumbling windows
- Sliding windows
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Deque,
    Generic,
    Iterator,
    List,
    Optional,
    TypeVar,
)


T = TypeVar("T")
U = TypeVar("U")


@dataclass
class WindowStats:
    """Statistics for a window."""

    count: int = 0
    sum: float = 0.0
    min_val: Optional[float] = None
    max_val: Optional[float] = None
    avg: float = 0.0

    def add(self, value: float) -> None:
        self.count += 1
        self.sum += value
        if self.min_val is None or value < self.min_val:
            self.min_val = value
        if self.max_val is None or value > self.max_val:
            self.max_val = value
        self.avg = self.sum / self.count


class TumblingWindow(Generic[T]):
    """Tumbling window that emits fixed-size windows.

    Example:
        window = TumblingWindow(size=3)
        for item in [1, 2, 3, 4, 5]:
            window.push(item)
            if window.is_complete:
                print(window.get())  # [1, 2, 3], then [4, 5]
    """

    def __init__(self, size: int) -> None:
        self._size = size
        self._buffer: List[T] = []

    def push(self, item: T) -> Optional[List[T]]:
        self._buffer.append(item)
        if len(self._buffer) >= self._size:
            result = self._buffer[:]
            self._buffer.clear()
            return result
        return None

    def is_complete(self) -> bool:
        return len(self._buffer) >= self._size

    def get(self) -> List[T]:
        return self._buffer[:]

    def __len__(self) -> int:
        return len(self._buffer)


class SlidingWindow(Generic[T]):
    """Sliding window with overlap.

    Example:
        window = SlidingWindow(size=3)
        for item in [1, 2, 3, 4, 5]:
            window.push(item)
            print(window.get())  # [1], [1,2], [1,2,3], [2,3,4], [3,4,5]
    """

    def __init__(self, size: int) -> None:
        self._size = size
        self._buffer: Deque[T] = deque(maxlen=size)

    def push(self, item: T) -> None:
        self._buffer.append(item)

    def get(self) -> List[T]:
        return list(self._buffer)

    def __len__(self) -> int:
        return len(self._buffer)


class TimeWindow(Generic[T]):
    """Time-based sliding window.

    Example:
        window = TimeWindow(window_seconds=5.0)
        for item in items:
            window.push(item, timestamp=time.time())
            for w in window.get_windows():
                print(w)
    """

    def __init__(
        self,
        window_seconds: float,
        slide_seconds: Optional[float] = None,
    ) -> None:
        self._window_seconds = window_seconds
        self._slide_seconds = slide_seconds or window_seconds
        self._events: Deque[tuple[float, T]] = deque()
        self._last_emit = 0.0

    def push(self, item: T, timestamp: Optional[float] = None) -> List[List[T]]:
        timestamp = timestamp or time.time()
        self._events.append((timestamp, item))
        self._cleanup(timestamp)

        results: List[List[T]] = []
        while timestamp - self._last_emit >= self._slide_seconds:
            results.append(self._emit(self._last_emit))
            self._last_emit += self._slide_seconds

        return results

    def _cleanup(self, now: float) -> None:
        cutoff = now - self._window_seconds
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()

    def _emit(self, window_start: float) -> List[T]:
        window_end = window_start + self._window_seconds
        return [item for ts, item in self._events if window_start <= ts < window_end]

    def get_windows(self) -> List[List[T]]:
        now = time.time()
        self._cleanup(now)
        results: List[List[T]] = []
        emit_time = self._last_emit
        while emit_time + self._window_seconds <= now:
            results.append(self._emit(emit_time))
            emit_time += self._slide_seconds
        return results

    def __len__(self) -> int:
        return len(self._events)


class AggregatingWindow(Generic[T]):
    """Window that aggregates values.

    Example:
        window = AggregatingWindow(size=5, aggregator=sum)
        for x in [1, 2, 3, 4, 5]:
            result = window.push(x)  # 15 after 5 elements
    """

    def __init__(
        self,
        size: int,
        aggregator: Callable[[List[T]], U] = lambda x: x,
    ) -> None:
        self._size = size
        self._buffer: List[T] = []
        self._aggregator = aggregator

    def push(self, item: T) -> Optional[U]:
        self._buffer.append(item)
        if len(self._buffer) > self._size:
            self._buffer.pop(0)
        if len(self._buffer) == self._size:
            return self._aggregator(self._buffer)
        return None

    def get(self) -> List[T]:
        return self._buffer[:]

    def __len__(self) -> int:
        return len(self._buffer)


def rolling_window(
    data: List[T],
    size: int,
) -> Iterator[List[T]]:
    """Create rolling windows over a list.

    Args:
        data: Input data.
        size: Window size.

    Yields:
        Windows of size elements.
    """
    for i in range(len(data) - size + 1):
        yield data[i:i + size]


def window_apply(
    data: List[T],
    size: int,
    func: Callable[[List[T]], U],
) -> List[U]:
    """Apply function to rolling windows.

    Args:
        data: Input data.
        size: Window size.
        func: Function to apply to each window.

    Returns:
        List of results.
    """
    return [func(window) for window in rolling_window(data, size)]
