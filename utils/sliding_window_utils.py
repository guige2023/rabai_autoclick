"""
Sliding window data structures and algorithms.

Provides sliding window implementations for various use cases including
rate limiting, moving averages, and temporal data aggregation.
Supports both count-based and time-based windows.

Example:
    >>> from utils.sliding_window_utils import SlidingWindowCounter
    >>> counter = SlidingWindowCounter(window_size=60)
    >>> counter.record()
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Any, Callable, Generic, List, Optional, TypeVar

T = TypeVar("T")


class SlidingWindowCounter:
    """
    Sliding window counter for rate limiting and frequency estimation.

    Counts events within a sliding time window. The window is divided
    into sub-windows for more granular tracking.

    Attributes:
        window_size: Window size in seconds.
        num_buckets: Number of sub-buckets within the window.
    """

    def __init__(
        self,
        window_size: float = 60.0,
        num_buckets: int = 10
    ) -> None:
        """
        Initialize the sliding window counter.

        Args:
            window_size: Total window size in seconds.
            num_buckets: Number of sub-buckets (higher = more accurate).
        """
        self.window_size = window_size
        self.num_buckets = num_buckets
        self._bucket_size = window_size / num_buckets
        self._counts: deque[int] = deque([0] * num_buckets, maxlen=num_buckets)
        self._bucket_times: deque[float] = deque(
            [time.monotonic()] * num_buckets, maxlen=num_buckets
        )
        self._total_count = 0
        self._lock = asyncio.Lock()

    async def record(self, count: int = 1) -> None:
        """
        Record events in the current bucket.

        Args:
            count: Number of events to record.
        """
        async with self._lock:
            self._advance_window()
            self._counts[-1] += count
            self._total_count += count

    async def get_count(self) -> int:
        """
        Get the total count within the current window.

        Returns:
            Total event count in the sliding window.
        """
        async with self._lock:
            self._advance_window()
            return sum(self._counts)

    async def try_record(self, tokens: int = 1) -> bool:
        """
        Try to record without blocking.

        Args:
            tokens: Number of events to record.

        Returns:
            True (always succeeds for this implementation).
        """
        await self.record(tokens)
        return True

    def _advance_window(self) -> None:
        """Advance buckets that have expired since last record."""
        now = time.monotonic()
        last_time = self._bucket_times[-1]

        if now - last_time >= self.window_size:
            self._counts = deque([0] * self.num_buckets, maxlen=self.num_buckets)
            self._bucket_times = deque([now] * self.num_buckets, maxlen=self.num_buckets)
            self._total_count = 0
        else:
            while self._bucket_times[0] <= now - self.window_size:
                expired_count = self._counts.popleft()
                self._total_count -= expired_count
                self._counts.appendleft(0)
                self._bucket_times.appendleft(now)

    def reset(self) -> None:
        """Reset all counts to zero."""
        self._counts = deque([0] * self.num_buckets, maxlen=self.num_buckets)
        self._bucket_times = deque(
            [time.monotonic()] * self.num_buckets, maxlen=self.num_buckets
        )
        self._total_count = 0


class SlidingWindowList(Generic[T]):
    """
    Sliding window that stores items within a time window.

    Useful for keeping a rolling history of events or values.

    Attributes:
        max_age: Maximum age of items in seconds.
        max_size: Maximum number of items to keep.
    """

    def __init__(
        self,
        max_age: float = 60.0,
        max_size: Optional[int] = None
    ) -> None:
        """
        Initialize the sliding window list.

        Args:
            max_age: Maximum item age in seconds before eviction.
            max_size: Maximum number of items (None for unlimited).
        """
        self.max_age = max_age
        self.max_size = max_size
        self._items: deque[tuple[float, T]] = deque(maxlen=max_size)
        self._lock = asyncio.Lock()

    async def append(self, item: T) -> None:
        """
        Append an item to the window.

        Args:
            item: Item to append.
        """
        async with self._lock:
            self._evict_expired()
            self._items.append((time.monotonic(), item))

    async def get_all(self) -> List[T]:
        """
        Get all items in the window.

        Returns:
            List of items within the window.
        """
        async with self._lock:
            self._evict_expired()
            return [item for _, item in self._items]

    async def get_since(self, seconds: float) -> List[T]:
        """
        Get items newer than a given age.

        Args:
            seconds: Age threshold in seconds.

        Returns:
            List of recent items.
        """
        async with self._lock:
            cutoff = time.monotonic() - seconds
            return [
                item
                for timestamp, item in self._items
                if timestamp >= cutoff
            ]

    def _evict_expired(self) -> None:
        """Remove expired items."""
        cutoff = time.monotonic() - self.max_age
        while self._items and self._items[0][0] < cutoff:
            self._items.popleft()

    async def clear(self) -> None:
        """Clear all items."""
        async with self._lock:
            self._items.clear()

    def __len__(self) -> int:
        """Get current number of items."""
        return len(self._items)


class SlidingWindowAverage:
    """
    Sliding window average calculator.

    Computes moving average over a time window.

    Attributes:
        window_size: Window size in seconds.
    """

    def __init__(self, window_size: float = 60.0) -> None:
        """
        Initialize the sliding window average.

        Args:
            window_size: Window size in seconds.
        """
        self.window_size = window_size
        self._values: deque[tuple[float, float]] = deque(maxlen=10000)

    async def record(self, value: float) -> None:
        """
        Record a value.

        Args:
            value: Numeric value to record.
        """
        self._evict_expired()
        self._values.append((time.monotonic(), value))

    async def get_average(self) -> Optional[float]:
        """
        Get the current average.

        Returns:
            Average value, or None if window is empty.
        """
        self._evict_expired()
        if not self._values:
            return None
        return sum(v for _, v in self._values) / len(self._values)

    async def get_stats(self) -> dict:
        """
        Get statistical summary of the window.

        Returns:
            Dictionary with count, sum, mean, min, max.
        """
        self._evict_expired()
        if not self._values:
            return {"count": 0, "sum": 0.0, "mean": None, "min": None, "max": None}

        values = [v for _, v in self._values]
        return {
            "count": len(values),
            "sum": sum(values),
            "mean": sum(values) / len(values),
            "min": min(values),
            "max": max(values),
        }

    def _evict_expired(self) -> None:
        """Remove expired values."""
        cutoff = time.monotonic() - self.window_size
        while self._values and self._values[0][0] < cutoff:
            self._values.popleft()


class TumblingWindow(Generic[T]):
    """
    Tumbling window that groups items into fixed non-overlapping windows.

    Useful for batch processing of time-series data.

    Attributes:
        window_size: Size of each tumbling window in seconds.
    """

    def __init__(
        self,
        window_size: float,
        collector: Optional[Callable[[List[T]], Any]] = None
    ) -> None:
        """
        Initialize the tumbling window.

        Args:
            window_size: Window size in seconds.
            collector: Optional function to call when window closes.
        """
        self.window_size = window_size
        self.collector = collector
        self._current_window_start: Optional[float] = None
        self._items: List[T] = []
        self._lock = asyncio.Lock()

    async def append(self, item: T) -> Optional[List[T]]:
        """
        Append an item to the current or next window.

        Args:
            item: Item to append.

        Returns:
            Closed window items if window just closed, None otherwise.
        """
        async with self._lock:
            now = time.monotonic()

            if self._current_window_start is None:
                self._current_window_start = now

            elapsed = now - self._current_window_start

            if elapsed >= self.window_size:
                closed = self._items
                self._items = [item]
                self._current_window_start = now

                if self.collector:
                    self.collector(closed)

                return closed

            self._items.append(item)
            return None

    async def flush(self) -> List[T]:
        """
        Flush the current window and start a new one.

        Returns:
            Items in the flushed window.
        """
        async with self._lock:
            items = self._items
            self._items = []
            self._current_window_start = None
            return items

    async def get_current(self) -> List[T]:
        """Get items in the current window."""
        async with self._lock:
            return list(self._items)


def create_sliding_window(
    window_type: str,
    **kwargs
) -> Any:
    """
    Factory function to create sliding window instances.

    Args:
        window_type: One of 'counter', 'list', 'average', 'tumbling'.
        **kwargs: Arguments passed to the window constructor.

    Returns:
        Configured sliding window instance.

    Raises:
        ValueError: If window_type is unknown.
    """
    types = {
        "counter": SlidingWindowCounter,
        "list": SlidingWindowList,
        "average": SlidingWindowAverage,
        "tumbling": TumblingWindow,
    }

    if window_type not in types:
        raise ValueError(
            f"Unknown window type: {window_type}. "
            f"Available: {list(types.keys())}"
        )

    return types[window_type](**kwargs)
