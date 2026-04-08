"""Deque utilities for RabAI AutoClick.

Provides:
- Deque operations with type safety
- Window/sliding window helpers
- Bounded deque with overflow callbacks
- Deque statistics and aggregation
"""

from __future__ import annotations

from collections import deque
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

import threading


T = TypeVar("T")


class BoundedDeque(Generic[T]):
    """A deque with a maximum size, dropping oldest items when full.

    Args:
        maxlen: Maximum number of items.
        on_overflow: Optional callback when items are dropped.
    """

    def __init__(
        self,
        maxlen: int,
        on_overflow: Optional[Callable[[T], None]] = None,
    ) -> None:
        if maxlen <= 0:
            raise ValueError("maxlen must be positive")
        self._deque: Deque[T] = deque(maxlen=maxlen)
        self._lock = threading.Lock()
        self._on_overflow = on_overflow

    def append(self, item: T) -> None:
        """Append an item to the right."""
        with self._lock:
            if len(self._deque) >= self._deque.maxlen:
                dropped = self._deque[0]
                if self._on_overflow:
                    self._on_overflow(dropped)
            self._deque.append(item)

    def appendleft(self, item: T) -> None:
        """Append an item to the left."""
        with self._lock:
            self._deque.appendleft(item)

    def pop(self) -> Optional[T]:
        """Remove and return the rightmost item."""
        with self._lock:
            return self._deque.pop() if self._deque else None

    def popleft(self) -> Optional[T]:
        """Remove and return the leftmost item."""
        with self._lock:
            return self._deque.popleft() if self._deque else None

    def get_all(self) -> List[T]:
        """Return all items as a list (oldest first)."""
        with self._lock:
            return list(self._deque)

    def __len__(self) -> int:
        return len(self._deque)

    def __contains__(self, item: T) -> bool:
        return item in self._deque

    def clear(self) -> None:
        """Remove all items."""
        with self._lock:
            self._deque.clear()

    def peek_right(self) -> Optional[T]:
        """View the rightmost item without removing."""
        with self._lock:
            return self._deque[-1] if self._deque else None

    def peek_left(self) -> Optional[T]:
        """View the leftmost item without removing."""
        with self._lock:
            return self._deque[0] if self._deque else None


def sliding_window(
    iterable: List[T],
    window_size: int,
) -> Iterator[List[T]]:
    """Yield consecutive sliding windows over an iterable.

    Args:
        iterable: Source list.
        window_size: Size of each window.

    Yields:
        Lists of window_size elements.
    """
    if window_size <= 0:
        raise ValueError("window_size must be positive")
    for i in range(len(iterable) - window_size + 1):
        yield iterable[i : i + window_size]


def windowed_mean(
    values: List[float],
    window_size: int,
) -> List[Optional[float]]:
    """Compute rolling mean over a window.

    Args:
        values: List of numeric values.
        window_size: Window size.

    Returns:
        List of means (None for windows that aren't full yet).
    """
    result: List[Optional[float]] = []
    for i in range(len(values)):
        if i < window_size - 1:
            result.append(None)
        else:
            window = values[i - window_size + 1 : i + 1]
            result.append(sum(window) / window_size)
    return result


def windowed_sum(
    values: List[T],
    window_size: int,
) -> Iterator[T]:
    """Yield the sum (or custom aggregator) for each sliding window.

    Uses a deque for O(n) sliding window sum.
    """
    if not values or window_size <= 0:
        return

    dq: Deque[float] = deque(maxlen=window_size)
    running_sum = 0.0

    for i, val in enumerate(values):
        dq.append(val)
        running_sum += val

        if len(dq) < window_size:
            continue

        yield running_sum  # type: ignore

        running_sum -= dq[0]


def dedupe_deque(
    items: List[T],
    key: Optional[Callable[[T], Any]] = None,
) -> List[T]:
    """Remove duplicates from a list while preserving order.

    Args:
        items: Source list.
        key: Optional key function for equality comparison.

    Returns:
        Deduplicated list.
    """
    seen: set = set()
    result: List[T] = []

    for item in items:
        k = key(item) if key else item
        if k not in seen:
            seen.add(k)
            result.append(item)

    return result


def chunked_deque(
    items: List[T],
    chunk_size: int,
) -> Iterator[List[T]]:
    """Yield the list as chunks of chunk_size using a deque.

    Args:
        items: Source list.
        chunk_size: Maximum size per chunk.

    Yields:
        Chunks of up to chunk_size items.
    """
    dq: Deque[T] = deque(items)
    while dq:
        chunk: List[T] = []
        for _ in range(chunk_size):
            if not dq:
                break
            chunk.append(dq.popleft())
        yield chunk


def median_of_deque(deque: Deque[float]) -> float:
    """Compute median of a deque without sorting in full.

    Args:
        deque: Deque of numeric values.

    Returns:
        Median value.
    """
    if not deque:
        raise ValueError("Cannot compute median of empty deque")
    sorted_vals = sorted(deque)
    n = len(sorted_vals)
    mid = n // 2
    if n % 2 == 0:
        return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2
    return sorted_vals[mid]


__all__ = [
    "BoundedDeque",
    "sliding_window",
    "windowed_mean",
    "windowed_sum",
    "dedupe_deque",
    "chunked_deque",
    "median_of_deque",
]
