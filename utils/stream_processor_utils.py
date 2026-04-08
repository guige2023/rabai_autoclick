"""
Stream processing utilities.

Provides streaming data processing primitives for
handling large datasets efficiently.
"""

from __future__ import annotations

import threading
from collections import deque
from typing import Callable, Iterator, TypeVar


T = TypeVar("T")
R = TypeVar("R")


def chunked(
    iterable: Iterator[T],
    size: int,
) -> Iterator[list[T]]:
    """
    Split iterable into chunks of specified size.

    Args:
        iterable: Input data
        size: Chunk size

    Yields:
        Chunks of up to size elements
    """
    chunk: list[T] = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def sliding_window(
    iterable: Iterator[T],
    size: int,
    fill: T | None = None,
) -> Iterator[list[T | None]]:
    """
    Create sliding window over iterable.

    Args:
        iterable: Input data
        size: Window size
        fill: Padding value for incomplete windows

    Yields:
        Window slices
    """
    window: deque[T | None] = deque([fill] * size, maxlen=size)
    for item in iterable:
        window.append(item)
        yield list(window)
    window.appendleft(fill)
    for _ in range(size - 1):
        window.appendleft(fill)
        yield list(window)


def filter_map(
    func: Callable[[T], R | None],
    iterable: Iterator[T],
) -> Iterator[R]:
    """
    Map then filter out None results.

    Args:
        func: Transform function (may return None)
        iterable: Input data

    Yields:
        Non-None results
    """
    for item in iterable:
        result = func(item)
        if result is not None:
            yield result


def running_reduce(
    func: Callable[[T, T], T],
    iterable: Iterator[T],
    initial: T | None = None,
) -> Iterator[T]:
    """
    Running reduction over iterable.

    Args:
        func: Binary reduction function
        iterable: Input data
        initial: Initial accumulator value

    Yields:
        Accumulated values after each step
    """
    acc = initial
    first = True
    for item in iterable:
        if first and initial is None:
            acc = item
            first = False
        else:
            acc = func(acc, item)  # type: ignore
        yield acc  # type: ignore


class StreamBuffer:
    """
    Thread-safe stream buffer for producer-consumer patterns.

    Provides buffering between fast producers and slow consumers.
    """

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self._buffer: deque = deque(maxlen=max_size)
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)
        self._closed = False

    def put(self, item: T, block: bool = True, timeout: float | None = None) -> bool:
        """
        Put item into buffer.

        Args:
            item: Item to add
            block: Block if buffer is full
            timeout: Max wait time

        Returns:
            True if added, False if closed
        """
        with self._not_full:
            if self._closed:
                return False
            if not block:
                if len(self._buffer) >= self.max_size:
                    return False
            while len(self._buffer) >= self.max_size and not self._closed:
                if not self._not_full.wait(timeout):
                    return False
            if self._closed:
                return False
            self._buffer.append(item)
            self._not_empty.notify()
            return True

    def get(self, block: bool = True, timeout: float | None = None) -> T | None:
        """
        Get item from buffer.

        Args:
            block: Block if buffer is empty
            timeout: Max wait time

        Returns:
            Item or None if closed
        """
        with self._not_empty:
            if self._closed and not self._buffer:
                return None
            if not block:
                if not self._buffer:
                    return None
            while not self._buffer and not self._closed:
                if not self._not_empty.wait(timeout):
                    return None
            if self._buffer:
                item = self._buffer.popleft()
                self._not_full.notify()
                return item
            return None

    def close(self) -> None:
        """Close buffer and unblock waiters."""
        with self._lock:
            self._closed = True
            self._not_empty.notify_all()
            self._not_full.notify_all()

    @property
    def is_closed(self) -> bool:
        return self._closed

    @property
    def size(self) -> int:
        return len(self._buffer)


def stream_tee(
    iterable: Iterator[T],
    n: int,
) -> list[Iterator[T]]:
    """
    Split stream into n independent iterators.

    Warning: Buffered in memory, use carefully.

    Args:
        iterable: Input stream
        n: Number of output streams

    Returns:
        List of n iterators
    """
    buffer: list[deque[T] | T] = [deque() for _ in range(n)]
    next_indices = [0] * n

    def make_stream(i: int) -> Iterator[T]:
        while True:
            while next_indices[i] >= len(buffer[i]):
                try:
                    item = next(iterable)
                    for j in range(n):
                        if j != i:
                            buffer[j].append(item)
                        next_indices[j] += 1
                except StopIteration:
                    return
            yield buffer[i][next_indices[i]]
            next_indices[i] += 1

    return [make_stream(i) for i in range(n)]
