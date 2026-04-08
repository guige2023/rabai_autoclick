"""
Batch accumulator and windowed aggregation utilities.

Provides time-windowed and count-windowed batching
for efficient bulk processing.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Callable, Generic, TypeVar


T = TypeVar("T")
R = TypeVar("R")


class BatchAccumulator(Generic[T]):
    """
    Accumulate items and flush when batch is ready.

    Supports both count-based and time-based flushing.
    """

    def __init__(
        self,
        batch_size: int = 100,
        flush_interval: float = 1.0,
        on_flush: Callable[[list[T]], None] | None = None,
    ):
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.on_flush = on_flush

        self._items: list[T] = []
        self._lock = threading.Lock()
        self._last_flush = time.time()
        self._closed = False

    def add(self, item: T) -> bool:
        """
        Add item to batch.

        Returns:
            True if batch was flushed
        """
        with self._lock:
            if self._closed:
                return False
            self._items.append(item)
            if self._should_flush():
                self._do_flush()
                return True
            return False

    def add_many(self, items: list[T]) -> int:
        """Add multiple items. Returns number of flushes triggered."""
        flushes = 0
        for item in items:
            if self.add(item):
                flushes += 1
        return flushes

    def _should_flush(self) -> bool:
        if len(self._items) >= self.batch_size:
            return True
        if time.time() - self._last_flush >= self.flush_interval:
            return True
        return False

    def _do_flush(self) -> None:
        if self._items and self.on_flush:
            self.on_flush(list(self._items))
        self._items = []
        self._last_flush = time.time()

    def force_flush(self) -> list[T]:
        """Force flush and return items."""
        with self._lock:
            items = list(self._items)
            self._items = []
            self._last_flush = time.time()
            return items

    def close(self) -> None:
        """Close and flush remaining items."""
        with self._lock:
            self._closed = True
            self._do_flush()

    @property
    def size(self) -> int:
        return len(self._items)


class TimeWindowedAccumulator(Generic[T]):
    """
    Accumulator with time-windowed flushing.

    Groups items by time windows and flushes each window.
    """

    def __init__(
        self,
        window_seconds: float = 10.0,
        on_window_close: Callable[[list[T]], None] | None = None,
    ):
        self.window_seconds = window_seconds
        self.on_window_close = on_window_close
        self._window_start = time.time()
        self._items: list[T] = []
        self._lock = threading.Lock()

    def add(self, item: T) -> list[T] | None:
        """
        Add item, closing window if expired.

        Returns:
            Closed window items if window closed, None otherwise
        """
        with self._lock:
            now = time.time()
            if now - self._window_start >= self.window_seconds:
                closed_items = list(self._items)
                if self.on_window_close and closed_items:
                    self.on_window_close(closed_items)
                self._items = [item]
                self._window_start = now
                return closed_items
            self._items.append(item)
            return None

    def close_window(self) -> list[T]:
        """Manually close current window."""
        with self._lock:
            items = list(self._items)
            self._items = []
            self._window_start = time.time()
            if self.on_window_close and items:
                self.on_window_close(items)
            return items

    @property
    def window_age(self) -> float:
        return time.time() - self._window_start


class CountWindowedAccumulator(Generic[T]):
    """
    Accumulator that groups items into fixed-count windows.

    Each window contains exactly batch_size items.
    """

    def __init__(
        self,
        batch_size: int = 50,
        on_batch: Callable[[list[T]], None] | None = None,
    ):
        self.batch_size = batch_size
        self.on_batch = on_batch
        self._current: list[T] = []
        self._completed: list[list[T]] = []
        self._lock = threading.Lock()

    def add(self, item: T) -> list[T] | None:
        """Add item. Returns completed batch when batch_size reached."""
        with self._lock:
            self._current.append(item)
            if len(self._current) >= self.batch_size:
                batch = list(self._current)
                self._current = []
                self._completed.append(batch)
                if self.on_batch:
                    self.on_batch(batch)
                return batch
            return None

    def add_many(self, items: list[T]) -> list[list[T]]:
        """Add multiple items. Returns completed batches."""
        batches = []
        for item in items:
            batch = self.add(item)
            if batch:
                batches.append(batch)
        return batches

    def flush(self) -> list[T]:
        """Flush current incomplete batch."""
        with self._lock:
            items = list(self._current)
            self._current = []
            return items

    @property
    def pending_count(self) -> int:
        return len(self._current)

    @property
    def completed_count(self) -> int:
        return len(self._completed)


class AdaptiveBatchAccumulator(Generic[T]):
    """
    Adaptive batching that adjusts batch size based on throughput.

    Starts with small batches, grows when throughput is high,
    shrinks during low throughput.
    """

    def __init__(
        self,
        min_size: int = 10,
        max_size: int = 1000,
        target_latency: float = 0.1,
        on_flush: Callable[[list[T]], None] | None = None,
    ):
        self.min_size = min_size
        self.max_size = max_size
        self.target_latency = target_latency
        self.on_flush = on_flush

        self._batch_size = min_size
        self._items: list[T] = []
        self._lock = threading.Lock()
        self._last_flush = time.time()

    def add(self, item: T) -> bool:
        """Add item with adaptive batching."""
        with self._lock:
            self._items.append(item)
            latency = time.time() - self._last_flush

            if latency > self.target_latency * 2 and self._batch_size > self.min_size:
                self._batch_size = max(self.min_size, self._batch_size // 2)
            elif latency < self.target_latency / 2 and self._batch_size < self.max_size:
                self._batch_size = min(self.max_size, int(self._batch_size * 1.5))

            if len(self._items) >= self._batch_size:
                if self.on_flush:
                    self.on_flush(list(self._items))
                self._items = []
                self._last_flush = time.time()
                return True
            return False

    @property
    def current_batch_size(self) -> int:
        return self._batch_size
