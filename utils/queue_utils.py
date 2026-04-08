"""Queue utilities for RabAI AutoClick.

Provides:
- Thread-safe queue wrappers
- Bounded queue with overflow handling
- Priority queue helpers
- Queue monitoring
"""

from __future__ import annotations

import queue
import threading
import time
from typing import (
    Any,
    Callable,
    Generic,
    List,
    Optional,
    TypeVar,
)


T = TypeVar("T")


class ThreadSafeQueue(Generic[T]):
    """Thread-safe queue with monitoring.

    Args:
        maxsize: Maximum queue size (0 = unlimited).
    """

    def __init__(self, maxsize: int = 0) -> None:
        self._queue: queue.Queue[T] = queue.Queue(maxsize=maxsize)
        self._put_times: List[float] = []
        self._get_times: List[float] = []

    def put(self, item: T, timeout: Optional[float] = None) -> None:
        """Put an item in the queue.

        Args:
            item: Item to enqueue.
            timeout: Max seconds to wait.

        Raises:
            queue.Full: If queue is full and timeout expires.
        """
        self._queue.put(item, timeout=timeout)
        self._put_times.append(time.monotonic())

    def get(self, timeout: Optional[float] = None) -> T:
        """Get an item from the queue.

        Args:
            timeout: Max seconds to wait.

        Returns:
            Dequeued item.

        Raises:
            queue.Empty: If queue is empty and timeout expires.
        """
        item = self._queue.get(timeout=timeout)
        self._get_times.append(time.monotonic())
        return item

    def try_put(self, item: T) -> bool:
        """Try to put an item without blocking.

        Args:
            item: Item to enqueue.

        Returns:
            True if item was queued.
        """
        try:
            self._queue.put_nowait(item)
            return True
        except queue.Full:
            return False

    def try_get(self) -> Optional[T]:
        """Try to get an item without blocking.

        Returns:
            Item or None if queue is empty.
        """
        try:
            return self._queue.get_nowait()
        except queue.Empty:
            return None

    @property
    def qsize(self) -> int:
        """Current queue size."""
        return self._queue.qsize()

    @property
    def empty(self) -> bool:
        """Whether queue is empty."""
        return self._queue.empty()

    @property
    def full(self) -> bool:
        """Whether queue is full."""
        return self._queue.full()

    @property
    def stats(self) -> dict:
        """Get queue statistics."""
        return {
            "qsize": self.qsize,
            "empty": self.empty,
            "full": self.full,
        }


class DroppingQueue(Generic[T]):
    """Queue that drops oldest items when full.

    Args:
        maxsize: Maximum size.
    """

    def __init__(self, maxsize: int = 100) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be positive")
        self._queue: List[T] = []
        self._maxsize = maxsize
        self._lock = threading.Lock()

    def put(self, item: T) -> None:
        """Put an item (drops oldest if full)."""
        with self._lock:
            if len(self._queue) >= self._maxsize:
                self._queue.pop(0)
            self._queue.append(item)

    def get(self) -> Optional[T]:
        """Get an item from the front."""
        with self._lock:
            if not self._queue:
                return None
            return self._queue.pop(0)

    def peek(self) -> Optional[T]:
        """View the first item."""
        with self._lock:
            if not self._queue:
                return None
            return self._queue[0]

    def __len__(self) -> int:
        return len(self._queue)


__all__ = [
    "ThreadSafeQueue",
    "DroppingQueue",
]
