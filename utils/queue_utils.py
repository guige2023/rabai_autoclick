"""
Priority Queue Utilities

Provides thread-safe priority queues with various features
like item expiration, bulk operations, and statistics.
"""

from __future__ import annotations

import copy
import heapq
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

T = TypeVar("T")


@dataclass
class PriorityItem(Generic[T]):
    """
    Item in a priority queue.

    Priority is ordered by:
    1. Priority (lower number = higher priority)
    2. Timestamp (earlier = higher priority)
    3. Sequence number (FIFO within same priority/time)
    """
    priority: int = 0
    timestamp: float = field(default_factory=time.time)
    sequence: int = 0
    data: T | None = None
    expires_at: float | None = None

    def __lt__(self, other: PriorityItem) -> bool:
        """Compare items for heap ordering."""
        if self.priority != other.priority:
            return self.priority < other.priority
        if abs(self.timestamp - other.timestamp) > 1e-9:
            return self.timestamp < other.timestamp
        return self.sequence < other.sequence

    def is_expired(self) -> bool:
        """Check if item has expired."""
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at


class PriorityQueue(Generic[T]):
    """
    Thread-safe priority queue with expiration support.
    """

    def __init__(self, maxsize: int = 0):
        self._heap: list[PriorityItem[T]] = []
        self._lock = threading.RLock()
        self._maxsize = maxsize
        self._sequence = 0
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)
        self._metrics: dict[str, int] = {
            "put": 0,
            "get": 0,
            "expired": 0,
            "rejected": 0,
        }

    def put(
        self,
        item: T,
        priority: int = 0,
        block: bool = True,
        timeout: float | None = None,
        expires_in: float | None = None,
    ) -> bool:
        """
        Put an item into the queue.

        Args:
            item: Item to add.
            priority: Priority level (lower = higher priority).
            block: Whether to block if queue is full.
            timeout: Timeout in seconds.
            expires_in: Seconds until item expires.

        Returns:
            True if item was added, False if rejected.
        """
        with self._not_full:
            if self._maxsize > 0:
                if not block:
                    if len(self._heap) >= self._maxsize:
                        self._metrics["rejected"] += 1
                        return False

                if timeout is not None:
                    end_time = time.time() + timeout
                    while len(self._heap) >= self._maxsize:
                        remaining = end_time - time.time()
                        if remaining <= 0:
                            self._metrics["rejected"] += 1
                            return False
                        self._not_full.wait(remaining)
                else:
                    while len(self._heap) >= self._maxsize:
                        self._not_full.wait()

            expires_at = None
            if expires_in is not None:
                expires_at = time.time() + expires_in

            priority_item = PriorityItem(
                priority=priority,
                timestamp=time.time(),
                sequence=self._sequence,
                data=item,
                expires_at=expires_at,
            )
            self._sequence += 1

            heapq.heappush(self._heap, priority_item)
            self._metrics["put"] += 1
            self._not_empty.notify()

        return True

    def get(
        self,
        block: bool = True,
        timeout: float | None = None,
        remove_expired: bool = True,
    ) -> T | None:
        """
        Get an item from the queue.

        Args:
            block: Whether to block if queue is empty.
            timeout: Timeout in seconds.
            remove_expired: Whether to automatically remove expired items.

        Returns:
            The item, or None if timeout/blocking failed.
        """
        with self._not_empty:
            if not block:
                if not self._heap:
                    return None
            else:
                if timeout is not None:
                    end_time = time.time() + timeout
                    while not self._heap:
                        remaining = end_time - time.time()
                        if remaining <= 0:
                            return None
                        self._not_empty.wait(remaining)
                else:
                    while not self._heap:
                        self._not_empty.wait()

            # Remove expired items if requested
            if remove_expired:
                self._remove_expired()

            if self._heap:
                item = heapq.heappop(self._heap)
                self._metrics["get"] += 1
                self._not_full.notify()
                return item.data

        return None

    def peek(self) -> T | None:
        """Peek at the next item without removing it."""
        with self._lock:
            if not self._heap:
                return None

            # Remove expired from top
            while self._heap:
                if self._heap[0].is_expired():
                    expired = heapq.heappop(self._heap)
                    self._metrics["expired"] += 1
                else:
                    break

            if self._heap:
                return self._heap[0].data

        return None

    def _remove_expired(self) -> int:
        """Remove all expired items."""
        removed = 0
        while self._heap and self._heap[0].is_expired():
            heapq.heappop(self._heap)
            self._metrics["expired"] += 1
            removed += 1
        return removed

    def size(self) -> int:
        """Get the number of items in the queue."""
        with self._lock:
            return len(self._heap)

    def clear(self) -> None:
        """Clear all items from the queue."""
        with self._lock:
            self._heap.clear()
            self._not_full.notify_all()

    @property
    def metrics(self) -> dict[str, int]:
        """Get queue metrics."""
        return copy.copy(self._metrics)


class PriorityQueueWithCallback(Generic[T]):
    """
    Priority queue that supports notification callbacks.
    """

    def __init__(self, maxsize: int = 0):
        self._queue = PriorityQueue[T](maxsize)
        self._callbacks: list[Callable[[T], None]] = []

    def put(
        self,
        item: T,
        priority: int = 0,
        block: bool = True,
        timeout: float | None = None,
    ) -> bool:
        """Put an item and notify callbacks."""
        result = self._queue.put(item, priority, block, timeout)

        if result:
            for callback in self._callbacks:
                try:
                    callback(item)
                except Exception:
                    pass

        return result

    def get(self, block: bool = True, timeout: float | None = None) -> T | None:
        """Get an item from the queue."""
        return self._queue.get(block, timeout)

    def on_put(self, callback: Callable[[T], None]) -> None:
        """Register a callback for put operations."""
        self._callbacks.append(callback)

    def peek(self) -> T | None:
        """Peek at the next item."""
        return self._queue.peek()

    def size(self) -> int:
        """Get queue size."""
        return self._queue.size()

    def clear(self) -> None:
        """Clear the queue."""
        self._queue.clear()

    @property
    def metrics(self) -> dict[str, int]:
        """Get queue metrics."""
        return self._queue.metrics


def create_priority_queue(
    maxsize: int = 0,
    with_callbacks: bool = False,
) -> PriorityQueue | PriorityQueueWithCallback:
    """
    Create a priority queue.

    Args:
        maxsize: Maximum queue size (0 = unlimited).
        with_callbacks: Whether to create a queue with callbacks.

    Returns:
        Configured priority queue.
    """
    if with_callbacks:
        return PriorityQueueWithCallback(maxsize)
    return PriorityQueue(maxsize)


# Alias for common usage
PriorityQueue.__init__.__doc__ = """
Priority Queue with thread-safe operations.

Args:
    maxsize: Maximum queue size. 0 means unlimited.

Example:
    q = PriorityQueue()
    q.put("task1", priority=1)  # Higher priority
    q.put("task2", priority=10)  # Lower priority
    item = q.get()  # Returns "task1" first
"""
