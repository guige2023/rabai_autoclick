"""
Bounded queue implementations with blocking semantics.

Provides thread-safe bounded queues with
put/get blocking and timeout support.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections import deque
from typing import Generic, TypeVar


T = TypeVar("T")


class BoundedQueue(Generic[T]):
    """
    Thread-safe bounded queue with blocking operations.
    """

    def __init__(self, capacity: int):
        if capacity <= 0:
            raise ValueError(f"Capacity must be positive, got {capacity}")
        self.capacity = capacity
        self._queue: deque[T] = deque()
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)
        self._closed = False

    def put(self, item: T, block: bool = True, timeout: float | None = None) -> bool:
        """
        Put item into queue.

        Args:
            item: Item to enqueue
            block: Block if full
            timeout: Max wait time (None = wait forever)

        Returns:
            True if enqueued, False if closed
        """
        deadline = time.time() + timeout if timeout else None

        with self._not_full:
            if self._closed:
                return False
            if not block:
                if len(self._queue) >= self.capacity:
                    return False
            while len(self._queue) >= self.capacity and not self._closed:
                remaining = deadline - time.time() if deadline else None
                if remaining is not None and remaining <= 0:
                    return False
                if not self._not_full.wait(remaining if remaining else None):
                    return False
            if self._closed:
                return False
            self._queue.append(item)
            self._not_empty.notify()
            return True

    def get(self, block: bool = True, timeout: float | None = None) -> T | None:
        """
        Get item from queue.

        Args:
            block: Block if empty
            timeout: Max wait time

        Returns:
            Item or None if closed/timeout
        """
        deadline = time.time() + timeout if timeout else None

        with self._not_empty:
            if self._closed and not self._queue:
                return None
            if not block:
                if not self._queue:
                    return None
            while not self._queue and not self._closed:
                remaining = deadline - time.time() if deadline else None
                if remaining is not None and remaining <= 0:
                    return None
                if not self._not_empty.wait(remaining if remaining else None):
                    return None
            if not self._queue:
                return None
            item = self._queue.popleft()
            self._not_full.notify()
            return item

    def close(self) -> None:
        """Close queue and unblock waiters."""
        with self._lock:
            self._closed = True
            self._not_empty.notify_all()
            self._not_full.notify_all()

    @property
    def qsize(self) -> int:
        """Current queue size."""
        with self._lock:
            return len(self._queue)

    @property
    def is_closed(self) -> bool:
        return self._closed

    @property
    def is_full(self) -> bool:
        with self._lock:
            return len(self._queue) >= self.capacity

    @property
    def is_empty(self) -> bool:
        with self._lock:
            return len(self._queue) == 0

    def peek(self) -> T | None:
        """View first item without removing."""
        with self._lock:
            return self._queue[0] if self._queue else None

    def clear(self) -> None:
        """Remove all items."""
        with self._lock:
            self._queue.clear()
            self._not_full.notify_all()


class AsyncBoundedQueue(Generic[T]):
    """Async version of BoundedQueue."""

    def __init__(self, capacity: int):
        if capacity <= 0:
            raise ValueError(f"Capacity must be positive, got {capacity}")
        self.capacity = capacity
        self._queue: asyncio.Queue[T] = asyncio.Queue(maxsize=capacity)

    async def put(self, item: T) -> None:
        await self._queue.put(item)

    async def get(self) -> T:
        return await self._queue.get()

    def close(self) -> None:
        self._queue.task_done()

    @property
    def qsize(self) -> int:
        return self._queue.qsize()

    @property
    def is_full(self) -> bool:
        return self._queue.full()

    @property
    def is_empty(self) -> bool:
        return self._queue.empty()


class PriorityBoundedQueue(Generic[T]):
    """Bounded queue with priority ordering."""

    def __init__(self, capacity: int, key: Callable[[T], float] = lambda x: x):
        self.capacity = capacity
        self.key = key
        self._lock = threading.Lock()
        self._heap: list[T] = []
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)
        self._closed = False

    def put(self, item: T, block: bool = True, timeout: float | None = None) -> bool:
        deadline = time.time() + timeout if timeout else None
        with self._not_full:
            if self._closed:
                return False
            if not block and len(self._heap) >= self.capacity:
                return False
            while len(self._heap) >= self.capacity and not self._closed:
                remaining = deadline - time.time() if deadline else None
                if remaining and remaining <= 0:
                    return False
                if not self._not_full.wait(remaining):
                    return False
            if self._closed:
                return False
            import heapq
            heapq.heappush(self._heap, (self.key(item), item))
            self._not_empty.notify()
            return True

    def get(self, block: bool = True, timeout: float | None = None) -> T | None:
        deadline = time.time() + timeout if timeout else None
        with self._not_empty:
            if self._closed and not self._heap:
                return None
            if not block and not self._heap:
                return None
            while not self._heap and not self._closed:
                remaining = deadline - time.time() if deadline else None
                if remaining and remaining <= 0:
                    return None
                if not self._not_empty.wait(remaining):
                    return None
            if not self._heap:
                return None
            import heapq
            _, item = heapq.heappop(self._heap)
            self._not_full.notify()
            return item

    @property
    def is_closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._not_empty.notify_all()
            self._not_full.notify_all()
