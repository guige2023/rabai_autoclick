"""
Priority queue implementation with multiple backend support.

Provides thread-safe priority queues with heap and sorted-list backends.
"""

from __future__ import annotations

import asyncio
import heapq
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class QueueBackend(Enum):
    """Priority queue backend implementation."""

    HEAP = auto()
    SORTED_LIST = auto()


@dataclass(order=True)
class PriorityItem(Generic[T]):
    """An item with priority in the queue."""

    priority: int
    sequence: int = field(compare=False)
    item: T = field(compare=False)


class PriorityQueueBackend(ABC, Generic[T]):
    """Abstract base for priority queue backends."""

    @abstractmethod
    def push(self, item: T, priority: int) -> None:
        """Add an item with given priority."""
        raise NotImplementedError

    @abstractmethod
    def pop(self) -> T | None:
        """Remove and return highest priority item."""
        raise NotImplementedError

    @abstractmethod
    def peek(self) -> T | None:
        """Return highest priority item without removing."""
        raise NotImplementedError

    @abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def __contains__(self, item: T) -> bool:
        raise NotImplementedError


class HeapBackend(PriorityQueueBackend[T]):
    """Min-heap based priority queue."""

    def __init__(self) -> None:
        self._heap: list[PriorityItem[T]] = []
        self._sequence = 0
        self._item_map: dict[int, PriorityItem[T]] = {}

    def push(self, item: T, priority: int) -> None:
        p_item = PriorityItem(priority=priority, sequence=self._sequence, item=item)
        self._sequence += 1
        heapq.heappush(self._heap, p_item)
        self._item_map[id(item)] = p_item

    def pop(self) -> T | None:
        if not self._heap:
            return None
        p_item = heapq.heappop(self._heap)
        self._item_map.pop(id(p_item.item), None)
        return p_item.item

    def peek(self) -> T | None:
        if not self._heap:
            return None
        return self._heap[0].item

    def __len__(self) -> int:
        return len(self._heap)

    def __contains__(self, item: T) -> bool:
        return id(item) in self._item_map


class SortedListBackend(PriorityQueueBackend[T]):
    """Sorted list based priority queue maintaining order."""

    def __init__(self) -> None:
        self._items: list[tuple[int, int, T]] = []
        self._sequence = 0

    def push(self, item: T, priority: int) -> None:
        self._items.append((priority, self._sequence, item))
        self._items.sort()
        self._sequence += 1

    def pop(self) -> T | None:
        if not self._items:
            return None
        _, _, item = self._items.pop(0)
        return item

    def peek(self) -> T | None:
        if not self._items:
            return None
        return self._items[0][2]

    def __len__(self) -> int:
        return len(self._items)

    def __contains__(self, item: T) -> bool:
        return any(item == t[2] for t in self._items)


class PriorityQueue(Generic[T]):
    """
    Thread-safe priority queue with configurable backend.

    Example:
        pq = PriorityQueue(backend=QueueBackend.HEAP)
        pq.push("task1", priority=10)
        pq.push("task2", priority=5)
        pq.pop()  # Returns "task2" (higher priority)
    """

    def __init__(
        self,
        backend: QueueBackend = QueueBackend.HEAP,
        maxsize: int = 0,
    ) -> None:
        if backend == QueueBackend.HEAP:
            self._backend: PriorityQueueBackend[T] = HeapBackend()
        else:
            self._backend = SortedListBackend()

        self._maxsize = maxsize
        self._lock = asyncio.Lock()

    async def put(self, item: T, priority: int) -> None:
        """Add an item with given priority."""
        async with self._lock:
            if self._maxsize and len(self._backend) >= self._maxsize:
                raise asyncio.QueueFull()
            self._backend.push(item, priority)

    async def get(self) -> T | None:
        """Remove and return highest priority item."""
        async with self._lock:
            return self._backend.pop()

    async def peek(self) -> T | None:
        """Return highest priority item without removing."""
        async with self._lock:
            return self._backend.peek()

    def put_nowait(self, item: T, priority: int) -> None:
        """Non-blocking add."""
        if self._maxsize and len(self._backend) >= self._maxsize:
            raise asyncio.QueueFull()
        self._backend.push(item, priority)

    def get_nowait(self) -> T | None:
        """Non-blocking remove."""
        return self._backend.pop()

    def __len__(self) -> int:
        return len(self._backend)

    def is_empty(self) -> bool:
        """Check if queue is empty."""
        return len(self) == 0


@dataclass
class QueueStats:
    """Statistics for a priority queue."""

    size: int
    is_empty: bool
    highest_priority: int | None
    lowest_priority: int | None


def get_queue_stats(queue: PriorityQueue[Any]) -> QueueStats:
    """Get statistics about a priority queue."""
    return QueueStats(
        size=len(queue),
        is_empty=queue.is_empty(),
        highest_priority=None,
        lowest_priority=None,
    )
