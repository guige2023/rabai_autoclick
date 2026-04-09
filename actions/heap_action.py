"""
Heap Action Module

Provides heap data structure implementations for priority queue operations
in UI automation workflows. Supports min-heap, max-heap, and custom comparators.

Author: AI Agent
Version: 1.0.0
"""

from __future__ import annotations

import heapq
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Generic, Iterator, Optional, TypeVar

T = TypeVar("T")


class HeapType(Enum):
    """Heap type enumeration."""
    MIN_HEAP = auto()
    MAX_HEAP = auto()


@dataclass(order=True)
class HeapItem(Generic[T]):
    """Wrapper for heap items with priority."""
    priority: float
    counter: int = field(compare=True)
    value: T = field(compare=False)


class Heap(Generic[T]):
    """
    Heap data structure with configurable comparison.

    Example:
        >>> heap = Heap[int](heap_type=HeapType.MIN_HEAP)
        >>> heap.push(3, priority=1)
        >>> heap.push(1, priority=0)
        >>> heap.push(2, priority=2)
        >>> heap.pop()
        1
    """

    def __init__(
        self,
        heap_type: HeapType = HeapType.MIN_HEAP,
        key: Optional[Callable[[T], float]] = None,
    ) -> None:
        self.heap_type = heap_type
        self.key = key
        self._heap: list[HeapItem[T]] = []
        self._counter = 0

    def push(self, value: T, priority: Optional[float] = None) -> None:
        """Add item to heap with priority."""
        if priority is None:
            if self.key is None:
                raise ValueError("Priority or key function required")
            priority = self.key(value)

        if self.heap_type == HeapType.MAX_HEAP:
            priority = -priority

        item = HeapItem(priority=priority, counter=self._counter, value=value)
        self._counter += 1
        heapq.heappush(self._heap, item)

    def pop(self) -> T:
        """Remove and return highest priority item."""
        if not self._heap:
            raise IndexError("Heap is empty")
        item = heapq.heappop(self._heap)
        return item.value

    def peek(self) -> T:
        """Return highest priority item without removing."""
        if not self._heap:
            raise IndexError("Heap is empty")
        return self._heap[0].value

    def pushpop(self, value: T, priority: Optional[float] = None) -> T:
        """Push value and return oldest if both present."""
        if priority is None:
            if self.key is None:
                raise ValueError("Priority or key function required")
            priority = self.key(value)

        if self.heap_type == HeapType.MAX_HEAP:
            priority = -priority

        item = HeapItem(priority=priority, counter=self._counter, value=value)
        self._counter += 1
        return heapq.heappushpop(self._heap, item).value

    def heappushpop(self, value: T, priority: Optional[float] = None) -> T:
        """Push value and return result of pop if applicable."""
        return self.pushpop(value, priority)

    def replace(self, value: T, priority: Optional[float] = None) -> T:
        """Return top and replace with new value."""
        if priority is None:
            if self.key is None:
                raise ValueError("Priority or key function required")
            priority = self.key(value)

        if self.heap_type == HeapType.MAX_HEAP:
            priority = -priority

        item = HeapItem(priority=priority, counter=self._counter, value=value)
        self._counter += 1
        return heapq.heapreplace(self._heap, item).value

    def __len__(self) -> int:
        return len(self._heap)

    def __bool__(self) -> bool:
        return bool(self._heap)

    def __iter__(self) -> Iterator[T]:
        return (item.value for item in self._heap)

    def __repr__(self) -> str:
        return f"Heap({self.heap_type.name}, size={len(self._heap)})"


class PriorityQueue(Generic[T]):
    """
    Priority queue implementation using heap.

    Example:
        >>> pq = PriorityQueue[str](max_heap=True)
        >>> pq.enqueue("task_a", priority=1)
        >>> pq.enqueue("task_b", priority=3)
        >>> pq.enqueue("task_c", priority=2)
        >>> pq.dequeue()
        'task_b'
    """

    def __init__(self, max_heap: bool = False) -> None:
        self._heap = Heap[T](
            heap_type=HeapType.MAX_HEAP if max_heap else HeapType.MIN_HEAP
        )

    def enqueue(self, value: T, priority: float) -> None:
        """Add item with priority."""
        self._heap.push(value, priority)

    def dequeue(self) -> T:
        """Remove and return highest priority item."""
        return self._heap.pop()

    def peek(self) -> T:
        """Return highest priority item without removing."""
        return self._heap.peek()

    def is_empty(self) -> bool:
        """Check if queue is empty."""
        return len(self._heap) == 0

    def size(self) -> int:
        """Get number of items."""
        return len(self._heap)

    def clear(self) -> None:
        """Remove all items."""
        self._heap._heap.clear()

    def __len__(self) -> int:
        return len(self._heap)

    def __bool__(self) -> bool:
        return bool(self._heap)

    def __repr__(self) -> str:
        return f"PriorityQueue(size={len(self._heap)})"


class HeapSort:
    """Heap sort implementation."""

    @staticmethod
    def sort(
        items: list[T],
        key: Optional[Callable[[T], float]] = None,
        reverse: bool = False,
    ) -> list[T]:
        """
        Sort items using heap sort.

        Example:
            >>> HeapSort.sort([3, 1, 4, 1, 5])
            [1, 1, 3, 4, 5]
        """
        if len(items) <= 1:
            return list(items)

        result = list(items)

        def get_priority(x: T) -> float:
            if key is None:
                if isinstance(x, (int, float)):
                    return float(x)
                raise ValueError("Non-numeric items require key function")
            return key(x)

        heap = [(get_priority(x), i, x) for i, x in enumerate(result)]
        heapq.heapify(heap)

        sorted_items = []
        for _ in range(len(heap)):
            _, _, value = heapq.heappop(heap)
            sorted_items.append(value)

        if reverse:
            sorted_items.reverse()

        return sorted_items


class MedianHeap(Generic[T]):
    """
    Dual heap structure for maintaining median.

    Example:
        >>> median = MedianHeap[int]()
        >>> for x in [1, 2, 3, 4, 5]:
        ...     median.add(x)
        >>> median.get_median()
        3
    """

    def __init__(self) -> None:
        self._max_heap: list[tuple[float, int, T]] = []
        self._min_heap: list[tuple[float, int, T]] = []
        self._counter = 0

    def add(self, value: T, priority: Optional[float] = None) -> None:
        """Add value to median heap."""
        if priority is None:
            priority = float(value)

        self._counter += 1

        if not self._max_heap or priority <= -self._max_heap[0][2]:
            heapq.heappush(self._max_heap, (-priority, self._counter, value))
        else:
            heapq.heappush(self._min_heap, (priority, self._counter, value))

        self._rebalance()

    def _rebalance(self) -> None:
        """Balance heaps to maintain size property."""
        if len(self._max_heap) > len(self._min_heap) + 1:
            _, _, value = heapq.heappop(self._max_heap)
            priority = -(-value if isinstance(value, (int, float)) else value)
            heapq.heappush(self._min_heap, (abs(priority), self._counter, value))
        elif len(self._min_heap) > len(self._max_heap) + 1:
            _, _, value = heapq.heappop(self._min_heap)
            priority = float(value) if isinstance(value, (int, float)) else 0
            heapq.heappush(self._max_heap, (-priority, self._counter, value))

    def get_median(self) -> Optional[float]:
        """Get current median value."""
        if not self._max_heap and not self._min_heap:
            return None

        if len(self._max_heap) > len(self._min_heap):
            return float(self._max_heap[0][2])
        elif len(self._min_heap) > len(self._max_heap):
            return float(self._min_heap[0][2])
        else:
            max_val = -self._max_heap[0][2]
            min_val = self._min_heap[0][2]
            return (max_val + min_val) / 2

    def __len__(self) -> int:
        return len(self._max_heap) + len(self._min_heap)

    def __repr__(self) -> str:
        return f"MedianHeap(total={len(self)})"
