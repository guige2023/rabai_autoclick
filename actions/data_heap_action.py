"""
Heap data structure implementation for priority operations.

Provides min-heap, max-heap, and custom comparator support for
efficient priority queue operations in automation pipelines.

Author: Aito Auto Agent
"""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Generic,
    Optional,
    TypeVar,
)


T = TypeVar('T')


class HeapType(Enum):
    """Heap type enumeration."""
    MIN_HEAP = auto()
    MAX_HEAP = auto()


@dataclass(order=True)
class HeapItem(Generic[T]):
    """
    Wrapper for items in the heap with priority.

    Uses priority for ordering, stores the actual item separately.
    """
    priority: float
    item: T = field(compare=False)
    sequence: int = field(default=0, compare=True)


class HeapStrategy(ABC):
    """Abstract base class for heap comparison strategies."""

    @abstractmethod
    def compare(self, a: float, b: float) -> bool:
        """Return True if a should be placed higher than b."""
        pass


class MinHeapStrategy(HeapStrategy):
    """Min-heap: smaller values have higher priority."""

    def compare(self, a: float, b: float) -> bool:
        return a < b


class MaxHeapStrategy(HeapStrategy):
    """Max-heap: larger values have higher priority."""

    def compare(self, a: float, b: float) -> bool:
        return a > b


class CustomHeapStrategy(HeapStrategy):
    """Custom comparison strategy using a provided function."""

    def __init__(self, compare_func: Callable[[float, float], bool]):
        self._compare_func = compare_func

    def compare(self, a: float, b: float) -> bool:
        return self._compare_func(a, b)


class Heap(Generic[T]):
    """
    Binary heap implementation with configurable comparison.

    Supports min-heap, max-heap, and custom comparison strategies.
    Thread-safe operations with optional locking.

    Example:
        # Min-heap for smallest items first
        min_heap = Heap[int](heap_type=HeapType.MIN_HEAP)
        min_heap.push(3, 3)
        min_heap.push(1, 1)
        min_heap.push(2, 2)
        assert min_heap.pop() == 1

        # Max-heap for largest items first
        max_heap = Heap[int](heap_type=HeapType.MAX_HEAP)
        max_heap.push(3, 3)
        max_heap.push(1, 1)
        assert max_heap.pop() == 3

        # Custom priority function
        custom = Heap[str](strategy=CustomHeapStrategy(lambda a, b: abs(a - 5) < abs(b - 5)))
    """

    def __init__(
        self,
        heap_type: Optional[HeapType] = None,
        strategy: Optional[HeapStrategy] = None,
        thread_safe: bool = True
    ):
        if strategy is not None:
            self._strategy = strategy
        elif heap_type == HeapType.MAX_HEAP:
            self._strategy = MaxHeapStrategy()
        else:
            self._strategy = MinHeapStrategy()

        self._heap: list[HeapItem[T]] = []
        self._sequence = 0
        self._lock = threading.RLock() if thread_safe else None

    def _acquire(self) -> Optional[threading.RLock]:
        """Get lock if thread-safe mode is enabled."""
        return self._lock

    def push(self, priority: float, item: T) -> None:
        """
        Add an item to the heap.

        Args:
            priority: Priority value (interpretation depends on strategy)
            item: The item to store
        """
        with self._acquire():
            heap_item = HeapItem(priority=priority, item=item, sequence=self._sequence)
            self._sequence += 1
            self._heap.append(heap_item)
            self._sift_up(len(self._heap) - 1)

    def pop(self) -> Optional[T]:
        """
        Remove and return the highest priority item.

        Returns:
            The item with highest priority, or None if heap is empty
        """
        with self._acquire():
            if not self._heap:
                return None

            result = self._heap[0].item
            last = self._heap.pop()

            if self._heap:
                self._heap[0] = last
                self._sift_down(0)

            return result

    def peek(self) -> Optional[T]:
        """
        Return the highest priority item without removing it.

        Returns:
            The item with highest priority, or None if heap is empty
        """
        with self._acquire():
            if not self._heap:
                return None
            return self._heap[0].item

    def push_pop(self, priority: float, item: T) -> Optional[T]:
        """
        Atomically push a new item and pop the highest priority.

        More efficient than separate push and pop operations.

        Returns:
            The popped highest priority item
        """
        with self._acquire():
            if not self._heap:
                self.push(priority, item)
                return None

            result = self._heap[0].item
            self._heap[0] = HeapItem(priority=priority, item=item, sequence=self._sequence)
            self._sequence += 1
            self._sift_down(0)
            return result

    def replace(self, priority: float, item: T) -> Optional[T]:
        """
        Replace the top item and return the old one.

        Returns:
            The previous top item
        """
        with self._acquire():
            if not self._heap:
                self.push(priority, item)
                return None

            result = self._heap[0].item
            self._heap[0] = HeapItem(priority=priority, item=item, sequence=self._sequence)
            self._sequence += 1
            self._sift_down(0)
            return result

    def _parent(self, index: int) -> int:
        """Get parent index."""
        return (index - 1) // 2

    def _left_child(self, index: int) -> int:
        """Get left child index."""
        return 2 * index + 1

    def _right_child(self, index: int) -> int:
        """Get right child index."""
        return 2 * index + 2

    def _sift_up(self, index: int) -> None:
        """Move item up to maintain heap property."""
        while index > 0:
            parent = self._parent(index)
            if self._should_swap(parent, index):
                self._heap[parent], self._heap[index] = self._heap[index], self._heap[parent]
                index = parent
            else:
                break

    def _sift_down(self, index: int) -> None:
        """Move item down to maintain heap property."""
        size = len(self._heap)

        while True:
            smallest = index
            left = self._left_child(index)
            right = self._right_child(index)

            if left < size and self._should_swap(left, smallest):
                smallest = left

            if right < size and self._should_swap(right, smallest):
                smallest = right

            if smallest != index:
                self._heap[index], self._heap[smallest] = self._heap[smallest], self._heap[index]
                index = smallest
            else:
                break

    def _should_swap(self, a: int, b: int) -> bool:
        """Determine if position a should swap with position b."""
        item_a = self._heap[a]
        item_b = self._heap[b]

        if self._strategy.compare(item_a.priority, item_b.priority):
            return True
        if item_a.priority == item_b.priority:
            return item_a.sequence > item_b.sequence
        return False

    def __len__(self) -> int:
        """Return the number of items in the heap."""
        with self._acquire():
            return len(self._heap)

    def __bool__(self) -> bool:
        """Return True if heap is not empty."""
        with self._acquire():
            return bool(self._heap)

    def clear(self) -> None:
        """Remove all items from the heap."""
        with self._acquire():
            self._heap.clear()

    def to_list(self) -> list[T]:
        """Return all items in priority order (does not modify heap)."""
        with self._acquire():
            items = []
            temp_heap = self._heap.copy()

            while temp_heap:
                items.append(temp_heap[0].item)
                temp_heap.pop(0)

            return items


class PriorityQueue(Generic[T]):
    """
    Thread-safe priority queue built on top of Heap.

    Provides a simpler interface for queue-style operations
    with FIFO ordering for items of equal priority.

    Example:
        queue = PriorityQueue[str](max_size=100)
        queue.enqueue("low", "task1")
        queue.enqueue("high", "task2")
        queue.enqueue("medium", "task3")
        assert queue.dequeue() == "task2"
    """

    def __init__(
        self,
        heap_type: Optional[HeapType] = None,
        max_size: Optional[int] = None
    ):
        self._heap = Heap[T](heap_type=heap_type, thread_safe=True)
        self._max_size = max_size

    def enqueue(self, priority: float, item: T) -> bool:
        """
        Add an item to the queue.

        Args:
            priority: Item priority
            item: Item to enqueue

        Returns:
            True if enqueued, False if queue is full
        """
        if self._max_size is not None and len(self._heap) >= self._max_size:
            return False
        self._heap.push(priority, item)
        return True

    def dequeue(self) -> Optional[T]:
        """
        Remove and return the highest priority item.

        Returns:
            The highest priority item, or None if queue is empty
        """
        return self._heap.pop()

    def peek(self) -> Optional[T]:
        """Return the highest priority item without removing it."""
        return self._heap.peek()

    def __len__(self) -> int:
        return len(self._heap)

    def __bool__(self) -> bool:
        return bool(self._heap)

    def is_full(self) -> bool:
        """Return True if queue is at capacity."""
        if self._max_size is None:
            return False
        return len(self._heap) >= self._max_size


class HeapSorter:
    """Utility class for heap sort operations."""

    @staticmethod
    def heap_sort(items: list[tuple[float, T]], reverse: bool = False) -> list[T]:
        """
        Sort items using heap sort.

        Args:
            items: List of (priority, item) tuples
            reverse: If True, sort in descending order

        Returns:
            List of items in sorted order
        """
        heap_type = HeapType.MAX_HEAP if reverse else HeapType.MIN_HEAP
        heap = Heap[T](heap_type=heap_type, thread_safe=False)

        for priority, item in items:
            heap.push(priority, item)

        result = []
        while heap:
            result.append(heap.pop())

        return result


def create_heap(
    heap_type: Optional[HeapType] = None,
    thread_safe: bool = True
) -> Heap:
    """Factory function to create a Heap."""
    return Heap(heap_type=heap_type, thread_safe=thread_safe)


def create_priority_queue(
    heap_type: Optional[HeapType] = None,
    max_size: Optional[int] = None
) -> PriorityQueue:
    """Factory function to create a PriorityQueue."""
    return PriorityQueue(heap_type=heap_type, max_size=max_size)
