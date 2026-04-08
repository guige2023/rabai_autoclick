"""
Heap data structure implementations and utilities.

Provides binary heap, min-heap, max-heap, and
priority queue implementations with various operations.

Example:
    >>> from utils.heap_utils_v2 import BinaryHeap, PriorityQueue
    >>> heap = BinaryHeap(min_heap=True)
    >>> heap.push(3)
    >>> heap.pop()
    3
"""

from __future__ import annotations

import heapq
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, List, Optional, TypeVar, Union


T = TypeVar("T")


@dataclass(order=True)
class HeapItem(Generic[T]):
    """Item stored in the heap with priority."""
    priority: float
    sequence: int = field(compare=True)
    value: T = field(compare=False)


class BinaryHeap:
    """
    Binary heap implementation with configurable min/max behavior.

    Provides O(log n) push and pop operations for
    priority queue and scheduling use cases.

    Attributes:
        min_heap: If True, smallest element at top.
    """

    def __init__(self, min_heap: bool = True) -> None:
        """
        Initialize the binary heap.

        Args:
            min_heap: If True, smallest element is at top.
        """
        self.min_heap = min_heap
        self._heap: List[Any] = []

    def push(self, item: Any) -> None:
        """
        Push an item onto the heap.

        Args:
            item: Item to push.
        """
        if not self.min_heap:
            item = -item
        heapq.heappush(self._heap, item)

    def pop(self) -> Any:
        """
        Pop and return the smallest (or largest) item.

        Returns:
            The top item.

        Raises:
            IndexError: If heap is empty.
        """
        if not self.min_heap:
            return -heapq.heappop(self._heap)
        return heapq.heappop(self._heap)

    def peek(self) -> Any:
        """
        Get the top item without removing it.

        Returns:
            The top item.

        Raises:
            IndexError: If heap is empty.
        """
        if not self._heap:
            raise IndexError("peek from empty heap")
        if not self.min_heap:
            return -self._heap[0]
        return self._heap[0]

    def pushpop(self, item: Any) -> Any:
        """
        Push item and pop the smallest/largest in one operation.

        Args:
            item: Item to push.

        Returns:
            The popped item.
        """
        if not self.min_heap:
            item = -item
            result = heapq.heappushpop(self._heap, item)
            return -result
        return heapq.heappushpop(self._heap, item)

    def replace(self, item: Any) -> Any:
        """
        Pop and push in one operation.

        Args:
            item: Item to push.

        Returns:
            The popped item.
        """
        if not self.min_heap:
            item = -item
            result = heapq.heapreplace(self._heap, item)
            return -result
        return heapq.heapreplace(self._heap, item)

    def __len__(self) -> int:
        """Get the number of items in the heap."""
        return len(self._heap)

    def __bool__(self) -> bool:
        """Check if heap is non-empty."""
        return bool(self._heap)

    def __iter__(self) -> Iterator[Any]:
        """Iterate over heap items (not in sorted order)."""
        return iter(self._heap)

    def __repr__(self) -> str:
        return f"BinaryHeap({self._heap!r}, min_heap={self.min_heap})"


class MaxHeap(BinaryHeap):
    """Binary heap where largest element is at the top."""

    def __init__(self) -> None:
        """Initialize a max-heap."""
        super().__init__(min_heap=False)


class MinHeap(BinaryHeap):
    """Binary heap where smallest element is at the top."""

    def __init__(self) -> None:
        """Initialize a min-heap."""
        super().__init__(min_heap=True)


class PriorityQueue(Generic[T]):
    """
    Priority queue implementation using a binary heap.

    Supports priority-based ordering with tie-breaking
    using a sequence counter for FIFO ordering.
    """

    def __init__(self, min_priority: bool = True) -> None:
        """
        Initialize the priority queue.

        Args:
            min_priority: If True, lowest priority value is at top.
        """
        self.min_priority = min_priority
        self._heap: List[HeapItem[T]] = []
        self._counter = 0

    def enqueue(
        self,
        value: T,
        priority: float = 0.0,
    ) -> None:
        """
        Add an item to the queue.

        Args:
            value: Item value.
            priority: Priority value (lower = higher priority if min_priority).
        """
        heap_item = HeapItem(
            priority=priority,
            sequence=self._counter,
            value=value,
        )
        self._counter += 1

        if not self.min_priority:
            heap_item.priority = -heap_item.priority

        heapq.heappush(self._heap, heap_item)

    def dequeue(self) -> T:
        """
        Remove and return the highest priority item.

        Returns:
            The dequeued item value.

        Raises:
            IndexError: If queue is empty.
        """
        if not self._heap:
            raise IndexError("dequeue from empty priority queue")
        item = heapq.heappop(self._heap)
        return item.value

    def peek(self) -> T:
        """
        Get the highest priority item without removing it.

        Returns:
            The top item value.

        Raises:
            IndexError: If queue is empty.
        """
        if not self._heap:
            raise IndexError("peek from empty priority queue")
        return self._heap[0].value

    def update(
        self,
        value: T,
        new_priority: float,
        key: Optional[Callable[[T], float]] = None,
    ) -> bool:
        """
        Update the priority of an item.

        Args:
            value: Item value to update.
            new_priority: New priority value.
            key: Function to extract priority from value.

        Returns:
            True if item was found and updated.
        """
        for item in self._heap:
            if item.value == value:
                if not self.min_priority:
                    new_priority = -new_priority
                item.priority = new_priority
                heapq.heapify(self._heap)
                return True
        return False

    def __len__(self) -> int:
        """Get the number of items in the queue."""
        return len(self._heap)

    def __bool__(self) -> bool:
        """Check if queue is non-empty."""
        return bool(self._heap)

    def __contains__(self, value: T) -> bool:
        """Check if value is in the queue."""
        return any(item.value == value for item in self._heap)

    def __iter__(self) -> Iterator[T]:
        """Iterate over items (not in priority order)."""
        return (item.value for item in self._heap)


class HeapItemWithExpiry:
    """Heap item that expires after a given time."""

    def __init__(
        self,
        expiry_time: float,
        value: Any,
    ) -> None:
        """
        Initialize the expiring item.

        Args:
            expiry_time: Time when item expires (from time.time()).
            value: Item value.
        """
        self.expiry_time = expiry_time
        self.value = value

    def __lt__(self, other: "HeapItemWithExpiry") -> bool:
        """Compare by expiry time."""
        return self.expiry_time < other.expiry_time

    def is_expired(self) -> bool:
        """Check if this item has expired."""
        import time
        return time.time() >= self.expiry_time


class TimedPriorityQueue(Generic[T]):
    """
    Priority queue with time-based item expiration.

    Items automatically expire and are skipped during dequeue.
    """

    def __init__(self, min_priority: bool = True) -> None:
        """Initialize the timed priority queue."""
        self.min_priority = min_priority
        self._heap: List[HeapItemWithExpiry] = []

    def schedule(
        self,
        value: T,
        delay: float,
        priority: float = 0.0,
    ) -> None:
        """
        Schedule an item to be available after a delay.

        Args:
            value: Item value.
            delay: Delay in seconds before item is available.
            priority: Priority value.
        """
        import time
        expiry_time = time.time() + delay
        heap_item = HeapItemWithExpiry(expiry_time, (priority, value))
        heapq.heappush(self._heap, heap_item)

    def dequeue(self, block: bool = True, timeout: Optional[float] = None) -> Optional[T]:
        """
        Dequeue the next available item.

        Args:
            block: If True, wait for an item.
            timeout: Maximum wait time.

        Returns:
            The dequeued item value or None.
        """
        import time

        if not self._heap:
            if not block:
                return None
            if timeout is not None:
                end_time = time.time() + timeout
                while not self._heap:
                    remaining = end_time - time.time()
                    if remaining <= 0:
                        return None
                    time.sleep(min(remaining, 0.1))
            else:
                while not self._heap:
                    time.sleep(0.1)

        while self._heap:
            item = heapq.heappop(self._heap)
            if not item.is_expired():
                return item.value[1]

        return None

    def __len__(self) -> int:
        """Get the number of scheduled items."""
        return len(self._heap)


def heap_sort(items: List[Any], min_heap: bool = True) -> List[Any]:
    """
    Sort items using a heap.

    Args:
        items: Items to sort.
        min_heap: If True, sort in ascending order.

    Returns:
        Sorted list.
    """
    result = list(items)
    heapq.heapify(result)

    if not min_heap:
        result = [-x for x in result]

    sorted_result: List[Any] = []
    while result:
        sorted_result.append(heapq.heappop(result))

    if not min_heap:
        sorted_result = [-x for x in sorted_result]

    return sorted_result


def k_smallest(items: List[Any], k: int) -> List[Any]:
    """
    Find the k smallest elements.

    Args:
        items: List of items.
        k: Number of smallest elements to find.

    Returns:
        List of k smallest elements.
    """
    return heapq.nsmallest(k, items)


def k_largest(items: List[Any], k: int) -> List[Any]:
    """
    Find the k largest elements.

    Args:
        items: List of items.
        k: Number of largest elements to find.

    Returns:
        List of k largest elements.
    """
    return heapq.nlargest(k, items)
