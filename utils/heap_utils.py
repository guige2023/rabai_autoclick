"""Heap utilities for RabAI AutoClick.

Provides:
- Min/max heap operations
- Priority queue implementation
- K-largest / K-smallest helpers
- Heap sort and merge
"""

from __future__ import annotations

import heapq
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
)

import threading


T = TypeVar("T")


class PriorityQueue(Generic[T]):
    """Thread-safe priority queue.

    Items with lower priority value are dequeued first.
    """

    def __init__(self) -> None:
        self._heap: List[Tuple[int, T]] = []
        self._lock = threading.Lock()
        self._counter = 0

    def push(self, item: T, priority: int = 0) -> None:
        """Add an item with a priority.

        Args:
            item: Item to enqueue.
            priority: Priority value (lower = higher priority).
        """
        with self._lock:
            heapq.heappush(self._heap, (priority, self._counter, item))
            self._counter += 1

    def pop(self) -> Optional[T]:
        """Remove and return the highest-priority item.

        Returns:
            Item or None if queue is empty.
        """
        with self._lock:
            if not self._heap:
                return None
            _, _, item = heapq.heappop(self._heap)
            return item

    def peek(self) -> Optional[T]:
        """View the highest-priority item without removing.

        Returns:
            Item or None if empty.
        """
        with self._lock:
            if not self._heap:
                return None
            return self._heap[0][2]

    def __len__(self) -> int:
        return len(self._heap)

    def __bool__(self) -> bool:
        return len(self._heap) > 0

    def clear(self) -> None:
        """Remove all items."""
        with self._lock:
            self._heap.clear()
            self._counter = 0


def k_smallest(
    items: List[T],
    k: int,
    key: Optional[Callable[[T], Any]] = None,
) -> List[T]:
    """Get the k smallest items.

    Args:
        items: List of items.
        k: Number of smallest items to return.
        key: Optional key function for comparison.

    Returns:
        List of k smallest items (unsorted).
    """
    if k >= len(items):
        return items[:]
    return heapq.nsmallest(k, items, key=key)


def k_largest(
    items: List[T],
    k: int,
    key: Optional[Callable[[T], Any]] = None,
) -> List[T]:
    """Get the k largest items.

    Args:
        items: List of items.
        k: Number of largest items to return.
        key: Optional key function for comparison.

    Returns:
        List of k largest items (unsorted).
    """
    if k >= len(items):
        return items[:]
    return heapq.nlargest(k, items, key=key)


def heap_sort(
    items: List[T],
    reverse: bool = False,
) -> List[T]:
    """Sort items using heap sort.

    Args:
        items: Items to sort.
        reverse: If True, sort descending.

    Returns:
        Sorted list.
    """
    result = items[:]
    if reverse:
        heapq._heapify_max(result)  # type: ignore
        for i in range(len(result) - 1, 0, -1):
            result[0], result[i] = result[i], result[0]
            heapq._heapify_max(result[:i])  # type: ignore
    else:
        heapq.heapify(result)
        result = [heapq.heappop(result) for _ in range(len(result))]
    return result


def merge_sorted(
    *iterables: List[T],
    key: Optional[Callable[[T], Any]] = None,
) -> Iterator[T]:
    """Merge multiple sorted iterables.

    Args:
        *iterables: Sorted lists to merge.
        key: Key function for comparison.

    Yields:
        Merged items in sorted order.
    """
    for item in heapq.merge(*iterables, key=key):
        yield item


def heap_push_pop(
    heap: List[T],
    item: T,
) -> Tuple[T, List[T]]:
    """Push item onto heap and pop the smallest atomically.

    Args:
        heap: Existing heap.
        item: Item to push.

    Returns:
        Tuple of (popped_item, new_heap).
    """
    pushed = heap + [item]
    heapq.heapify(pushed)
    popped = heapq.heappop(pushed)
    return popped, pushed


def replace_heap(
    heap: List[T],
    item: T,
) -> Tuple[T, List[T]]:
    """Replace the smallest element of a heap with item.

    Args:
        heap: Existing heap (non-empty).
        item: Replacement item.

    Returns:
        Tuple of (replaced_item, new_heap).
    """
    replaced = heapq.heapreplace(heap, item)
    return replaced, heap


__all__ = [
    "PriorityQueue",
    "k_smallest",
    "k_largest",
    "heap_sort",
    "merge_sorted",
    "heap_push_pop",
    "replace_heap",
]


from typing import Iterator  # noqa: E402
