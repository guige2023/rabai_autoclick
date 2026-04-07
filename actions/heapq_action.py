"""Heapq action for rabai_autoclick.

Provides heap operations using Python's heapq module,
including min/max heaps, priority queues, and k-way merges.
"""

from __future__ import annotations

import heapq
from heapq import (
    heapify,
    heappop,
    heappush,
    heapreplace,
    nlargest,
    nsmallest,
)
from typing import Any, Callable, Generic, Iterable, Iterator, TypeVar

__all__ = [
    "heapify",
    "heappop",
    "heappush",
    "heapreplace",
    "nlargest",
    "nsmallest",
    "heap_min",
    "heap_max",
    "heap_peek",
    "heap_push_pop",
    "heap_replace",
    "heap_push",
    "heap_to_list",
    "list_to_heap",
    "heap_sort",
    "heap_insert",
    "heap_delete",
    "heap_merge",
    "heap_k_smallest",
    "heap_k_largest",
    "heap_contains",
    "heap_update",
    "heappushpop",
    "heapify_max",
    "heappop_max",
    "heappush_max",
    "heapreplace_max",
    "heap_min_max",
    "heap_median",
    "heap_balanced_merge",
    "heap_zip_sorted",
    "heap_dedup",
    "heap_intersection",
    "heap_union",
    "MinHeap",
    "MaxHeap",
    "PriorityQueue",
    "IndexedHeap",
    "HeapItem",
    "HeapStats",
    "KWayMerge",
]


T = TypeVar("T")


def heap_min(heap: list[T]) -> T:
    """Get minimum element without popping.

    Args:
        heap: Heap list.

    Returns:
        Minimum element.

    Raises:
        IndexError: If heap is empty.
    """
    if not heap:
        raise IndexError("Heap is empty")
    return heap[0]


def heap_max(heap: list[T]) -> T:
    """Get maximum element in a max-heap.

    Args:
        heap: Max-heap list.

    Returns:
        Maximum element.

    Raises:
        IndexError: If heap is empty.
    """
    if not heap:
        raise IndexError("Heap is empty")
    return heap[0]


def heap_peek(heap: list[T]) -> T:
    """Peek at top of min-heap."""
    return heap_min(heap)


def heap_push_pop(heap: list[T], item: T) -> T:
    """Push item onto heap and pop the smallest.

    Args:
        heap: Heap list.
        item: Item to push.

    Returns:
        Popped minimum element.
    """
    return heapq.heappushpop(heap, item)


def heap_replace(heap: list[T], item: T) -> T:
    """Replace top element and maintain heap property.

    Args:
        heap: Heap list.
        item: New item.

    Returns:
        Replaced element.

    Raises:
        IndexError: If heap is empty.
    """
    return heapq.heapreplace(heap, item)


def heap_push(heap: list[T], item: T) -> None:
    """Push item onto heap."""
    heapq.heappush(heap, item)


def heap_to_list(heap: list[T]) -> list[T]:
    """Get sorted list from heap without modifying original.

    Args:
        heap: Heap list.

    Returns:
        Sorted copy of heap.
    """
    return sorted(heap)


def list_to_heap(lst: list[T]) -> list[T]:
    """Convert list to heap in-place.

    Args:
        lst: List to convert.

    Returns:
        The same list, now a heap.
    """
    heapq.heapify(lst)
    return lst


def heap_sort(heap: list[T]) -> list[T]:
    """Sort heap into ascending order.

    Args:
        heap: Heap list.

    Returns:
        New sorted list.
    """
    return sorted(heap)


def heap_insert(heap: list[T], item: T) -> int:
    """Insert item and return number of items.

    Args:
        heap: Heap list.
        item: Item to insert.

    Returns:
        New heap size.
    """
    heapq.heappush(heap, item)
    return len(heap)


def heap_delete(heap: list[T], item: T) -> bool:
    """Delete first occurrence of item from heap.

    Args:
        heap: Heap list.
        item: Item to delete.

    Returns:
        True if item was found and deleted.
    """
    try:
        idx = heap.index(item)
        last = heap.pop()
        if idx < len(heap):
            heap[idx] = last
            heapq._siftup(heap, idx)
            heapq._siftdown(heap, 0, idx)
        return True
    except ValueError:
        return False


def heap_merge(*iterables: Iterable[T]) -> Iterator[T]:
    """Merge multiple sorted iterables into a single sorted iterator.

    Args:
        *iterables: Sorted iterables to merge.

    Returns:
        Iterator yielding items in sorted order.
    """
    return heapq.merge(*iterables)


def heap_k_smallest(k: int, heap: list[T]) -> list[T]:
    """Get k smallest elements from heap.

    Args:
        k: Number of elements.
        heap: Heap list.

    Returns:
        List of k smallest elements (sorted).
    """
    return heapq.nsmallest(k, heap)


def heap_k_largest(k: int, heap: list[T]) -> list[T]:
    """Get k largest elements from heap.

    Args:
        k: Number of elements.
        heap: Heap list.

    Returns:
        List of k largest elements (sorted descending).
    """
    return heapq.nlargest(k, heap)


def heap_contains(heap: list[T], item: T) -> bool:
    """Check if item is in heap.

    Args:
        heap: Heap list.
        item: Item to check.

    Returns:
        True if item is in heap.
    """
    return item in heap


def heap_update(heap: list[T], old_item: T, new_item: T) -> bool:
    """Replace old_item with new_item in heap.

    Args:
        heap: Heap list.
        old_item: Item to replace.
        new_item: Replacement item.

    Returns:
        True if replacement was made.
    """
    try:
        idx = heap.index(old_item)
        heap[idx] = new_item
        heapq._siftup(heap, idx)
        heapq._siftdown(heap, 0, idx)
        return True
    except ValueError:
        return False


def heappushpop(heap: list[T], item: T) -> T:
    """Push item then pop and return smallest."""
    return heapq.heappushpop(heap, item)


def heapify_max(heap: list[T]) -> None:
    """Transform list into a max-heap in-place."""
    heapq._heapify_max(heap)


def heappop_max(heap: list[T]) -> T:
    """Pop and return largest item from max-heap."""
    return heapq._heappop_max(heap)


def heappush_max(heap: list[T], item: T) -> None:
    """Push item onto max-heap."""
    heapq._heappush_max(heap, item)


def heapreplace_max(heap: list[T], item: T) -> T:
    """Replace top of max-heap and return replaced item."""
    return heapq._heapreplace_max(heap, item)


def heap_min_max(heap: list[T]) -> tuple[T, T]:
    """Get both min and max efficiently.

    Args:
        heap: Heap list.

    Returns:
        Tuple of (min, max).
    """
    if not heap:
        raise IndexError("Heap is empty")
    return (heap[0], max(heap))


def heap_median(heap: list[T]) -> T:
    """Get median element from heap.

    Args:
        heap: Heap list.

    Returns:
        Median element.
    """
    sorted_heap = sorted(heap)
    n = len(sorted_heap)
    mid = n // 2
    if n % 2 == 0:
        return (sorted_heap[mid - 1] + sorted_heap[mid]) / 2
    return sorted_heap[mid]


def heap_balanced_merge(*iterables: Iterable[T], reverse: bool = False) -> Iterator[T]:
    """Merge sorted iterables using balanced k-way merge.

    Args:
        *iterables: Sorted iterables.
        reverse: If True, merge in descending order.

    Returns:
        Iterator yielding merged items.
    """
    merged = heapq.merge(*iterables, reverse=reverse)
    return merged


def heap_zip_sorted(*iterables: Iterable[T]) -> Iterator[tuple[T, ...]]:
    """Zip sorted iterables element by element.

    Args:
        *iterables: Sorted iterables of equal length.

    Returns:
        Iterator of tuples of corresponding elements.
    """
    iters = [iter(i) for i in iterables]
    while True:
        try:
            yield tuple(next(it) for it in iters)
        except StopIteration:
            break


def heap_dedup(sorted_iterable: Iterable[T]) -> Iterator[T]:
    """Remove consecutive duplicates from sorted iterable.

    Args:
        sorted_iterable: Sorted iterable with possible duplicates.

    Returns:
        Iterator yielding unique consecutive elements.
    """
    prev = None
    for item in sorted_iterable:
        if item != prev:
            yield item
            prev = item


def heap_intersection(*sorted_iterables: Iterable[T]) -> Iterator[T]:
    """Find intersection of sorted iterables.

    Args:
        *sorted_iterables: Two or more sorted iterables.

    Returns:
        Iterator yielding elements in all iterables.
    """
    if not sorted_iterables:
        return
    iters = [iter(i) for i in sorted_iterables]
    from collections import Counter
    counters = []
    for it in iters:
        counters.append(Counter(it))
    if not counters:
        return
    result_keys = counters[0].keys()
    for counter in counters[1:]:
        result_keys = [k for k in result_keys if k in counter]
    for key in sorted(result_keys):
        min_count = min(c[key] for c in counters)
        for _ in range(min_count):
            yield key


def heap_union(*sorted_iterables: Iterable[T]) -> Iterator[T]:
    """Find union of sorted iterables (all unique elements).

    Args:
        *sorted_iterables: Sorted iterables.

    Returns:
        Iterator yielding unique sorted elements.
    """
    seen: set[T] = set()
    for it in sorted_iterables:
        for item in it:
            if item not in seen:
                seen.add(item)
                yield item


class HeapItem(Generic[T]):
    """Wrapper for heap items with priority."""

    def __init__(self, priority: float, item: T) -> None:
        self.priority = priority
        self.item = item

    def __lt__(self, other: HeapItem) -> bool:
        return self.priority < other.priority

    def __repr__(self) -> str:
        return f"HeapItem({self.priority}, {self.item!r})"


class MinHeap(Generic[T]):
    """Min-heap with standard operations."""

    def __init__(self, data: Iterable[T] | None = None) -> None:
        self._heap: list[T] = []
        if data:
            self._heap = list(data)
            heapq.heapify(self._heap)

    def push(self, item: T) -> None:
        """Push item onto heap."""
        heapq.heappush(self._heap, item)

    def pop(self) -> T:
        """Pop and return smallest item."""
        return heapq.heappop(self._heap)

    def peek(self) -> T:
        """Return smallest item without removing."""
        return heap_min(self._heap)

    def pushpop(self, item: T) -> T:
        """Push item then pop smallest."""
        return heapq.heappushpop(self._heap, item)

    def replace(self, item: T) -> T:
        """Replace top and maintain heap."""
        return heapq.heapreplace(self._heap, item)

    def __len__(self) -> int:
        return len(self._heap)

    def __bool__(self) -> bool:
        return len(self._heap) > 0

    def __iter__(self) -> Iterator[T]:
        return iter(self._heap)

    def __repr__(self) -> str:
        return f"MinHeap({self._heap!r})"


class MaxHeap(Generic[T]):
    """Max-heap implementation."""

    def __init__(self, data: Iterable[T] | None = None) -> None:
        self._heap: list[T] = []
        if data:
            self._heap = [(-abs(x) if isinstance(x, (int, float)) else x) for x in data]
            heapq.heapify(self._heap)

    def push(self, item: T) -> None:
        """Push item onto max-heap."""
        if isinstance(item, (int, float)):
            heapq.heappush(self._heap, -item)
        else:
            heapq.heappush(self._heap, item)

    def pop(self) -> T:
        """Pop and return largest item."""
        item = heapq.heappop(self._heap)
        if isinstance(item, (int, float)):
            return -item
        return item

    def peek(self) -> T:
        """Return largest item without removing."""
        item = self._heap[0]
        if isinstance(item, (int, float)):
            return -item
        return item

    def __len__(self) -> int:
        return len(self._heap)

    def __bool__(self) -> bool:
        return len(self._heap) > 0


class PriorityQueue(Generic[T]):
    """Thread-safe priority queue based on heap."""

    def __init__(self, max_priority: bool = False) -> None:
        self._heap: list[tuple[int, T]] = []
        self._counter = 0
        self._max_priority = max_priority

    def enqueue(self, item: T, priority: float = 0) -> None:
        """Add item with priority.

        Args:
            item: Item to enqueue.
            priority: Priority value (lower = higher priority for min-heap).
        """
        if self._max_priority:
            heapq.heappush(self._heap, (-priority, self._counter, item))
        else:
            heapq.heappush(self._heap, (priority, self._counter, item))
        self._counter += 1

    def dequeue(self) -> T:
        """Remove and return highest priority item.

        Returns:
            Highest priority item.

        Raises:
            IndexError: If queue is empty.
        """
        if not self._heap:
            raise IndexError("Queue is empty")
        return heapq.heappop(self._heap)[-1]

    def peek(self) -> T:
        """View highest priority item without removing."""
        if not self._heap:
            raise IndexError("Queue is empty")
        return self._heap[0][-1]

    def __len__(self) -> int:
        return len(self._heap)

    def __bool__(self) -> bool:
        return len(self._heap) > 0


class IndexedHeap(Generic[T]):
    """Heap that supports efficient priority updates by index."""

    def __init__(self) -> None:
        self._heap: list[tuple[float, int, T]] = []
        self._index: dict[int, T] = {}
        self._counter = 0

    def push(self, item: T, priority: float) -> None:
        """Push item with priority."""
        heapq.heappush(self._heap, (priority, self._counter, item))
        self._index[self._counter] = item
        self._counter += 1

    def pop(self) -> T:
        """Pop highest priority item."""
        priority, counter, item = heapq.heappop(self._heap)
        del self._index[counter]
        return item

    def update(self, item: T, priority: float) -> None:
        """Update priority of existing item."""
        for i, (p, c, it) in enumerate(self._heap):
            if it == item:
                self._heap[i] = (priority, c, it)
                heapq._siftup(self._heap, i)
                heapq._siftdown(self._heap, 0, i)
                return

    def peek(self) -> T:
        """View highest priority item."""
        return self._heap[0][-1]

    def __len__(self) -> int:
        return len(self._heap)


class HeapStats:
    """Statistics for heap operations."""

    def __init__(self) -> None:
        self.pushes = 0
        self.pops = 0
        self.replaces = 0
        self.peak_size = 0

    def record_push(self, size: int) -> None:
        self.pushes += 1
        self.peak_size = max(self.peak_size, size)

    def record_pop(self, size: int) -> None:
        self.pops += 1

    def record_replace(self) -> None:
        self.replaces += 1

    def utilization(self) -> float:
        """Average heap size between operations."""
        if self.pushes + self.pops == 0:
            return 0.0
        return self.peak_size / (self.pushes + self.pops)


class KWayMerge:
    """K-way merge of sorted iterables."""

    def __init__(self, *iterables: Iterable[T], reverse: bool = False) -> None:
        self._iterables = iterables
        self._reverse = reverse

    def merge(self) -> Iterator[T]:
        """Perform k-way merge.

        Returns:
            Iterator yielding merged items.
        """
        return heapq.merge(*self._iterables, reverse=self._reverse)

    def top_k(self, k: int) -> list[T]:
        """Get k smallest or largest items."""
        all_items: list[T] = []
        for it in self._iterables:
            all_items.extend(it)
        if self._reverse:
            return heapq.nlargest(k, all_items)
        return heapq.nsmallest(k, all_items)
