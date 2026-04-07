"""Heap data structure and utilities for RabAI AutoClick.

Provides:
- Min-heap implementation
- Max-heap implementation
- Priority queue
- Indexed heap (decrease-key support)
- Heap sort
- Top-K algorithms
- Merge K sorted sequences
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Generic,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
)


T = TypeVar("T")
K = TypeVar("K")


@dataclass(order=True)
class HeapItem(Generic[T]):
    """Wrapper for heap items with priority.

    Used internally for heaps that need to track items separately.
    """

    priority: float
    item: T = field(compare=False)


class Heap(Generic[T]):
    """Binary min-heap implementation.

    A min-heap is a complete binary tree where each node is less than
    or equal to its children. The root is always the minimum element.

    Example:
        heap = Heap[int]()
        heap.push(3)
        heap.push(1)
        heap.push(4)
        heap.push(1)

        heap.peek()   # 1
        heap.pop()    # 1
        heap.pop()    # 3
    """

    def __init__(self, data: Optional[List[T]] = None) -> None:
        """Initialize heap.

        Args:
            data: Optional initial data. If provided, heap is built in O(n).
        """
        self._heap: List[T] = []
        if data is not None:
            self._heap = list(data)
            self._heapify()

    def _parent(self, i: int) -> int:
        """Get parent index."""
        return (i - 1) // 2

    def _left(self, i: int) -> int:
        """Get left child index."""
        return 2 * i + 1

    def _right(self, i: int) -> int:
        """Get right child index."""
        return 2 * i + 2

    def _swap(self, i: int, j: int) -> None:
        """Swap elements at indices i and j."""
        self._heap[i], self._heap[j] = self._heap[j], self._heap[i]

    def _sift_up(self, i: int) -> None:
        """Move element up to maintain heap property."""
        while i > 0:
            parent = self._parent(i)
            if self._heap[i] < self._heap[parent]:
                self._swap(i, parent)
                i = parent
            else:
                break

    def _sift_down(self, i: int) -> None:
        """Move element down to maintain heap property."""
        n = len(self._heap)
        while True:
            smallest = i
            left = self._left(i)
            right = self._right(i)

            if left < n and self._heap[left] < self._heap[smallest]:
                smallest = left
            if right < n and self._heap[right] < self._heap[smallest]:
                smallest = right

            if smallest != i:
                self._swap(i, smallest)
                i = smallest
            else:
                break

    def _heapify(self) -> None:
        """Build heap from arbitrary array in O(n)."""
        n = len(self._heap)
        for i in range(n // 2 - 1, -1, -1):
            self._sift_down(i)

    def push(self, item: T) -> None:
        """Push item onto heap.

        Args:
            item: Item to push.
        """
        self._heap.append(item)
        self._sift_up(len(self._heap) - 1)

    def pop(self) -> T:
        """Remove and return smallest item.

        Returns:
            Smallest item.

        Raises:
            IndexError: If heap is empty.
        """
        if not self._heap:
            raise IndexError("pop from empty heap")
        result = self._heap[0]
        last = self._heap.pop()
        if self._heap:
            self._heap[0] = last
            self._sift_down(0)
        return result

    def peek(self) -> T:
        """Return smallest item without removing it.

        Returns:
            Smallest item.

        Raises:
            IndexError: If heap is empty.
        """
        if not self._heap:
            raise IndexError("peek from empty heap")
        return self._heap[0]

    def pushpop(self, item: T) -> T:
        """Push item and return smallest.

        More efficient than push then pop.

        Args:
            item: Item to push.

        Returns:
            Smallest item (either item or existing heap minimum).
        """
        if not self._heap:
            return item
        if item < self._heap[0]:
            result = self._heap[0]
            self._heap[0] = item
            self._sift_down(0)
            return result
        return item

    def replace(self, item: T) -> T:
        """Return smallest and push new item.

        Returns:
            Smallest item that was at top.
        """
        if not self._heap:
            self._heap.append(item)
            return item
        result = self._heap[0]
        self._heap[0] = item
        self._sift_down(0)
        return result

    def __len__(self) -> int:
        """Number of items in heap."""
        return len(self._heap)

    def __bool__(self) -> bool:
        """Check if heap is non-empty."""
        return bool(self._heap)

    def __contains__(self, item: T) -> bool:
        """Check if item is in heap."""
        return item in self._heap

    def __iter__(self) -> Iterator[T]:
        """Iterate over heap items (no particular order)."""
        return iter(self._heap)

    def __repr__(self) -> str:
        return f"Heap({self._heap[:5]}{'...' if len(self._heap) > 5 else ''})"


class MaxHeap(Generic[T]):
    """Binary max-heap implementation.

    A max-heap is the opposite of a min-heap - the root is always
    the maximum element.

    Example:
        heap = MaxHeap[int]()
        heap.push(3)
        heap.push(1)
        heap.push(4)

        heap.peek()   # 4
        heap.pop()    # 4
    """

    def __init__(self, data: Optional[List[T]] = None) -> None:
        """Initialize max-heap.

        Args:
            data: Optional initial data.
        """
        self._heap: List[T] = []
        if data is not None:
            self._heap = list(data)
            self._heapify()

    def _parent(self, i: int) -> int:
        return (i - 1) // 2

    def _left(self, i: int) -> int:
        return 2 * i + 1

    def _right(self, i: int) -> int:
        return 2 * i + 2

    def _swap(self, i: int, j: int) -> None:
        self._heap[i], self._heap[j] = self._heap[j], self._heap[i]

    def _sift_up(self, i: int) -> None:
        while i > 0:
            parent = self._parent(i)
            if self._heap[i] > self._heap[parent]:
                self._swap(i, parent)
                i = parent
            else:
                break

    def _sift_down(self, i: int) -> None:
        n = len(self._heap)
        while True:
            largest = i
            left = self._left(i)
            right = self._right(i)

            if left < n and self._heap[left] > self._heap[largest]:
                largest = left
            if right < n and self._heap[right] > self._heap[largest]:
                largest = right

            if largest != i:
                self._swap(i, largest)
                i = largest
            else:
                break

    def _heapify(self) -> None:
        n = len(self._heap)
        for i in range(n // 2 - 1, -1, -1):
            self._sift_down(i)

    def push(self, item: T) -> None:
        self._heap.append(item)
        self._sift_up(len(self._heap) - 1)

    def pop(self) -> T:
        if not self._heap:
            raise IndexError("pop from empty heap")
        result = self._heap[0]
        last = self._heap.pop()
        if self._heap:
            self._heap[0] = last
            self._sift_down(0)
        return result

    def peek(self) -> T:
        if not self._heap:
            raise IndexError("peek from empty heap")
        return self._heap[0]

    def __len__(self) -> int:
        return len(self._heap)

    def __bool__(self) -> bool:
        return bool(self._heap)

    def __repr__(self) -> str:
        return f"MaxHeap({self._heap[:5]}{'...' if len(self._heap) > 5 else ''})"


class PriorityQueue(Generic[T]):
    """Priority queue implementation using heap.

    Items are removed in priority order (lowest priority by default).
    Uses a min-heap internally.

    Example:
        pq = PriorityQueue()
        pq.push(1, "low priority")
        pq.push(3, "high priority")
        pq.push(2, "medium priority")

        pq.pop()    # (1, "low priority")
        pq.pop()    # (2, "medium priority")
    """

    def __init__(self) -> None:
        self._heap: List[HeapItem[T]] = []
        self._counter = 0

    def push(self, priority: float, item: T) -> None:
        """Add item with given priority.

        Args:
            priority: Lower values = higher priority.
            item: Item to add.
        """
        heapq.heappush(self._heap, HeapItem(priority=priority, item=item))
        self._counter += 1

    def pop(self) -> Tuple[float, T]:
        """Remove and return highest priority item.

        Returns:
            Tuple of (priority, item).

        Raises:
            IndexError: If queue is empty.
        """
        if not self._heap:
            raise IndexError("pop from empty priority queue")
        item = heapq.heappop(self._heap)
        return (item.priority, item.item)

    def peek(self) -> Tuple[float, T]:
        """Return highest priority item without removing.

        Returns:
            Tuple of (priority, item).
        """
        if not self._heap:
            raise IndexError("peek from empty priority queue")
        item = self._heap[0]
        return (item.priority, item.item)

    def __len__(self) -> int:
        return len(self._heap)

    def __bool__(self) -> bool:
        return bool(self._heap)

    def __repr__(self) -> str:
        return f"PriorityQueue({len(self._heap)} items)"


class IndexedHeap(Generic[K, T]):
    """Indexed min-heap with decrease-key support.

    Allows updating the priority of items in O(log n) by index/key.

    Example:
        heap = IndexedHeap[str]()
        heap.push("a", 3)
        heap.push("b", 1)
        heap.push("c", 2)

        heap.peek_key()   # "b"
        heap.decrease_key("c", 0.5)
        heap.peek_key()   # "c"
    """

    def __init__(self) -> None:
        self._heap: List[Tuple[float, int, T]] = []
        self._index: Dict[K, int] = {}
        self._counter = 0

    def push(self, key: K, priority: float, item: T) -> None:
        """Add item with key and priority.

        Args:
            key: Unique key for this item.
            priority: Initial priority.
            item: Item data.
        """
        if key in self._index:
            raise ValueError(f"Key {key} already exists")
        self._heap.append((priority, self._counter, item))
        self._index[key] = len(self._heap) - 1
        self._counter += 1
        self._sift_up(self._index[key])

    def decrease_key(self, key: K, new_priority: float) -> bool:
        """Decrease the priority of an item.

        Args:
            key: Key of item to update.
            new_priority: New (lower) priority.

        Returns:
            True if updated, False if key not found.
        """
        if key not in self._index:
            return False
        i = self._index[key]
        if new_priority < self._heap[i][0]:
            self._heap[i] = (new_priority, self._heap[i][1], self._heap[i][2])
            self._sift_up(i)
        return True

    def get_priority(self, key: K) -> Optional[float]:
        """Get current priority of item."""
        if key not in self._index:
            return None
        return self._heap[self._index[key]][0]

    def pop(self) -> Tuple[K, float, T]:
        """Remove and return minimum item.

        Returns:
            Tuple of (key, priority, item).
        """
        if not self._heap:
            raise IndexError("pop from empty indexed heap")

        min_item = self._heap[0]
        last = self._heap.pop()

        key = self._find_key(0)
        del self._index[key]

        if self._heap:
            self._heap[0] = last
            self._update_index_after_pop()
            self._sift_down(0)

        return (key, min_item[0], min_item[2])

    def peek(self) -> Tuple[K, float, T]:
        """Return minimum without removing."""
        if not self._heap:
            raise IndexError("peek from empty indexed heap")
        item = self._heap[0]
        key = self._find_key(0)
        return (key, item[0], item[2])

    def peek_key(self) -> K:
        """Return key of minimum without removing."""
        if not self._heap:
            raise IndexError("peek from empty indexed heap")
        return self._find_key(0)

    def _find_key(self, index: int) -> K:
        for k, i in self._index.items():
            if i == index:
                return k
        raise KeyError("key not found")

    def _update_index_after_pop(self) -> None:
        self._index.clear()
        for i, item in enumerate(self._heap):
            pass

    def _sift_up(self, i: int) -> None:
        while i > 0:
            parent = (i - 1) // 2
            if self._heap[i][0] < self._heap[parent][0]:
                self._swap(i, parent)
                i = parent
            else:
                break

    def _sift_down(self, i: int) -> None:
        n = len(self._heap)
        while True:
            smallest = i
            left = 2 * i + 1
            right = 2 * i + 2

            if left < n and self._heap[left][0] < self._heap[smallest][0]:
                smallest = left
            if right < n and self._heap[right][0] < self._heap[smallest][0]:
                smallest = right

            if smallest != i:
                self._swap(i, smallest)
                i = smallest
            else:
                break

    def _swap(self, i: int, j: int) -> None:
        self._heap[i], self._heap[j] = self._heap[j], self._heap[i]

    def __len__(self) -> int:
        return len(self._heap)

    def __bool__(self) -> bool:
        return bool(self._heap)


def heap_sort(data: List[T], reverse: bool = False) -> List[T]:
    """Sort data using heap sort algorithm.

    Args:
        data: List to sort (will be copied first).
        reverse: If True, sort in descending order.

    Returns:
        Sorted list.
    """
    result = list(data)
    heapq.heapify(result)
    sorted_result = [heapq.heappop(result) for _ in range(len(result))]
    if reverse:
        return sorted_result[::-1]
    return sorted_result


def top_k(data: List[T], k: int, key: Callable[[T], float] = lambda x: x) -> List[T]:
    """Get top k elements from data.

    Args:
        data: Input data.
        k: Number of top elements to return.
        key: Function to extract sort key.

    Returns:
        List of top k elements.
    """
    if k <= 0:
        return []
    if k >= len(data):
        return sorted(data, key=key, reverse=True)

    heap: List[Tuple[float, int, T]] = []
    for i, item in enumerate(data):
        priority = -key(item)
        if len(heap) < k:
            heapq.heappush(heap, (priority, i, item))
        else:
            heapq.heappushpop(heap, (priority, i, item))

    return [item for _, _, item in sorted(heap, key=lambda x: x[0], reverse=True)]


def bottom_k(data: List[T], k: int, key: Callable[[T], float] = lambda x: x) -> List[T]:
    """Get bottom k elements from data.

    Args:
        data: Input data.
        k: Number of bottom elements to return.
        key: Function to extract sort key.

    Returns:
        List of bottom k elements.
    """
    if k <= 0:
        return []
    if k >= len(data):
        return sorted(data, key=key)

    heap: List[Tuple[float, int, T]] = []
    for i, item in enumerate(data):
        priority = key(item)
        if len(heap) < k:
            heapq.heappush(heap, (priority, i, item))
        else:
            heapq.heappushpop(heap, (priority, i, item))

    return [item for _, _, item in sorted(heap)]


def merge_k_sorted(
    iterables: List[List[T]],
    key: Callable[[T], float] = lambda x: x,
) -> Iterator[T]:
    """Merge multiple sorted iterables into one sorted sequence.

    Args:
        iterables: List of sorted iterables.
        key: Function to extract sort key.

    Yields:
        Elements in sorted order.
    """
    heap: List[Tuple[float, int, int, T]] = []

    for i, iterable in enumerate(iterables):
        iterator = iter(iterable)
        try:
            item = next(iterator)
            priority = key(item)
            heapq.heappush(heap, (priority, i, 0, item))
        except StopIteration:
            pass

    while heap:
        priority, i, idx, item = heapq.heappop(heap)
        yield item
        try:
            next_iter = iterables[i]
            next_item = next(iter(next_iter.__class__()))
            next_priority = key(next_item)
            heapq.heappush(heap, (next_priority, i, idx + 1, next_item))
        except StopIteration:
            pass
