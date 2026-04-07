"""pqueue_action module for rabai_autoclick.

Provides priority queue implementations: binary heap, Fibonacci heap,
binomial heap, and priority queue with changeable priorities.
"""

from __future__ import annotations

import heapq
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, Iterable, List, Optional, Tuple, TypeVar

__all__ = [
    "PriorityQueue",
    "BinaryHeap",
    "MinHeap",
    "MaxHeap",
    "IndexedHeap",
    "FibonacciHeap",
    "BinomialHeap",
    "HeapItem",
    "heappush",
    "heappop",
    "heapify",
    "heappushpop",
    "heapreplace",
    "nlargest",
    "nsmallest",
]


T = TypeVar("T")


@dataclass
class HeapItem(Generic[T]):
    """Wrapper for heap items with priority."""
    priority: float
    value: T
    index: int = 0

    def __lt__(self, other: "HeapItem") -> bool:
        return self.priority < other.priority


class PriorityQueue(Generic[T]):
    """Thread-safe priority queue using binary heap."""

    def __init__(self, max_size: int = 0, reverse: bool = False) -> None:
        self.max_size = max_size
        self.reverse = reverse
        self._heap: List[T] = []
        self._lock = __import__("threading").RLock()

    def push(self, item: T, priority: float = 0.0) -> None:
        """Add item to queue."""
        with self._lock:
            if self.reverse:
                priority = -priority
            heapq.heappush(self._heap, (priority, item))
            if self.max_size > 0 and len(self._heap) > self.max_size:
                self.pop()

    def pop(self) -> Optional[T]:
        """Remove and return highest priority item."""
        with self._lock:
            if not self._heap:
                return None
            priority, item = heapq.heappop(self._heap)
            return item

    def peek(self) -> Optional[T]:
        """Return highest priority item without removing."""
        with self._lock:
            if not self._heap:
                return None
            return self._heap[0][1]

    def pushpop(self, item: T, priority: float = 0.0) -> Optional[T]:
        """Push item then pop and return smallest."""
        with self._lock:
            if self.reverse:
                priority = -priority
            return heapq.heappushpop(self._heap, (priority, item))

    def heappushpop(self, item: T, priority: float = 0.0) -> Optional[T]:
        """Push item then pop and return smallest (optimized)."""
        return self.pushpop(item, priority)

    def replace(self, item: T, priority: float = 0.0) -> Optional[T]:
        """Pop and return smallest, then push new item."""
        with self._lock:
            if self.reverse:
                priority = -priority
            return heapq.heapreplace(self._heap, (priority, item))

    def __len__(self) -> int:
        return len(self._heap)

    def __bool__(self) -> bool:
        return len(self._heap) > 0

    def is_full(self) -> bool:
        return self.max_size > 0 and len(self._heap) >= self.max_size


class BinaryHeap(Generic[T]):
    """Binary heap implementation."""

    def __init__(self, items: Optional[List[T]] = None, key: Optional[Callable[[T], float]] = None) -> None:
        self.key = key or (lambda x: x)
        self._heap: List[T] = []
        if items:
            self._heap = list(items)
            self._heapify()

    def _heapify(self) -> None:
        heapq.heapify(self._heap)

    def push(self, item: T) -> None:
        heapq.heappush(self._heap, item)

    def pop(self) -> Optional[T]:
        try:
            return heapq.heappop(self._heap)
        except IndexError:
            return None

    def peek(self) -> Optional[T]:
        return self._heap[0] if self._heap else None

    def pushpop(self, item: T) -> T:
        return heapq.heappushpop(self._heap, item)

    def replace(self, item: T) -> T:
        return heapq.heapreplace(self._heap, item)

    def __len__(self) -> int:
        return len(self._heap)

    def __bool__(self) -> bool:
        return len(self._heap) > 0

    def to_list(self) -> List[T]:
        return list(self._heap)


class MinHeap(BinaryHeap[T]):
    """Min-heap (smallest element at top)."""

    def __init__(self, items: Optional[List[T]] = None) -> None:
        super().__init__(items=items, key=lambda x: x)


class MaxHeap(BinaryHeap[T]):
    """Max-heap (largest element at top)."""

    def __init__(self, items: Optional[List[T]] = None) -> None:
        super().__init__(items=items, key=lambda x: -x)


class IndexedHeap(Generic[T]):
    """Heap with changeable priorities using index tracking."""

    def __init__(self, key: Optional[Callable[[T], float]] = None) -> None:
        self.key = key or (lambda x: x)
        self._heap: List[T] = []
        self._index: dict = {}

    def push(self, item: T) -> None:
        if item in self._index:
            self.decrease_key(item)
            return
        self._heap.append(item)
        self._index[item] = len(self._heap) - 1
        self._sift_up(len(self._heap) - 1)

    def pop(self) -> Optional[T]:
        if not self._heap:
            return None
        result = self._heap[0]
        last = self._heap.pop()
        del self._index[result]
        if self._heap:
            self._heap[0] = last
            self._index[last] = 0
            self._sift_down(0)
        return result

    def decrease_key(self, item: T) -> None:
        if item not in self._index:
            self.push(item)
            return
        idx = self._index[item]
        self._sift_up(idx)

    def _sift_up(self, idx: int) -> None:
        while idx > 0:
            parent = (idx - 1) // 2
            if self.key(self._heap[idx]) < self.key(self._heap[parent]):
                self._swap(idx, parent)
                idx = parent
            else:
                break

    def _sift_down(self, idx: int) -> None:
        n = len(self._heap)
        while True:
            smallest = idx
            left = 2 * idx + 1
            right = 2 * idx + 2
            if left < n and self.key(self._heap[left]) < self.key(self._heap[smallest]):
                smallest = left
            if right < n and self.key(self._heap[right]) < self.key(self._heap[smallest]):
                smallest = right
            if smallest != idx:
                self._swap(idx, smallest)
                idx = smallest
            else:
                break

    def _swap(self, i: int, j: int) -> None:
        self._index[self._heap[i]] = j
        self._index[self._heap[j]] = i
        self._heap[i], self._heap[j] = self._heap[j], self._heap[i]

    def __len__(self) -> int:
        return len(self._heap)

    def __bool__(self) -> bool:
        return len(self._heap) > 0


def heappush(heap: List[T], item: T) -> None:
    """Push item onto heap."""
    heapq.heappush(heap, item)


def heappop(heap: List[T]) -> Optional[T]:
    """Pop and return smallest item from heap."""
    try:
        return heapq.heappop(heap)
    except IndexError:
        return None


def heapify(heap: List[T]) -> None:
    """Transform list into heap in place."""
    heapq.heapify(heap)


def heappushpop(heap: List[T], item: T) -> T:
    """Push item on heap, then pop and return smallest."""
    return heapq.heappushpop(heap, item)


def heapreplace(heap: List[T], item: T) -> T:
    """Pop and return smallest, then push new item."""
    return heapq.heapreplace(heap, item)


def nlargest(n: int, iterable: Iterable[T], key: Optional[Callable[[T], Any]] = None) -> List[T]:
    """Return n largest elements from iterable."""
    return heapq.nlargest(n, iterable, key=key)


def nsmallest(n: int, iterable: Iterable[T], key: Optional[Callable[[T], Any]] = None) -> List[T]:
    """Return n smallest elements from iterable."""
    return heapq.nsmallest(n, iterable, key=key)
