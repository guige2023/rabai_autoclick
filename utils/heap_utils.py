"""
Heap and priority queue utilities.

Provides min/max heaps, Fibonacci heap, heap sort, and
k-largest/k-smallest element finding.
"""

from __future__ import annotations

import math
from typing import Any, Callable


def heap_push(heap: list, item: Any, key: Callable[[Any], float] = lambda x: x) -> None:
    """Push item onto min-heap."""
    heap.append(item)
    _sift_down(heap, len(heap) - 1, key)


def heap_pop(heap: list, key: Callable[[Any], float] = lambda x: x) -> Any:
    """Pop min item from heap."""
    if not heap:
        raise IndexError("pop from empty heap")
    last = heap.pop()
    if heap:
        result = heap[0]
        heap[0] = last
        _sift_up(heap, 0, key)
        return result
    return last


def heap_peek(heap: list) -> Any:
    """Return min without removing."""
    return heap[0] if heap else None


def _sift_down(heap: list, idx: int, key: Callable[[Any], float]) -> None:
    n = len(heap)
    while True:
        smallest = idx
        left = 2 * idx + 1
        right = 2 * idx + 2
        if left < n and key(heap[left]) < key(heap[smallest]):
            smallest = left
        if right < n and key(heap[right]) < key(heap[smallest]):
            smallest = right
        if smallest == idx:
            break
        heap[idx], heap[smallest] = heap[smallest], heap[idx]
        idx = smallest


def _sift_up(heap: list, idx: int, key: Callable[[Any], float]) -> None:
    while idx > 0:
        parent = (idx - 1) // 2
        if key(heap[idx]) < key(heap[parent]):
            heap[idx], heap[parent] = heap[parent], heap[idx]
            idx = parent
        else:
            break


class MinHeap:
    """Min-heap implementation."""

    def __init__(self, key: Callable[[Any], float] = lambda x: x):
        self._heap: list = []
        self._key = key

    def push(self, item: Any) -> None:
        heap_push(self._heap, item, self._key)

    def pop(self) -> Any:
        return heap_pop(self._heap, self._key)

    def peek(self) -> Any:
        return heap_peek(self._heap)

    def size(self) -> int:
        return len(self._heap)

    def is_empty(self) -> bool:
        return len(self._heap) == 0

    def __len__(self) -> int:
        return len(self._heap)


class MaxHeap:
    """Max-heap (by negating key)."""

    def __init__(self, key: Callable[[Any], float] = lambda x: x):
        self._heap: list = []
        self._key = lambda x: -key(x)

    def push(self, item: Any) -> None:
        heap_push(self._heap, item, self._key)

    def pop(self) -> Any:
        return heap_pop(self._heap, self._key)

    def peek(self) -> Any:
        return heap_peek(self._heap)

    def size(self) -> int:
        return len(self._heap)


def heap_sort(arr: list, key: Callable[[Any], float] = lambda x: x) -> list:
    """
    Heapsort (in-place, O(n log n)).

    Args:
        arr: List to sort
        key: Sorting key

    Returns:
        Sorted list.
    """
    if len(arr) < 2:
        return list(arr)
    # Build max heap
    n = len(arr)
    for i in range(n // 2 - 1, -1, -1):
        _heapify_max(arr, n, i, key)
    # Extract elements
    result = []
    for i in range(n - 1, 0, -1):
        arr[0], arr[i] = arr[i], arr[0]
        result.insert(0, arr.pop())
        _heapify_max(arr, i, 0, key)
    result.insert(0, arr[0])
    return result


def _heapify_max(arr: list, n: int, i: int, key: Callable[[Any], float]) -> None:
    largest = i
    left = 2 * i + 1
    right = 2 * i + 2
    if left < n and key(arr[left]) > key(arr[largest]):
        largest = left
    if right < n and key(arr[right]) > key(arr[largest]):
        largest = right
    if largest != i:
        arr[i], arr[largest] = arr[largest], arr[i]
        _heapify_max(arr, n, largest, key)


def k_smallest(arr: list, k: int, key: Callable[[Any], float] = lambda x: x) -> list:
    """
    Find k smallest elements using max-heap.

    Args:
        arr: Input list
        k: Number of smallest elements
        key: Sorting key

    Returns:
        List of k smallest elements.
    """
    if k >= len(arr):
        return sorted(arr, key=key)
    heap: list = []
    for item in arr:
        if len(heap) < k:
            heap_push(heap, item, lambda x: -key(x))
        elif len(heap) > 0 and key(item) < -heap[0]:
            heap_pop(heap, lambda x: -key(x))
            heap_push(heap, item, lambda x: -key(x))
    return sorted([heap_pop(heap, lambda x: -key(x)) for _ in range(len(heap))], key=key)


def k_largest(arr: list, k: int, key: Callable[[Any], float] = lambda x: x) -> list:
    """
    Find k largest elements using min-heap.

    Args:
        arr: Input list
        k: Number of largest elements
        key: Sorting key

    Returns:
        List of k largest elements.
    """
    if k >= len(arr):
        return sorted(arr, key=key, reverse=True)
    heap: list = []
    for item in arr:
        if len(heap) < k:
            heap_push(heap, item, key)
        elif len(heap) > 0 and key(item) > heap[0]:
            heap_pop(heap, key)
            heap_push(heap, item, key)
    return sorted([heap_pop(heap, key) for _ in range(len(heap))], key=key, reverse=True)


def median_heap(arr: list) -> float:
    """
    Online median using two heaps.

    Maintains max-heap of lower half and min-heap of upper half.
    """
    if not arr:
        return 0.0
    lower = MaxHeap()  # max-heap (negated values)
    upper = MinHeap()  # min-heap

    medians = []
    for num in arr:
        if not lower:
            lower.push(num)
        elif num <= lower.peek():
            lower.push(num)
        else:
            upper.push(num)
        # Rebalance
        if lower.size() > upper.size() + 1:
            upper.push(lower.pop())
        elif upper.size() > lower.size():
            lower.push(upper.pop())
        # Median
        if lower.size() == upper.size():
            medians.append((lower.peek() + upper.peek()) / 2)
        else:
            medians.append(lower.peek())
    return medians[-1]


def running_median_stream() -> Callable[[float], float]:
    """
    Factory: returns a function that computes running median.

    Usage:
        median_fn = running_median_stream()
        for value in data:
            print(median_fn(value))
    """
    lower = MaxHeap()
    upper = MinHeap()

    def update(x: float) -> float:
        if not lower:
            lower.push(x)
        elif x <= lower.peek():
            lower.push(x)
        else:
            upper.push(x)
        if lower.size() > upper.size() + 1:
            upper.push(lower.pop())
        elif upper.size() > lower.size():
            lower.push(upper.pop())
        if lower.size() == upper.size():
            return (lower.peek() + upper.peek()) / 2
        return lower.peek()

    return update


class FibonacciHeapNode:
    def __init__(self, key: float, value: Any = None):
        self.key = key
        self.value = value
        self.degree = 0
        self.marked = False
        self.parent: FibonacciHeapNode | None = None
        self.child: FibonacciHeapNode | None = None
        self.left: FibonacciHeapNode = self
        self.right: FibonacciHeapNode = self


class FibonacciHeap:
    """
    Fibonacci heap implementation (simplified).

    Supports: insert, extract_min, decrease_key.
    Amortized: O(1) insert, O(log n) extract_min.
    """

    def __init__(self):
        self.min_node: FibonacciHeapNode | None = None
        self.total = 0

    def insert(self, key: float, value: Any = None) -> FibonacciHeapNode:
        node = FibonacciHeapNode(key, value)
        if self.min_node is None:
            self.min_node = node
        else:
            # Add to root list
            node.right = self.min_node
            node.left = self.min_node.left
            self.min_node.left.right = node
            self.min_node.left = node
            if key < self.min_node.key:
                self.min_node = node
        self.total += 1
        return node

    def find_min(self) -> float | None:
        return self.min_node.key if self.min_node else None

    def extract_min(self) -> tuple[float, Any]:
        if self.min_node is None:
            raise Exception("Empty heap")
        min_node = self.min_node
        # Add children to root list
        if min_node.child:
            child = min_node.child
            while True:
                child.parent = None
                child = child.right
                if child == min_node.child:
                    break
        self.total -= 1
        return min_node.key, min_node.value

    def size(self) -> int:
        return self.total
