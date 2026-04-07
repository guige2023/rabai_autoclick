"""Heapq action v2 - advanced heap operations.

Extended heap utilities including dijkstra, A*, treap, and more.
"""

from __future__ import annotations

import heapq
from heapq import heapify, heappop, heappush, heapreplace, nlargest, nsmallest
from typing import Any, Callable, Generic, Iterable, Iterator, TypeVar

__all__ = [
    "heap_dijkstra",
    "heap_astar",
    "heap_merge_k",
    "heap_median_maintenance",
    "heap_two_stacks_median",
    "heap_running_median",
    "heap_running_sum",
    "heap_running_variance",
    "heap_running_stats",
    "heap_k_way_merge",
    "heap_zip_merge",
    "heap_chunk",
    "heap_accumulate",
    "heap_deduplicate",
    "heap_window_median",
    "heap_smooth",
    "heap_blend",
    "heap_isfinite",
    "heap_isheap",
    "heap_validate",
    "heap_to_bst",
    "TreapNode",
    "Treap",
    "HeapProfiler",
    "DualHeap",
    "RunningStats",
    "k_way_merge_gen",
]


T = TypeVar("T")


def heap_dijkstra(graph: dict[Any, list[tuple[Any, float]]], start: Any, end: Any) -> tuple[float, list[Any]]:
    """Dijkstra's shortest path using heap.

    Args:
        graph: Adjacency dict {node: [(neighbor, weight), ...]}.
        start: Start node.
        end: End node.

    Returns:
        (distance, path) or (inf, []) if unreachable.
    """
    dist = {start: 0}
    prev = {start: None}
    pq = [(0, start)]
    visited = set()
    while pq:
        d, u = heapq.heappop(pq)
        if u in visited:
            continue
        visited.add(u)
        if u == end:
            path = []
            curr = end
            while curr is not None:
                path.append(curr)
                curr = prev.get(curr)
            return (d, list(reversed(path)))
        for v, w in graph.get(u, []):
            if v in visited:
                continue
            alt = d + w
            if v not in dist or alt < dist[v]:
                dist[v] = alt
                prev[v] = u
                heapq.heappush(pq, (alt, v))
    return (float("inf"), [])


def heap_astar(graph: dict[Any, list[tuple[Any, float]]], start: Any, end: Any, heuristic: Callable[[Any], float]) -> tuple[float, list[Any]]:
    """A* shortest path using heap.

    Args:
        graph: Adjacency dict.
        start: Start node.
        end: End node.
        heuristic: Function from node to estimated cost to end.

    Returns:
        (distance, path).
    """
    g_score = {start: 0}
    f_score = {start: heuristic(start)}
    prev = {start: None}
    pq = [(f_score[start], start)]
    visited = set()
    while pq:
        _, u = heapq.heappop(pq)
        if u in visited:
            continue
        visited.add(u)
        if u == end:
            path = []
            curr = end
            while curr is not None:
                path.append(curr)
                curr = prev.get(curr)
            return (g_score[u], list(reversed(path)))
        for v, w in graph.get(u, []):
            if v in visited:
                continue
            tentative_g = g_score[u] + w
            if v not in g_score or tentative_g < g_score[v]:
                g_score[v] = tentative_g
                f = tentative_g + heuristic(v)
                f_score[v] = f
                prev[v] = u
                heapq.heappush(pq, (f, v))
    return (float("inf"), [])


def heap_merge_k(*iterables: Iterable[T], reverse: bool = False) -> Iterator[T]:
    """Merge k sorted iterables.

    Args:
        *iterables: K sorted iterables.
        reverse: If True, merge descending.

    Returns:
        Merged iterator.
    """
    return heapq.merge(*iterables, reverse=reverse)


def heap_k_way_merge(*iterables: Iterable[T]) -> list[T]:
    """Merge and return as list."""
    return list(heap_merge_k(*iterables))


def heap_median_maintenance(stream: Iterable[float]) -> list[float]:
    """Compute running median using two heaps.

    Args:
        stream: Iterable of numbers.

    Returns:
        List of medians after each input.
    """
    low = []
    high = []
    medians = []
    for x in stream:
        if not low:
            heapq.heappush(low, -x)
        elif -low[0] >= x:
            heapq.heappush(low, -x)
        else:
            heapq.heappush(high, x)
        if len(low) > len(high) + 1:
            heapq.heappush(high, -heapq.heappop(low))
        elif len(high) > len(low):
            heapq.heappush(low, -heapq.heappop(high))
        medians.append(-low[0] if len(low) >= len(high) else (low[0] + high[0]) / 2)
    return medians


def heap_two_stacks_median() -> None:
    """Placeholder - median with two stacks is classic queue problem."""
    raise NotImplementedError("Use heap_median_maintenance for stream median")


def heap_running_median(data: list[float]) -> list[float]:
    """Running median over a list."""
    return heap_median_maintenance(data)


def heap_running_sum(data: Iterable[float]) -> list[float]:
    """Running sum."""
    result = []
    total = 0.0
    for x in data:
        total += x
        result.append(total)
    return result


def heap_running_variance(data: Iterable[float]) -> list[float]:
    """Running population variance."""
    result = []
    n = 0
    mean = 0.0
    m2 = 0.0
    for x in data:
        n += 1
        delta = x - mean
        mean += delta / n
        delta2 = x - mean
        m2 += delta * delta2
        if n < 2:
            result.append(0.0)
        else:
            result.append(m2 / n)
    return result


def heap_running_stats(data: Iterable[float]) -> list[dict]:
    """Running statistics (count, mean, variance, min, max)."""
    result = []
    n = 0
    mean = 0.0
    m2 = 0.0
    min_val = None
    max_val = None
    for x in data:
        n += 1
        delta = x - mean
        mean += delta / n
        delta2 = x - mean
        m2 += delta * delta2
        min_val = x if min_val is None else min(min_val, x)
        max_val = x if max_val is None else max(max_val, x)
        var = m2 / n if n > 1 else 0.0
        result.append({
            "count": n,
            "mean": mean,
            "variance": var,
            "stddev": var ** 0.5,
            "min": min_val,
            "max": max_val,
        })
    return result


def heap_zip_merge(*iterables: Iterable[T]) -> Iterator[tuple[T, ...]]:
    """Round-robin merge of k iterables."""
    iterators = [iter(it) for it in iterables]
    while iterators:
        new_iters = []
        for it in iterators:
            try:
                yield (next(it),)
                new_iters.append(it)
            except StopIteration:
                pass
        iterators = new_iters
        if not iterators:
            break


def heap_chunk(heap: list[T], chunk_size: int) -> Iterator[list[T]]:
    """Yield successive chunks from heap.

    Args:
        heap: Heap list.
        chunk_size: Size of each chunk.

    Returns:
        Iterator of chunks.
    """
    for i in range(0, len(heap), chunk_size):
        yield heap[i:i + chunk_size]


def heap_accumulate(heap: list[float]) -> list[float]:
    """Running sum of heap values in place."""
    for i in range(1, len(heap)):
        heap[i] += heap[i - 1]
    return heap


def heap_deduplicate(sorted_heap: list[T]) -> list[T]:
    """Remove consecutive duplicates from sorted heap."""
    if not sorted_heap:
        return []
    result = [sorted_heap[0]]
    for item in sorted_heap[1:]:
        if item != result[-1]:
            result.append(item)
    return result


def heap_window_median(data: list[float], window: int) -> list[float]:
    """Compute sliding window median."""
    if window < 1:
        raise ValueError("Window must be >= 1")
    if len(data) < window:
        raise ValueError("Data shorter than window")
    import statistics
    result = []
    for i in range(len(data) - window + 1):
        window_data = data[i:i + window]
        result.append(statistics.median(window_data))
    return result


def heap_smooth(data: list[float], alpha: float = 0.5) -> list[float]:
    """Exponential smoothing.

    Args:
        data: Data to smooth.
        alpha: Smoothing factor (0 < alpha <= 1).

    Returns:
        Smoothed values.
    """
    if not data:
        return []
    result = [data[0]]
    for x in data[1:]:
        result.append(alpha * x + (1 - alpha) * result[-1])
    return result


def heap_blend(heap1: list[float], heap2: list[float], weight: float = 0.5) -> list[float]:
    """Blend two heaps with weight.

    Args:
        heap1: First sorted heap.
        heap2: Second sorted heap.
        weight: Weight for heap1 (0-1).

    Returns:
        Blended sorted heap.
    """
    if weight < 0 or weight > 1:
        raise ValueError("weight must be 0-1")
    n1, n2 = len(heap1), len(heap2)
    max_n = max(n1, n2)
    result = []
    for i in range(max_n):
        v1 = heap1[i] if i < n1 else heap1[-1]
        v2 = heap2[i] if i < n2 else heap2[-1]
        result.append(weight * v1 + (1 - weight) * v2)
    result.sort()
    return result


def heap_isfinite(heap: list[float]) -> bool:
    """Check if all values in heap are finite."""
    return all(float("-inf") < x < float("inf") for x in heap)


def heap_isheap(heap: list[float]) -> bool:
    """Check if list is a valid min-heap."""
    n = len(heap)
    for i in range(n // 2):
        if heap[i] > heap[2 * i + 1] if 2 * i + 1 < n else False:
            return False
        if 2 * i + 2 < n and heap[i] > heap[2 * i + 2]:
            return False
    return True


def heap_validate(heap: list[float]) -> tuple[bool, list[tuple[int, int]]]:
    """Validate heap property, return violations.

    Args:
        heap: Heap to validate.

    Returns:
        (is_valid, list of violations).
    """
    violations = []
    n = len(heap)
    for i in range(n // 2):
        if 2 * i + 1 < n and heap[i] > heap[2 * i + 1]:
            violations.append((i, 2 * i + 1))
        if 2 * i + 2 < n and heap[i] > heap[2 * i + 2]:
            violations.append((i, 2 * i + 2))
    return (len(violations) == 0, violations)


def heap_to_bst(heap: list[float]) -> Any:
    """Convert heap to binary tree representation."""
    class Node:
        def __init__(self, val: float, left: Any = None, right: Any = None) -> None:
            self.val = val
            self.left = left
            self.right = right
    if not heap:
        return None
    return Node(heap[0])


def k_way_merge_gen(*iterables: Iterable[T]) -> Iterator[T]:
    """Generator for k-way merge."""
    return heapq.merge(*iterables)


class TreapNode(Generic[T]):
    """Node for treap (randomized BST)."""

    def __init__(self, key: T, priority: float) -> None:
        self.key = key
        self.priority = priority
        self.left: TreapNode[T] | None = None
        self.right: TreapNode[T] | None = None


class Treap(Generic[T]):
    """Treap (BST + Heap) with O(log n) operations."""

    def __init__(self) -> None:
        self._root: TreapNode[T] | None = None

    def _rotate_right(self, y: TreapNode[T]) -> TreapNode[T]:
        x = y.left
        y.left = x.right
        x.right = y
        return x

    def _rotate_left(self, x: TreapNode[T]) -> TreapNode[T]:
        y = x.right
        x.right = y.left
        y.left = x
        return y

    def insert(self, key: T, priority: float | None = None) -> None:
        """Insert key with priority."""
        import random
        if priority is None:
            priority = random.random()
        node = TreapNode(key, priority)
        if self._root is None:
            self._root = node
            return
        if key < self._root.key:
            self._root.left = self._insert(self._root.left, node)
            if self._root.left and self._root.left.priority < self._root.priority:
                self._root = self._rotate_right(self._root)
        elif key > self._root.key:
            self._root.right = self._insert(self._root.right, node)
            if self._root.right and self._root.right.priority < self._root.priority:
                self._root = self._rotate_left(self._root)

    def _insert(self, parent: TreapNode[T] | None, node: TreapNode[T]) -> TreapNode[T]:
        if parent is None:
            return node
        if node.key < parent.key:
            parent.left = self._insert(parent.left, node)
            if parent.left and parent.left.priority < parent.priority:
                parent = self._rotate_right(parent)
        else:
            parent.right = self._insert(parent.right, node)
            if parent.right and parent.right.priority < parent.priority:
                parent = self._rotate_left(parent)
        return parent

    def search(self, key: T) -> bool:
        """Search for key."""
        node = self._root
        while node:
            if key == node.key:
                return True
            elif key < node.key:
                node = node.left
            else:
                node = node.right
        return False

    def inorder(self) -> Iterator[T]:
        """Inorder traversal."""
        def _inorder(node: TreapNode[T] | None) -> Iterator[T]:
            if node:
                yield from _inorder(node.left)
                yield node.key
                yield from _inorder(node.right)
        yield from _inorder(self._root)


class DualHeap:
    """Dual heap for sliding window problems."""

    def __init__(self, k: int) -> None:
        self._k = k
        self._low: list[float] = []
        self._high: list[float] = []
        self._counter: dict[float, int] = {}

    def add(self, num: float) -> None:
        """Add number to window."""
        heapq.heappush(self._low, -num)
        largest_low = -heapq.heappop(self._low)
        heapq.heappush(self._high, largest_low)
        self._counter[largest_low] = self._counter.get(largest_low, 0) + 1
        self._rebalance()
        if len(self._low) + len(self._high) > self._k:
            self._remove_old()

    def _rebalance(self) -> None:
        if len(self._low) < len(self._high):
            val = heapq.heappop(self._high)
            self._counter[val] -= 1
            heapq.heappush(self._low, -val)

    def _remove_old(self) -> None:
        if self._low:
            val = -heapq.heappop(self._low)
        else:
            val = heapq.heappop(self._high)
        self._counter[val] -= 1

    def median(self) -> float:
        """Get current median."""
        if len(self._low) > len(self._high):
            return -self._low[0]
        return (-self._low[0] + self._high[0]) / 2


class RunningStats:
    """Welford's algorithm for running statistics."""

    def __init__(self) -> None:
        self._n = 0
        self._mean = 0.0
        self._m2 = 0.0
        self._min = float("inf")
        self._max = float("-inf")

    def add(self, value: float) -> None:
        self._n += 1
        delta = value - self._mean
        self._mean += delta / self._n
        delta2 = value - self._mean
        self._m2 += delta * delta2
        self._min = min(self._min, value)
        self._max = max(self._max, value)

    @property
    def count(self) -> int:
        return self._n

    @property
    def mean(self) -> float:
        return self._mean

    @property
    def variance(self) -> float:
        return self._m2 / self._n if self._n > 0 else 0.0

    @property
    def stddev(self) -> float:
        return self.variance ** 0.5


class HeapProfiler:
    """Profile heap operations."""

    def __init__(self) -> None:
        self.pushes = 0
        self.pops = 0
        self.replaces = 0
        self.max_size = 0

    def record_push(self, size: int) -> None:
        self.pushes += 1
        self.max_size = max(self.max_size, size)

    def record_pop(self, size: int) -> None:
        self.pops += 1

    def record_replace(self) -> None:
        self.replaces += 1

    def report(self) -> dict:
        return {
            "pushes": self.pushes,
            "pops": self.pops,
            "replaces": self.replaces,
            "max_size": self.max_size,
            "total_ops": self.pushes + self.pops + self.replaces,
        }
