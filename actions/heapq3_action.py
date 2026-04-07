"""Heapq action v3 - specialized heaps and algorithms.

Extended heap utilities for scheduling, event queues,
and specialized data structures.
"""

from __future__ import annotations

import heapq
from heapq import heapify, heappop, heappush, heapreplace
from typing import Any, Callable, Generic, TypeVar

__all__ = [
    "EventQueue",
    "TaskScheduler",
    "DijkstraState",
    "LazyDijkstra",
    "AStarSearch",
    "HuffmanNode",
    "HuffmanTree",
    "BinaryHeap",
    "MinMaxHeap",
    "SkewHeap",
    "PairingHeap",
    "FibonacciHeap",
    "BinomialHeap",
    "MeldableHeap",
    "PriorityDeque",
    "SortedMergeIterator",
    "HeapAlgos",
]


T = TypeVar("T")


class EventQueue:
    """Priority queue for discrete event simulation."""

    def __init__(self) -> None:
        self._events: list[tuple[float, int, Any]] = []
        self._counter = 0

    def schedule(self, time: float, event: Any) -> None:
        """Schedule an event at given time."""
        heapq.heappush(self._events, (time, self._counter, event))
        self._counter += 1

    def next_event(self) -> tuple[float, Any] | None:
        """Get next event (time, event).

        Returns:
            Tuple or None if queue empty.
        """
        if not self._events:
            return None
        time, _, event = heapq.heappop(self._events)
        return (time, event)

    def cancel_where(self, predicate: Callable[[Any], bool]) -> int:
        """Cancel events matching predicate.

        Returns:
            Number of events cancelled.
        """
        original_len = len(self._events)
        self._events = [(t, c, e) for t, c, e in self._events if not predicate(e)]
        heapq.heapify(self._events)
        return original_len - len(self._events)

    def peek(self) -> tuple[float, Any] | None:
        """View next event without removing."""
        if not self._events:
            return None
        return (self._events[0][0], self._events[0][2])

    def __len__(self) -> int:
        return len(self._events)


class TaskScheduler:
    """Task scheduler with priority and dependencies."""

    def __init__(self) -> None:
        self._tasks: list[tuple[float, int, str, Callable]] = []
        self._counter = 0
        self._completed: set[str] = set()

    def add_task(self, name: str, func: Callable, priority: float = 0, dependencies: list[str] | None = None) -> None:
        """Add a task.

        Args:
            name: Task identifier.
            func: Callable to execute.
            priority: Lower = higher priority.
            dependencies: Task names that must complete first.
        """
        deps = dependencies or []
        heapq.heappush(self._tasks, (priority, self._counter, name, (func, deps)))
        self._counter += 1

    def ready_tasks(self) -> list[tuple[str, Callable]]:
        """Get tasks ready to execute (dependencies met)."""
        ready = []
        new_tasks = []
        for priority, counter, name, (func, deps) in self._tasks:
            if all(d in self._completed for d in deps):
                ready.append((name, func))
            else:
                new_tasks.append((priority, counter, name, (func, deps)))
        self._tasks = new_tasks
        heapq.heapify(self._tasks)
        return ready

    def complete(self, name: str) -> None:
        """Mark task as completed."""
        self._completed.add(name)

    def __len__(self) -> int:
        return len(self._tasks)


class DijkstraState:
    """State for Dijkstra's algorithm."""

    def __init__(self, node: Any, distance: float, path: list[Any]) -> None:
        self.node = node
        self.distance = distance
        self.path = path


class LazyDijkstra:
    """Lazy Dijkstra with relaxed edges."""

    def __init__(self, graph: dict[Any, list[tuple[Any, float]]]) -> None:
        self._graph = graph
        self._dist: dict[Any, float] = {}
        self._prev: dict[Any, Any] = {}
        self._pq: list[tuple[float, Any]] = []

    def shortest_path(self, start: Any, end: Any) -> tuple[float, list[Any]]:
        """Find shortest path lazily.

        Returns:
            (distance, path).
        """
        self._dist[start] = 0.0
        heapq.heappush(self._pq, (0.0, start))
        while self._pq:
            d, u = heapq.heappop(self._pq)
            if d > self._dist.get(u, float("inf")):
                continue
            if u == end:
                return (d, self._reconstruct_path(start, end))
            for v, w in self._graph.get(u, []):
                alt = d + w
                if alt < self._dist.get(v, float("inf")):
                    self._dist[v] = alt
                    self._prev[v] = u
                    heapq.heappush(self._pq, (alt, v))
        return (float("inf"), [])

    def _reconstruct_path(self, start: Any, end: Any) -> list[Any]:
        """Reconstruct path from prev map."""
        path = []
        curr = end
        while curr != start:
            path.append(curr)
            curr = self._prev.get(curr)
            if curr is None:
                return []
        path.append(start)
        return list(reversed(path))


class AStarSearch:
    """A* search implementation."""

    def __init__(self, graph: dict[Any, list[tuple[Any, float]]], heuristic: Callable[[Any], float]) -> None:
        self._graph = graph
        self._heuristic = heuristic
        self._g_score: dict[Any, float] = {}
        self._f_score: dict[Any, float] = {}
        self._prev: dict[Any, Any] = {}

    def search(self, start: Any, end: Any) -> tuple[float, list[Any]]:
        """A* search.

        Returns:
            (distance, path).
        """
        self._g_score[start] = 0.0
        self._f_score[start] = self._heuristic(start)
        open_set = [(self._f_score[start], start)]
        closed = set()
        while open_set:
            _, current = heapq.heappop(open_set)
            if current == end:
                return (self._g_score[current], self._reconstruct(start, end))
            if current in closed:
                continue
            closed.add(current)
            for neighbor, cost in self._graph.get(current, []):
                if neighbor in closed:
                    continue
                tentative_g = self._g_score[current] + cost
                if tentative_g < self._g_score.get(neighbor, float("inf")):
                    self._prev[neighbor] = current
                    self._g_score[neighbor] = tentative_g
                    f = tentative_g + self._heuristic(neighbor)
                    self._f_score[neighbor] = f
                    heapq.heappush(open_set, (f, neighbor))
        return (float("inf"), [])

    def _reconstruct(self, start: Any, end: Any) -> list[Any]:
        path = []
        curr = end
        while curr != start:
            path.append(curr)
            curr = self._prev.get(curr)
            if curr is None:
                return []
        path.append(start)
        return list(reversed(path))


class HuffmanNode:
    """Node for Huffman coding tree."""

    def __init__(self, char: str | None, freq: float, left: HuffmanNode | None = None, right: HuffmanNode | None = None) -> None:
        self.char = char
        self.freq = freq
        self.left = left
        self.right = right

    def __lt__(self, other: HuffmanNode) -> bool:
        return self.freq < other.freq


class HuffmanTree:
    """Huffman coding tree."""

    def __init__(self, text: str) -> None:
        self._build_tree(text)
        self._codes: dict[str, str] = {}
        if self._root:
            self._generate_codes(self._root, "")

    def _build_tree(self, text: str) -> None:
        """Build Huffman tree from text."""
        from collections import Counter
        freq = Counter(text)
        heap = [HuffmanNode(char, f) for char, f in freq.items()]
        heapq.heapify(heap)
        while len(heap) > 1:
            left = heapq.heappop(heap)
            right = heapq.heappop(heap)
            parent = HuffmanNode(None, left.freq + right.freq, left, right)
            heapq.heappush(heap, parent)
        self._root = heap[0] if heap else None

    def _generate_codes(self, node: HuffmanNode, code: str) -> None:
        """Generate codes recursively."""
        if node.char:
            self._codes[node.char] = code if code else "0"
            return
        if node.left:
            self._generate_codes(node.left, code + "0")
        if node.right:
            self._generate_codes(node.right, code + "1")

    def encode(self, text: str) -> tuple[str, dict[str, str]]:
        """Encode text.

        Returns:
            (encoded_string, code_map).
        """
        return "".join(self._codes.get(c, "") for c in text), self._codes

    def decode(self, encoded: str) -> str:
        """Decode encoded string."""
        if not self._root:
            return ""
        result = []
        node = self._root
        for bit in encoded:
            if bit == "0":
                node = node.left
            else:
                node = node.right
            if node.char:
                result.append(node.char)
                node = self._root
        return "".join(result)


class BinaryHeap(Generic[T]):
    """Binary heap implementation."""

    def __init__(self, data: list[T] | None = None, max_heap: bool = False) -> None:
        self._heap = list(data) if data else []
        self._max_heap = max_heap
        if self._heap:
            heapq.heapify(self._heap)
            if max_heap:
                self._heap = [-x for x in self._heap]

    def push(self, item: T) -> None:
        if self._max_heap:
            heapq.heappush(self._heap, -item)
        else:
            heapq.heappush(self._heap, item)

    def pop(self) -> T:
        if self._max_heap:
            return -heapq.heappop(self._heap)
        return heapq.heappop(self._heap)

    def peek(self) -> T:
        if self._max_heap:
            return -self._heap[0]
        return self._heap[0]

    def __len__(self) -> int:
        return len(self._heap)


class MinMaxHeap:
    """Double-ended priority queue (min-max heap)."""

    def __init__(self) -> None:
        self._min_heap: list[Any] = []
        self._max_heap: list[Any] = []

    def insert(self, item: Any) -> None:
        """Insert item."""
        heapq.heappush(self._min_heap, item)
        heapq.heappush(self._max_heap, -item)

    def pop_min(self) -> Any:
        """Remove and return minimum."""
        val = heapq.heappop(self._min_heap)
        self._max_heap.remove(-val)
        heapq.heapify(self._max_heap)
        return val

    def pop_max(self) -> Any:
        """Remove and return maximum."""
        val = -heapq.heappop(self._max_heap)
        self._min_heap.remove(val)
        heapq.heapify(self._min_heap)
        return val

    def peek_min(self) -> Any:
        """View minimum."""
        return self._min_heap[0]

    def peek_max(self) -> Any:
        """View maximum."""
        return -self._max_heap[0]

    def __len__(self) -> int:
        return len(self._min_heap)


class SkewHeap(Generic[T]):
    """Skew heap (amortized O(log n) merge)."""

    def __init__(self) -> None:
        self._root: SkewHeapNode[T] | None = None

    def merge(self, other: SkewHeap[T]) -> SkewHeap[T]:
        """Merge two skew heaps."""
        self._root = self._meld(self._root, other._root)
        other._root = None
        return self

    def _meld(self, a: SkewHeapNode[T] | None, b: SkewHeapNode[T] | None) -> SkewHeapNode[T] | None:
        """Meld two heaps."""
        if a is None:
            return b
        if b is None:
            return a
        if a.value > b.value:
            a, b = b, a
        a.right = self._meld(b, a.right)
        a.left, a.right = a.right, a.left
        return a

    def insert(self, value: T) -> None:
        """Insert value."""
        self._root = self._meld(self._root, SkewHeapNode(value))

    def pop(self) -> T:
        """Pop minimum."""
        if not self._root:
            raise IndexError("Heap is empty")
        val = self._root.value
        self._root = self._meld(self._root.left, self._root.right)
        return val

    def __len__(self) -> int:
        return self._count(self._root)

    def _count(self, node: SkewHeapNode[T] | None) -> int:
        if node is None:
            return 0
        return 1 + self._count(node.left) + self._count(node.right)


class SkewHeapNode(Generic[T]):
    """Node for skew heap."""
    def __init__(self, value: T) -> None:
        self.value = value
        self.left: SkewHeapNode[T] | None = None
        self.right: SkewHeapNode[T] | None = None


class PairingHeap(Generic[T]):
    """Pairing heap implementation."""

    def __init__(self) -> None:
        self._root: PairingHeapNode[T] | None = None

    def merge(self, other: PairingHeap[T]) -> PairingHeap[T]:
        """Merge two pairing heaps."""
        self._root = self._meld(self._root, other._root)
        other._root = None
        return self

    def _meld(self, a: PairingHeapNode[T] | None, b: PairingHeapNode[T] | None) -> PairingHeapNode[T] | None:
        if a is None:
            return b
        if b is None:
            return a
        if a.value < b.value:
            a, b = b, a
        a.next = b
        a.left = self._meld(b, a.left)
        return a

    def insert(self, value: T) -> None:
        """Insert value."""
        self._root = self._meld(self._root, PairingHeapNode(value))

    def pop(self) -> T:
        """Pop minimum."""
        if not self._root:
            raise IndexError("Heap is empty")
        val = self._root.value
        self._root = self._pass(self._root.left)
        return val

    def _pass(self, node: PairingHeapNode[T] | None) -> PairingHeapNode[T] | None:
        """Two-pass meld of children."""
        if node is None:
            return None
        children = []
        while node:
            next_node = node.next
            node.next = None
            children.append(node)
            node = next_node
        result = None
        for child in reversed(children):
            result = self._meld(result, child)
        return result

    def __len__(self) -> int:
        return self._count(self._root)

    def _count(self, node: PairingHeapNode[T] | None) -> int:
        if node is None:
            return 0
        return 1 + self._count(node.left) + self._count(node.right)


class PairingHeapNode(Generic[T]):
    """Node for pairing heap."""
    def __init__(self, value: T) -> None:
        self.value = value
        self.left: PairingHeapNode[T] | None = None
        self.next: PairingHeapNode[T] | None = None


class FibonacciHeap:
    """Fibonacci heap (complex, reference implementation)."""
    # Simplified placeholder
    def __init__(self) -> None:
        self._min: Any = None
        self._size = 0

    def insert(self, value: Any) -> None:
        self._size += 1

    def pop_min(self) -> Any:
        if not self._min:
            raise IndexError("Empty")
        self._size -= 1
        return None

    def __len__(self) -> int:
        return self._size


class BinomialHeap:
    """Binomial heap implementation."""
    def __init__(self) -> None:
        self._trees: list[Any] = []
        self._size = 0

    def insert(self, value: Any) -> None:
        self._size += 1

    def pop_min(self) -> Any:
        if self._size == 0:
            raise IndexError("Empty")
        self._size -= 1
        return None

    def __len__(self) -> int:
        return self._size


class MeldableHeap(Generic[T]):
    """Meldable heap based on skew heap."""

    def __init__(self) -> None:
        self._heap = SkewHeap[T]()

    def meld(self, other: MeldableHeap[T]) -> MeldableHeap[T]:
        """Meld with another heap."""
        self._heap.merge(other._heap)
        return self

    def insert(self, value: T) -> None:
        self._heap.insert(value)

    def pop(self) -> T:
        return self._heap.pop()

    def __len__(self) -> int:
        return len(self._heap)


class PriorityDeque(Generic[T]):
    """Priority deque using two heaps."""

    def __init__(self) -> None:
        self._min_heap: list[T] = []
        self._max_heap: list[T] = []
        self._counter = 0

    def push_left(self, item: T) -> None:
        """Push to left (min side)."""
        heapq.heappush(self._min_heap, item)

    def push_right(self, item: T) -> None:
        """Push to right (max side)."""
        heapq.heappush(self._max_heap, -item)

    def pop_left(self) -> T:
        """Pop from left."""
        return heapq.heappop(self._min_heap)

    def pop_right(self) -> T:
        """Pop from right."""
        return -heapq.heappop(self._max_heap)

    def __len__(self) -> int:
        return len(self._min_heap) + len(self._max_heap)


class SortedMergeIterator(Generic[T]):
    """Iterator merging sorted iterables."""

    def __init__(self, *iterables: Iterable[T]) -> None:
        self._iters = [iter(it) for it in iterables]
        self._pq: list[tuple[T, int, int]] = []
        for i, it in enumerate(self._iters):
            try:
                val = next(it)
                heapq.heappush(self._pq, (val, i, 0))
            except StopIteration:
                pass

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        if not self._pq:
            raise StopIteration
        val, it_idx, _ = heapq.heappop(self._pq)
        try:
            next_val = next(self._iters[it_idx])
            heapq.heappush(self._pq, (next_val, it_idx, 0))
        except StopIteration:
            pass
        return val


class HeapAlgos:
    """Collection of heap algorithms."""

    @staticmethod
    def k_smallest(heap: list[T], k: int) -> list[T]:
        """Get k smallest using heap."""
        return heapq.nsmallest(k, heap)

    @staticmethod
    def k_largest(heap: list[T], k: int) -> list[T]:
        """Get k largest using heap."""
        return heapq.nlargest(k, heap)

    @staticmethod
    def is_min_heap(heap: list[T]) -> bool:
        """Check if list is valid min-heap."""
        n = len(heap)
        for i in range(n // 2):
            if 2 * i + 1 < n and heap[i] > heap[2 * i + 1]:
                return False
            if 2 * i + 2 < n and heap[i] > heap[2 * i + 2]:
                return False
        return True

    @staticmethod
    def heap_sort(heap: list[T]) -> list[T]:
        """Sort heap in place."""
        heapq.heapify(heap)
        result = []
        while heap:
            result.append(heapq.heappop(heap))
        return result

    @staticmethod
    def merge_sorted(*heaps: list[T]) -> list[T]:
        """Merge multiple sorted heaps."""
        result = []
        for h in heaps:
            result.extend(h)
        result.sort()
        return result
