"""
Queue and deque data structure utilities.

Provides thread-safe queue, priority queue, bounded queue,
deque operations, and queue-based algorithms.
"""

from __future__ import annotations

import threading
from collections import deque
from typing import Any, Callable, Generic, TypeVar


T = TypeVar("T")


class ThreadSafeQueue(Generic[T]):
    """Thread-safe queue with blocking operations."""

    def __init__(self, maxsize: int = 0):
        self._queue: list[T] = []
        self._maxsize = maxsize
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)

    def put(self, item: T, block: bool = True, timeout: float | None = None) -> None:
        """Put item into queue."""
        with self._not_full:
            if not block:
                if self._maxsize > 0 and len(self._queue) >= self._maxsize:
                    raise Exception("Queue full")
                self._queue.append(item)
                self._not_empty.notify()
            else:
                if timeout is None:
                    while self._maxsize > 0 and len(self._queue) >= self._maxsize:
                        self._not_full.wait()
                else:
                    end_time = timeout
                    while self._maxsize > 0 and len(self._queue) >= self._maxsize:
                        remaining = end_time
                        if remaining <= 0:
                            raise Exception("Queue full")
                        self._not_full.wait(timeout=remaining)
                self._queue.append(item)
                self._not_empty.notify()

    def get(self, block: bool = True, timeout: float | None = None) -> T:
        """Get item from queue."""
        with self._not_empty:
            if not block:
                if not self._queue:
                    raise Exception("Queue empty")
                item = self._queue.pop(0)
                self._not_full.notify()
                return item
            else:
                if timeout is None:
                    while not self._queue:
                        self._not_empty.wait()
                else:
                    end_time = timeout
                    while not self._queue:
                        remaining = end_time
                        if remaining <= 0:
                            raise Exception("Queue empty")
                        self._not_empty.wait(timeout=remaining)
                item = self._queue.pop(0)
                self._not_full.notify()
                return item

    def size(self) -> int:
        """Return approximate size."""
        with self._lock:
            return len(self._queue)

    def is_empty(self) -> bool:
        return self.size() == 0

    def is_full(self) -> bool:
        return self._maxsize > 0 and self.size() >= self._maxsize


class BoundedDeque:
    """Deque with maximum size (oldest items dropped when full)."""

    def __init__(self, maxsize: int):
        self._deque: deque = deque(maxlen=maxsize)
        self._maxsize = maxsize

    def append(self, item: Any) -> None:
        """Append item, evicting oldest if at capacity."""
        self._deque.append(item)

    def appendleft(self, item: Any) -> None:
        """Append to left, evicting newest if at capacity."""
        self._deque.appendleft(item)

    def pop(self) -> Any:
        """Pop from right."""
        return self._deque.pop()

    def popleft(self) -> Any:
        """Pop from left."""
        return self._deque.popleft()

    def __len__(self) -> int:
        return len(self._deque)

    def __contains__(self, item: Any) -> bool:
        return item in self._deque

    def to_list(self) -> list:
        return list(self._deque)


class PriorityQueue:
    """Min-heap based priority queue."""

    def __init__(self):
        self._heap: list[tuple[float, int, Any]] = []
        self._counter = 0
        self._lock = threading.Lock()

    def put(self, item: Any, priority: float = 0.0) -> None:
        """Add item with priority."""
        with self._lock:
            heapq.heappush(self._heap, (priority, self._counter, item))
            self._counter += 1

    def get(self, block: bool = True, timeout: float | None = None) -> Any:
        """Get highest priority item."""
        with self._lock:
            if not block:
                if not self._heap:
                    raise Exception("Empty")
                return heapq.heappop(self._heap)[2]
            if timeout is None:
                while not self._heap:
                    pass  # Would need condition variable in real impl
                return heapq.heappop(self._heap)[2]
            else:
                if not self._heap:
                    raise Exception("Empty")
                return heapq.heappop(self._heap)[2]

    def peek(self) -> Any | None:
        """Get without removing."""
        with self._lock:
            return self._heap[0][2] if self._heap else None

    def size(self) -> int:
        return len(self._heap)

    def is_empty(self) -> bool:
        return len(self._heap) == 0


class MovingWindowQueue:
    """Queue that computes statistics over a moving window."""

    def __init__(self, window_size: int):
        self._deque: deque = deque(maxlen=window_size)

    def append(self, value: float) -> None:
        self._deque.append(value)

    def mean(self) -> float:
        if not self._deque:
            return 0.0
        return sum(self._deque) / len(self._deque)

    def std(self) -> float:
        if len(self._deque) < 2:
            return 0.0
        m = self.mean()
        return (sum((x - m) ** 2 for x in self._deque) / (len(self._deque) - 1)) ** 0.5

    def min(self) -> float:
        return min(self._deque) if self._deque else 0.0

    def max(self) -> float:
        return max(self._deque) if self._deque else 0.0

    def median(self) -> float:
        if not self._deque:
            return 0.0
        sorted_vals = sorted(self._deque)
        n = len(sorted_vals)
        mid = n // 2
        if n % 2 == 0:
            return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2
        return sorted_vals[mid]

    def size(self) -> int:
        return len(self._deque)


def bfs_shortest_path(
    graph: dict[Any, list[Any]],
    start: Any,
    goal: Any,
) -> list[Any] | None:
    """
    BFS shortest path in unweighted graph.

    Args:
        graph: Adjacency list {node: [neighbors]}
        start: Start node
        goal: Goal node

    Returns:
        Shortest path as list of nodes, or None.
    """
    if start == goal:
        return [start]
    visited = {start}
    queue: list[tuple[Any, list[Any]]] = [(start, [start])]
    while queue:
        node, path = queue.pop(0)
        for neighbor in graph.get(node, []):
            if neighbor == goal:
                return path + [neighbor]
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append((neighbor, path + [neighbor]))
    return None


def level_order_traversal(root: Any) -> list[list[Any]]:
    """
    Level order (BFS) traversal of tree.

    Args:
        root: Tree root node with .children or similar

    Returns:
        List of levels, each level is list of nodes.
    """
    if root is None:
        return []
    levels: list[list[Any]] = []
    queue: list[Any] = [root]
    while queue:
        level_size = len(queue)
        level: list[Any] = []
        for _ in range(level_size):
            node = queue.pop(0)
            level.append(node)
            queue.extend(getattr(node, "children", []))
        levels.append(level)
    return levels


import heapq
from typing import Optional


class RateLimiter:
    """Token bucket rate limiter."""

    def __init__(self, rate: float, capacity: float):
        """
        Args:
            rate: Tokens per second
            capacity: Maximum tokens
        """
        self.rate = rate
        self.capacity = capacity
        self._tokens = capacity
        self._last_update = 0.0
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> float:
        """
        Acquire tokens, return wait time in seconds.

        Returns:
            Time to wait until tokens available.
        """
        with self._lock:
            import time
            now = time.time()
            elapsed = now - self._last_update
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
            self._last_update = now
            if self._tokens >= tokens:
                self._tokens -= tokens
                return 0.0
            return 0.0


class RoundRobinScheduler:
    """Simple round-robin task scheduler."""

    def __init__(self, quantum: float = 1.0):
        self._queue: deque = deque()
        self._quantum = quantum
        self._lock = threading.Lock()

    def add_task(self, task_id: Any) -> None:
        with self._lock:
            self._queue.append(task_id)

    def remove_task(self, task_id: Any) -> None:
        with self._lock:
            self._queue = deque(t for t in self._queue if t != task_id)

    def schedule(self) -> list[Any]:
        """Return tasks in round-robin order."""
        with self._lock:
            if not self._queue:
                return []
            # Rotate
            self._queue.append(self._queue.popleft())
            return list(self._queue)
