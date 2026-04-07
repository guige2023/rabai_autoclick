"""Queue utilities for RabAI AutoClick.

Provides:
- Priority queue
- Work queue
- Bounded queue
"""

import heapq
import threading
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Generic, List, Optional, TypeVar


T = TypeVar("T")


@dataclass(order=True)
class PriorityItem:
    """Priority queue item."""
    priority: int
    item: Any = None


class PriorityQueue(Generic[T]):
    """Thread-safe priority queue."""

    def __init__(self, max_size: int = 0) -> None:
        """Initialize priority queue.

        Args:
            max_size: Maximum size (0 = unlimited).
        """
        self._max_size = max_size
        self._heap: List[PriorityItem] = []
        self._lock = threading.Lock()

    def put(self, item: T, priority: int = 0) -> bool:
        """Add item to queue.

        Args:
            item: Item to add.
            priority: Item priority.

        Returns:
            True if added.
        """
        with self._lock:
            if self._max_size > 0 and len(self._heap) >= self._max_size:
                return False
            heapq.heappush(self._heap, PriorityItem(priority, item))
            return True

    def get(self, blocking: bool = True, timeout: float = -1) -> Optional[T]:
        """Get item from queue.

        Args:
            blocking: Wait if empty.
            timeout: Timeout in seconds.

        Returns:
            Item or None.
        """
        if blocking:
            if timeout > 0:
                end_time = threading.current_thread().start_time + timeout
                while not self._heap:
                    remaining = end_time - threading.current_thread().start_time
                    if remaining <= 0:
                        return None
                    self._lock.wait(remaining)
            else:
                while not self._heap:
                    self._lock.wait()

        if self._heap:
            item = heapq.heappop(self._heap).item
            return item
        return None

    def peek(self) -> Optional[T]:
        """Peek at next item.

        Returns:
            Next item or None.
        """
        with self._lock:
            if self._heap:
                return self._heap[0].item
        return None

    def remove(self, item: T) -> bool:
        """Remove item from queue.

        Args:
            item: Item to remove.

        Returns:
            True if removed.
        """
        with self._lock:
            for i, pitem in enumerate(self._heap):
                if pitem.item == item:
                    del self._heap[i]
                    heapq.heapify(self._heap)
                    return True
        return False

    @property
    def size(self) -> int:
        """Get queue size."""
        with self._lock:
            return len(self._heap)

    @property
    def is_empty(self) -> bool:
        """Check if empty."""
        with self._lock:
            return len(self._heap) == 0

    @property
    def is_full(self) -> bool:
        """Check if full."""
        with self._lock:
            if self._max_size == 0:
                return False
            return len(self._heap) >= self._max_size

    def clear(self) -> None:
        """Clear queue."""
        with self._lock:
            self._heap.clear()


class WorkQueue:
    """Queue for work items with processors."""

    def __init__(self, num_workers: int = 1) -> None:
        """Initialize work queue.

        Args:
            num_workers: Number of worker threads.
        """
        self._queue: deque = deque()
        self._num_workers = num_workers
        self._workers: List[threading.Thread] = []
        self._running = False
        self._processor: Optional[Callable] = None
        self._lock = threading.Lock()

    def set_processor(self, processor: Callable[[Any], None]) -> None:
        """Set work item processor.

        Args:
            processor: Function to process items.
        """
        self._processor = processor

    def start(self) -> None:
        """Start workers."""
        if self._running:
            return

        self._running = True
        for _ in range(self._num_workers):
            worker = threading.Thread(target=self._worker_loop, daemon=True)
            worker.start()
            self._workers.append(worker)

    def stop(self) -> None:
        """Stop workers."""
        if not self._running:
            return

        self._running = False
        for worker in self._workers:
            worker.join(timeout=1)
        self._workers.clear()

    def submit(self, item: Any) -> None:
        """Submit work item.

        Args:
            item: Work item.
        """
        with self._lock:
            self._queue.append(item)

    def _worker_loop(self) -> None:
        """Worker thread loop."""
        while self._running:
            item = None
            with self._lock:
                if self._queue:
                    item = self._queue.popleft()

            if item and self._processor:
                try:
                    self._processor(item)
                except Exception:
                    pass

    @property
    def pending(self) -> int:
        """Get pending items."""
        with self._lock:
            return len(self._queue)


class BoundedQueue(Generic[T]):
    """Queue with bounded capacity."""

    def __init__(self, capacity: int) -> None:
        """Initialize bounded queue.

        Args:
            capacity: Maximum capacity.
        """
        self._capacity = capacity
        self._queue: List[T] = []
        self._lock = threading.Lock()
        self._not_full = threading.Condition(self._lock)
        self._not_empty = threading.Condition(self._lock)

    def put(self, item: T, timeout: Optional[float] = None) -> bool:
        """Put item in queue.

        Args:
            item: Item to add.
            timeout: Optional timeout.

        Returns:
            True if added.
        """
        with self._not_full:
            if not self._not_full.wait_for(
                lambda: len(self._queue) < self._capacity,
                timeout=timeout,
            ):
                return False
            self._queue.append(item)
            self._not_empty.notify()
            return True

    def get(self, timeout: Optional[float] = None) -> Optional[T]:
        """Get item from queue.

        Args:
            timeout: Optional timeout.

        Returns:
            Item or None.
        """
        with self._not_empty:
            if not self._not_empty.wait_for(
                lambda: len(self._queue) > 0,
                timeout=timeout,
            ):
                return None
            item = self._queue.pop(0)
            self._not_full.notify()
            return item

    @property
    def size(self) -> int:
        """Get queue size."""
        with self._lock:
            return len(self._queue)

    @property
    def is_empty(self) -> bool:
        """Check if empty."""
        with self._lock:
            return len(self._queue) == 0

    @property
    def is_full(self) -> bool:
        """Check if full."""
        with self._lock:
            return len(self._queue) >= self._capacity


class QueueStats:
    """Track queue statistics."""

    def __init__(self) -> None:
        """Initialize stats."""
        self._total_put = 0
        self._total_get = 0
        self._total_failed = 0
        self._lock = threading.Lock()

    def record_put(self) -> None:
        """Record a put operation."""
        with self._lock:
            self._total_put += 1

    def record_get(self) -> None:
        """Record a get operation."""
        with self._lock:
            self._total_get += 1

    def record_failed(self) -> None:
        """Record a failed operation."""
        with self._lock:
            self._total_failed += 1

    def get_stats(self) -> dict:
        """Get statistics.

        Returns:
            Dict of stats.
        """
        with self._lock:
            return {
                "total_put": self._total_put,
                "total_get": self._total_get,
                "total_failed": self._total_failed,
                "success_rate": (
                    self._total_get / self._total_put
                    if self._total_put > 0 else 0
                ),
            }


def create_queue(max_size: int = 0) -> PriorityQueue:
    """Create a queue.

    Args:
        max_size: Maximum size.

    Returns:
        PriorityQueue instance.
    """
    return PriorityQueue(max_size=max_size)
