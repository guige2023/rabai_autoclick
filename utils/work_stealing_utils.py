"""
Work-stealing queue and thread pool utilities.

Provides work-stealing implementations for efficient load
balancing across worker threads with priority support.

Example:
    >>> from utils.work_stealing_utils import WorkStealingQueue, WorkStealingPool
    >>> queue = WorkStealingQueue()
    >>> queue.push(task)
"""

from __future__ import annotations

import asyncio
import heapq
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional


@dataclass(order=True)
class PrioritizedTask:
    """Task with priority for priority queues."""
    priority: int
    task_id: str = field(compare=False)
    data: Any = field(compare=False, default=None)
    created_at: float = field(compare=False, default_factory=time.monotonic)


class WorkStealingQueue:
    """
    Thread-safe work-stealing queue.

    Allows efficient work distribution across threads by
    enabling workers to steal from others' queues when idle.

    Attributes:
        max_size: Maximum queue size (0 for unlimited).
    """

    def __init__(self, max_size: int = 0) -> None:
        """
        Initialize the work-stealing queue.

        Args:
            max_size: Maximum queue size (0 for unlimited).
        """
        self.max_size = max_size
        self._queue: deque = deque()
        self._lock = threading.RLock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)
        self._closed = False

    def push(self, item: Any, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Push an item onto the queue.

        Args:
            item: Item to add.
            blocking: If True, wait if queue is full.
            timeout: Maximum wait time.

        Returns:
            True if item was added, False if timeout.
        """
        with self._not_full:
            if self.max_size > 0:
                while len(self._queue) >= self.max_size and not self._closed:
                    if not blocking:
                        return False
                    if not self._not_full.wait(timeout):
                        return False

            if self._closed:
                return False

            self._queue.append(item)
            self._not_empty.notify()
            return True

    def pop(self, blocking: bool = True, timeout: Optional[float] = None) -> Any:
        """
        Pop an item from the queue.

        Args:
            blocking: If True, wait if queue is empty.
            timeout: Maximum wait time.

        Returns:
            Item from queue.

        Raises:
            EOFError: If queue is closed and empty.
        """
        with self._not_empty:
            while len(self._queue) == 0:
                if self._closed:
                    raise EOFError("Queue is closed")
                if not blocking:
                    raise EOFError("Queue is empty")
                if not self._not_empty.wait(timeout):
                    raise EOFError("Timeout waiting for item")

            item = self._queue.popleft()
            self._not_full.notify()
            return item

    def steal(self) -> Optional[Any]:
        """
        Steal an item from the end of the queue.

        Used by idle workers to steal work from busy workers.

        Returns:
            Item if available, None otherwise.
        """
        with self._lock:
            if len(self._queue) > 1:
                item = self._queue.pop()
                return item
            return None

    def close(self) -> None:
        """Close the queue and wake up waiting threads."""
        with self._lock:
            self._closed = True
            self._not_full.notify_all()
            self._not_empty.notify_all()

    @property
    def size(self) -> int:
        """Get current queue size."""
        with self._lock:
            return len(self._queue)

    def is_empty(self) -> bool:
        """Check if queue is empty."""
        with self._lock:
            return len(self._queue) == 0


class WorkStealingPool:
    """
    Work-stealing thread pool.

    Distributes work across multiple worker threads with
    work-stealing for load balancing.
    """

    def __init__(
        self,
        num_workers: int = 4,
        queue_size: int = 100,
    ) -> None:
        """
        Initialize the work-stealing pool.

        Args:
            num_workers: Number of worker threads.
            queue_size: Size of each worker's queue.
        """
        self.num_workers = num_workers
        self._queues: List[WorkStealingQueue] = [
            WorkStealingQueue(max_size=queue_size)
            for _ in range(num_workers)
        ]
        self._threads: List[threading.Thread] = []
        self._running = False
        self._task_id = 0
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start the worker threads."""
        with self._lock:
            if self._running:
                return
            self._running = True

            for i in range(self.num_workers):
                thread = threading.Thread(
                    target=self._worker_loop,
                    args=(i,),
                    daemon=True,
                )
                thread.start()
                self._threads.append(thread)

    def _worker_loop(self, worker_id: int) -> None:
        """Worker thread main loop."""
        queue = self._queues[worker_id]

        while self._running:
            try:
                task = queue.pop(blocking=True, timeout=0.1)
                if task is not None:
                    self._execute_task(task)
            except EOFError:
                continue
            except Exception:
                pass

            if self._running and queue.is_empty():
                stolen = self._steal_work(worker_id)
                if stolen is not None:
                    self._execute_task(stolen)

    def _steal_work(self, worker_id: int) -> Optional[Any]:
        """Try to steal work from another worker."""
        for i in range(self.num_workers):
            target_id = (worker_id + i + 1) % self.num_workers
            stolen = self._queues[target_id].steal()
            if stolen is not None:
                return stolen
        return None

    def _execute_task(self, task: Any) -> None:
        """Execute a task."""
        try:
            func, args, kwargs = task
            func(*args, **kwargs)
        except Exception:
            pass

    def submit(
        self,
        func: Callable,
        *args: Any,
        **kwargs: Any
    ) -> None:
        """
        Submit a task to the pool.

        Args:
            func: Function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        """
        task = (func, args, kwargs)
        self._pick_queue().push(task)

    def _pick_queue(self) -> WorkStealingQueue:
        """Pick a queue using work-stealing aware strategy."""
        with self._lock:
            self._task_id += 1
            return self._queues[self._task_id % self.num_workers]

    def shutdown(self, wait: bool = True) -> None:
        """
        Shutdown the pool.

        Args:
            wait: If True, wait for tasks to complete.
        """
        with self._lock:
            self._running = False

        for queue in self._queues:
            queue.close()

        if wait:
            for thread in self._threads:
                thread.join(timeout=1.0)


class PriorityWorkStealingQueue(WorkStealingQueue):
    """
    Priority work-stealing queue using a heap.
    """

    def __init__(self, max_size: int = 0) -> None:
        """Initialize the priority work-stealing queue."""
        super().__init__(max_size)
        self._heap: List[PrioritizedTask] = []
        self._lock = threading.RLock()

    def push(
        self,
        item: Any,
        priority: int = 0,
        blocking: bool = True,
        timeout: Optional[float] = None,
    ) -> bool:
        """Push an item with priority."""
        task = PrioritizedTask(
            priority=priority,
            task_id=str(uuid.uuid4()),
            data=item,
        )
        with self._not_full:
            if self.max_size > 0:
                while len(self._heap) >= self.max_size and not self._closed:
                    if not blocking:
                        return False
                    if not self._not_full.wait(timeout):
                        return False

            if self._closed:
                return False

            heapq.heappush(self._heap, task)
            self._not_empty.notify()
            return True

    def pop(self, blocking: bool = True, timeout: Optional[float] = None) -> Any:
        """Pop the highest priority item."""
        with self._not_empty:
            while len(self._heap) == 0:
                if self._closed:
                    raise EOFError("Queue is closed")
                if not blocking:
                    raise EOFError("Queue is empty")
                if not self._not_empty.wait(timeout):
                    raise EOFError("Timeout waiting for item")

            task = heapq.heappop(self._heap)
            self._not_full.notify()
            return task.data

    def steal(self) -> Optional[Any]:
        """Steal the lowest priority item."""
        with self._lock:
            if len(self._heap) > 1:
                return heapq.heappop(self._heap).data
            return None

    @property
    def size(self) -> int:
        """Get current queue size."""
        with self._lock:
            return len(self._heap)


def create_work_stealing_pool(num_workers: int = 4) -> WorkStealingPool:
    """
    Factory function to create a work-stealing pool.

    Args:
        num_workers: Number of worker threads.

    Returns:
        Configured WorkStealingPool instance.
    """
    pool = WorkStealingPool(num_workers=num_workers)
    pool.start()
    return pool
