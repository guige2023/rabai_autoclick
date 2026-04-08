"""
Priority Queue Utility

Priority queue implementation for task scheduling.
Supports min-heap, max-heap, and custom priority functions.

Example:
    >>> pq = PriorityQueue()
    >>> pq.push(Task("B", priority=2))
    >>> pq.push(Task("A", priority=1))
    >>> task = pq.pop()  # Returns task with priority=1 first
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class PriorityItem:
    """An item in the priority queue."""
    priority: float
    item: Any
    timestamp: float = field(default_factory=time.time)
    sequence: int = 0  # For tie-breaking

    def __lt__(self, other: "PriorityItem") -> bool:
        if self.priority != other.priority:
            return self.priority < other.priority
        return self.sequence < other.sequence


class PriorityQueue:
    """
    Thread-safe priority queue using a min-heap.

    Items with lower priority value are extracted first.
    For max-heap behavior, negate priority values.

    Args:
        maxsize: Maximum queue size (0 = unlimited).
    """

    def __init__(self, maxsize: int = 0) -> None:
        self.maxsize = maxsize
        self._heap: list[PriorityItem] = []
        self._lock = threading.RLock()
        self._counter = 0
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)

    def push(
        self,
        item: Any,
        priority: float = 0.0,
        block: bool = True,
        timeout: Optional[float] = None,
    ) -> bool:
        """
        Add an item to the queue.

        Args:
            item: Item to add.
            priority: Priority value (lower = higher priority).
            block: Whether to block if queue is full.
            timeout: Maximum seconds to wait.

        Returns:
            True if added, False on timeout.
        """
        with self._not_full:
            if self.maxsize > 0:
                if block:
                    end_time = time.time() + timeout if timeout else None
                    while len(self._heap) >= self.maxsize:
                        if timeout is not None:
                            remaining = end_time - time.time()
                            if remaining <= 0:
                                return False
                            self._not_full.wait(remaining)
                        else:
                            self._not_full.wait()

            self._counter += 1
            heap_item = PriorityItem(
                priority=priority,
                item=item,
                timestamp=time.time(),
                sequence=self._counter,
            )
            self._heap_append(heap_item)
            self._not_empty.notify()
            return True

    def pop(
        self,
        block: bool = True,
        timeout: Optional[float] = None,
    ) -> Optional[Any]:
        """
        Remove and return the highest priority item.

        Args:
            block: Whether to block if queue is empty.
            timeout: Maximum seconds to wait.

        Returns:
            The item, or None on timeout.
        """
        with self._not_empty:
            if block:
                end_time = time.time() + timeout if timeout else None
                while len(self._heap) == 0:
                    if timeout is not None:
                        remaining = end_time - time.time()
                        if remaining <= 0:
                            return None
                        self._not_empty.wait(remaining)
                    else:
                        self._not_empty.wait()

            if len(self._heap) == 0:
                return None

            item = self._heap_pop()
            self._not_full.notify()
            return item.item if item else None

    def peek(self) -> Optional[Any]:
        """
        Return the highest priority item without removing it.

        Returns:
            The item, or None if queue is empty.
        """
        with self._lock:
            if len(self._heap) == 0:
                return None
            return self._heap[0].item

    def remove(
        self,
        predicate: Callable[[Any], bool],
    ) -> int:
        """
        Remove items matching predicate.

        Args:
            predicate: Function(item) -> bool.

        Returns:
            Number of items removed.
        """
        with self._lock:
            original_len = len(self._heap)
            self._heap = [item for item in self._heap if not predicate(item.item)]
            removed = original_len - len(self._heap)
            # Rebuild heap
            import heapq
            heapq.heapify(self._heap)
            return removed

    def __len__(self) -> int:
        """Return queue size."""
        with self._lock:
            return len(self._heap)

    def __bool__(self) -> bool:
        """Return True if queue is not empty."""
        return len(self) > 0

    def _heap_append(self, item: PriorityItem) -> None:
        """Add item to heap."""
        import heapq
        heapq.heappush(self._heap, item)

    def _heap_pop(self) -> Optional[PriorityItem]:
        """Remove and return smallest item from heap."""
        import heapq
        try:
            return heapq.heappop(self._heap)
        except IndexError:
            return None

    def clear(self) -> None:
        """Remove all items from queue."""
        with self._lock:
            self._heap.clear()
            self._not_full.notify_all()


class ScheduledTask:
    """A task scheduled for future execution."""

    def __init__(
        self,
        task_id: str,
        func: Callable,
        args: tuple = (),
        kwargs: dict | None = None,
        run_at: float = 0.0,
        priority: float = 0.0,
    ) -> None:
        self.task_id = task_id
        self.func = func
        self.args = args
        self.kwargs = kwargs or {}
        self.run_at = run_at
        self.priority = priority

    def __lt__(self, other: "ScheduledTask") -> bool:
        if self.run_at != other.run_at:
            return self.run_at < other.run_at
        return self.priority < other.priority


class TaskScheduler:
    """
    Schedules tasks for future execution.

    Args:
        queue: PriorityQueue to use.
    """

    def __init__(self, queue: Optional[PriorityQueue] = None) -> None:
        self._queue = queue or PriorityQueue()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def schedule(
        self,
        task_id: str,
        func: Callable,
        delay: float = 0.0,
        priority: float = 0.0,
        args: tuple = (),
        kwargs: dict | None = None,
    ) -> None:
        """
        Schedule a task for execution.

        Args:
            task_id: Unique task identifier.
            func: Function to execute.
            delay: Delay in seconds before execution.
            priority: Task priority.
            args: Function arguments.
            kwargs: Function keyword arguments.
        """
        run_at = time.time() + delay
        task = ScheduledTask(
            task_id=task_id,
            func=func,
            args=args,
            kwargs=kwargs,
            run_at=run_at,
            priority=priority,
        )
        self._queue.push(task, priority=run_at)

    def start(self) -> None:
        """Start the scheduler worker thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the scheduler worker thread."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _run_loop(self) -> None:
        """Background task execution loop."""
        while self._running:
            task = self._queue.pop(block=True, timeout=0.5)
            if task is None:
                continue

            if isinstance(task, ScheduledTask):
                delay = task.run_at - time.time()
                if delay > 0:
                    time.sleep(delay)

                try:
                    task.func(*task.args, **task.kwargs)
                except Exception:
                    pass
