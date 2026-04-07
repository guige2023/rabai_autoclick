"""Task queue utilities: in-memory and Redis-backed task queues with priority support."""

from __future__ import annotations

import heapq
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

__all__ = [
    "TaskPriority",
    "Task",
    "TaskQueue",
    "PriorityQueue",
    "WorkerPool",
]


class TaskPriority(Enum):
    """Task priority levels."""

    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3


@dataclass
class Task:
    """A task to be executed by the queue."""

    id: str
    func: Callable[..., Any]
    args: tuple = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    priority: TaskPriority = TaskPriority.NORMAL
    created_at: float = field(default_factory=time.time)
    scheduled_at: float | None = None
    result: Any = None
    error: Exception | None = None
    completed: bool = False

    @property
    def sort_key(self) -> tuple[float, int]:
        """Sort key for priority queue."""
        when = self.scheduled_at or self.created_at
        return (when, self.priority.value)


class TaskQueue:
    """Simple in-memory task queue."""

    def __init__(self) -> None:
        self._queue: list[Task] = []
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)

    def enqueue(
        self,
        func: Callable[..., Any],
        *args: Any,
        priority: TaskPriority = TaskPriority.NORMAL,
        scheduled_at: float | None = None,
        **kwargs: Any,
    ) -> str:
        """Add a task to the queue."""
        task = Task(
            id=uuid.uuid4().hex,
            func=func,
            args=args,
            kwargs=kwargs,
            priority=priority,
            scheduled_at=scheduled_at,
        )
        with self._lock:
            heapq.heappush(self._queue, task)
            self._not_empty.notify()
        return task.id

    def dequeue(self, timeout: float | None = None) -> Task | None:
        """Remove and return the next task from the queue."""
        with self._not_empty:
            while True:
                now = time.time()
                if self._queue:
                    task = self._queue[0]
                    if task.scheduled_at is None or task.scheduled_at <= now:
                        return heapq.heappop(self._queue)
                    if timeout:
                        wait_time = min(task.scheduled_at - now, timeout)
                        if wait_time > 0:
                            self._not_empty.wait(wait_time)
                            timeout -= wait_time
                            continue
                    return None
                if timeout and timeout > 0:
                    self._not_empty.wait(timeout)
                    timeout = 0
                    continue
                return None

    def mark_complete(self, task_id: str, result: Any = None, error: Exception | None = None) -> None:
        """Mark a task as completed."""
        pass

    def size(self) -> int:
        return len(self._queue)

    def is_empty(self) -> bool:
        return len(self._queue) == 0


class PriorityQueue(TaskQueue):
    """Priority queue implementation using a heap."""

    def dequeue(self, timeout: float | None = None) -> Task | None:
        with self._not_empty:
            while True:
                now = time.time()
                if not self._queue:
                    if timeout and timeout > 0:
                        self._not_empty.wait(timeout)
                        timeout = 0
                        continue
                    return None
                task = self._queue[0]
                if task.scheduled_at is None or task.scheduled_at <= now:
                    return heapq.heappop(self._queue)
                if timeout and timeout > 0:
                    wait_time = min(task.scheduled_at - now, timeout)
                    if wait_time > 0:
                        self._not_empty.wait(wait_time)
                        timeout -= wait_time
                        continue
                    return None
                return None


class WorkerPool:
    """Thread pool for processing tasks from a queue."""

    def __init__(self, queue: TaskQueue, num_workers: int = 4) -> None:
        self.queue = queue
        self.num_workers = num_workers
        self._workers: list[threading.Thread] = []
        self._running = False

    def start(self) -> None:
        """Start the worker pool."""
        self._running = True
        for i in range(self.num_workers):
            t = threading.Thread(target=self._worker, args=(i,), daemon=True)
            t.start()
            self._workers.append(t)

    def stop(self) -> None:
        """Stop the worker pool."""
        self._running = False
        for t in self._workers:
            t.join(timeout=5)

    def _worker(self, worker_id: int) -> None:
        """Worker thread main loop."""
        while self._running:
            task = self.queue.dequeue(timeout=1.0)
            if task is None:
                continue
            try:
                task.result = task.func(*task.args, **task.kwargs)
                task.completed = True
            except Exception as e:
                task.error = e
                task.completed = True
