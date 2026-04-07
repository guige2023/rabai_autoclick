"""Worker queue utilities: thread-safe task queue with priority and scheduled execution."""

from __future__ import annotations

import heapq
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "PrioritizedTask",
    "WorkerQueue",
    "PriorityQueue",
]


@dataclass
class PrioritizedTask:
    """A task with priority for the priority queue."""

    priority: int
    scheduled_at: float
    task_id: str
    func: Callable[..., Any]
    args: tuple = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)

    def __lt__(self, other: "PrioritizedTask") -> bool:
        if self.scheduled_at != other.scheduled_at:
            return self.scheduled_at < other.scheduled_at
        return self.priority < other.priority


class WorkerQueue:
    """Thread-safe task queue with priority and scheduling support."""

    def __init__(self) -> None:
        self._heap: list[PrioritizedTask] = []
        self._lock = threading.RLock()
        self._not_empty = threading.Condition(self._lock)
        self._running = False
        self._worker_thread: threading.Thread | None = None

    def enqueue(
        self,
        func: Callable[..., Any],
        *args: Any,
        priority: int = 0,
        scheduled_at: float | None = None,
        **kwargs: Any,
    ) -> str:
        """Add a task to the queue."""
        task_id = uuid.uuid4().hex
        scheduled = scheduled_at or time.time()
        task = PrioritizedTask(
            priority=priority,
            scheduled_at=scheduled,
            task_id=task_id,
            func=func,
            args=args,
            kwargs=kwargs,
        )
        with self._not_empty:
            heapq.heappush(self._heap, task)
            self._not_empty.notify()
        return task_id

    def dequeue(self, timeout: float | None = None) -> PrioritizedTask | None:
        """Remove and return the next task."""
        with self._not_empty:
            while True:
                if self._heap:
                    next_task = self._heap[0]
                    wait_time = next_task.scheduled_at - time.time()
                    if wait_time <= 0:
                        return heapq.heappop(self._heap)
                    if timeout and timeout > 0:
                        self._not_empty.wait(min(wait_time, timeout))
                        timeout -= wait_time
                        continue
                    return None
                if timeout and timeout > 0:
                    self._not_empty.wait(timeout)
                    timeout = 0
                    continue
                return None

    def size(self) -> int:
        with self._lock:
            return len(self._heap)

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._worker_thread = threading.Thread(target=self._run, daemon=True)
        self._worker_thread.start()

    def stop(self) -> None:
        self._running = False

    def _run(self) -> None:
        while self._running:
            task = self.dequeue(timeout=1.0)
            if task:
                try:
                    task.func(*task.args, **task.kwargs)
                except Exception:
                    pass


class PriorityQueue(WorkerQueue):
    """Priority queue with priority-based ordering."""

    def __init__(self) -> None:
        super().__init__()
