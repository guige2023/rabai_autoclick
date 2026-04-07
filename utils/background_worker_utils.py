"""Background worker utilities: worker threads, task queues, and graceful shutdown."""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "Task",
    "BackgroundWorker",
    "WorkerPool",
]


@dataclass
class Task:
    """A background task."""

    id: str
    func: Callable[..., Any]
    args: tuple = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    result: Any = None
    error: Exception | None = None
    completed: bool = False


class BackgroundWorker:
    """Single background worker with a task queue."""

    def __init__(self, name: str = "worker") -> None:
        self.name = name
        self._queue: deque[Task] = deque()
        self._running = False
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def enqueue(self, task: Task) -> None:
        with self._lock:
            self._queue.append(task)

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=timeout)

    def _run(self) -> None:
        while self._running:
            task = self._dequeue()
            if task:
                try:
                    task.result = task.func(*task.args, **task.kwargs)
                    task.completed = True
                except Exception as e:
                    task.error = e
                    task.completed = True

    def _dequeue(self) -> Task | None:
        with self._lock:
            if self._queue:
                return self._queue.popleft()
        time.sleep(0.01)
        return None

    @property
    def queue_size(self) -> int:
        with self._lock:
            return len(self._queue)


class WorkerPool:
    """Pool of background workers."""

    def __init__(self, num_workers: int = 4, queue_size: int = 100) -> None:
        self.num_workers = num_workers
        self.queue_size = queue_size
        self._workers: list[BackgroundWorker] = []
        self._queue: deque[Task] = deque(maxlen=queue_size)
        self._lock = threading.Lock()
        self._round_robin = 0

    def start(self) -> None:
        for i in range(self.num_workers):
            worker = BackgroundWorker(name=f"worker-{i}")
            worker.start()
            self._workers.append(worker)

    def stop(self, timeout: float = 5.0) -> None:
        for worker in self._workers:
            worker.stop(timeout=timeout)

    def submit(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> str:
        """Submit a task to the pool."""
        import uuid
        task_id = uuid.uuid4().hex
        task = Task(id=task_id, func=func, args=args, kwargs=kwargs)
        with self._lock:
            self._queue.append(task)
        self._dispatch(task)
        return task_id

    def _dispatch(self, task: Task) -> None:
        with self._lock:
            worker = self._workers[self._round_robin % self.num_workers]
            self._round_robin += 1
        worker.enqueue(task)
