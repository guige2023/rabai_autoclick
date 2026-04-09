"""Async task action module.

Provides async task processing:
- AsyncTaskQueue: Queue for async tasks
- AsyncTaskWorker: Worker for async task execution
- AsyncTaskTracker: Track task status
- TaskBatchProcessor: Batch async task processing
"""

from __future__ import annotations

import asyncio
import time
import uuid
import logging
import threading
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, Future
import functools

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    """Async task status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class AsyncTask:
    """An async task."""
    id: str
    name: str
    handler: Callable[..., Any]
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    timeout: Optional[float] = None


class AsyncTaskTracker:
    """Track async task execution."""

    def __init__(self):
        self._tasks: Dict[str, AsyncTask] = {}
        self._lock = threading.Lock()

    def create_task(
        self,
        name: str,
        handler: Callable[..., Any],
        args: tuple = (),
        kwargs: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> AsyncTask:
        """Create a new async task."""
        task_id = str(uuid.uuid4())
        task = AsyncTask(
            id=task_id,
            name=name,
            handler=handler,
            args=args,
            kwargs=kwargs or {},
            timeout=timeout,
        )
        with self._lock:
            self._tasks[task_id] = task
        return task

    def get_task(self, task_id: str) -> Optional[AsyncTask]:
        """Get a task by ID."""
        with self._lock:
            return self._tasks.get(task_id)

    def update_status(
        self,
        task_id: str,
        status: TaskStatus,
        result: Any = None,
        error: Optional[str] = None,
    ) -> None:
        """Update task status."""
        with self._lock:
            task = self._tasks.get(task_id)
            if task:
                task.status = status
                if result is not None:
                    task.result = result
                if error:
                    task.error = error
                if status == TaskStatus.RUNNING:
                    task.started_at = time.time()
                if status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED):
                    task.completed_at = time.time()

    def list_tasks(
        self,
        status: Optional[TaskStatus] = None,
        limit: int = 100,
    ) -> List[AsyncTask]:
        """List tasks, optionally filtered by status."""
        with self._lock:
            tasks = list(self._tasks.values())
        if status:
            tasks = [t for t in tasks if t.status == status]
        return sorted(tasks, key=lambda t: t.created_at, reverse=True)[:limit]

    def get_metrics(self) -> Dict[str, Any]:
        """Get task metrics."""
        with self._lock:
            tasks = list(self._tasks.values())
        total = len(tasks)
        by_status = {}
        for status in TaskStatus:
            by_status[status.value] = sum(1 for t in tasks if t.status == status)

        completed = [t for t in tasks if t.completed_at and t.started_at]
        avg_duration = (
            sum(t.completed_at - t.started_at for t in completed) / len(completed)
            if completed else 0
        )

        return {
            "total": total,
            "by_status": by_status,
            "avg_duration_seconds": avg_duration,
        }


class AsyncTaskQueue:
    """Queue for async tasks."""

    def __init__(self, max_size: int = 0):
        self._queue: List[AsyncTask] = []
        self._max_size = max_size
        self._lock = threading.Condition()
        self._closed = False

    def put(self, task: AsyncTask, block: bool = True, timeout: Optional[float] = None) -> bool:
        """Add a task to the queue."""
        with self._lock:
            if self._closed:
                raise ValueError("Queue is closed")
            if self._max_size > 0 and len(self._queue) >= self._max_size:
                if not block:
                    return False
                if timeout:
                    end_time = time.time() + timeout
                    while len(self._queue) >= self._max_size:
                        remaining = timeout - (time.time() - end_time)
                        if remaining <= 0:
                            return False
                        self._lock.wait(remaining)
                else:
                    while len(self._queue) >= self._max_size:
                        self._lock.wait()
            self._queue.append(task)
            self._lock.notify()
        return True

    def get(self, block: bool = True, timeout: Optional[float] = None) -> Optional[AsyncTask]:
        """Get a task from the queue."""
        with self._lock:
            if not block:
                if self._queue:
                    return self._queue.pop(0)
                return None
            if timeout is None:
                while not self._queue and not self._closed:
                    self._lock.wait()
            else:
                end_time = time.time() + timeout
                while not self._queue and not self._closed:
                    remaining = timeout - (time.time() - end_time)
                    if remaining <= 0:
                        return None
                    self._lock.wait(remaining)
            if self._queue:
                return self._queue.pop(0)
            return None

    def close(self) -> None:
        """Close the queue."""
        with self._lock:
            self._closed = True
            self._lock.notify_all()

    def __len__(self) -> int:
        with self._lock:
            return len(self._queue)


class AsyncTaskWorker:
    """Worker for async task execution."""

    def __init__(
        self,
        queue: AsyncTaskQueue,
        tracker: AsyncTaskTracker,
        executor: ThreadPoolExecutor,
    ):
        self.queue = queue
        self.tracker = tracker
        self.executor = executor
        self._running = False

    def start(self) -> None:
        """Start the worker."""
        self._running = True
        logger.info("Async task worker started")

    def stop(self) -> None:
        """Stop the worker."""
        self._running = False
        self.queue.close()
        logger.info("Async task worker stopped")

    def submit(self, task: AsyncTask) -> Future:
        """Submit a task for execution."""
        self.tracker.update_status(task.id, TaskStatus.RUNNING)

        def run_task():
            try:
                if task.timeout:
                    result = task.handler(*task.args, **task.kwargs)
                else:
                    result = task.handler(*task.args, **task.kwargs)
                self.tracker.update_status(task.id, TaskStatus.COMPLETED, result=result)
                return result
            except Exception as e:
                self.tracker.update_status(task.id, TaskStatus.FAILED, error=str(e))
                raise

        return self.executor.submit(run_task)


class TaskBatchProcessor:
    """Process tasks in batches."""

    def __init__(
        self,
        batch_size: int = 10,
        concurrency: int = 5,
    ):
        self.batch_size = batch_size
        self.executor = ThreadPoolExecutor(max_workers=concurrency)
        self.tracker = AsyncTaskTracker()

    def submit_batch(
        self,
        tasks: List[Dict[str, Any]],
        handler: Callable[[List[Dict[str, Any]]], Any],
    ) -> List[AsyncTask]:
        """Submit a batch of tasks."""
        batch_tasks = []
        for task_data in tasks:
            task = self.tracker.create_task(
                name=task_data.get("name", "batch_task"),
                handler=lambda td=task_data, h=handler: h([td]),
            )
            batch_tasks.append(task)
        return batch_tasks

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the processor."""
        self.executor.shutdown(wait=wait)


def create_task_queue(max_size: int = 0) -> AsyncTaskQueue:
    """Create an async task queue."""
    return AsyncTaskQueue(max_size=max_size)


def create_task_tracker() -> AsyncTaskTracker:
    """Create an async task tracker."""
    return AsyncTaskTracker()
