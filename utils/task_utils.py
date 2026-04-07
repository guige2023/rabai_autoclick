"""Task utilities for RabAI AutoClick.

Provides:
- Task queuing and scheduling
- Task result handling
- Task cancellation helpers
"""

import asyncio
import threading
import time
from concurrent.futures import Future
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    TypeVar,
)
import uuid

T = TypeVar("T")


class TaskStatus(Enum):
    """Task status enumeration."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class Task(Generic[T]):
    """A tracked task."""

    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    name: str = ""
    status: TaskStatus = TaskStatus.PENDING
    result: Optional[T] = None
    error: Optional[Exception] = None
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    duration: Optional[float] = None

    def mark_running(self) -> None:
        """Mark task as running."""
        self.status = TaskStatus.RUNNING
        self.started_at = time.time()

    def mark_completed(self, result: T) -> None:
        """Mark task as completed."""
        self.status = TaskStatus.COMPLETED
        self.result = result
        self.completed_at = time.time()
        if self.started_at:
            self.duration = self.completed_at - self.started_at

    def mark_failed(self, error: Exception) -> None:
        """Mark task as failed."""
        self.status = TaskStatus.FAILED
        self.error = error
        self.completed_at = time.time()
        if self.started_at:
            self.duration = self.completed_at - self.started_at

    def mark_cancelled(self) -> None:
        """Mark task as cancelled."""
        self.status = TaskStatus.CANCELLED
        self.completed_at = time.time()
        if self.started_at:
            self.duration = self.completed_at - self.started_at

    @property
    def is_done(self) -> bool:
        """Check if task is done."""
        return self.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED)

    def get_elapsed(self) -> float:
        """Get elapsed time so far."""
        if self.completed_at:
            return self.duration or 0.0
        if self.started_at:
            return time.time() - self.started_at
        return time.time() - self.created_at


class TaskQueue:
    """Thread-safe task queue with tracking."""

    def __init__(self, maxsize: int = 0) -> None:
        self._queue: List[Task[Any]] = []
        self._maxsize = maxsize
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)

    def put(self, task: Task[T], block: bool = True, timeout: Optional[float] = None) -> None:
        """Add a task to the queue."""
        with self._not_full:
            if not block:
                if len(self._queue) >= self._maxsize:
                    raise Exception("Queue full")
                self._queue.append(task)
            else:
                while len(self._queue) >= self._maxsize:
                    self._not_full.wait(timeout)
                self._queue.append(task)
            self._not_empty.notify()

    def get(self, block: bool = True, timeout: Optional[float] = None) -> Optional[Task[T]]:
        """Get a task from the queue."""
        with self._not_empty:
            if not block:
                return self._queue.pop(0) if self._queue else None
            while not self._queue:
                self._not_empty.wait(timeout)
                if not self._queue:
                    return None
            return self._queue.pop(0)

    def size(self) -> int:
        """Get queue size."""
        with self._lock:
            return len(self._queue)

    def is_empty(self) -> bool:
        """Check if queue is empty."""
        with self._lock:
            return len(self._queue) == 0

    def clear(self) -> None:
        """Clear all tasks."""
        with self._lock:
            self._queue.clear()


class TaskManager:
    """Manages task lifecycle and tracking."""

    def __init__(self) -> None:
        self._tasks: Dict[str, Task[Any]] = {}
        self._lock = threading.Lock()

    def create_task(
        self,
        func: Callable[..., T],
        *args: Any,
        name: str = "",
        **kwargs: Any,
    ) -> Task[T]:
        """Create a new tracked task.

        Args:
            func: Function to execute.
            *args: Positional arguments.
            name: Task name.
            **kwargs: Keyword arguments.

        Returns:
            Task object.
        """
        task = Task(name=name or func.__name__)

        def _run() -> None:
            task.mark_running()
            try:
                result = func(*args, **kwargs)
                task.mark_completed(result)
            except Exception as e:
                task.mark_failed(e)

        with self._lock:
            self._tasks[task.id] = task

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()

        return task

    def get_task(self, task_id: str) -> Optional[Task[Any]]:
        """Get a task by ID."""
        with self._lock:
            return self._tasks.get(task_id)

    def list_tasks(self, status: Optional[TaskStatus] = None) -> List[Task[Any]]:
        """List all tasks, optionally filtered by status."""
        with self._lock:
            tasks = list(self._tasks.values())
        if status is not None:
            tasks = [t for t in tasks if t.status == status]
        return tasks

    def cancel_task(self, task_id: str) -> bool:
        """Cancel a task (best effort)."""
        with self._lock:
            task = self._tasks.get(task_id)
        if task and not task.is_done:
            task.mark_cancelled()
            return True
        return False

    def remove_completed(self) -> int:
        """Remove completed tasks from tracking.

        Returns:
            Number of tasks removed.
        """
        with self._lock:
            completed = [tid for tid, t in self._tasks.items() if t.is_done]
            for tid in completed:
                del self._tasks[tid]
            return len(completed)

    def get_stats(self) -> dict:
        """Get task statistics."""
        with self._lock:
            tasks = list(self._tasks.values())
            return {
                "total": len(tasks),
                "pending": sum(1 for t in tasks if t.status == TaskStatus.PENDING),
                "running": sum(1 for t in tasks if t.status == TaskStatus.RUNNING),
                "completed": sum(1 for t in tasks if t.status == TaskStatus.COMPLETED),
                "failed": sum(1 for t in tasks if t.status == TaskStatus.FAILED),
                "cancelled": sum(1 for t in tasks if t.status == TaskStatus.CANCELLED),
            }


class ScheduledTask:
    """A task scheduled for future execution."""

    def __init__(
        self,
        func: Callable[..., T],
        *args: Any,
        delay: float = 0.0,
        interval: float = 0.0,
        **kwargs: Any,
    ) -> None:
        """Initialize scheduled task.

        Args:
            func: Function to execute.
            *args: Positional arguments.
            delay: Initial delay in seconds.
            interval: Repeat interval (0 for one-shot).
            **kwargs: Keyword arguments.
        """
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self._delay = delay
        self._interval = interval
        self._active = True
        self._lock = threading.Lock()

    def run(self) -> Optional[Any]:
        """Execute the task."""
        with self._lock:
            if not self._active:
                return None
            return self._func(*self._args, **self._kwargs)

    def stop(self) -> None:
        """Stop the scheduled task."""
        with self._lock:
            self._active = False

    @property
    def is_active(self) -> bool:
        """Check if task is active."""
        with self._lock:
            return self._active


class Scheduler:
    """Simple task scheduler."""

    def __init__(self) -> None:
        self._tasks: Dict[str, ScheduledTask[Any]] = {}
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._lock = threading.Lock()

    def schedule(
        self,
        func: Callable[..., T],
        *args: Any,
        delay: float = 0.0,
        interval: float = 0.0,
        name: str = "",
        **kwargs: Any,
    ) -> str:
        """Schedule a task.

        Args:
            func: Function to execute.
            *args: Positional arguments.
            delay: Initial delay in seconds.
            interval: Repeat interval (0 for one-shot).
            name: Task name.
            **kwargs: Keyword arguments.

        Returns:
            Task ID.
        """
        task_id = str(uuid.uuid4())[:8]
        scheduled = ScheduledTask(func, *args, delay=delay, interval=interval, **kwargs)
        with self._lock:
            self._tasks[task_id] = scheduled
        return task_id

    def cancel(self, task_id: str) -> bool:
        """Cancel a scheduled task."""
        with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id].stop()
                del self._tasks[task_id]
                return True
        return False

    def start(self) -> None:
        """Start the scheduler."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def _run_loop(self) -> None:
        """Main scheduler loop."""
        while self._running:
            to_remove: List[str] = []
            with self._lock:
                for task_id, task in self._tasks.items():
                    if not task.is_active:
                        to_remove.append(task_id)
                        continue
                    task.run()

            for task_id in to_remove:
                with self._lock:
                    if task_id in self._tasks:
                        del self._tasks[task_id]

            time.sleep(1.0)

    def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
