"""
Automation Dispatcher Action Module.

Dispatches automation tasks to workers with load balancing,
 priority queuing, and automatic failover.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
from collections import deque
import logging

logger = logging.getLogger(__name__)


class TaskPriority(Enum):
    """Task priority levels."""
    LOW = 1
    NORMAL = 5
    HIGH = 10
    CRITICAL = 20


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class Task:
    """An automation task."""
    task_id: str
    func: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    priority: TaskPriority = TaskPriority.NORMAL
    status: TaskStatus = TaskStatus.PENDING
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    result: Any = None
    error: Optional[str] = None


@dataclass
class DispatcherStats:
    """Dispatcher statistics."""
    total_tasks: int = 0
    pending_tasks: int = 0
    running_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    avg_execution_time_ms: float = 0.0


class AutomationDispatcherAction:
    """
    Task dispatcher with priority queuing and load balancing.

    Dispatches automation tasks to workers with support for
    priority queues, automatic retries, and execution tracking.

    Example:
        dispatcher = AutomationDispatcherAction(max_workers=4)
        task_id = dispatcher.dispatch(my_func, args=(1, 2), priority=TaskPriority.HIGH)
        result = dispatcher.get_result(task_id)
    """

    def __init__(
        self,
        max_workers: int = 4,
        enable_priorities: bool = True,
    ) -> None:
        self.max_workers = max_workers
        self.enable_priorities = enable_priorities
        self._tasks: dict[str, Task] = {}
        self._pending_queue: list[Task] = []
        self._running: set[str] = set()
        self._completed: deque = deque(maxlen=1000)
        self._semaphore = asyncio.Semaphore(max_workers)

    def dispatch(
        self,
        func: Callable,
        *args: Any,
        priority: TaskPriority = TaskPriority.NORMAL,
        task_id: Optional[str] = None,
        **kwargs: Any,
    ) -> str:
        """Dispatch a task for execution."""
        task_id = task_id or f"task_{int(time.time() * 1000)}"

        task = Task(
            task_id=task_id,
            func=func,
            args=args,
            kwargs=kwargs,
            priority=priority,
        )

        self._tasks[task_id] = task
        self._pending_queue.append(task)

        if self.enable_priorities:
            self._pending_queue.sort(key=lambda t: t.priority.value, reverse=True)

        return task_id

    async def execute_dispatch(self) -> None:
        """Execute pending tasks from the queue."""
        while self._pending_queue:
            task = self._pending_queue.pop(0)

            async with self._semaphore:
                self._running.add(task.task_id)
                task.status = TaskStatus.RUNNING
                task.started_at = time.time()

                try:
                    if asyncio.iscoroutinefunction(task.func):
                        result = await task.func(*task.args, **task.kwargs)
                    else:
                        result = task.func(*task.args, **task.kwargs)

                    task.status = TaskStatus.COMPLETED
                    task.result = result

                except Exception as e:
                    task.status = TaskStatus.FAILED
                    task.error = str(e)
                    logger.error(f"Task {task.task_id} failed: {e}")

                finally:
                    task.completed_at = time.time()
                    self._running.discard(task.task_id)
                    self._completed.append(task)

    def get_task_status(self, task_id: str) -> Optional[TaskStatus]:
        """Get the status of a task."""
        task = self._tasks.get(task_id)
        return task.status if task else None

    def get_result(self, task_id: str) -> Any:
        """Get the result of a completed task."""
        task = self._tasks.get(task_id)
        if not task:
            return None

        if task.status == TaskStatus.FAILED:
            raise RuntimeError(f"Task failed: {task.error}")

        return task.result

    def get_stats(self) -> DispatcherStats:
        """Get dispatcher statistics."""
        completed = [t for t in self._tasks.values() if t.status == TaskStatus.COMPLETED]
        failed = [t for t in self._tasks.values() if t.status == TaskStatus.FAILED]

        execution_times = [
            (t.completed_at - t.started_at) * 1000
            for t in completed
            if t.completed_at and t.started_at
        ]

        avg_time = sum(execution_times) / len(execution_times) if execution_times else 0.0

        return DispatcherStats(
            total_tasks=len(self._tasks),
            pending_tasks=len(self._pending_queue),
            running_tasks=len(self._running),
            completed_tasks=len(completed),
            failed_tasks=len(failed),
            avg_execution_time_ms=avg_time,
        )

    def cancel_task(self, task_id: str) -> bool:
        """Cancel a pending task."""
        task = self._tasks.get(task_id)

        if not task:
            return False

        if task.status == TaskStatus.PENDING:
            task.status = TaskStatus.CANCELLED
            if task in self._pending_queue:
                self._pending_queue.remove(task)
            return True

        return False

    def clear_completed(self) -> None:
        """Clear completed tasks from memory."""
        for task_id in list(self._tasks.keys()):
            task = self._tasks[task_id]
            if task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED):
                del self._tasks[task_id]
