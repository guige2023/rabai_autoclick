"""Task scheduling utilities for automation workflows.

Schedules automation tasks for immediate or delayed execution,
with support for repeating tasks, priority queues, and cancellation.
Designed for orchestrating complex multi-step automation sequences.

Example:
    >>> from utils.automation_scheduler_utils import AutomationScheduler, ScheduledTask
    >>> scheduler = AutomationScheduler()
    >>> task = scheduler.schedule("Click OK button", action=click_ok, delay=2.0, repeat=3)
    >>> scheduler.run()
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Any

__all__ = [
    "TaskPriority",
    "TaskStatus",
    "ScheduledTask",
    "AutomationScheduler",
]


class TaskPriority(Enum):
    """Priority levels for scheduled tasks."""

    LOW = auto()
    NORMAL = auto()
    HIGH = auto()
    CRITICAL = auto()


class TaskStatus(Enum):
    """Status of a scheduled task."""

    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()


@dataclass
class ScheduledTask:
    """A scheduled automation task.

    Attributes:
        task_id: Unique identifier for this task.
        name: Human-readable task name.
        action: Callable to execute.
        args: Positional arguments for the action.
        kwargs: Keyword arguments for the action.
        delay: Delay in seconds before execution.
        repeat: Number of times to repeat (0 = once).
        interval: Interval in seconds between repetitions.
        priority: Task priority level.
        status: Current task status.
        created_at: Creation timestamp.
    """

    name: str
    action: Callable[..., Any]
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    delay: float = 0.0
    repeat: int = 0
    interval: float = 0.0
    priority: TaskPriority = TaskPriority.NORMAL
    task_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    status: TaskStatus = field(default=TaskStatus.PENDING)
    created_at: float = field(default_factory=time.time)
    last_run: float | None = field(default=None, init=False)
    run_count: int = field(default=0, init=False)
    error: str | None = field(default=None, init=False)

    @property
    def is_due(self) -> bool:
        """Check if the task is due for execution."""
        if self.status not in (TaskStatus.PENDING, TaskStatus.RUNNING):
            return False
        elapsed = time.time() - self.created_at
        if self.last_run is None:
            return elapsed >= self.delay
        return (time.time() - self.last_run) >= self.interval

    def execute(self) -> Any:
        """Execute the task action."""
        self.status = TaskStatus.RUNNING
        self.last_run = time.time()
        self.run_count += 1
        try:
            result = self.action(*self.args, **self.kwargs)
            self.status = TaskStatus.PENDING if self.run_count <= self.repeat else TaskStatus.COMPLETED
            return result
        except Exception as exc:
            self.status = TaskStatus.FAILED
            self.error = str(exc)
            raise


class AutomationScheduler:
    """Scheduler for automation tasks with priority and repeat support.

    Tasks are executed in priority order. Supports delayed execution,
    repeating tasks, and task cancellation.

    Example:
        >>> scheduler = AutomationScheduler()
        >>> scheduler.schedule("Open Safari", action=open_safari, priority=TaskPriority.HIGH)
        >>> scheduler.schedule("Wait", action=lambda: time.sleep(2), delay=2.0)
        >>> scheduler.run()  # blocking
    """

    def __init__(self) -> None:
        self._tasks: list[ScheduledTask] = []
        self._running = False
        self._cancelled_ids: set[str] = set()

    def schedule(
        self,
        name: str,
        action: Callable[..., Any],
        args: tuple | None = None,
        kwargs: dict | None = None,
        delay: float = 0.0,
        repeat: int = 0,
        interval: float = 0.0,
        priority: TaskPriority = TaskPriority.NORMAL,
    ) -> ScheduledTask:
        """Schedule a new automation task.

        Args:
            name: Human-readable task name.
            action: Callable to execute.
            args: Positional arguments for the action.
            kwargs: Keyword arguments for the action.
            delay: Initial delay in seconds.
            repeat: Number of times to repeat (0 = once only).
            interval: Seconds between repetitions.
            priority: Task priority.

        Returns:
            The created ScheduledTask.
        """
        task = ScheduledTask(
            name=name,
            action=action,
            args=args or (),
            kwargs=kwargs or {},
            delay=delay,
            repeat=repeat,
            interval=interval,
            priority=priority,
        )
        self._tasks.append(task)
        self._tasks.sort(key=lambda t: (t.priority.value, t.created_at))
        return task

    def cancel(self, task_id: str) -> bool:
        """Cancel a scheduled or running task.

        Args:
            task_id: The task identifier.

        Returns:
            True if the task was found and cancelled.
        """
        for task in self._tasks:
            if task.task_id == task_id:
                task.status = TaskStatus.CANCELLED
                self._cancelled_ids.add(task_id)
                return True
        return False

    def cancel_all(self) -> int:
        """Cancel all tasks.

        Returns:
            Number of tasks cancelled.
        """
        count = 0
        for task in self._tasks:
            if task.status in (TaskStatus.PENDING, TaskStatus.RUNNING):
                task.status = TaskStatus.CANCELLED
                count += 1
        return count

    def get_task(self, task_id: str) -> ScheduledTask | None:
        """Return a task by ID, or None if not found."""
        for task in self._tasks:
            if task.task_id == task_id:
                return task
        return None

    def pending_tasks(self) -> list[ScheduledTask]:
        """Return all pending tasks."""
        return [t for t in self._tasks if t.status == TaskStatus.PENDING]

    def run(self, timeout: float | None = None) -> list[ScheduledTask]:
        """Run all scheduled tasks (blocking).

        Args:
            timeout: Maximum time to run in seconds (None = until done).

        Returns:
            List of all tasks after execution.
        """
        self._running = True
        start = time.time()
        while self._running and self._tasks:
            if timeout and (time.time() - start) >= timeout:
                break
            pending = self.pending_tasks()
            if not pending:
                break
            for task in pending:
                if task.is_due:
                    task.execute()
            time.sleep(0.01)
        return self._tasks

    def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False

    def stats(self) -> dict[str, int]:
        """Return task statistics."""
        return {
            "total": len(self._tasks),
            "pending": sum(1 for t in self._tasks if t.status == TaskStatus.PENDING),
            "running": sum(1 for t in self._tasks if t.status == TaskStatus.RUNNING),
            "completed": sum(1 for t in self._tasks if t.status == TaskStatus.COMPLETED),
            "failed": sum(1 for t in self._tasks if t.status == TaskStatus.FAILED),
            "cancelled": sum(1 for t in self._tasks if t.status == TaskStatus.CANCELLED),
        }
