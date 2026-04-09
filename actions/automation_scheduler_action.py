"""Automation Task Scheduler.

This module provides task scheduling for automation:
- Cron-style scheduling
- One-time and recurring tasks
- Task dependency management
- Execution queuing

Example:
    >>> from actions.automation_scheduler_action import TaskScheduler
    >>> scheduler = TaskScheduler()
    >>> scheduler.schedule("daily_report", run_report, cron="0 9 * * *")
"""

from __future__ import annotations

import time
import logging
import threading
import croniter
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class ScheduleType(Enum):
    """Type of schedule."""
    CRON = "cron"
    INTERVAL = "interval"
    ONCE = "once"


@dataclass
class ScheduledTask:
    """A scheduled task."""
    task_id: str
    name: str
    func: Callable
    schedule_type: ScheduleType
    cron_expr: Optional[str] = None
    interval_seconds: Optional[float] = None
    run_at: Optional[float] = None
    enabled: bool = True
    last_run: Optional[float] = None
    next_run: Optional[float] = None
    run_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


class TaskScheduler:
    """Schedules and executes automation tasks."""

    def __init__(
        self,
        max_concurrent: int = 10,
        default_timeout: float = 300.0,
    ) -> None:
        """Initialize the task scheduler.

        Args:
            max_concurrent: Maximum concurrent task executions.
            default_timeout: Default task timeout in seconds.
        """
        self._tasks: dict[str, ScheduledTask] = {}
        self._lock = threading.RLock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._max_concurrent = max_concurrent
        self._default_timeout = default_timeout
        self._active_tasks: set[str] = set()
        self._stats = {"tasks_scheduled": 0, "tasks_executed": 0, "tasks_failed": 0}

    def schedule(
        self,
        task_id: str,
        name: str,
        func: Callable,
        cron_expr: Optional[str] = None,
        interval_seconds: Optional[float] = None,
        run_at: Optional[float] = None,
        enabled: bool = True,
        metadata: Optional[dict[str, Any]] = None,
    ) -> ScheduledTask:
        """Schedule a task.

        Args:
            task_id: Unique task identifier.
            name: Human-readable task name.
            func: Function to execute.
            cron_expr: Cron expression for recurring tasks.
            interval_seconds: Interval for recurring tasks.
            run_at: Unix timestamp for one-time tasks.
            enabled: Whether task starts enabled.
            metadata: Additional task metadata.

        Returns:
            The ScheduledTask.
        """
        if cron_expr:
            schedule_type = ScheduleType.CRON
            cron = croniter.CronTrigger(cron_expr)
            next_run = cron.get_next(time.time)
        elif interval_seconds:
            schedule_type = ScheduleType.INTERVAL
            next_run = time.time() + interval_seconds
        elif run_at:
            schedule_type = ScheduleType.ONCE
            next_run = run_at
        else:
            raise ValueError("Must specify cron_expr, interval_seconds, or run_at")

        task = ScheduledTask(
            task_id=task_id,
            name=name,
            func=func,
            schedule_type=schedule_type,
            cron_expr=cron_expr,
            interval_seconds=interval_seconds,
            run_at=run_at,
            enabled=enabled,
            next_run=next_run,
            metadata=metadata or {},
        )

        with self._lock:
            self._tasks[task_id] = task
            self._stats["tasks_scheduled"] += 1
            logger.info("Scheduled task: %s (%s)", name, task_id)

        return task

    def unschedule(self, task_id: str) -> bool:
        """Remove a scheduled task.

        Args:
            task_id: Task identifier.

        Returns:
            True if removed, False if not found.
        """
        with self._lock:
            if task_id in self._tasks:
                del self._tasks[task_id]
                logger.info("Unscheduled task: %s", task_id)
                return True
            return False

    def enable_task(self, task_id: str) -> bool:
        """Enable a task."""
        with self._lock:
            task = self._tasks.get(task_id)
            if task:
                task.enabled = True
                return True
            return False

    def disable_task(self, task_id: str) -> bool:
        """Disable a task."""
        with self._lock:
            task = self._tasks.get(task_id)
            if task:
                task.enabled = False
                return True
            return False

    def run_task(self, task_id: str) -> bool:
        """Manually trigger a task.

        Args:
            task_id: Task identifier.

        Returns:
            True if triggered, False if not found or at capacity.
        """
        with self._lock:
            if len(self._active_tasks) >= self._max_concurrent:
                logger.warning("At max concurrent tasks, cannot run %s", task_id)
                return False
            task = self._tasks.get(task_id)
            if task is None:
                return False

        self._execute_task(task)
        return True

    def start(self) -> None:
        """Start the scheduler loop."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._thread = threading.Thread(target=self._scheduler_loop, daemon=True)
            self._thread.start()
            logger.info("Task scheduler started")

    def stop(self) -> None:
        """Stop the scheduler loop."""
        with self._lock:
            self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)
        logger.info("Task scheduler stopped")

    def _scheduler_loop(self) -> None:
        """Main scheduler loop."""
        while self._running:
            now = time.time()

            with self._lock:
                tasks_to_run = [
                    task for task in self._tasks.values()
                    if task.enabled and task.next_run and task.next_run <= now
                ]

            for task in tasks_to_run:
                if len(self._active_tasks) >= self._max_concurrent:
                    break
                self._execute_task(task)

            time.sleep(0.5)

    def _execute_task(self, task: ScheduledTask) -> None:
        """Execute a scheduled task."""
        task.last_run = time.time()
        self._active_tasks.add(task.task_id)

        if task.schedule_type == ScheduleType.CRON and task.cron_expr:
            cron = croniter.CronTrigger(task.cron_expr)
            task.next_run = cron.get_next(time.time)
        elif task.schedule_type == ScheduleType.INTERVAL and task.interval_seconds:
            task.next_run = time.time() + task.interval_seconds
        elif task.schedule_type == ScheduleType.ONCE:
            task.next_run = None
            task.enabled = False

        self._stats["tasks_executed"] += 1

        try:
            result = task.func()
            logger.info("Task %s completed", task.name)
        except Exception as e:
            logger.error("Task %s failed: %s", task.name, e)
            self._stats["tasks_failed"] += 1
        finally:
            self._active_tasks.discard(task.task_id)
            task.run_count += 1

    def get_task(self, task_id: str) -> Optional[ScheduledTask]:
        """Get a task by ID."""
        with self._lock:
            return self._tasks.get(task_id)

    def list_tasks(self) -> list[ScheduledTask]:
        """List all scheduled tasks."""
        with self._lock:
            return list(self._tasks.values())

    def get_stats(self) -> dict[str, Any]:
        """Get scheduler statistics."""
        with self._lock:
            return {
                **self._stats,
                "total_tasks": len(self._tasks),
                "active_tasks": len(self._active_tasks),
                "enabled_tasks": sum(1 for t in self._tasks.values() if t.enabled),
            }
