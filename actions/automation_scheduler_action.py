"""Automation Scheduler Action Module.

Schedule and execute automation tasks with cron-like support.
"""

from __future__ import annotations

import asyncio
import croniter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, Any
import time


class ScheduleType(Enum):
    """Type of schedule."""
    ONCE = "once"
    CRON = "cron"
    INTERVAL = "interval"
    IMMEDIATE = "immediate"


@dataclass
class Schedule:
    """Schedule definition."""
    schedule_type: ScheduleType
    cron_expression: str | None = None
    interval_seconds: float | None = None
    run_at: datetime | None = None


@dataclass
class ScheduledTask:
    """A scheduled automation task."""
    task_id: str
    name: str
    schedule: Schedule
    func: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    enabled: bool = True
    last_run: datetime | None = None
    next_run: datetime | None = None
    run_count: int = 0
    error_count: int = 0


class TaskExecutionError(Exception):
    """Raised when task execution fails."""
    pass


class AutomationScheduler:
    """Scheduler for automation tasks."""

    def __init__(self) -> None:
        self.tasks: dict[str, ScheduledTask] = {}
        self._running = False
        self._task_handle: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    async def add_task(
        self,
        task_id: str,
        name: str,
        schedule: Schedule,
        func: Callable,
        *args,
        **kwargs
    ) -> ScheduledTask:
        """Add a task to the scheduler."""
        async with self._lock:
            task = ScheduledTask(
                task_id=task_id,
                name=name,
                schedule=schedule,
                func=func,
                args=args,
                kwargs=kwargs
            )
            task.next_run = self._calculate_next_run(task)
            self.tasks[task_id] = task
            return task

    def _calculate_next_run(self, task: ScheduledTask) -> datetime | None:
        """Calculate next run time for a task."""
        now = datetime.now(timezone.utc)
        stype = task.schedule.schedule_type
        if stype == ScheduleType.IMMEDIATE:
            return now
        if stype == ScheduleType.ONCE and task.schedule.run_at:
            return task.schedule.run_at
        if stype == ScheduleType.INTERVAL:
            if task.last_run:
                from datetime import timedelta
                return task.last_run + timedelta(seconds=task.schedule.interval_seconds or 0)
            return now
        if stype == ScheduleType.CRON and task.schedule.cron_expression:
            try:
                cron = croniter.croniter(task.schedule.cron_expression, now)
                return cron.get_next(datetime)
            except Exception:
                return None
        return None

    async def remove_task(self, task_id: str) -> bool:
        """Remove a task from scheduler."""
        async with self._lock:
            if task_id in self.tasks:
                del self.tasks[task_id]
                return True
            return False

    async def enable_task(self, task_id: str) -> bool:
        """Enable a task."""
        async with self._lock:
            if task_id in self.tasks:
                self.tasks[task_id].enabled = True
                return True
            return False

    async def disable_task(self, task_id: str) -> bool:
        """Disable a task."""
        async with self._lock:
            if task_id in self.tasks:
                self.tasks[task_id].enabled = False
                return True
            return False

    async def execute_task(self, task: ScheduledTask) -> Any:
        """Execute a single task."""
        try:
            if asyncio.iscoroutinefunction(task.func):
                result = await task.func(*task.args, **task.kwargs)
            else:
                result = task.func(*task.args, **task.kwargs)
            task.last_run = datetime.now(timezone.utc)
            task.run_count += 1
            return result
        except Exception as e:
            task.error_count += 1
            raise TaskExecutionError(f"Task {task.task_id} failed: {e}") from e

    async def _run_loop(self) -> None:
        """Main scheduler loop."""
        while self._running:
            now = datetime.now(timezone.utc)
            async with self._lock:
                for task in list(self.tasks.values()):
                    if not task.enabled:
                        continue
                    if task.next_run and now >= task.next_run:
                        asyncio.create_task(self.execute_task(task))
                        task.next_run = self._calculate_next_run(task)
            await asyncio.sleep(1)

    async def start(self) -> None:
        """Start the scheduler."""
        self._running = True
        self._task_handle = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        if self._task_handle:
            self._task_handle.cancel()
            try:
                await self._task_handle
            except asyncio.CancelledError:
                pass

    def get_pending_tasks(self) -> list[ScheduledTask]:
        """Get list of tasks ready to run."""
        now = datetime.now(timezone.utc)
        return [t for t in self.tasks.values() if t.enabled and t.next_run and now >= t.next_run]
