"""Automation Scheduler Action module.

Provides cron-like scheduling capabilities for automation tasks.
Supports cron expressions, interval-based scheduling, and
priority-based task queues.
"""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Optional
from croniter import croniter


class ScheduleType(Enum):
    """Type of schedule."""

    CRON = "cron"
    INTERVAL = "interval"
    ONCE = "once"
    MANUAL = "manual"


@dataclass
class ScheduledTask:
    """A scheduled automation task."""

    id: str
    name: str
    func: Callable[..., Any]
    schedule_type: ScheduleType
    schedule_value: str
    args: tuple = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    priority: int = 5
    max_retries: int = 3
    timeout: Optional[float] = None
    _last_run: Optional[float] = field(default=None, repr=False)
    _next_run: Optional[float] = field(default=None, repr=False)
    _run_count: int = field(default=0, repr=False)

    @property
    def last_run_time(self) -> Optional[datetime]:
        """Get last run time as datetime."""
        if self._last_run is None:
            return None
        return datetime.fromtimestamp(self._last_run)

    @property
    def next_run_time(self) -> Optional[datetime]:
        """Get next run time as datetime."""
        if self._next_run is None:
            return None
        return datetime.fromtimestamp(self._next_run)


@dataclass
class CronSchedule:
    """Cron expression schedule."""

    expression: str

    def __post_init__(self):
        self._parser = croniter(self.expression)

    def get_next_run(self, base_time: Optional[datetime] = None) -> float:
        """Get next run timestamp."""
        base = base_time or datetime.now()
        self._parser.set_current(base)
        return self._parser.get_next(float).timestamp()

    def get_next_run_after(self, after: float) -> float:
        """Get next run after given timestamp."""
        base = datetime.fromtimestamp(after)
        self._parser.set_current(base)
        return self._parser.get_next(float).timestamp()


@dataclass
class IntervalSchedule:
    """Interval-based schedule."""

    seconds: float
    jitter: float = 0.0

    def get_next_run(self, base_time: Optional[datetime] = None) -> float:
        """Get next run timestamp."""
        import random

        base = base_time or datetime.now()
        interval = self.seconds
        if self.jitter > 0:
            interval += random.uniform(-self.jitter, self.jitter)
        return (base + timedelta(seconds=interval)).timestamp()


class AutomationScheduler:
    """Scheduler for automation tasks."""

    def __init__(self, max_concurrent: int = 10):
        self.max_concurrent = max_concurrent
        self._tasks: dict[str, ScheduledTask] = {}
        self._running = False
        self._executor: Optional[asyncio.Task] = None
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._task_results: dict[str, Any] = {}
        self._task_errors: dict[str, Exception] = {}

    def add_task(
        self,
        task: ScheduledTask,
    ) -> None:
        """Add a task to the scheduler."""
        self._tasks[task.id] = task
        self._update_next_run(task)

    def remove_task(self, task_id: str) -> bool:
        """Remove a task from the scheduler."""
        if task_id in self._tasks:
            del self._tasks[task_id]
            return True
        return False

    def get_task(self, task_id: str) -> Optional[ScheduledTask]:
        """Get a task by ID."""
        return self._tasks.get(task_id)

    def enable_task(self, task_id: str) -> bool:
        """Enable a task."""
        task = self._tasks.get(task_id)
        if task:
            task.enabled = True
            self._update_next_run(task)
            return True
        return False

    def disable_task(self, task_id: str) -> bool:
        """Disable a task."""
        task = self._tasks.get(task_id)
        if task:
            task.enabled = False
            return True
        return False

    def _update_next_run(self, task: ScheduledTask) -> None:
        """Update next run time for a task."""
        if not task.enabled:
            task._next_run = None
            return

        if task.schedule_type == ScheduleType.CRON:
            schedule = CronSchedule(task.schedule_value)
            task._next_run = schedule.get_next_run()
        elif task.schedule_type == ScheduleType.INTERVAL:
            interval = float(task.schedule_value)
            schedule = IntervalSchedule(interval)
            task._next_run = schedule.get_next_run()
        elif task.schedule_type == ScheduleType.ONCE:
            try:
                dt = datetime.fromisoformat(task.schedule_value)
                task._next_run = dt.timestamp()
            except ValueError:
                task._next_run = None
        else:
            task._next_run = None

    async def _execute_task(self, task: ScheduledTask) -> Any:
        """Execute a scheduled task."""
        async with self._semaphore:
            task._last_run = time.time()
            task._run_count += 1

            try:
                if task.timeout:
                    result = await asyncio.wait_for(
                        task.func(*task.args, **task.kwargs),
                        timeout=task.timeout,
                    )
                else:
                    result = await task.func(*task.args, **task.kwargs)
                self._task_results[task.id] = result
                self._task_errors.pop(task.id, None)
            except Exception as e:
                self._task_errors[task.id] = e
                self._task_results.pop(task.id, None)
                raise e
            finally:
                self._update_next_run(task)

    async def _run_loop(self) -> None:
        """Main scheduler loop."""
        while self._running:
            now = time.time()
            due_tasks = [
                task
                for task in self._tasks.values()
                if task.enabled and task._next_run is not None and task._next_run <= now
            ]

            if due_tasks:
                due_tasks.sort(key=lambda t: (t.priority, t._next_run))
                for task in due_tasks:
                    asyncio.create_task(self._execute_task(task))

            await asyncio.sleep(0.1)

    def start(self) -> None:
        """Start the scheduler."""
        if not self._running:
            self._running = True
            self._executor = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        if self._executor:
            self._executor.cancel()
            try:
                await self._executor
            except asyncio.CancelledError:
                pass

    def get_status(self) -> dict[str, Any]:
        """Get scheduler status."""
        return {
            "running": self._running,
            "total_tasks": len(self._tasks),
            "enabled_tasks": sum(1 for t in self._tasks.values() if t.enabled),
            "tasks": [
                {
                    "id": t.id,
                    "name": t.name,
                    "enabled": t.enabled,
                    "priority": t.priority,
                    "next_run": t._next_run,
                    "last_run": t._last_run,
                    "run_count": t._run_count,
                }
                for t in self._tasks.values()
            ],
        }

    def get_next_due_tasks(self, limit: int = 10) -> list[ScheduledTask]:
        """Get tasks sorted by next run time."""
        tasks = [t for t in self._tasks.values() if t.enabled and t._next_run is not None]
        tasks.sort(key=lambda t: t._next_run)
        return tasks[:limit]
