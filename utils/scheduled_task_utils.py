"""
Scheduled task utilities for cron-like job scheduling.

Provides scheduler with cron expressions, interval tasks,
one-time tasks, and calendar-based scheduling.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
from typing import Any, Callable, Coroutine, Optional

logger = logging.getLogger(__name__)


class ScheduleType(Enum):
    INTERVAL = auto()
    CRON = auto()
    AT = auto()
    ONE_TIME = auto()


@dataclass
class ScheduledTask:
    """A scheduled task definition."""
    name: str
    func: Callable[..., Any]
    schedule_type: ScheduleType
    interval_seconds: Optional[float] = None
    cron_expression: Optional[str] = None
    run_at: Optional[datetime] = None
    enabled: bool = True
    max_instances: int = 1
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskResult:
    """Result of a task execution."""
    task_name: str
    success: bool
    started_at: float
    completed_at: float
    duration_seconds: float
    result: Any = None
    error: Optional[str] = None


class Scheduler:
    """Async task scheduler with multiple schedule types."""

    def __init__(self, timezone_offset: int = 0) -> None:
        self._tasks: dict[str, ScheduledTask] = {}
        self._running = False
        self._task_handles: dict[str, asyncio.Task] = {}
        self._history: list[TaskResult] = []
        self._max_history = 1000
        self._lock = asyncio.Lock()

    def add_interval_task(
        self,
        name: str,
        func: Callable[..., Any],
        interval_seconds: float,
        enabled: bool = True,
        **kwargs: Any,
    ) -> ScheduledTask:
        """Add an interval-based task."""
        task = ScheduledTask(
            name=name,
            func=func,
            schedule_type=ScheduleType.INTERVAL,
            interval_seconds=interval_seconds,
            enabled=enabled,
            **kwargs,
        )
        self._tasks[name] = task
        return task

    def add_cron_task(
        self,
        name: str,
        func: Callable[..., Any],
        cron_expression: str,
        enabled: bool = True,
        **kwargs: Any,
    ) -> ScheduledTask:
        """Add a cron-based task."""
        task = ScheduledTask(
            name=name,
            func=func,
            schedule_type=ScheduleType.CRON,
            cron_expression=cron_expression,
            enabled=enabled,
            **kwargs,
        )
        self._tasks[name] = task
        return task

    def add_one_time_task(
        self,
        name: str,
        func: Callable[..., Any],
        run_at: datetime,
        enabled: bool = True,
        **kwargs: Any,
    ) -> ScheduledTask:
        """Add a one-time task."""
        task = ScheduledTask(
            name=name,
            func=func,
            schedule_type=ScheduleType.ONE_TIME,
            run_at=run_at,
            enabled=enabled,
            **kwargs,
        )
        self._tasks[name] = task
        return task

    def remove_task(self, name: str) -> bool:
        """Remove a task from the scheduler."""
        if name in self._task_handles:
            self._task_handles[name].cancel()
            del self._task_handles[name]
        if name in self._tasks:
            del self._tasks[name]
            return True
        return False

    async def start(self) -> None:
        """Start the scheduler."""
        self._running = True
        for task in self._tasks.values():
            if task.enabled:
                await self._schedule_task(task)
        logger.info("Scheduler started with %d tasks", len(self._tasks))

    async def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        for handle in self._task_handles.values():
            handle.cancel()
        self._task_handles.clear()
        logger.info("Scheduler stopped")

    async def _schedule_task(self, task: ScheduledTask) -> None:
        """Schedule a task to run."""
        if task.schedule_type == ScheduleType.INTERVAL:
            handle = asyncio.create_task(self._run_interval_task(task))
        elif task.schedule_type == ScheduleType.CRON:
            handle = asyncio.create_task(self._run_cron_task(task))
        elif task.schedule_type == ScheduleType.ONE_TIME:
            handle = asyncio.create_task(self._run_one_time_task(task))
        else:
            return

        self._task_handles[task.name] = handle

    async def _run_interval_task(self, task: ScheduledTask) -> None:
        """Run an interval-based task."""
        await asyncio.sleep(task.interval_seconds or 1)
        while self._running:
            await self._execute_task(task)
            if task.interval_seconds:
                await asyncio.sleep(task.interval_seconds)

    async def _run_cron_task(self, task: ScheduledTask) -> None:
        """Run a cron-based task."""
        while self._running:
            delay = self._calculate_cron_delay(task.cron_expression)
            if delay > 0:
                await asyncio.sleep(delay)
            if self._running:
                await self._execute_task(task)
            await asyncio.sleep(1)

    async def _run_one_time_task(self, task: ScheduledTask) -> None:
        """Run a one-time task at a specific time."""
        if task.run_at:
            delay = (task.run_at - datetime.now()).total_seconds()
            if delay > 0:
                await asyncio.sleep(delay)
        if self._running:
            await self._execute_task(task)
            self.remove_task(task.name)

    def _calculate_cron_delay(self, cron_expr: Optional[str]) -> float:
        """Calculate seconds until next cron match."""
        if not cron_expr:
            return 60.0
        parts = cron_expr.split()
        if len(parts) >= 5:
            minute = int(parts[0]) if parts[0] != "*" else 0
            hour = int(parts[1]) if parts[1] != "*" else 0
            now = datetime.now()
            next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(days=1)
            return (next_run - now).total_seconds()
        return 60.0

    async def _execute_task(self, task: ScheduledTask) -> None:
        """Execute a scheduled task."""
        start = time.perf_counter()
        logger.info("Executing task: %s", task.name)
        try:
            if asyncio.iscoroutinefunction(task.func):
                result = await task.func()
            else:
                result = task.func()

            duration = time.perf_counter() - start
            task_result = TaskResult(
                task_name=task.name,
                success=True,
                started_at=start,
                completed_at=time.time(),
                duration_seconds=duration,
                result=result,
            )
            logger.info("Task %s completed in %.2fs", task.name, duration)
        except Exception as e:
            duration = time.perf_counter() - start
            task_result = TaskResult(
                task_name=task.name,
                success=False,
                started_at=start,
                completed_at=time.time(),
                duration_seconds=duration,
                error=str(e),
            )
            logger.error("Task %s failed: %s", task.name, e)

        self._history.append(task_result)
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history:]

    def get_task_status(self, name: str) -> dict[str, Any]:
        """Get status of a task."""
        if name not in self._tasks:
            return {"exists": False}
        task = self._tasks[name]
        history = [r for r in self._history if r.task_name == name]
        last_run = history[-1] if history else None
        return {
            "exists": True,
            "name": task.name,
            "schedule_type": task.schedule_type.name,
            "enabled": task.enabled,
            "is_running": name in self._task_handles,
            "last_run": {
                "success": last_run.success if last_run else None,
                "duration": last_run.duration_seconds if last_run else None,
                "error": last_run.error if last_run else None,
            },
        }

    def get_history(self, limit: int = 100) -> list[TaskResult]:
        """Get task execution history."""
        return self._history[-limit:]
