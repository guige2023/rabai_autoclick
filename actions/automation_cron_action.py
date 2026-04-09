"""Cron-like scheduler for periodic task execution.

Supports cron expressions, intervals, and one-shot scheduling.
"""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from croniter import croniter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable

logger = logging.getLogger(__name__)


class ScheduleType(Enum):
    """Type of schedule."""

    CRON = "cron"
    INTERVAL = "interval"
    ONCE = "once"
    IMMEDIATE = "immediate"


@dataclass
class Schedule:
    """Base schedule configuration."""

    type: ScheduleType = ScheduleType.IMMEDIATE
    next_run: float | None = None


@dataclass
class CronSchedule(Schedule):
    """Cron expression schedule."""

    expression: str = ""
    type: ScheduleType = ScheduleType.CRON

    def __post_init__(self) -> None:
        self._cron = croniter(self.expression, datetime.now())

    def get_next(self, from_time: datetime | None = None) -> datetime:
        """Get next scheduled time."""
        base = from_time or datetime.now()
        self._cron = croniter(self.expression, base)
        return self._cron.get_next(datetime)

    def get_next_timestamp(self, from_time: float | None = None) -> float:
        """Get next scheduled timestamp."""
        dt = self.get_next(datetime.fromtimestamp(from_time or time.time()))
        return dt.timestamp()


@dataclass
class IntervalSchedule(Schedule):
    """Interval-based schedule."""

    seconds: float = 60.0
    type: ScheduleType = ScheduleType.INTERVAL

    def get_next_timestamp(self, from_time: float | None = None) -> float:
        """Get next scheduled timestamp."""
        now = from_time or time.time()
        return now + self.seconds


@dataclass
class OneShotSchedule(Schedule):
    """One-shot schedule at specific time."""

    run_at: datetime | None = None
    run_at_timestamp: float | None = None
    type: ScheduleType = ScheduleType.ONCE

    def __post_init__(self) -> None:
        if self.run_at and not self.run_at_timestamp:
            self.run_at_timestamp = self.run_at.timestamp()

    def get_next_timestamp(self) -> float | None:
        """Get scheduled timestamp."""
        return self.run_at_timestamp


@dataclass
class ScheduledTask:
    """A scheduled task with its configuration."""

    id: str
    name: str
    schedule: Schedule
    func: Callable[..., Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    max_instances: int = 1
    misfire_grace_time: float = 300.0
    last_run: float | None = None
    next_run: float | None = field(default=None)
    run_count: int = 0
    error_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


class Scheduler(ABC):
    """Abstract scheduler base class."""

    @abstractmethod
    async def start(self) -> None:
        """Start the scheduler."""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Stop the scheduler."""
        pass

    @abstractmethod
    async def add_task(self, task: ScheduledTask) -> None:
        """Add a task to the scheduler."""
        pass

    @abstractmethod
    async def remove_task(self, task_id: str) -> None:
        """Remove a task from the scheduler."""
        pass

    @abstractmethod
    async def get_next_run(self) -> float | None:
        """Get next scheduled run time."""
        pass


class CronScheduler(Scheduler):
    """Cron-based task scheduler.

    Args:
        timezone: Timezone for cron expressions.
        default_misfire_grace: Default grace time for missed runs.
    """

    def __init__(self, timezone: str = "UTC", default_misfire_grace: float = 300.0) -> None:
        self.timezone = timezone
        self.default_misfire_grace = default_misfire_grace
        self._tasks: dict[str, ScheduledTask] = {}
        self._running = False
        self._task_instances: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        """Start the scheduler."""
        self._running = True
        asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False

    async def add_task(self, task: ScheduledTask) -> None:
        """Add a task to the scheduler."""
        async with self._lock:
            if task.schedule.type == ScheduleType.CRON:
                task.next_run = task.schedule.get_next_timestamp()
            elif task.schedule.type == ScheduleType.INTERVAL:
                task.next_run = task.schedule.get_next_timestamp()
            elif task.schedule.type == ScheduleType.ONCE:
                task.next_run = task.schedule.get_next_timestamp()
            self._tasks[task.id] = task
            logger.info("Task added: %s (%s)", task.name, task.id)

    async def remove_task(self, task_id: str) -> None:
        """Remove a task from the scheduler."""
        async with self._lock:
            self._tasks.pop(task_id, None)
            self._task_instances.pop(task_id, None)

    async def get_next_run(self) -> float | None:
        """Get next scheduled run time across all tasks."""
        async with self._lock:
            next_runs = [t.next_run for t in self._tasks.values() if t.enabled and t.next_run]
            return min(next_runs) if next_runs else None

    async def _run_loop(self) -> None:
        """Main scheduler loop."""
        while self._running:
            try:
                now = time.time()
                due_tasks = []

                async with self._lock:
                    for task in self._tasks.values():
                        if not task.enabled:
                            continue
                        if task.next_run is None:
                            continue
                        if now >= task.next_run - 1:
                            if self._task_instances.get(task.id, 0) < task.max_instances:
                                due_tasks.append(task)

                for task in due_tasks:
                    asyncio.create_task(self._execute_task(task))

                await asyncio.sleep(1)

            except Exception as e:
                logger.error("Scheduler loop error: %s", e)
                await asyncio.sleep(5)

    async def _execute_task(self, task: ScheduledTask) -> None:
        """Execute a scheduled task."""
        async with self._lock:
            if self._task_instances.get(task.id, 0) >= task.max_instances:
                return
            self._task_instances[task.id] = self._task_instances.get(task.id, 0) + 1
            task.last_run = time.time()

        try:
            logger.info("Executing task: %s", task.name)

            if asyncio.iscoroutinefunction(task.func):
                await task.func(*task.args, **task.kwargs)
            else:
                task.func(*task.args, **task.kwargs)

            task.run_count += 1

            if task.schedule.type == ScheduleType.ONCE:
                async with self._lock:
                    await self.remove_task(task.id)
                    return

            async with self._lock:
                if task.schedule.type == ScheduleType.CRON:
                    task.next_run = task.schedule.get_next_timestamp(task.last_run)
                elif task.schedule.type == ScheduleType.INTERVAL:
                    task.next_run = task.schedule.get_next_timestamp(task.last_run)

            logger.info("Task completed: %s, next run at %s", task.name, datetime.fromtimestamp(task.next_run))

        except Exception as e:
            task.error_count += 1
            logger.error("Task error: %s - %s", task.name, e)

            async with self._lock:
                if task.schedule.type == ScheduleType.CRON:
                    task.next_run = task.schedule.get_next_timestamp(time.time())
                elif task.schedule.type == ScheduleType.INTERVAL:
                    task.next_run = task.schedule.get_next_timestamp(time.time())

        finally:
            async with self._lock:
                self._task_instances[task.id] = max(0, self._task_instances.get(task.id, 1) - 1)

    def get_task(self, task_id: str) -> ScheduledTask | None:
        """Get task by ID."""
        return self._tasks.get(task_id)

    def list_tasks(self) -> list[ScheduledTask]:
        """List all tasks."""
        return list(self._tasks.values())

    def get_stats(self) -> dict[str, Any]:
        """Get scheduler statistics."""
        return {
            "total_tasks": len(self._tasks),
            "enabled_tasks": sum(1 for t in self._tasks.values() if t.enabled),
            "total_runs": sum(t.run_count for t in self._tasks.values()),
            "total_errors": sum(t.error_count for t in self._tasks.values()),
            "next_run": self.get_next_run(),
        }


def cron(expression: str, func: Callable[..., Any] | None = None, **kwargs) -> CronSchedule | Callable:
    """Create a cron schedule.

    Usage:
        @cron("0 9 * * *")
        def daily_task():
            pass

        schedule = cron("*/5 * * * *")
    """
    schedule = CronSchedule(expression=expression)

    if func is None:

        def decorator(f: Callable) -> Callable:
            schedule._cron = croniter(expression, datetime.now())
            return f

        return decorator
    else:
        return schedule


def interval(seconds: float, func: Callable[..., Any] | None = None, **kwargs) -> IntervalSchedule | Callable:
    """Create an interval schedule.

    Usage:
        @interval(60)
        def every_minute():
            pass
    """
    schedule = IntervalSchedule(seconds=seconds)

    if func is None:

        def decorator(f: Callable) -> Callable:
            return f

        return decorator
    else:
        return schedule


def at(datetime_or_timestamp: datetime | float, func: Callable[..., Any] | None = None) -> OneShotSchedule | Callable:
    """Create a one-shot schedule.

    Usage:
        @at(some_datetime)
        def once_task():
            pass
    """
    if isinstance(datetime_or_timestamp, datetime):
        schedule = OneShotSchedule(run_at=datetime_or_timestamp)
    else:
        schedule = OneShotSchedule(run_at_timestamp=datetime_or_timestamp)

    if func is None:

        def decorator(f: Callable) -> Callable:
            return f

        return decorator
    else:
        return schedule
