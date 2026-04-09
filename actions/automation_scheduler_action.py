"""
Automation Scheduler Action Module.

Schedule-based automation with cron expressions,
interval triggers, one-time tasks, and timezone support.
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ScheduleType(Enum):
    """Schedule type identifiers."""
    ONCE = "once"
    INTERVAL = "interval"
    CRON = "cron"


@dataclass
class Schedule:
    """
    Schedule configuration for automation tasks.

    Attributes:
        name: Task identifier.
        schedule_type: Type of schedule.
        interval_seconds: Interval for INTERVAL type.
        cron_expression: Cron expression for CRON type.
        next_run: Next scheduled run time.
        timezone: Timezone string (e.g., 'America/New_York').
    """
    name: str
    schedule_type: ScheduleType
    interval_seconds: Optional[float] = None
    cron_expression: Optional[str] = None
    next_run: Optional[datetime] = None
    timezone: str = "UTC"
    enabled: bool = True

    def get_next_fire_time(self) -> Optional[datetime]:
        """Calculate next fire time based on schedule type."""
        now = datetime.now(timezone.utc)

        if self.schedule_type == ScheduleType.ONCE:
            return self.next_run

        elif self.schedule_type == ScheduleType.INTERVAL:
            if self.next_run is None:
                return now
            return self.next_run + timedelta(seconds=self.interval_seconds or 0)

        elif self.schedule_type == ScheduleType.CRON:
            return self._cron_next(self.cron_expression)

        return None

    def _cron_next(self, expression: Optional[str]) -> Optional[datetime]:
        """Calculate next cron fire time (simplified cron parser)."""
        if not expression:
            return None

        parts = expression.split()
        if len(parts) < 5:
            return None

        now = datetime.now(timezone.utc)

        try:
            minute, hour, day, month, dow = parts[:5]

            next_time = now.replace(second=0, microsecond=0)
            next_time += timedelta(minutes=1)

            for _ in range(366 * 24 * 60):
                if self._matches_cron_part(minute, next_time.minute, 0, 59) and \
                   self._matches_cron_part(hour, next_time.hour, 0, 23) and \
                   self._matches_cron_part(day, next_time.day, 1, 31) and \
                   self._matches_cron_part(month, next_time.month, 1, 12) and \
                   self._matches_cron_part(dow, next_time.weekday(), 0, 6):
                    return next_time

                next_time += timedelta(minutes=1)

        except Exception as e:
            logger.error(f"Cron parsing error: {e}")

        return None

    def _matches_cron_part(self, part: str, value: int, min_val: int, max_val: int) -> bool:
        """Check if value matches cron part."""
        if part == "*":
            return True

        if "/" in part:
            base, step = part.split("/")
            step = int(step)
            if base == "*":
                return value % step == 0
            return (value - int(base)) % step == 0 if base != "*" else value % step == 0

        if "-" in part:
            start, end = part.split("-")
            return int(start) <= value <= int(end)

        if "," in part:
            return value in [int(x) for x in part.split(",")]

        try:
            return int(part) == value
        except ValueError:
            return True


@dataclass
class ScheduledTask:
    """Represents a scheduled automation task."""
    schedule: Schedule
    func: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    last_run: Optional[datetime] = field(default=None, init=False)
    run_count: int = field(default=0, init=False)
    is_running: bool = field(default=False, init=False)


class AutomationSchedulerAction:
    """
    Scheduler for time-based automation execution.

    Example:
        scheduler = AutomationSchedulerAction()
        scheduler.add_interval_task("heartbeat", heartbeat_func, 60.0)
        scheduler.add_cron_task("daily_report", report_func, "0 8 * * *")
        await scheduler.start()
    """

    def __init__(self):
        """Initialize automation scheduler."""
        self.tasks: dict[str, ScheduledTask] = {}
        self._running = False
        self._task_handle: Optional[asyncio.Task] = None

    def add_interval_task(
        self,
        name: str,
        func: Callable,
        interval_seconds: float,
        *args: Any,
        **kwargs: Any
    ) -> Schedule:
        """
        Add a task that runs at fixed intervals.

        Args:
            name: Task identifier.
            func: Async function to execute.
            interval_seconds: Interval between runs.
            *args: Positional arguments for func.
            **kwargs: Keyword arguments for func.

        Returns:
            Created Schedule object.
        """
        schedule = Schedule(
            name=name,
            schedule_type=ScheduleType.INTERVAL,
            interval_seconds=interval_seconds,
            next_run=datetime.now(timezone.utc)
        )

        self.tasks[name] = ScheduledTask(
            schedule=schedule,
            func=func,
            args=args,
            kwargs=kwargs
        )

        logger.info(f"Added interval task '{name}' every {interval_seconds}s")
        return schedule

    def add_cron_task(
        self,
        name: str,
        func: Callable,
        cron_expression: str,
        *args: Any,
        **kwargs: Any
    ) -> Schedule:
        """
        Add a task that runs based on cron expression.

        Args:
            name: Task identifier.
            func: Async function to execute.
            cron_expression: 5-field cron expression (min hour day month dow).
            *args: Positional arguments for func.
            **kwargs: Keyword arguments for func.

        Returns:
            Created Schedule object.
        """
        schedule = Schedule(
            name=name,
            schedule_type=ScheduleType.CRON,
            cron_expression=cron_expression,
            next_run=datetime.now(timezone.utc)
        )

        self.tasks[name] = ScheduledTask(
            schedule=schedule,
            func=func,
            args=args,
            kwargs=kwargs
        )

        logger.info(f"Added cron task '{name}' with expression: {cron_expression}")
        return schedule

    def add_once_task(
        self,
        name: str,
        func: Callable,
        run_at: datetime,
        *args: Any,
        **kwargs: Any
    ) -> Schedule:
        """
        Add a task that runs once at specified time.

        Args:
            name: Task identifier.
            func: Async function to execute.
            run_at: Datetime to execute the task.
            *args: Positional arguments for func.
            **kwargs: Keyword arguments for func.

        Returns:
            Created Schedule object.
        """
        schedule = Schedule(
            name=name,
            schedule_type=ScheduleType.ONCE,
            next_run=run_at
        )

        self.tasks[name] = ScheduledTask(
            schedule=schedule,
            func=func,
            args=args,
            kwargs=kwargs
        )

        logger.info(f"Added one-time task '{name}' for {run_at}")
        return schedule

    def remove_task(self, name: str) -> bool:
        """
        Remove a scheduled task.

        Args:
            name: Task identifier.

        Returns:
            True if task was removed, False if not found.
        """
        if name in self.tasks:
            del self.tasks[name]
            logger.info(f"Removed task '{name}'")
            return True
        return False

    def enable_task(self, name: str) -> bool:
        """Enable a task."""
        if name in self.tasks:
            self.tasks[name].schedule.enabled = True
            return True
        return False

    def disable_task(self, name: str) -> bool:
        """Disable a task."""
        if name in self.tasks:
            self.tasks[name].schedule.enabled = False
            return True
        return False

    async def start(self) -> None:
        """Start the scheduler loop."""
        self._running = True
        logger.info("Scheduler started")

        while self._running:
            try:
                now = datetime.now(timezone.utc)

                for name, task in list(self.tasks.items()):
                    if not task.schedule.enabled:
                        continue

                    if task.schedule.schedule_type == ScheduleType.ONCE:
                        if task.schedule.next_run and now >= task.schedule.next_run:
                            await self._execute_task(task)
                            self.tasks.pop(name, None)
                            continue

                    next_run = task.schedule.get_next_fire_time()
                    if next_run and now >= next_run:
                        await self._execute_task(task)
                        task.schedule.next_run = next_run

                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Scheduler loop error: {e}")
                await asyncio.sleep(5)

    async def _execute_task(self, task: ScheduledTask) -> None:
        """Execute a scheduled task."""
        if task.is_running:
            logger.warning(f"Task '{task.schedule.name}' already running, skipping")
            return

        task.is_running = True
        task.last_run = datetime.now(timezone.utc)
        task.run_count += 1

        logger.info(f"Executing task '{task.schedule.name}'")

        try:
            if asyncio.iscoroutinefunction(task.func):
                await task.func(*task.args, **task.kwargs)
            else:
                task.func(*task.args, **task.kwargs)
        except Exception as e:
            logger.error(f"Task '{task.schedule.name}' failed: {e}")
        finally:
            task.is_running = False

    async def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        if self._task_handle:
            self._task_handle.cancel()
        logger.info("Scheduler stopped")

    def get_task_info(self, name: str) -> Optional[dict]:
        """Get information about a task."""
        if name not in self.tasks:
            return None

        task = self.tasks[name]
        return {
            "name": name,
            "type": task.schedule.schedule_type.value,
            "enabled": task.schedule.enabled,
            "next_run": task.schedule.get_next_fire_time(),
            "last_run": task.last_run,
            "run_count": task.run_count,
            "is_running": task.is_running
        }

    def list_tasks(self) -> list[dict]:
        """List all scheduled tasks."""
        return [self.get_task_info(name) for name in self.tasks]
