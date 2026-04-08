"""
Automation Scheduler Action Module.

Provides task scheduling with cron expressions,
intervals, and one-time scheduling.
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging
import re

logger = logging.getLogger(__name__)


class ScheduleType(Enum):
    """Schedule types."""
    INTERVAL = "interval"
    CRON = "cron"
    ONE_TIME = "one_time"
    DAILY = "daily"


@dataclass
class Schedule:
    """Base schedule."""
    schedule_id: str
    name: str
    schedule_type: ScheduleType
    handler: Callable
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class IntervalSchedule(Schedule):
    """Interval-based schedule."""
    interval_seconds: float
    initial_delay: float = 0


@dataclass
class CronSchedule(Schedule):
    """Cron-based schedule."""
    cron_expression: str


@dataclass
class OneTimeSchedule(Schedule):
    """One-time schedule."""
    run_at: datetime


class CronParser:
    """Parses cron expressions."""

    def __init__(self, expression: str):
        self.expression = expression
        self.parts = expression.split()

    def get_next_run(self, after: datetime = None) -> Optional[datetime]:
        """Get next run time after given datetime."""
        if after is None:
            after = datetime.now()

        try:
            minute, hour, day, month, dow = self.parts

            next_run = after.replace(second=0, microsecond=0) + timedelta(minutes=1)

            for _ in range(366 * 24 * 60):
                if self._matches(next_run, minute, hour, day, month, dow):
                    return next_run

                next_run += timedelta(minutes=1)

            return None

        except Exception as e:
            logger.error(f"Cron parse error: {e}")
            return None

    def _matches(
        self,
        dt: datetime,
        minute: str,
        hour: str,
        day: str,
        month: str,
        dow: str
    ) -> bool:
        """Check if datetime matches cron expression."""
        return (
            self._match_field(dt.minute, minute) and
            self._match_field(dt.hour, hour) and
            self._match_field(dt.day, day) and
            self._match_field(dt.month, month) and
            self._match_field(dt.isoweekday() % 7, dow)
        )

    def _match_field(self, value: int, field: str) -> bool:
        """Match a single cron field."""
        if field == "*":
            return True

        if "," in field:
            return any(self._match_field(value, f) for f in field.split(","))

        if "/" in field:
            start, step = field.split("/")
            start = int(start) if start != "*" else 0
            return value >= start and (value - start) % int(step) == 0

        if "-" in field:
            start, end = field.split("-")
            return int(start) <= value <= int(end)

        return int(field) == value


class Scheduler:
    """Task scheduler."""

    def __init__(self):
        self.schedules: Dict[str, Schedule] = {}
        self._running = False
        self._tasks: Dict[str, asyncio.Task] = {}

    def add_interval(
        self,
        name: str,
        handler: Callable,
        interval_seconds: float,
        initial_delay: float = 0,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """Add interval-based schedule."""
        import uuid
        schedule_id = str(uuid.uuid4())

        schedule = IntervalSchedule(
            schedule_id=schedule_id,
            name=name,
            schedule_type=ScheduleType.INTERVAL,
            handler=handler,
            interval_seconds=interval_seconds,
            initial_delay=initial_delay,
            metadata=metadata or {}
        )

        self.schedules[schedule_id] = schedule
        return schedule_id

    def add_cron(
        self,
        name: str,
        handler: Callable,
        cron_expression: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """Add cron-based schedule."""
        import uuid
        schedule_id = str(uuid.uuid4())

        schedule = CronSchedule(
            schedule_id=schedule_id,
            name=name,
            schedule_type=ScheduleType.CRON,
            handler=handler,
            cron_expression=cron_expression,
            metadata=metadata or {}
        )

        self.schedules[schedule_id] = schedule
        return schedule_id

    def add_one_time(
        self,
        name: str,
        handler: Callable,
        run_at: datetime,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """Add one-time schedule."""
        import uuid
        schedule_id = str(uuid.uuid4())

        schedule = OneTimeSchedule(
            schedule_id=schedule_id,
            name=name,
            schedule_type=ScheduleType.ONE_TIME,
            handler=handler,
            run_at=run_at,
            metadata=metadata or {}
        )

        self.schedules[schedule_id] = schedule
        return schedule_id

    def remove(self, schedule_id: str) -> bool:
        """Remove a schedule."""
        if schedule_id in self.schedules:
            if schedule_id in self._tasks:
                self._tasks[schedule_id].cancel()
                del self._tasks[schedule_id]
            del self.schedules[schedule_id]
            return True
        return False

    def enable(self, schedule_id: str) -> bool:
        """Enable a schedule."""
        if schedule_id in self.schedules:
            self.schedules[schedule_id].enabled = True
            return True
        return False

    def disable(self, schedule_id: str) -> bool:
        """Disable a schedule."""
        if schedule_id in self.schedules:
            self.schedules[schedule_id].enabled = False
            return True
        return False

    async def start(self):
        """Start the scheduler."""
        self._running = True

        for schedule in self.schedules.values():
            if schedule.enabled:
                self._start_schedule(schedule)

        while self._running:
            await asyncio.sleep(1)

            for schedule_id, task in list(self._tasks.items()):
                if task.done():
                    del self._tasks[schedule_id]

                    schedule = self.schedules.get(schedule_id)
                    if schedule and schedule.enabled:
                        self._start_schedule(schedule)

    async def stop(self):
        """Stop the scheduler."""
        self._running = False
        for task in self._tasks.values():
            task.cancel()
        self._tasks.clear()

    def _start_schedule(self, schedule: Schedule):
        """Start a single schedule."""
        if schedule.schedule_type == ScheduleType.INTERVAL:
            task = asyncio.create_task(self._run_interval(schedule))
            self._tasks[schedule.schedule_id] = task

        elif schedule.schedule_type == ScheduleType.CRON:
            task = asyncio.create_task(self._run_cron(schedule))
            self._tasks[schedule.schedule_id] = task

        elif schedule.schedule_type == ScheduleType.ONE_TIME:
            task = asyncio.create_task(self._run_one_time(schedule))
            self._tasks[schedule.schedule_id] = task

    async def _run_interval(self, schedule: IntervalSchedule):
        """Run interval schedule."""
        if schedule.initial_delay > 0:
            await asyncio.sleep(schedule.initial_delay)

        while schedule.enabled and schedule.schedule_id in self.schedules:
            try:
                if asyncio.iscoroutinefunction(schedule.handler):
                    await schedule.handler()
                else:
                    schedule.handler()
            except Exception as e:
                logger.error(f"Schedule error: {e}")

            await asyncio.sleep(schedule.interval_seconds)

    async def _run_cron(self, schedule: CronSchedule):
        """Run cron schedule."""
        parser = CronParser(schedule.cron_expression)

        while schedule.enabled and schedule.schedule_id in self.schedules:
            next_run = parser.get_next_run()

            if next_run:
                delay = (next_run - datetime.now()).total_seconds()
                if delay > 0:
                    await asyncio.sleep(min(delay, 60))

                try:
                    if asyncio.iscoroutinefunction(schedule.handler):
                        await schedule.handler()
                    else:
                        schedule.handler()
                except Exception as e:
                    logger.error(f"Schedule error: {e}")

            await asyncio.sleep(60)

    async def _run_one_time(self, schedule: OneTimeSchedule):
        """Run one-time schedule."""
        delay = (schedule.run_at - datetime.now()).total_seconds()

        if delay > 0:
            await asyncio.sleep(delay)

        if schedule.schedule_id in self.schedules:
            try:
                if asyncio.iscoroutinefunction(schedule.handler):
                    await schedule.handler()
                else:
                    schedule.handler()
            except Exception as e:
                logger.error(f"Schedule error: {e}")

            self.remove(schedule.schedule_id)


def main():
    """Demonstrate scheduling."""
    scheduler = Scheduler()

    def job():
        print("Job ran!")

    scheduler.add_interval("every_5s", job, 5.0)

    print("Scheduler started")
    print(f"Total schedules: {len(scheduler.schedules)}")


if __name__ == "__main__":
    asyncio.run(main())
