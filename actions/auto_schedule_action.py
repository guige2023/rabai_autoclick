"""Auto-schedule automation action module.

Provides time-based task scheduling with cron expressions,
interval-based scheduling, and calendar-based triggers.
"""

from __future__ import annotations

import time
import logging
import threading
from typing import Optional, Dict, Any, Callable, List
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import re

logger = logging.getLogger(__name__)


class ScheduleType(Enum):
    """Type of schedule."""
    INTERVAL = "interval"
    CRON = "cron"
    ONCE = "once"
    DAILY = "daily"
    HOURLY = "hourly"
    WEEKLY = "weekly"


@dataclass
class ScheduleEntry:
    """A scheduled task entry."""
    name: str
    func: Callable[[], Any]
    schedule_type: ScheduleType
    interval_seconds: float = 0
    cron_expr: str = ""
    next_run: float = 0
    enabled: bool = True
    max_runs: Optional[int] = None
    run_count: int = 0
    description: str = ""


class AutoScheduleAction:
    """Time-based task scheduler.

    Supports interval, cron, daily, hourly, and weekly scheduling.

    Example:
        scheduler = AutoScheduleAction()
        scheduler.interval("heartbeat", heartbeat_task, seconds=60)
        scheduler.cron("midnight_backup", backup_task, "0 0 * * *")
        scheduler.daily("report", report_task, hour=9, minute=0)
        scheduler.start()
    """

    def __init__(self, timezone: str = "UTC") -> None:
        """Initialize scheduler.

        Args:
            timezone: Timezone for scheduling (default UTC).
        """
        self.timezone = timezone
        self._entries: Dict[str, ScheduleEntry] = {}
        self._running = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def interval(
        self,
        name: str,
        func: Callable[[], Any],
        seconds: float = 60,
        max_runs: Optional[int] = None,
        description: str = "",
    ) -> "AutoScheduleAction":
        """Schedule a task to run at fixed intervals.

        Args:
            name: Unique task name.
            func: Callable to execute.
            seconds: Interval in seconds.
            max_runs: Maximum number of runs (None = infinite).
            description: Task description.

        Returns:
            Self for chaining.
        """
        self._entries[name] = ScheduleEntry(
            name=name,
            func=func,
            schedule_type=ScheduleType.INTERVAL,
            interval_seconds=seconds,
            next_run=time.time() + seconds,
            max_runs=max_runs,
            description=description,
        )
        return self

    def daily(
        self,
        name: str,
        func: Callable[[], Any],
        hour: int = 0,
        minute: int = 0,
        second: int = 0,
        max_runs: Optional[int] = None,
        description: str = "",
    ) -> "AutoScheduleAction":
        """Schedule a task to run daily at a specific time.

        Args:
            name: Task name.
            func: Callable to execute.
            hour: Hour (0-23).
            minute: Minute (0-59).
            second: Second (0-59).
            max_runs: Maximum runs.
            description: Task description.

        Returns:
            Self for chaining.
        """
        next_run = self._next_daily_time(hour, minute, second)
        self._entries[name] = ScheduleEntry(
            name=name,
            func=func,
            schedule_type=ScheduleType.DAILY,
            next_run=next_run,
            max_runs=max_runs,
            description=description,
        )
        return self

    def hourly(
        self,
        name: str,
        func: Callable[[], Any],
        minute: int = 0,
        max_runs: Optional[int] = None,
        description: str = "",
    ) -> "AutoScheduleAction":
        """Schedule a task to run hourly at a specific minute.

        Args:
            name: Task name.
            func: Callable to execute.
            minute: Minute within the hour (0-59).
            max_runs: Maximum runs.
            description: Task description.

        Returns:
            Self for chaining.
        """
        next_run = self._next_hourly_time(minute)
        self._entries[name] = ScheduleEntry(
            name=name,
            func=func,
            schedule_type=ScheduleType.HOURLY,
            next_run=next_run,
            max_runs=max_runs,
            description=description,
        )
        return self

    def cron(
        self,
        name: str,
        func: Callable[[], Any],
        cron_expr: str,
        max_runs: Optional[int] = None,
        description: str = "",
    ) -> "AutoScheduleAction":
        """Schedule a task using a cron expression.

        Args:
            name: Task name.
            func: Callable to execute.
            cron_expr: 5-field cron expression (min hour dom mon dow).
            max_runs: Maximum runs.
            description: Task description.

        Returns:
            Self for chaining.

        Example:
            scheduler.cron("weekdays", task, "0 9 * * 1-5")
        """
        next_run = self._parse_cron_next_run(cron_expr)
        self._entries[name] = ScheduleEntry(
            name=name,
            func=func,
            schedule_type=ScheduleType.CRON,
            cron_expr=cron_expr,
            next_run=next_run,
            max_runs=max_runs,
            description=description,
        )
        return self

    def once(
        self,
        name: str,
        func: Callable[[], Any],
        delay_seconds: float = 0,
        description: str = "",
    ) -> "AutoScheduleAction":
        """Schedule a task to run once after a delay.

        Args:
            name: Task name.
            func: Callable to execute.
            delay_seconds: Delay in seconds before execution.
            description: Task description.

        Returns:
            Self for chaining.
        """
        self._entries[name] = ScheduleEntry(
            name=name,
            func=func,
            schedule_type=ScheduleType.ONCE,
            next_run=time.time() + delay_seconds,
            description=description,
        )
        return self

    def remove(self, name: str) -> bool:
        """Remove a scheduled task.

        Args:
            name: Task name.

        Returns:
            True if removed, False if not found.
        """
        if name in self._entries:
            del self._entries[name]
            return True
        return False

    def enable(self, name: str) -> bool:
        """Enable a scheduled task."""
        entry = self._entries.get(name)
        if entry:
            entry.enabled = True
            return True
        return False

    def disable(self, name: str) -> bool:
        """Disable a scheduled task."""
        entry = self._entries.get(name)
        if entry:
            entry.enabled = False
            return True
        return False

    def start(self, blocking: bool = True) -> None:
        """Start the scheduler.

        Args:
            blocking: If True, blocks the current thread.
        """
        self._running.set()
        if blocking:
            self._run_loop()
        else:
            self._thread = threading.Thread(target=self._run_loop, daemon=True)
            self._thread.start()

    def stop(self) -> None:
        """Stop the scheduler."""
        self._running.clear()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

    def get_next_runs(self) -> Dict[str, float]:
        """Get the next run time for each task."""
        return {name: entry.next_run for name, entry in self._entries.items()}

    def get_status(self) -> List[Dict[str, Any]]:
        """Get status of all scheduled tasks."""
        return [
            {
                "name": e.name,
                "type": e.schedule_type.value,
                "enabled": e.enabled,
                "next_run": datetime.fromtimestamp(e.next_run).isoformat(),
                "run_count": e.run_count,
                "max_runs": e.max_runs,
            }
            for e in self._entries.values()
        ]

    def _run_loop(self) -> None:
        """Main scheduler loop."""
        while self._running.is_set():
            now = time.time()
            for entry in list(self._entries.values()):
                if not entry.enabled:
                    continue
                if now >= entry.next_run:
                    self._execute_entry(entry)
            time.sleep(1)

    def _execute_entry(self, entry: ScheduleEntry) -> None:
        """Execute a scheduled entry."""
        try:
            logger.info("Executing scheduled task: %s", entry.name)
            result = entry.func()
            entry.run_count += 1
            logger.info("Task '%s' completed successfully", entry.name)

            if entry.schedule_type == ScheduleType.ONCE:
                entry.enabled = False
            else:
                entry.next_run = self._calculate_next_run(entry)

            if entry.max_runs and entry.run_count >= entry.max_runs:
                entry.enabled = False
                logger.info("Task '%s' reached max runs (%d)", entry.name, entry.max_runs)

        except Exception as e:
            logger.error("Task '%s' failed: %s", entry.name, e)
            entry.next_run = self._calculate_next_run(entry)

    def _calculate_next_run(self, entry: ScheduleEntry) -> float:
        """Calculate the next run time for an entry."""
        if entry.schedule_type == ScheduleType.INTERVAL:
            return time.time() + entry.interval_seconds
        elif entry.schedule_type == ScheduleType.DAILY:
            return self._next_daily_time_from(entry.next_run)
        elif entry.schedule_type == ScheduleType.HOURLY:
            return self._next_hourly_time_from(entry.next_run)
        elif entry.schedule_type == ScheduleType.CRON:
            return self._parse_cron_next_run(entry.cron_expr)
        return time.time() + 60

    def _next_daily_time(self, hour: int, minute: int, second: int) -> float:
        """Get timestamp for next daily occurrence."""
        now = datetime.now()
        target = now.replace(hour=hour, minute=minute, second=second, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        return target.timestamp()

    def _next_daily_time_from(self, prev_run: float) -> float:
        """Get timestamp for next daily occurrence from previous run."""
        prev_dt = datetime.fromtimestamp(prev_run)
        next_dt = prev_dt + timedelta(days=1)
        return next_dt.timestamp()

    def _next_hourly_time(self, minute: int) -> float:
        """Get timestamp for next hourly occurrence."""
        now = datetime.now()
        target = now.replace(minute=minute, second=0, microsecond=0)
        if target <= now:
            target += timedelta(hours=1)
        return target.timestamp()

    def _next_hourly_time_from(self, prev_run: float) -> float:
        """Get timestamp for next hourly occurrence from previous run."""
        prev_dt = datetime.fromtimestamp(prev_run)
        next_dt = prev_dt + timedelta(hours=1)
        return next_dt.timestamp()

    def _parse_cron_next_run(self, cron_expr: str) -> float:
        """Parse cron expression and return next run timestamp.

        Simplified 5-field cron: minute hour dom month dow
        """
        parts = cron_expr.strip().split()
        if len(parts) != 5:
            logger.warning("Invalid cron expression: %s", cron_expr)
            return time.time() + 3600

        now = datetime.now()
        next_run = now + timedelta(minutes=1)
        next_run = next_run.replace(second=0, microsecond=0)

        for _ in range(366 * 24 * 60):
            if self._matches_cron(next_run, parts):
                return next_run.timestamp()
            next_run += timedelta(minutes=1)

        return time.time() + 3600

    def _matches_cron(self, dt: datetime, fields: List[str]) -> bool:
        """Check if a datetime matches a cron expression."""
        minute, hour, dom, month, dow = fields

        def matches(field: str, value: int) -> bool:
            if field == "*":
                return True
            if "/" in field:
                base, step = field.split("/")
                base = int(base) if base != "*" else 0
                return (value - base) % int(step) == 0
            if "," in field:
                return str(value) in field.split(",")
            if "-" in field:
                start, end = field.split("-")
                return int(start) <= value <= int(end)
            return int(field) == value

        return (
            matches(minute, dt.minute) and
            matches(hour, dt.hour) and
            matches(dom, dt.day) and
            matches(month, dt.month) and
            matches(dow, dt.weekday())
        )
