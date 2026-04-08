"""Scheduler utilities for time-based automation execution.

Provides cron-like scheduling, delayed execution,
interval-based repetition, and calendar-aware scheduling
for running automation workflows at specific times.

Example:
    >>> from utils.scheduler_utils import schedule, IntervalTask, CronTask
    >>> schedule('0 9 * * *', lambda: open_browser())
    >>> task = IntervalTask(interval=60, action=lambda: check_email())
    >>> task.start()
"""

from __future__ import annotations

import sched
import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional
from datetime import datetime

__all__ = [
    "schedule",
    "IntervalTask",
    "CronTask",
    "Scheduler",
    "SchedulerError",
]


class SchedulerError(Exception):
    """Raised when scheduling operations fail."""
    pass


@dataclass
class ScheduledTask:
    """A scheduled task."""

    id: str
    action: Callable
    next_run: float
    interval: Optional[float] = None  # None for one-shot tasks
    cron_expr: Optional[str] = None


def schedule(
    when: str | float,
    action: Callable,
    repeat: bool = False,
    interval: Optional[float] = None,
) -> ScheduledTask:
    """Schedule an action for execution.

    Args:
        when: Cron expression string or Unix timestamp.
        action: Callable to execute.
        repeat: If True, repeat the task.
        interval: Repeat interval in seconds.

    Returns:
        ScheduledTask object.

    Example:
        >>> task = schedule('0 9 * * *', action_fn)
        >>> task = schedule(time.time() + 60, action_fn, repeat=True, interval=3600)
    """
    now = time.time()

    if isinstance(when, str):
        next_run = _parse_cron(when, now)
    else:
        next_run = float(when)

    task = ScheduledTask(
        id=str(id(action)),
        action=action,
        next_run=next_run,
        interval=interval if repeat else None,
    )
    return task


def _parse_cron(cron_expr: str, base_time: float) -> float:
    """Parse a cron expression and return the next run timestamp.

    This is a simplified parser supporting:
    * * * * * (minute, hour, day, month, weekday)

    Args:
        cron_expr: Cron expression string.
        base_time: Base timestamp to calculate from.

    Returns:
        Next matching Unix timestamp.
    """
    parts = cron_expr.strip().split()
    if len(parts) != 5:
        raise SchedulerError(f"Invalid cron expression: {cron_expr}")

    dt = datetime.fromtimestamp(base_time)
    minute_p, hour_p, day_p, month_p, weekday_p = parts

    # Simple implementation: find the next matching time
    for offset in range(366 * 24 * 60):  # max 1 year ahead
        candidate = base_time + offset * 60
        cand_dt = datetime.fromtimestamp(candidate)

        if not _cron_part_matches(minute_p, cand_dt.minute):
            continue
        if not _cron_part_matches(hour_p, cand_dt.hour):
            continue
        if not _cron_part_matches(day_p, cand_dt.day):
            continue
        if not _cron_part_matches(month_p, cand_dt.month):
            continue
        if not _cron_part_matches(weekday_p, cand_dt.weekday()):
            continue

        return candidate

    raise SchedulerError(f"No matching time found for cron: {cron_expr}")


def _cron_part_matches(pattern: str, value: int) -> bool:
    """Check if a cron field part matches a value."""
    if pattern == "*":
        return True
    if "," in pattern:
        return str(value) in pattern.split(",")
    if "/" in pattern:
        base, step = pattern.split("/")
        step = int(step)
        if base == "*":
            return value % step == 0
        return (value - int(base)) % step == 0
    if "-" in pattern:
        start, end = pattern.split("-")
        return int(start) <= value <= int(end)
    return int(pattern) == value


class IntervalTask:
    """A task that runs at a fixed interval.

    Example:
        >>> task = IntervalTask(interval=60, action=lambda: print('tick'))
        >>> task.start()
        >>> task.stop()
    """

    def __init__(
        self,
        interval: float,
        action: Callable,
        name: str = "",
        immediately: bool = False,
    ):
        self.interval = interval
        self.action = action
        self.name = name or f"IntervalTask-{id(self)}"
        self.immediately = immediately
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the interval task."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the interval task."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None

    def _run(self) -> None:
        """Main task loop."""
        if self.immediately:
            try:
                self.action()
            except Exception:
                pass

        while self._running:
            time.sleep(self.interval)
            if not self._running:
                break
            try:
                self.action()
            except Exception:
                pass

    @property
    def is_running(self) -> bool:
        return self._running


class Scheduler:
    """Event scheduler supporting one-shot and recurring tasks.

    Example:
        >>> scheduler = Scheduler()
        >>> scheduler.every(60).seconds.do(lambda: print('tick'))
        >>> scheduler.every().day.at('09:00').do(morning_task)
        >>> scheduler.run()
    """

    def __init__(self):
        self._tasks: list[ScheduledTask] = []
        self._running = False
        self._lock = threading.Lock()

    def add_task(self, task: ScheduledTask) -> None:
        with self._lock:
            self._tasks.append(task)

    def every(self, interval: float) -> "SchedulerBuilder":
        return SchedulerBuilder(self, interval)

    def run(self, blocking: bool = True) -> None:
        """Start the scheduler loop."""
        self._running = True

        if blocking:
            self._run_loop()
        else:
            t = threading.Thread(target=self._run_loop, daemon=True)
            t.start()

    def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False

    def _run_loop(self) -> None:
        """Main scheduler loop."""
        while self._running:
            now = time.time()

            with self._lock:
                due_tasks = [t for t in self._tasks if t.next_run <= now]

            for task in due_tasks:
                try:
                    task.action()
                except Exception:
                    pass

                with self._lock:
                    if task.interval:
                        task.next_run = now + task.interval
                    else:
                        self._tasks.remove(task)

            # Sleep until next task or 1 second
            with self._lock:
                if self._tasks:
                    next_run = min(t.next_run for t in self._tasks)
                    sleep_time = max(0.1, next_run - now)
                else:
                    sleep_time = 1.0

            time.sleep(sleep_time)


class SchedulerBuilder:
    """Fluent builder for scheduling tasks."""

    def __init__(self, scheduler: Scheduler, interval: float):
        self._scheduler = scheduler
        self._interval = interval

    def seconds(self) -> "SchedulerBuilder":
        return self

    def minutes(self) -> "SchedulerBuilder":
        self._interval *= 60
        return self

    def hours(self) -> "SchedulerBuilder":
        self._interval *= 3600
        return self

    def do(self, action: Callable) -> ScheduledTask:
        task = ScheduledTask(
            id=str(id(action)),
            action=action,
            next_run=time.time() + self._interval,
            interval=self._interval,
        )
        self._scheduler.add_task(task)
        return task
