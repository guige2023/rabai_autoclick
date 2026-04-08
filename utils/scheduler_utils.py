"""Scheduler utilities for timed task execution.

Provides periodic and delayed task scheduling for
automation workflows with cron-like expressions.
"""

import sched
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional


@dataclass
class ScheduledTask:
    """Represents a scheduled task."""
    name: str
    interval: float  # seconds, 0 for one-shot
    func: Callable[..., Any]
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    _next_run: float = field(default=0.0, init=False)
    _scheduler: Optional["TaskScheduler"] = field(default=None, init=False)

    @property
    def next_run(self) -> float:
        return self._next_run

    @property
    def next_run_time(self) -> Optional[str]:
        if self._next_run <= 0:
            return None
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self._next_run))


class TaskScheduler:
    """Event-based task scheduler with threading support.

    Example:
        scheduler = TaskScheduler()
        scheduler.every(5).seconds.do(print, "tick")
        scheduler.every(1).minute.do(print, "minute")
        for _ in range(30):
            scheduler.run_pending()
            time.sleep(1)
    """

    def __init__(self) -> None:
        self._scheduler = sched.scheduler(time.time, time.sleep)
        self._tasks: List[ScheduledTask] = []
        self._running = False
        self._lock = threading.Lock()

    def every(self, interval: float) -> "_EveryHelper":
        """Start defining a recurring task.

        Args:
            interval: Interval in seconds.

        Returns:
            Helper for configuring the task.
        """
        return _EveryHelper(self, interval)

    def _add_task(self, task: ScheduledTask) -> None:
        with self._lock:
            task._scheduler = self
            self._tasks.append(task)
            self._reschedule(task)

    def _reschedule(self, task: ScheduledTask) -> None:
        delay = task.interval
        if task._next_run == 0:
            delay = 0
        else:
            delay = task._next_run - time.time()
        if delay < 0:
            delay = 0

        self._scheduler.cancel(task)
        task._next_run = time.time() + delay
        self._scheduler.enter(delay, 0, self._run_task, (task,))

    def _run_task(self, task: ScheduledTask) -> None:
        try:
            task.func(*task.args, **task.kwargs)
        except Exception as e:
            print(f"Task {task.name} failed: {e}")

        if task.interval > 0:
            self._reschedule(task)

    def run_pending(self) -> None:
        """Run any due tasks."""
        self._scheduler.run(blocking=False)

    def run(self, blocking: bool = True) -> None:
        """Run the scheduler.

        Args:
            blocking: If True, run forever.
        """
        self._running = True
        if blocking:
            self._scheduler.run()
        else:
            self._scheduler.run(blocking=False)
        self._running = False

    def cancel(self, task: ScheduledTask) -> None:
        """Cancel a task.

        Args:
            task: Task to cancel.
        """
        with self._lock:
            if task in self._tasks:
                self._tasks.remove(task)
                self._scheduler.cancel(task)

    def cancel_all(self) -> None:
        """Cancel all tasks."""
        with self._lock:
            for task in self._tasks:
                self._scheduler.cancel(task)
            self._tasks.clear()

    def list_tasks(self) -> List[ScheduledTask]:
        """List all scheduled tasks."""
        with self._lock:
            return list(self._tasks)

    @property
    def running(self) -> bool:
        return self._running


class _EveryHelper:
    """Helper for configuring recurring tasks."""

    def __init__(self, scheduler: TaskScheduler, interval: float) -> None:
        self._scheduler = scheduler
        self._interval = interval
        self._task: Optional[ScheduledTask] = None

    def do(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> ScheduledTask:
        """Set the function to run.

        Args:
            func: Function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            ScheduledTask.
        """
        name = getattr(func, "__name__", repr(func))
        self._task = ScheduledTask(
            name=name,
            interval=self._interval,
            func=func,
            args=args,
            kwargs=kwargs,
        )
        self._scheduler._add_task(self._task)
        return self._task

    @property
    def seconds(self) -> "_EveryHelper":
        return self

    @property
    def minutes(self) -> "_EveryHelper":
        self._interval *= 60
        return self

    @property
    def hours(self) -> "_EveryHelper":
        self._interval *= 3600
        return self

    @property
    def days(self) -> "_EveryHelper":
        self._interval *= 86400
        return self


class CronSchedule:
    """Simple cron-like scheduler.

    Supports: minute, hour, day, month, weekday
    """

    def __init__(
        self,
        minute: str = "*",
        hour: str = "*",
        day: str = "*",
        month: str = "*",
        weekday: str = "*",
    ) -> None:
        self._minute = minute
        self._hour = hour
        self._day = day
        self._month = month
        self._weekday = weekday

    def should_run(self, t: time.struct_time) -> bool:
        """Check if cron expression matches time.

        Args:
            t: Time struct.

        Returns:
            True if should run at this time.
        """
        return (
            self._matches(self._minute, t.tm_min) and
            self._matches(self._hour, t.tm_hour) and
            self._matches(self._day, t.tm_mday) and
            self._matches(self._month, t.tm_mon) and
            self._matches(self._weekday, t.tm_wday)
        )

    def _matches(self, pattern: str, value: int) -> bool:
        if pattern == "*":
            return True
        if "," in pattern:
            return str(value) in pattern.split(",")
        if "/" in pattern:
            base, step = pattern.split("/")
            base = base if base != "*" else "0"
            return value >= int(base) and (value - int(base)) % int(step) == 0
        if "-" in pattern:
            start, end = pattern.split("-")
            return int(start) <= value <= int(end)
        return int(pattern) == value
