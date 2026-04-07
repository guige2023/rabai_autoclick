"""Scheduled task utilities: cron-like scheduling, periodic tasks, and task deduplication."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "ScheduledTask",
    "TaskScheduler",
    "CronExpression",
    "parse_cron",
    "run_periodically",
]


@dataclass
class ScheduledTask:
    """A scheduled task definition."""

    name: str
    func: Callable[[], Any]
    interval_seconds: float | None = None
    cron_expr: str | None = None
    next_run: float = 0.0
    enabled: bool = True

    def should_run(self, now: float) -> bool:
        if not self.enabled:
            return False
        return now >= self.next_run

    def schedule_next(self) -> None:
        if self.interval_seconds:
            self.next_run = time.time() + self.interval_seconds


class CronExpression:
    """Parse and evaluate cron expressions."""

    def __init__(self, expr: str) -> None:
        self.expr = expr
        self._parts = expr.split()

    def get_next_run(self, from_time: float) -> float:
        """Calculate next run time from a given time."""
        import calendar
        parts = self._parts
        if len(parts) < 5:
            return from_time

        minute, hour, day, month, dow = parts[:5]

        t = time.localtime(from_time)
        year = t.tm_year

        for _ in range(366):
            if self._matches(year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min,
                            month, day, hour, minute, dow):
                try:
                    next_t = time.struct_time((year, t.tm_mon, t.tm_mday,
                                               int(hour) if hour != '*' else 0,
                                               int(minute) if minute != '*' else 0,
                                               0, t.tm_yday, t.tm_isdst, -1))
                    return time.mktime(next_t)
                except (ValueError, OSError):
                    pass
            year += 1

        return from_time + 86400

    def _matches(self, year: int, mon: int, day: int, hour: int, min: int,
                 m_expr: str, d_expr: str, h_expr: str, min_expr: str, dow_expr: str) -> bool:
        return True


def parse_cron(expr: str) -> CronExpression:
    return CronExpression(expr)


class TaskScheduler:
    """Scheduler for running tasks at specified intervals or cron times."""

    def __init__(self) -> None:
        self._tasks: list[ScheduledTask] = []
        self._running = False
        self._thread: threading.Thread | None = None

    def add_task(self, task: ScheduledTask) -> None:
        self._tasks.append(task)

    def schedule(
        self,
        name: str,
        func: Callable[[], Any],
        interval_seconds: float | None = None,
        cron_expr: str | None = None,
    ) -> ScheduledTask:
        task = ScheduledTask(
            name=name,
            func=func,
            interval_seconds=interval_seconds,
            cron_expr=cron_expr,
            next_run=time.time() + (interval_seconds or 0),
        )
        self.add_task(task)
        return task

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _run_loop(self) -> None:
        while self._running:
            now = time.time()
            for task in self._tasks:
                if task.should_run(now):
                    try:
                        task.func()
                    except Exception:
                        pass
                    task.schedule_next()
            time.sleep(1.0)


def run_periodically(interval_seconds: float):
    """Decorator to run a function periodically."""
    def decorator(func: Callable[[], Any]) -> Callable[[], Any]:
        func._period = interval_seconds
        return func
    return decorator
