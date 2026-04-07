"""
Scheduler utilities for task scheduling and time management.

Provides cron parsing, task scheduling, and priority queue
utilities for job scheduling.
"""

from __future__ import annotations

import datetime
import heapq
from typing import Callable, NamedTuple


class ScheduledTask(NamedTuple):
    """A scheduled task."""
    run_at: datetime.datetime
    task_id: str
    func: Callable
    args: tuple = ()
    kwargs: dict = None


class Scheduler:
    """
    In-memory task scheduler.

    Example:
        >>> scheduler = Scheduler()
        >>> scheduler.schedule("task1", datetime.datetime.now(), lambda: print("hi"))
        >>> scheduler.run_pending()
    """

    def __init__(self) -> None:
        self._tasks: list[tuple[datetime.datetime, str, ScheduledTask]] = []
        self._task_map: dict[str, ScheduledTask] = {}

    def schedule(
        self,
        task_id: str,
        run_at: datetime.datetime,
        func: Callable,
        *args,
        **kwargs,
    ) -> None:
        """Schedule a task to run at specific time."""
        task = ScheduledTask(run_at, task_id, func, args, kwargs)
        self._task_map[task_id] = task
        heapq.heappush(self._tasks, (run_at, task_id, task))

    def cancel(self, task_id: str) -> bool:
        """Cancel a scheduled task."""
        if task_id in self._task_map:
            del self._task_map[task_id]
            self._tasks = [(dt, tid, t) for dt, tid, t in self._tasks if tid != task_id]
            heapq.heapify(self._tasks)
            return True
        return False

    def run_pending(self) -> list:
        """Run all tasks that are due. Returns list of task IDs run."""
        now = datetime.datetime.now()
        run = []
        while self._tasks and self._tasks[0][0] <= now:
            _, task_id, task = heapq.heappop(self._tasks)
            if task_id in self._task_map:
                try:
                    task.func(*task.args, **(task.kwargs or {}))
                    run.append(task_id)
                except Exception as e:
                    pass
                finally:
                    del self._task_map[task_id]
        return run

    def next_run(self) -> datetime.datetime | None:
        """Get datetime of next scheduled task."""
        if self._tasks:
            return self._tasks[0][0]
        return None


class CronField:
    """Represents a cron field with support for *, ranges, and steps."""

    def __init__(self, field: str, min_val: int, max_val: int) -> None:
        self.min_val = min_val
        self.max_val = max_val
        self.values: set[int] = set()
        self._parse(field)

    def _parse(self, field: str) -> None:
        if field == "*":
            self.values = set(range(self.min_val, self.max_val + 1))
            return
        for part in field.split(","):
            if "/" in part:
                base, step = part.split("/")
                step = int(step)
                if base == "*":
                    rng = range(self.min_val, self.max_val + 1)
                elif "-" in base:
                    start, end = map(int, base.split("-"))
                    rng = range(start, end + 1)
                else:
                    rng = range(int(base), self.max_val + 1)
                self.values.update(rng[::step])
            elif "-" in part:
                start, end = map(int, part.split("-"))
                self.values.update(range(start, end + 1))
            else:
                val = int(part)
                if self.min_val <= val <= self.max_val:
                    self.values.add(val)

    def matches(self, value: int) -> bool:
        return value in self.values


class CronSchedule:
    """
    Simple cron expression parser.

    Format: minute hour day month weekday
    Supports: * (any), n (exact), n-m (range), n/m (step), n,m (list)
    """

    def __init__(self, expression: str) -> None:
        parts = expression.split()
        if len(parts) != 5:
            raise ValueError("Cron expression must have 5 fields")
        self.minute = CronField(parts[0], 0, 59)
        self.hour = CronField(parts[1], 0, 23)
        self.day = CronField(parts[2], 1, 31)
        self.month = CronField(parts[3], 1, 12)
        self.weekday = CronField(parts[4], 0, 6)

    def matches(self, dt: datetime.datetime) -> bool:
        """Check if datetime matches the cron schedule."""
        return (
            self.minute.matches(dt.minute)
            and self.hour.matches(dt.hour)
            and self.day.matches(dt.day)
            and self.month.matches(dt.month)
            and self.weekday.matches(dt.weekday())
        )

    def next_run(self, after: datetime.datetime | None = None) -> datetime.datetime:
        """Get next datetime matching this cron schedule."""
        if after is None:
            after = datetime.datetime.now()
        dt = after.replace(second=0, microsecond=0) + datetime.timedelta(minutes=1)
        for _ in range(366 * 24 * 60):
            if self.matches(dt):
                return dt
            dt += datetime.timedelta(minutes=1)
        raise ValueError("No matching date found")


def priority_queue_merge(
    *iterables,
    key: Callable = lambda x: x,
) -> Generator:
    """Merge multiple sorted iterables using a priority queue."""
    heaps = [(key(item), i, item) for i, item in enumerate(iterables[0])]
    heapq.heapify(heaps)
    while heaps:
        val, src, item = heapq.heappop(heaps)
        yield item
    yield from iterables[1:]


from typing import Generator
