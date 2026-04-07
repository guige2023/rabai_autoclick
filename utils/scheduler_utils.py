"""
Scheduler Utilities

Provides task scheduling with cron expressions, intervals,
and priority queues for task execution.
"""

from __future__ import annotations

import calendar
import copy
import heapq
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Generic, TypeVar
from enum import Enum, auto

T = TypeVar("T")


class ScheduleType(Enum):
    """Type of schedule."""
    ONCE = auto()
    INTERVAL = auto()
    CRON = auto()
    FIXED_RATE = auto()


@dataclass
class ScheduledTask:
    """A task scheduled for execution."""
    id: str = field(default_factory=lambda: f"task_{time.time():.6f}")
    name: str = ""
    func: Callable[..., Any] | None = None
    args: tuple = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    schedule_type: ScheduleType = ScheduleType.ONCE
    next_run: float = 0.0  # Unix timestamp
    interval: float = 0.0  # For interval/cron schedules
    cron_expr: str = ""
    enabled: bool = True
    priority: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def __lt__(self, other: ScheduledTask) -> bool:
        """Compare by next_run time for heap ordering."""
        return self.next_run < other.next_run

    @property
    def is_due(self) -> bool:
        """Check if task is due for execution."""
        return time.time() >= self.next_run


class Scheduler(ABC):
    """
    Abstract scheduler interface.
    """

    @abstractmethod
    def schedule(self, task: ScheduledTask) -> str:
        """Schedule a task and return its ID."""
        pass

    @abstractmethod
    def cancel(self, task_id: str) -> bool:
        """Cancel a scheduled task."""
        pass

    @abstractmethod
    def get_next_run(self) -> ScheduledTask | None:
        """Get the next task to run."""
        pass

    @abstractmethod
    def tick(self) -> list[ScheduledTask]:
        """Get all tasks that are due and remove them from the schedule."""
        pass


class PriorityScheduler(Scheduler):
    """
    Scheduler using priority queue for task ordering.
    """

    def __init__(self):
        self._tasks: list[ScheduledTask] = []
        self._task_map: dict[str, ScheduledTask] = {}
        self._lock = threading.RLock()

    def schedule(self, task: ScheduledTask) -> str:
        """Add a task to the schedule."""
        with self._lock:
            if task.id in self._task_map:
                self.cancel(task.id)

            heapq.heappush(self._tasks, task)
            self._task_map[task.id] = task
            return task.id

    def cancel(self, task_id: str) -> bool:
        """Cancel and remove a task."""
        with self._lock:
            if task_id not in self._task_map:
                return False

            task = self._task_map.pop(task_id)
            self._tasks.remove(task)
            heapq.heapify(self._tasks)
            return True

    def get_next_run(self) -> ScheduledTask | None:
        """Get the task with earliest next_run."""
        with self._lock:
            if not self._tasks:
                return None
            return self._tasks[0]

    def tick(self) -> list[ScheduledTask]:
        """Get and remove all due tasks."""
        with self._lock:
            due = []
            now = time.time()

            while self._tasks and self._tasks[0].next_run <= now:
                task = heapq.heappop(self._tasks)
                if task.enabled:
                    due.append(task)

            return due

    def get_task(self, task_id: str) -> ScheduledTask | None:
        """Get a task by ID without removing it."""
        with self._lock:
            return self._task_map.get(task_id)

    def list_tasks(self) -> list[ScheduledTask]:
        """List all scheduled tasks."""
        with self._lock:
            return list(self._task_map.values())


class IntervalCalculator:
    """Calculate next run times for various schedule types."""

    @staticmethod
    def every(interval_seconds: float) -> float:
        """Calculate next run for interval-based schedule."""
        return time.time() + interval_seconds

    @staticmethod
    def cron_next(cron_expr: str) -> float:
        """
        Calculate next run for cron expression.

        Simplified cron: "minute hour day month weekday"

        Args:
            cron_expr: Cron expression (e.g., "30 14 * * 1-5" for 2:30 PM weekdays)

        Returns:
            Unix timestamp of next run.
        """
        parts = cron_expr.split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression: {cron_expr}")

        minute_str, hour_str, day_str, month_str, weekday_str = parts

        now = datetime.now()
        next_run = now.replace(second=0, microsecond=0)

        # Simple implementation - increment by minute and check
        for _ in range(60 * 24 * 366):  # Max ~1 year ahead
            next_run += timedelta(minutes=1)

            if IntervalCalculator._matches(next_run.minute, minute_str) and \
               IntervalCalculator._matches(next_run.hour, hour_str) and \
               IntervalCalculator._matches(next_run.day, day_str) and \
               IntervalCalculator._matches(next_run.month, month_str) and \
               IntervalCalculator._matches(next_run.weekday() + 1, weekday_str):  # weekday() is 0-6, cron is 0-6
                return next_run.timestamp()

        raise ValueError(f"Could not find next run for cron: {cron_expr}")

    @staticmethod
    def _matches(value: int, pattern: str) -> bool:
        """Check if a value matches a cron pattern component."""
        if pattern == "*":
            return True

        if "," in pattern:
            return str(value) in pattern.split(",")

        if "/" in pattern:
            base, step = pattern.split("/")
            base = int(base) if base != "*" else 0
            step = int(step)
            return (value - base) % step == 0

        if "-" in pattern:
            start, end = pattern.split("-")
            return int(start) <= value <= int(end)

        return int(pattern) == value

    @staticmethod
    def daily_at(hour: int, minute: int = 0) -> float:
        """Calculate next run for daily execution at specific time."""
        now = datetime.now()
        next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

        if next_run <= now:
            next_run += timedelta(days=1)

        return next_run.timestamp()

    @staticmethod
    def weekly_on(weekday: int, hour: int = 0, minute: int = 0) -> float:
        """
        Calculate next run for weekly execution.

        Args:
            weekday: Day of week (0=Monday, 6=Sunday)
            hour: Hour of day (0-23)
            minute: Minute of hour (0-59)
        """
        now = datetime.now()
        days_ahead = weekday - now.weekday()
        if days_ahead <= 0:
            days_ahead += 7

        next_run = now + timedelta(days=days_ahead)
        next_run = next_run.replace(hour=hour, minute=minute, second=0, microsecond=0)

        return next_run.timestamp()


class SchedulerRunner:
    """
    Runs a scheduler in a background thread.
    """

    def __init__(self, scheduler: Scheduler, tick_interval: float = 1.0):
        self._scheduler = scheduler
        self._tick_interval = tick_interval
        self._running = False
        self._thread: threading.Thread | None = None
        self._handlers: dict[str, Callable[[ScheduledTask], Any]] = {}
        self._execution_stats: dict[str, dict[str, Any]] = {}

    def add_handler(self, task_name: str, handler: Callable[[ScheduledTask], Any]) -> None:
        """Register a handler for a task."""
        self._handlers[task_name] = handler

    def start(self) -> None:
        """Start the scheduler in a background thread."""
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None

    def _run_loop(self) -> None:
        """Main scheduler loop."""
        while self._running:
            due_tasks = self._scheduler.tick()

            for task in due_tasks:
                self._execute_task(task)

            time.sleep(self._tick_interval)

    def _execute_task(self, task: ScheduledTask) -> None:
        """Execute a scheduled task."""
        start = time.time()
        success = False
        error: str | None = None

        try:
            handler = self._handlers.get(task.name) or task.func
            if handler:
                handler(*task.args, **task.kwargs)
                success = True
        except Exception as e:
            error = str(e)

        elapsed = (time.time() - start) * 1000

        self._execution_stats[task.id] = {
            "success": success,
            "error": error,
            "duration_ms": elapsed,
            "timestamp": time.time(),
        }

        # Reschedule if recurring
        if task.schedule_type == ScheduleType.INTERVAL and task.interval > 0:
            task.next_run = time.time() + task.interval
            self._scheduler.schedule(task)
        elif task.schedule_type == ScheduleType.CRON and task.cron_expr:
            try:
                task.next_run = IntervalCalculator.cron_next(task.cron_expr)
                self._scheduler.schedule(task)
            except ValueError:
                pass  # Invalid cron, don't reschedule

    @property
    def execution_stats(self) -> dict[str, dict[str, Any]]:
        """Get execution statistics."""
        return copy.copy(self._execution_stats)
