"""Scheduler utilities for RabAI AutoClick.

Provides:
- Periodic task scheduler
- Cron-like scheduling
- Task queue with scheduling
- Delayed execution
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
)


class ScheduleType(Enum):
    """Type of schedule."""

    ONCE = "once"
    INTERVAL = "interval"
    CRON = "cron"


@dataclass
class ScheduledTask:
    """A task scheduled for execution."""

    id: str
    func: Callable[..., Any]
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    schedule_type: ScheduleType = ScheduleType.ONCE
    next_run: Optional[float] = None
    interval: Optional[float] = None
    enabled: bool = True
    max_runs: Optional[int] = None
    run_count: int = 0

    def should_run(self, now: float) -> bool:
        if not self.enabled:
            return False
        if self.next_run is None:
            return True
        return now >= self.next_run

    def advance(self, now: float) -> None:
        self.run_count += 1
        if self.schedule_type == ScheduleType.INTERVAL and self.interval:
            self.next_run = now + self.interval
        elif self.schedule_type == ScheduleType.ONCE:
            self.next_run = None


class Scheduler:
    """In-memory task scheduler.

    Example:
        scheduler = Scheduler()

        def my_task():
            print("Running!")

        scheduler.add_interval(my_task, interval=5.0, task_id="my_task")
        scheduler.start()

        # Later...
        scheduler.remove("my_task")
        scheduler.stop()
    """

    def __init__(self) -> None:
        self._tasks: Dict[str, ScheduledTask] = {}
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def add_once(
        self,
        func: Callable[..., Any],
        delay: float,
        task_id: Optional[str] = None,
        args: tuple = (),
        kwargs: dict = None,
    ) -> str:
        """Schedule task to run once after delay.

        Args:
            func: Function to execute.
            delay: Delay in seconds.
            task_id: Optional task identifier.
            args: Positional arguments.
            kwargs: Keyword arguments.

        Returns:
            Task ID.
        """
        task_id = task_id or f"task_{len(self._tasks)}"
        task = ScheduledTask(
            id=task_id,
            func=func,
            args=args,
            kwargs=kwargs or {},
            schedule_type=ScheduleType.ONCE,
            next_run=time.time() + delay,
        )
        self._tasks[task_id] = task
        return task_id

    def add_interval(
        self,
        func: Callable[..., Any],
        interval: float,
        task_id: Optional[str] = None,
        args: tuple = (),
        kwargs: dict = None,
        max_runs: Optional[int] = None,
    ) -> str:
        """Schedule task to run at fixed interval.

        Args:
            func: Function to execute.
            interval: Interval in seconds.
            task_id: Optional task identifier.
            args: Positional arguments.
            kwargs: Keyword arguments.
            max_runs: Maximum number of runs (None for unlimited).

        Returns:
            Task ID.
        """
        task_id = task_id or f"task_{len(self._tasks)}"
        task = ScheduledTask(
            id=task_id,
            func=func,
            args=args,
            kwargs=kwargs or {},
            schedule_type=ScheduleType.INTERVAL,
            interval=interval,
            next_run=time.time() + interval,
            max_runs=max_runs,
        )
        self._tasks[task_id] = task
        return task_id

    def add_daily(
        self,
        func: Callable[..., Any],
        hour: int,
        minute: int,
        second: int = 0,
        task_id: Optional[str] = None,
        args: tuple = (),
        kwargs: dict = None,
    ) -> str:
        """Schedule task to run daily at specific time.

        Args:
            func: Function to execute.
            hour: Hour (0-23).
            minute: Minute (0-59).
            second: Second (0-59).
            task_id: Optional task identifier.
            args: Positional arguments.
            kwargs: Keyword arguments.

        Returns:
            Task ID.
        """
        task_id = task_id or f"task_{len(self._tasks)}"
        now = datetime.now()
        scheduled_time = now.replace(hour=hour, minute=minute, second=second, microsecond=0)
        if scheduled_time <= now:
            scheduled_time += timedelta(days=1)
        delay = (scheduled_time - now).total_seconds()

        task = ScheduledTask(
            id=task_id,
            func=func,
            args=args,
            kwargs=kwargs or {},
            schedule_type=ScheduleType.ONCE,
            next_run=time.time() + delay,
        )
        self._tasks[task_id] = task
        return task_id

    def remove(self, task_id: str) -> bool:
        """Remove a scheduled task.

        Args:
            task_id: Task ID to remove.

        Returns:
            True if task was removed.
        """
        if task_id in self._tasks:
            del self._tasks[task_id]
            return True
        return False

    def pause(self, task_id: str) -> bool:
        """Pause a scheduled task."""
        if task_id in self._tasks:
            self._tasks[task_id].enabled = False
            return True
        return False

    def resume(self, task_id: str) -> bool:
        """Resume a paused task."""
        if task_id in self._tasks:
            self._tasks[task_id].enabled = True
            return True
        return False

    def start(self) -> None:
        """Start the scheduler in a background thread."""
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2.0)

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            now = time.time()
            for task_id, task in list(self._tasks.items()):
                if task.should_run(now):
                    try:
                        task.func(*task.args, **task.kwargs)
                    except Exception:
                        pass
                    task.advance(now)
                    if task.max_runs and task.run_count >= task.max_runs:
                        self.remove(task_id)
                    elif task.schedule_type == ScheduleType.ONCE:
                        self.remove(task_id)
            self._stop_event.wait(0.1)

    def get_task(self, task_id: str) -> Optional[ScheduledTask]:
        return self._tasks.get(task_id)

    def list_tasks(self) -> List[ScheduledTask]:
        return list(self._tasks.values())

    @property
    def task_count(self) -> int:
        return len(self._tasks)


class AsyncScheduler:
    """Async task scheduler."""

    def __init__(self) -> None:
        self._tasks: Dict[str, ScheduledTask] = {}
        self._running = False
        self._handle: Optional[asyncio.Task] = None

    async def add_interval(
        self,
        func: Callable[..., Any],
        interval: float,
        task_id: Optional[str] = None,
        args: tuple = (),
        kwargs: dict = None,
    ) -> str:
        task_id = task_id or f"task_{len(self._tasks)}"
        task = ScheduledTask(
            id=task_id,
            func=func,
            args=args,
            kwargs=kwargs or {},
            schedule_type=ScheduleType.INTERVAL,
            interval=interval,
            next_run=asyncio.get_event_loop().time() + interval,
        )
        self._tasks[task_id] = task
        return task_id

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._handle = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._running = False
        if self._handle:
            self._handle.cancel()
            try:
                await self._handle
            except asyncio.CancelledError:
                pass

    async def _run_loop(self) -> None:
        while self._running:
            now = asyncio.get_event_loop().time()
            for task_id, task in list(self._tasks.items()):
                if task.should_run(now):
                    try:
                        result = task.func(*task.args, **task.kwargs)
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception:
                        pass
                    task.advance(now)
                    if task.schedule_type == ScheduleType.ONCE:
                        self._tasks.pop(task_id, None)
            await asyncio.sleep(0.1)
