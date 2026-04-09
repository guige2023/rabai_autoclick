"""Automation scheduler with cron-like scheduling support."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Coroutine, Optional


class ScheduleType(str, Enum):
    """Type of schedule."""

    INTERVAL = "interval"
    CRON = "cron"
    ONE_TIME = "one_time"
    DAILY = "daily"


@dataclass
class ScheduleConfig:
    """Configuration for a schedule."""

    name: str
    schedule_type: ScheduleType
    interval_seconds: Optional[float] = None
    cron_expression: Optional[str] = None
    run_at: Optional[datetime] = None
    enabled: bool = True
    max_runs: Optional[int] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ScheduledTask:
    """A scheduled task execution."""

    schedule_name: str
    scheduled_time: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    success: bool = False
    error: Optional[str] = None
    result: Any = None


class AutomationSchedulerAction:
    """Schedules and executes automation tasks."""

    def __init__(
        self,
        timezone: str = "UTC",
        on_task_start: Optional[Callable[[str], None]] = None,
        on_task_complete: Optional[Callable[[ScheduledTask], None]] = None,
        on_task_error: Optional[Callable[[str, Exception], None]] = None,
    ):
        """Initialize the scheduler.

        Args:
            timezone: Timezone for scheduling.
            on_task_start: Callback when task starts.
            on_task_complete: Callback when task completes.
            on_task_error: Callback when task errors.
        """
        self._schedules: dict[str, ScheduleConfig] = {}
        self._tasks: dict[str, Callable[[], Coroutine[Any, Any, Any]]] = {}
        self._running_tasks: dict[str, asyncio.Task] = {}
        self._execution_history: list[ScheduledTask] = []
        self._timezone = timezone
        self._stop_event = asyncio.Event()
        self._on_task_start = on_task_start
        self._on_task_complete = on_task_complete
        self._on_task_error = on_task_error
        self._run_counts: dict[str, int] = {}

    def add_schedule(
        self,
        config: ScheduleConfig,
        task: Callable[[], Coroutine[Any, Any, Any]],
    ) -> None:
        """Add a scheduled task.

        Args:
            config: Schedule configuration.
            task: Async callable to execute.
        """
        self._schedules[config.name] = config
        self._tasks[config.name] = task
        self._run_counts[config.name] = 0

    def remove_schedule(self, name: str) -> bool:
        """Remove a scheduled task."""
        if name in self._running_tasks:
            self._running_tasks[name].cancel()
        self._schedules.pop(name, None)
        self._tasks.pop(name, None)
        return name not in self._schedules

    async def _run_task(self, name: str) -> None:
        """Execute a single task."""
        schedule = self._schedules[name]
        task_record = ScheduledTask(
            schedule_name=name,
            scheduled_time=datetime.now(),
        )

        if self._on_task_start:
            self._on_task_start(name)

        task_record.started_at = datetime.now()

        try:
            task_func = self._tasks[name]
            result = await task_func()
            task_record.success = True
            task_record.result = result
            if self._on_task_complete:
                self._on_task_complete(task_record)
        except Exception as e:
            task_record.success = False
            task_record.error = str(e)
            if self._on_task_error:
                self._on_task_error(name, e)

        task_record.completed_at = datetime.now()
        self._execution_history.append(task_record)
        self._run_counts[name] = self._run_counts.get(name, 0) + 1

        if schedule.max_runs and self._run_counts[name] >= schedule.max_runs:
            self.remove_schedule(name)

    async def _scheduler_loop(self) -> None:
        """Main scheduler loop."""
        while not self._stop_event.is_set():
            now = datetime.now()

            for name, schedule in self._schedules.items():
                if not schedule.enabled:
                    continue

                if name in self._running_tasks:
                    continue

                should_run = False

                if schedule.schedule_type == ScheduleType.INTERVAL:
                    run_count = self._run_counts.get(name, 0)
                    if run_count == 0:
                        should_run = True
                    else:
                        last_run = None
                        for record in reversed(self._execution_history):
                            if record.schedule_name == name:
                                last_run = record
                                break
                        if last_run and last_run.completed_at:
                            next_run = last_run.completed_at + timedelta(
                                seconds=schedule.interval_seconds or 0
                            )
                            if now >= next_run:
                                should_run = True

                elif schedule.schedule_type == ScheduleType.ONE_TIME:
                    if schedule.run_at and now >= schedule.run_at:
                        should_run = True

                elif schedule.schedule_type == ScheduleType.DAILY:
                    if schedule.run_at:
                        run_today = now.replace(
                            hour=schedule.run_at.hour,
                            minute=schedule.run_at.minute,
                            second=schedule.run_at.second,
                        )
                        if now >= run_today and now < run_today + timedelta(minutes=1):
                            should_run = True

                if should_run:
                    self._running_tasks[name] = asyncio.create_task(self._run_task(name))

            await asyncio.sleep(1)

    async def start(self) -> None:
        """Start the scheduler."""
        asyncio.create_task(self._scheduler_loop())

    async def stop(self) -> None:
        """Stop the scheduler."""
        self._stop_event.set()
        for task in self._running_tasks.values():
            task.cancel()
        await asyncio.gather(*self._running_tasks.values(), return_exceptions=True)

    def get_history(self, limit: int = 100) -> list[ScheduledTask]:
        """Get execution history."""
        return self._execution_history[-limit:]

    def get_status(self) -> dict[str, Any]:
        """Get scheduler status."""
        return {
            "schedules": {
                name: {
                    "enabled": s.enabled,
                    "type": s.schedule_type.value,
                    "run_count": self._run_counts.get(name, 0),
                    "is_running": name in self._running_tasks,
                }
                for name, s in self._schedules.items()
            },
            "total_executions": len(self._execution_history),
        }
