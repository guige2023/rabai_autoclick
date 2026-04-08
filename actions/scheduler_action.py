"""
Task scheduler module for cron-based and interval-based job scheduling.

Supports cron expressions, interval scheduling, one-time tasks,
task queuing, and execution persistence.
"""
from __future__ import annotations

import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Optional


class ScheduleType(Enum):
    """Type of schedule."""
    CRON = "cron"
    INTERVAL = "interval"
    ONE_TIME = "one_time"
    DELAYED = "delayed"


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


@dataclass
class Schedule:
    """A task schedule definition."""
    id: str
    name: str
    task: Callable
    schedule_type: ScheduleType
    cron_expression: Optional[str] = None
    interval_seconds: Optional[float] = None
    run_at: Optional[float] = None
    enabled: bool = True
    timezone: str = "UTC"
    metadata: dict = field(default_factory=dict)

    def __post_init__(self):
        if not self.id:
            self.id = str(uuid.uuid4())[:8]


@dataclass
class ScheduledTask:
    """A scheduled task execution instance."""
    id: str
    schedule_id: str
    status: TaskStatus = TaskStatus.PENDING
    input_data: Any = None
    output_data: Any = None
    error: Optional[str] = None
    scheduled_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    execution_time_ms: float = 0.0
    retry_count: int = 0
    max_retries: int = 0


@dataclass
class CronField:
    """A single cron field."""
    name: str
    min_value: int
    max_value: int


class CronParser:
    """Parser for cron expressions."""

    FIELDS = [
        CronField("minute", 0, 59),
        CronField("hour", 0, 23),
        CronField("day", 1, 31),
        CronField("month", 1, 12),
        CronField("weekday", 0, 6),
    ]

    @staticmethod
    def parse(expression: str) -> dict:
        """Parse a cron expression into components."""
        parts = expression.split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression: {expression}")

        return {
            "minute": parts[0],
            "hour": parts[1],
            "day": parts[2],
            "month": parts[3],
            "weekday": parts[4],
        }

    @staticmethod
    def next_run(cron_expr: str, from_time: Optional[datetime] = None) -> datetime:
        """Calculate the next run time for a cron expression."""
        from datetime import datetime, timedelta

        parts = cron_expr.split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression: {cron_expr}")

        current = from_time or datetime.now()
        current = current.replace(second=0, microsecond=0)

        minute_str, hour_str, day_str, month_str, weekday_str = parts

        for delta in range(366 * 24 * 60):
            check_time = current + timedelta(minutes=delta)

            if not CronParser._matches_field(minute_str, check_time.minute):
                continue
            if not CronParser._matches_field(hour_str, check_time.hour):
                continue
            if not CronParser._matches_field(day_str, check_time.day):
                continue
            if not CronParser._matches_field(month_str, check_time.month):
                continue
            if not CronParser._matches_field(weekday_str, check_time.weekday()):
                continue

            return check_time.replace(second=0, microsecond=0)

        return current + timedelta(days=1)

    @staticmethod
    def _matches_field(field_expr: str, value: int) -> bool:
        """Check if a value matches a cron field expression."""
        if field_expr == "*":
            return True

        if "/" in field_expr:
            base, step = field_expr.split("/")
            step = int(step)
            if base == "*":
                return value % step == 0
            base_val = int(base)
            return (value - base_val) % step == 0

        if "," in field_expr:
            values = [int(v) for v in field_expr.split(",")]
            return value in values

        if "-" in field_expr:
            start, end = field_expr.split("-")
            return int(start) <= value <= int(end)

        return int(field_expr) == value


class TaskScheduler:
    """
    Task scheduler for cron-based and interval-based scheduling.

    Supports cron expressions, interval scheduling, one-time tasks,
    and task persistence.
    """

    def __init__(self, timezone: str = "UTC"):
        self.timezone = timezone
        self._schedules: dict[str, Schedule] = {}
        self._tasks: dict[str, ScheduledTask] = {}
        self._task_queue: deque = deque()
        self._running = False

    def add_schedule(
        self,
        name: str,
        task: Callable,
        schedule_type: ScheduleType,
        cron_expression: Optional[str] = None,
        interval_seconds: Optional[float] = None,
        run_at: Optional[float] = None,
        enabled: bool = True,
        metadata: Optional[dict] = None,
    ) -> Schedule:
        """Add a new schedule."""
        schedule = Schedule(
            id=str(uuid.uuid4())[:8],
            name=name,
            task=task,
            schedule_type=schedule_type,
            cron_expression=cron_expression,
            interval_seconds=interval_seconds,
            run_at=run_at,
            enabled=enabled,
            metadata=metadata or {},
        )

        self._schedules[schedule.id] = schedule
        return schedule

    def remove_schedule(self, schedule_id: str) -> bool:
        """Remove a schedule."""
        if schedule_id in self._schedules:
            del self._schedules[schedule_id]
            return True
        return False

    def enable_schedule(self, schedule_id: str) -> None:
        """Enable a schedule."""
        schedule = self._schedules.get(schedule_id)
        if schedule:
            schedule.enabled = True

    def disable_schedule(self, schedule_id: str) -> None:
        """Disable a schedule."""
        schedule = self._schedules.get(schedule_id)
        if schedule:
            schedule.enabled = False

    def execute_now(self, schedule_id: str, input_data: Any = None) -> ScheduledTask:
        """Execute a schedule immediately."""
        schedule = self._schedules.get(schedule_id)
        if not schedule:
            raise ValueError(f"Schedule not found: {schedule_id}")

        return self._execute_task(schedule, input_data)

    def _execute_task(
        self,
        schedule: Schedule,
        input_data: Any = None,
    ) -> ScheduledTask:
        """Execute a scheduled task."""
        task = ScheduledTask(
            id=str(uuid.uuid4())[:8],
            schedule_id=schedule.id,
            input_data=input_data,
        )

        self._tasks[task.id] = task
        task.status = TaskStatus.RUNNING
        task.started_at = time.time()

        try:
            result = schedule.task(input_data)
            task.output_data = result
            task.status = TaskStatus.COMPLETED
        except Exception as e:
            task.error = str(e)
            task.status = TaskStatus.FAILED

            if task.retry_count < task.max_retries:
                task.retry_count += 1
                task.status = TaskStatus.PENDING
                next_run = time.time() + 60 * task.retry_count
                self._task_queue.append((task.id, next_run))

        task.completed_at = time.time()
        task.execution_time_ms = (task.completed_at - task.started_at) * 1000

        return task

    def _get_next_run_time(self, schedule: Schedule) -> Optional[float]:
        """Calculate the next run time for a schedule."""
        if not schedule.enabled:
            return None

        if schedule.schedule_type == ScheduleType.CRON and schedule.cron_expression:
            next_dt = CronParser.next_run(schedule.cron_expression)
            return next_dt.timestamp()

        elif schedule.schedule_type == ScheduleType.INTERVAL and schedule.interval_seconds:
            return time.time() + schedule.interval_seconds

        elif schedule.schedule_type == ScheduleType.ONE_TIME and schedule.run_at:
            return schedule.run_at if schedule.run_at > time.time() else None

        elif schedule.schedule_type == ScheduleType.DELAYED and schedule.run_at:
            return schedule.run_at if schedule.run_at > time.time() else None

        return None

    def get_due_tasks(self) -> list[tuple[Schedule, float]]:
        """Get tasks that are due to run."""
        now = time.time()
        due = []

        for schedule in self._schedules.values():
            if not schedule.enabled:
                continue

            next_run = self._get_next_run_time(schedule)
            if next_run and next_run <= now:
                due.append((schedule, next_run))

        return sorted(due, key=lambda x: x[1])

    def run_once(self, timeout_seconds: float = 1.0) -> list[ScheduledTask]:
        """Run due tasks once (call this in a loop)."""
        due_tasks = self.get_due_tasks()
        executed = []

        for schedule, _ in due_tasks:
            task = self._execute_task(schedule)
            executed.append(task)

            if schedule.schedule_type != ScheduleType.INTERVAL:
                schedule.enabled = False

        return executed

    def get_schedule(self, schedule_id: str) -> Optional[Schedule]:
        """Get a schedule by ID."""
        return self._schedules.get(schedule_id)

    def get_task(self, task_id: str) -> Optional[ScheduledTask]:
        """Get a task execution by ID."""
        return self._tasks.get(task_id)

    def list_schedules(
        self,
        enabled: Optional[bool] = None,
        schedule_type: Optional[ScheduleType] = None,
    ) -> list[dict]:
        """List all schedules with their next run times."""
        schedules = list(self._schedules.values())

        if enabled is not None:
            schedules = [s for s in schedules if s.enabled == enabled]
        if schedule_type:
            schedules = [s for s in schedules if s.schedule_type == schedule_type]

        result = []
        for schedule in schedules:
            next_run = self._get_next_run_time(schedule)
            result.append({
                "id": schedule.id,
                "name": schedule.name,
                "type": schedule.schedule_type.value,
                "enabled": schedule.enabled,
                "next_run": next_run,
                "metadata": schedule.metadata,
            })

        return result

    def list_tasks(
        self,
        schedule_id: Optional[str] = None,
        status: Optional[TaskStatus] = None,
        limit: int = 100,
    ) -> list[ScheduledTask]:
        """List task executions."""
        tasks = list(self._tasks.values())

        if schedule_id:
            tasks = [t for t in tasks if t.schedule_id == schedule_id]
        if status:
            tasks = [t for t in tasks if t.status == status]

        return sorted(tasks, key=lambda t: t.scheduled_at, reverse=True)[:limit]

    def cancel_task(self, task_id: str) -> bool:
        """Cancel a pending task."""
        task = self._tasks.get(task_id)
        if not task or task.status != TaskStatus.PENDING:
            return False

        task.status = TaskStatus.CANCELLED
        return True

    def get_scheduler_stats(self) -> dict:
        """Get scheduler statistics."""
        tasks = list(self._tasks.values())

        return {
            "total_schedules": len(self._schedules),
            "enabled_schedules": sum(1 for s in self._schedules.values() if s.enabled),
            "total_tasks": len(tasks),
            "pending_tasks": sum(1 for t in tasks if t.status == TaskStatus.PENDING),
            "running_tasks": sum(1 for t in tasks if t.status == TaskStatus.RUNNING),
            "completed_tasks": sum(1 for t in tasks if t.status == TaskStatus.COMPLETED),
            "failed_tasks": sum(1 for t in tasks if t.status == TaskStatus.FAILED),
        }
