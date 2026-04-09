"""
Workflow Scheduling and Cron-like Automation Module.

Schedules and executes workflows at specific times, intervals,
or in response to triggers. Supports calendar-based scheduling,
dependencies, and priority queuing.

Author: AutoGen
"""
from __future__ import annotations

import asyncio
import heapq
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class ScheduleType(Enum):
    ONCE = auto()
    INTERVAL = auto()
    CRON = auto()
    CALENDAR = auto()


class TriggerType(Enum):
    TIME = auto()
    EVENT = auto()
    MANUAL = auto()
    DEPENDENCY = auto()
    WEBHOOK = auto()


@dataclass(frozen=True)
class Schedule:
    """Defines when a workflow should run."""
    schedule_type: ScheduleType
    next_run: Optional[datetime] = None
    interval_seconds: float = 0.0
    cron_expression: Optional[str] = None
    calendar_dates: FrozenSet[int] = field(default_factory=frozenset)
    calendar_months: FrozenSet[int] = field(default_factory=frozenset)
    weekdays: FrozenSet[int] = field(default_factory=frozenset)

    def calculate_next_run(self, from_time: Optional[datetime] = None) -> Optional[datetime]:
        if self.schedule_type == ScheduleType.ONCE:
            return self.next_run
        if self.schedule_type == ScheduleType.INTERVAL:
            base = from_time or datetime.utcnow()
            if self.next_run and self.next_run > base:
                return self.next_run
            return base + timedelta(seconds=self.interval_seconds)
        if self.schedule_type == ScheduleType.CALENDAR:
            return self._next_calendar_run(from_time)
        return None

    def _next_calendar_run(self, from_time: Optional[datetime] = None) -> Optional[datetime]:
        now = from_time or datetime.utcnow()
        current = now + timedelta(minutes=1)
        for _ in range(366 * 24 * 60):
            if (
                current.month in self.calendar_months or not self.calendar_months
            ) and (
                current.day in self.calendar_dates or not self.calendar_dates
            ) and (
                current.weekday() in self.weekdays or not self.weekdays
            ):
                return current.replace(second=0, microsecond=0)
            current += timedelta(minutes=1)
        return None


@dataclass
class ScheduledJob:
    """A workflow scheduled for execution."""
    job_id: str
    workflow_name: str
    schedule: Schedule
    trigger: TriggerType
    priority: int = 0
    max_retries: int = 3
    retry_delay: float = 60.0
    timeout_seconds: float = 3600.0
    enabled: bool = True
    depends_on: FrozenSet[str] = field(default_factory=frozenset)
    metadata: Tuple[Tuple[str, str], ...] = field(default_factory=tuple)
    last_run: Optional[datetime] = None
    last_status: Optional[str] = None
    run_count: int = 0

    def param_dict(self) -> Dict[str, Any]:
        return dict(self.metadata)


@dataclass
class JobExecution:
    """Record of a single job execution."""
    execution_id: str
    job_id: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str = "pending"
    result: Optional[Any] = None
    error: Optional[str] = None
    retry_count: int = 0


class CronParser:
    """Lightweight cron expression parser (5-field: min hour day month dow)."""

    FIELD_RANGES = [
        (0, 59),
        (0, 23),
        (1, 31),
        (1, 12),
        (0, 6),
    ]

    @classmethod
    def parse(cls, expression: str) -> Dict[str, Set[int]]:
        parts = expression.split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression: {expression}")

        fields: Dict[str, Set[int]] = {}
        names = ["minute", "hour", "day", "month", "dow"]

        for i, (part, (min_val, max_val)) in enumerate(zip(parts, cls.FIELD_RANGES)):
            values = cls._parse_field(part, min_val, max_val)
            fields[names[i]] = values

        return fields

    @classmethod
    def _parse_field(cls, field_str: str, min_val: int, max_val: int) -> Set[int]:
        values: Set[int] = set()
        for part in field_str.split(","):
            if "/" in part:
                base, step = part.split("/")
                base_values = cls._expand_range(base, min_val, max_val)
                step_int = int(step)
                for v in base_values:
                    if (v - min_val) % step_int == 0:
                        values.add(v)
            elif "-" in part:
                start, end = part.split("-")
                start_int, end_int = int(start), int(end)
                for v in range(start_int, end_int + 1):
                    if min_val <= v <= max_val:
                        values.add(v)
            elif part == "*":
                for v in range(min_val, max_val + 1):
                    values.add(v)
            else:
                v = int(part)
                if min_val <= v <= max_val:
                    values.add(v)
        return values

    @classmethod
    def _expand_range(cls, part: str, min_val: int, max_val: int) -> Set[int]:
        if part == "*":
            return set(range(min_val, max_val + 1))
        if "-" in part:
            start, end = part.split("-")
            return set(range(int(start), int(end) + 1))
        return {int(part)}

    @classmethod
    def matches(cls, expression: str, dt: datetime) -> bool:
        fields = cls.parse(expression)
        return (
            dt.minute in fields["minute"]
            and dt.hour in fields["hour"]
            and dt.day in fields["day"]
            and dt.month in fields["month"]
            and dt.weekday() in fields["dow"]
        )


class WorkflowScheduler:
    """
    Schedules and executes workflows based on time, events, or triggers.
    """

    def __init__(self, max_concurrent: int = 10):
        self.max_concurrent = max_concurrent
        self._jobs: Dict[str, ScheduledJob] = {}
        self._executions: Dict[str, JobExecution] = {}
        self._pending: List[Tuple[float, str]] = []
        self._running: Set[str] = set()
        self._completed: Set[str] = set()
        self._workflow_handlers: Dict[str, Callable] = {}
        self._trigger_handlers: Dict[TriggerType, Callable] = {}
        self._running_task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None

    def register_workflow(
        self, name: str, handler: Callable, metadata: Optional[Dict[str, str]] = None
    ) -> None:
        self._workflow_handlers[name] = handler
        logger.info("Registered workflow: %s", name)

    def register_trigger(
        self, trigger_type: TriggerType, handler: Callable
    ) -> None:
        self._trigger_handlers[trigger_type] = handler

    def schedule_job(
        self,
        job_id: str,
        workflow_name: str,
        schedule: Schedule,
        trigger: TriggerType = TriggerType.TIME,
        **kwargs,
    ) -> ScheduledJob:
        job = ScheduledJob(
            job_id=job_id,
            workflow_name=workflow_name,
            schedule=schedule,
            trigger=trigger,
            metadata=tuple((k, str(v)) for k, v in (kwargs.get("metadata") or {}).items()),
            priority=kwargs.get("priority", 0),
            max_retries=kwargs.get("max_retries", 3),
            retry_delay=kwargs.get("retry_delay", 60.0),
            timeout_seconds=kwargs.get("timeout_seconds", 3600.0),
            depends_on=frozenset(kwargs.get("depends_on", [])),
        )

        self._jobs[job_id] = job
        self._reschedule_job(job)
        logger.info("Scheduled job: %s -> %s", job_id, workflow_name)
        return job

    def _reschedule_job(self, job: ScheduledJob) -> None:
        if not job.enabled:
            return
        next_run = job.schedule.calculate_next_run()
        if next_run:
            job.schedule = Schedule(
                schedule_type=job.schedule.schedule_type,
                next_run=next_run,
                interval_seconds=job.schedule.interval_seconds,
                cron_expression=job.schedule.cron_expression,
                calendar_dates=job.schedule.calendar_dates,
                calendar_months=job.schedule.calendar_months,
                weekdays=job.schedule.weekdays,
            )
            heapq.heappush(self._pending, (next_run.timestamp(), job.job_id))

    def cancel_job(self, job_id: str) -> bool:
        if job_id in self._jobs:
            self._jobs[job_id].enabled = False
            logger.info("Cancelled job: %s", job_id)
            return True
        return False

    def trigger_now(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        if not job:
            return False
        next_run = datetime.utcnow()
        heapq.heappush(self._pending, (next_run.timestamp(), job_id))
        logger.info("Triggered job immediately: %s", job_id)
        return True

    async def start(self) -> None:
        """Start the scheduler loop."""
        self._stop_event = asyncio.Event()
        self._running_task = asyncio.create_task(self._scheduler_loop())
        logger.info("Scheduler started")

    async def stop(self) -> None:
        """Stop the scheduler."""
        if self._stop_event:
            self._stop_event.set()
        if self._running_task:
            await self._running_task
        logger.info("Scheduler stopped")

    async def _scheduler_loop(self) -> None:
        while not (self._stop_event and self._stop_event.is_set()):
            now = datetime.utcnow().timestamp()
            due_jobs: List[Tuple[float, str]] = []

            while self._pending and self._pending[0][0] <= now:
                _, job_id = heapq.heappop(self._pending)
                job = self._jobs.get(job_id)
                if job and job.enabled:
                    due_jobs.append((job.priority, job_id))

            for priority, job_id in sorted(due_jobs, reverse=True):
                asyncio.create_task(self._execute_job(job_id))

            await asyncio.sleep(1.0)

    async def _execute_job(self, job_id: str) -> None:
        job = self._jobs.get(job_id)
        if not job:
            return

        if len(self._running) >= self.max_concurrent:
            heapq.heappush(self._pending, (time.time() + 5, job_id))
            return

        if job.depends_on:
            for dep_id in job.depends_on:
                if self._executions.get(dep_id, JobExecution("", "", datetime.utcnow())).status != "completed":
                    heapq.heappush(self._pending, (time.time() + 10, job_id))
                    return

        import uuid
        execution = JobExecution(
            execution_id=str(uuid.uuid4())[:8],
            job_id=job_id,
            started_at=datetime.utcnow(),
        )

        self._executions[execution.execution_id] = execution
        self._running.add(job_id)
        job.last_run = datetime.utcnow()
        job.run_count += 1

        logger.info("Executing job: %s (attempt %d)", job_id, job.run_count)

        handler = self._workflow_handlers.get(job.workflow_name)
        if not handler:
            execution.status = "failed"
            execution.error = f"Handler not found: {job.workflow_name}"
        else:
            try:
                if asyncio.iscoroutinefunction(handler):
                    result = await asyncio.wait_for(
                        handler(job.param_dict()), timeout=job.timeout_seconds
                    )
                else:
                    result = handler(job.param_dict())

                execution.status = "completed"
                execution.result = result
                job.last_status = "success"
            except asyncio.TimeoutError:
                execution.status = "timeout"
                execution.error = f"Execution timed out after {job.timeout_seconds}s"
                job.last_status = "timeout"
            except Exception as exc:
                execution.status = "failed"
                execution.error = str(exc)
                job.last_status = "failed"
                logger.error("Job execution error: %s", exc)

        execution.completed_at = datetime.utcnow()
        self._running.discard(job_id)

        if job.schedule.schedule_type == ScheduleType.INTERVAL:
            self._reschedule_job(job)

    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        job = self._jobs.get(job_id)
        if not job:
            return None
        return {
            "job_id": job.job_id,
            "workflow": job.workflow_name,
            "enabled": job.enabled,
            "last_run": job.last_run.isoformat() if job.last_run else None,
            "last_status": job.last_status,
            "run_count": job.run_count,
            "schedule_type": job.schedule.schedule_type.name,
            "next_run": job.schedule.next_run.isoformat() if job.schedule.next_run else None,
        }

    def list_jobs(self) -> List[Dict[str, Any]]:
        return [self.get_job_status(jid) for jid in self._jobs]

    def get_execution_history(
        self, job_id: Optional[str] = None, limit: int = 100
    ) -> List[Dict[str, Any]]:
        executions = sorted(
            self._executions.values(),
            key=lambda e: e.started_at,
            reverse=True,
        )
        if job_id:
            executions = [e for e in executions if e.job_id == job_id]
        return [
            {
                "execution_id": e.execution_id,
                "job_id": e.job_id,
                "started_at": e.started_at.isoformat(),
                "completed_at": e.completed_at.isoformat() if e.completed_at else None,
                "status": e.status,
                "error": e.error,
            }
            for e in executions[:limit]
        ]
