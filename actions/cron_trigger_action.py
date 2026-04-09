"""
Cron Trigger Action Module.

Provides cron-style scheduling with support for complex
cron expressions, timezones, and event-driven triggers.

Author: rabai_autoclick team
"""

import time
import asyncio
import logging
from typing import (
    Optional, Dict, Any, List, Callable, Awaitable,
    Set, Union
)
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
from croniter import croniter
import pytz

logger = logging.getLogger(__name__)


class TriggerType(Enum):
    """Types of triggers."""
    CRON = "cron"
    INTERVAL = "interval"
    ONCE = "once"
    CALENDAR = "calendar"


@dataclass
class CronTrigger:
    """Cron-based trigger configuration."""
    cron_expr: str
    timezone: str = "UTC"
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    jitter: int = 0

    def get_next_run(self, base_time: Optional[datetime] = None) -> Optional[datetime]:
        """
        Get next run time based on cron expression.

        Args:
            base_time: Base time to calculate from

        Returns:
            Next run datetime or None if no more runs
        """
        base = base_time or datetime.now(pytz.timezone(self.timezone))

        try:
            cron = croniter(self.cron_expr, base, tz=self.timezone)
            next_time = cron.get_next(datetime)

            if self.end_date and next_time > self.end_date:
                return None

            if self.jitter > 0:
                import random
                next_time += timedelta(seconds=random.randint(0, self.jitter))

            return next_time

        except Exception as e:
            logger.error(f"Cron parsing error: {e}")
            return None

    def get_prev_run(self, base_time: Optional[datetime] = None) -> Optional[datetime]:
        """Get previous run time based on cron expression."""
        base = base_time or datetime.now(pytz.timezone(self.timezone))

        try:
            cron = croniter(self.cron_expr, base, tz=self.timezone)
            return cron.get_prev(datetime)
        except Exception:
            return None


@dataclass
class IntervalTrigger:
    """Interval-based trigger configuration."""
    interval_seconds: float
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

    def get_next_run(self, base_time: Optional[datetime] = None) -> Optional[datetime]:
        """Get next run time based on interval."""
        now = base_time or datetime.now()

        if self.start_date and now < self.start_date:
            return self.start_date

        if self.end_date and now > self.end_date:
            return None

        return now + timedelta(seconds=self.interval_seconds)


@dataclass
class ScheduledJob:
    """A scheduled job configuration."""
    job_id: str
    name: str
    trigger: Union[CronTrigger, IntervalTrigger]
    func: Callable[..., Awaitable[Any]]
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    max_instances: int = 1
    coalesce: bool = True
    tags: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class JobExecution:
    """Record of a job execution."""
    job_id: str
    execution_id: str
    scheduled_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    status: str = "pending"
    result: Any = None
    error: Optional[str] = None


class CronTriggerAction:
    """
    Cron Trigger and Job Scheduler.

    Supports complex cron expressions, timezones, job coalescing,
    and event-driven scheduling.

    Example:
        >>> scheduler = CronTriggerAction()
        >>> scheduler.add_job("my_job", "*/5 * * * *", my_task)
        >>> scheduler.start()
    """

    def __init__(self, timezone: str = "UTC"):
        self.timezone = timezone
        self._jobs: Dict[str, ScheduledJob] = {}
        self._running_jobs: Dict[str, Set[str]] = {}
        self._execution_history: List[JobExecution] = []
        self._running = False
        self._lock = asyncio.Lock()

    def add_cron_job(
        self,
        job_id: str,
        cron_expr: str,
        func: Callable[..., Awaitable[Any]],
        name: Optional[str] = None,
        **kwargs,
    ) -> ScheduledJob:
        """
        Add a cron-based job.

        Args:
            job_id: Unique job identifier
            cron_expr: Cron expression (5 or 6 fields)
            func: Async function to execute
            name: Optional job name
            **kwargs: Additional job configuration

        Returns:
            ScheduledJob
        """
        trigger = CronTrigger(
            cron_expr=cron_expr,
            timezone=kwargs.pop("timezone", self.timezone),
            start_date=kwargs.pop("start_date", None),
            end_date=kwargs.pop("end_date", None),
            jitter=kwargs.pop("jitter", 0),
        )

        return self._add_job(
            job_id,
            name or job_id,
            trigger,
            func,
            **kwargs,
        )

    def add_interval_job(
        self,
        job_id: str,
        interval_seconds: float,
        func: Callable[..., Awaitable[Any]],
        name: Optional[str] = None,
        **kwargs,
    ) -> ScheduledJob:
        """
        Add an interval-based job.

        Args:
            job_id: Unique job identifier
            interval_seconds: Interval in seconds
            func: Async function to execute
            name: Optional job name
            **kwargs: Additional job configuration

        Returns:
            ScheduledJob
        """
        trigger = IntervalTrigger(
            interval_seconds=interval_seconds,
            start_date=kwargs.pop("start_date", None),
            end_date=kwargs.pop("end_date", None),
        )

        return self._add_job(
            job_id,
            name or job_id,
            trigger,
            func,
            **kwargs,
        )

    def _add_job(
        self,
        job_id: str,
        name: str,
        trigger: Union[CronTrigger, IntervalTrigger],
        func: Callable[..., Awaitable[Any]],
        **kwargs,
    ) -> ScheduledJob:
        """Internal method to add a job."""
        if job_id in self._jobs:
            raise ValueError(f"Job '{job_id}' already exists")

        job = ScheduledJob(
            job_id=job_id,
            name=name,
            trigger=trigger,
            func=func,
            **kwargs,
        )

        self._jobs[job_id] = job
        logger.info(f"Added job: {job_id} ({name})")
        return job

    def remove_job(self, job_id: str) -> bool:
        """
        Remove a job from the scheduler.

        Args:
            job_id: Job identifier

        Returns:
            True if removed
        """
        if job_id in self._jobs:
            del self._jobs[job_id]
            logger.info(f"Removed job: {job_id}")
            return True
        return False

    def get_job(self, job_id: str) -> Optional[ScheduledJob]:
        """Get job by ID."""
        return self._jobs.get(job_id)

    def get_next_run_time(self, job_id: str) -> Optional[datetime]:
        """Get next scheduled run time for a job."""
        job = self._jobs.get(job_id)
        if job:
            return job.trigger.get_next_run()
        return None

    def get_next_run_times(self, limit: int = 10) -> List[tuple]:
        """Get next run times for all enabled jobs."""
        results = []
        now = datetime.now(pytz.timezone(self.timezone))

        for job_id, job in self._jobs.items():
            if job.enabled:
                next_run = job.trigger.get_next_run(now)
                if next_run:
                    results.append((job_id, job.name, next_run))

        results.sort(key=lambda x: x[2])
        return results[:limit]

    def pause_job(self, job_id: str) -> bool:
        """Pause a job."""
        job = self._jobs.get(job_id)
        if job:
            job.enabled = False
            logger.info(f"Paused job: {job_id}")
            return True
        return False

    def resume_job(self, job_id: str) -> bool:
        """Resume a paused job."""
        job = self._jobs.get(job_id)
        if job:
            job.enabled = True
            logger.info(f"Resumed job: {job_id}")
            return True
        return False

    async def execute_job(self, job: ScheduledJob) -> JobExecution:
        """Execute a job and record execution."""
        execution_id = f"{job.job_id}:{time.time()}"
        execution = JobExecution(
            job_id=job.job_id,
            execution_id=execution_id,
            scheduled_at=datetime.now(),
        )

        async with self._lock:
            if job.job_id in self._running_jobs:
                if len(self._running_jobs[job.job_id]) >= job.max_instances:
                    if job.coalesce:
                        logger.info(f"Job {job.job_id} skipped (already running, coalesce)")
                        execution.status = "skipped"
                    else:
                        execution.status = "rejected"
                        execution.error = "Max instances exceeded"
                    return execution
            self._running_jobs.setdefault(job.job_id, set()).add(execution_id)

        execution.started_at = datetime.now()
        execution.status = "running"

        try:
            if job.kwargs:
                execution.result = await job.func(*job.args, **job.kwargs)
            else:
                execution.result = await job.func(*job.args)
            execution.status = "completed"
            logger.info(f"Job {job.job_id} completed successfully")

        except Exception as e:
            execution.status = "failed"
            execution.error = str(e)
            logger.error(f"Job {job.job_id} failed: {e}")

        finally:
            execution.completed_at = datetime.now()
            async with self._lock:
                self._running_jobs.get(job.job_id, set()).discard(execution_id)

            self._execution_history.append(execution)
            if len(self._execution_history) > 1000:
                self._execution_history = self._execution_history[-500:]

        return execution

    async def _scheduler_loop(self) -> None:
        """Main scheduler loop."""
        while self._running:
            now = datetime.now(pytz.timezone(self.timezone))
            tasks = []

            for job_id, job in list(self._jobs.items()):
                if not job.enabled:
                    continue

                next_run = job.trigger.get_next_run(now)

                if next_run and next_run <= now + timedelta(seconds=1):
                    logger.info(f"Triggering job: {job_id}")
                    tasks.append(self.execute_job(job))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            await asyncio.sleep(1)

    async def start(self) -> None:
        """Start the scheduler."""
        if self._running:
            logger.warning("Scheduler already running")
            return

        self._running = True
        logger.info("Starting cron scheduler")
        await self._scheduler_loop()

    def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        logger.info("Stopping cron scheduler")

    def get_execution_history(
        self,
        job_id: Optional[str] = None,
        limit: int = 100,
    ) -> List[JobExecution]:
        """Get execution history."""
        history = self._execution_history
        if job_id:
            history = [e for e in history if e.job_id == job_id]
        return history[-limit:]

    def get_stats(self) -> Dict[str, Any]:
        """Get scheduler statistics."""
        return {
            "total_jobs": len(self._jobs),
            "enabled_jobs": sum(1 for j in self._jobs.values() if j.enabled),
            "running_jobs": len(self._running_jobs),
            "total_executions": len(self._execution_history),
            "timezone": self.timezone,
        }
