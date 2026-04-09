"""
Job Scheduler Action Module

Job scheduling with cron expressions, interval-based scheduling,
and job deduplication. Supports one-time and recurring jobs.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class ScheduleType(Enum):
    """Types of job schedules."""
    
    ONCE = "once"
    INTERVAL = "interval"
    CRON = "cron"


class JobStatus(Enum):
    """Job execution status."""
    
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class Job:
    """Scheduled job definition."""
    
    id: str
    name: str
    func: Callable
    schedule_type: ScheduleType
    interval_seconds: float = 0
    cron_expression: Optional[str] = None
    run_at: Optional[datetime] = None
    max_runs: Optional[int] = None
    max_concurrent: int = 1
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    runs: int = 0
    status: JobStatus = JobStatus.PENDING
    last_run: Optional[float] = None
    last_result: Any = None
    last_error: Optional[str] = None


@dataclass
class JobResult:
    """Result of a job execution."""
    
    job_id: str
    job_name: str
    started_at: float
    completed_at: Optional[float] = None
    duration_ms: float = 0
    success: bool = False
    result: Any = None
    error: Optional[str] = None


class CronParser:
    """Parse and validate cron expressions."""
    
    CRON_FIELDS = ["minute", "hour", "day", "month", "weekday"]
    
    def __init__(self, expression: str):
        self.expression = expression
        self._parts = expression.split()
    
    def is_valid(self) -> bool:
        """Validate cron expression."""
        return len(self._parts) >= 5
    
    def get_next_run(self, from_time: Optional[datetime] = None) -> Optional[datetime]:
        """Calculate next run time based on cron expression."""
        if not self.is_valid():
            return None
        
        minute, hour, day, month, weekday = self._parse_fields()
        
        now = from_time or datetime.now()
        candidate = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
        
        for _ in range(366 * 24 * 60):
            if self._matches(candidate, minute, hour, day, month, weekday):
                return candidate
            
            candidate += timedelta(minutes=1)
            
            if candidate.hour == 0 and candidate.minute == 0:
                candidate += timedelta(days=1)
        
        return None
    
    def _parse_fields(self) -> tuple:
        """Parse cron fields."""
        parts = self._parts[:5]
        parsed = []
        for i, part in enumerate(parts):
            if part == "*":
                parsed.append(set(range(60 if i == 0 else 24 if i == 1 else 31 if i == 2 else 12 if i == 3 else 7)))
            elif "," in part:
                values = set()
                for p in part.split(","):
                    if "-" in p:
                        start, end = map(int, p.split("-"))
                        values.update(range(start, end + 1))
                    else:
                        values.add(int(p))
                parsed.append(values)
            elif "-" in part:
                start, end = map(int, part.split("-"))
                parsed.append(set(range(start, end + 1)))
            elif "/" in part:
                base, step = part.split("/")
                base = int(base) if base != "*" else 0
                step = int(step)
                max_val = 60 if parsed == [] else 24 if len(parsed) == 1 else 31 if len(parsed) == 2 else 12 if len(parsed) == 3 else 7
                parsed.append(set(range(base, max_val + 1, step)))
            else:
                parsed.append({int(part)})
        
        return tuple(parsed) if len(parsed) == 5 else tuple(parsed + [set(range(7))] * (5 - len(parsed)))
    
    def _matches(self, dt: datetime, minute, hour, day, month, weekday) -> bool:
        """Check if datetime matches cron expression."""
        return (
            dt.minute in minute and
            dt.hour in hour and
            (dt.day in day or day == set(range(1, 32))) and
            dt.month in month and
            (dt.weekday() in weekday or weekday == set(range(7)))
        )


class JobScheduler:
    """Core job scheduling logic."""
    
    def __init__(self):
        self._jobs: Dict[str, Job] = {}
        self._running_jobs: Set[str] = set()
        self._lock = asyncio.Lock()
    
    def add_job(self, job: Job) -> None:
        """Add a job to the scheduler."""
        self._jobs[job.id] = job
    
    def remove_job(self, job_id: str) -> bool:
        """Remove a job from the scheduler."""
        if job_id in self._jobs:
            del self._jobs[job_id]
            return True
        return False
    
    def get_job(self, job_id: str) -> Optional[Job]:
        """Get a job by ID."""
        return self._jobs.get(job_id)
    
    def get_next_run(self, job: Job) -> Optional[datetime]:
        """Calculate next run time for a job."""
        if job.schedule_type == ScheduleType.ONCE:
            return job.run_at
        
        elif job.schedule_type == ScheduleType.INTERVAL:
            if job.last_run:
                return datetime.fromtimestamp(job.last_run) + timedelta(seconds=job.interval_seconds)
            return datetime.now()
        
        elif job.schedule_type == ScheduleType.CRON and job.cron_expression:
            parser = CronParser(job.cron_expression)
            return parser.get_next_run()
        
        return None
    
    async def execute_job(self, job: Job) -> JobResult:
        """Execute a single job."""
        job_id = job.id
        
        if job_id in self._running_jobs:
            if job.max_concurrent <= len(self._running_jobs):
                logger.warning(f"Job {job_id} already running, skipping")
                return JobResult(
                    job_id=job_id,
                    job_name=job.name,
                    started_at=time.time(),
                    success=False,
                    error="Already running"
                )
        
        self._running_jobs.add(job_id)
        job.status = JobStatus.RUNNING
        
        start_time = time.time()
        result = JobResult(
            job_id=job_id,
            job_name=job.name,
            started_at=start_time
        )
        
        try:
            if asyncio.iscoroutinefunction(job.func):
                job.last_result = await job.func()
            else:
                job.last_result = job.func()
            
            result.completed_at = time.time()
            result.duration_ms = (result.completed_at - start_time) * 1000
            result.success = True
            job.status = JobStatus.COMPLETED
        
        except Exception as e:
            result.completed_at = time.time()
            result.duration_ms = (result.completed_at - start_time) * 1000
            result.error = str(e)
            job.status = JobStatus.FAILED
            job.last_error = str(e)
        
        finally:
            self._running_jobs.discard(job_id)
            job.last_run = start_time
            job.runs += 1
        
        return result


class JobSchedulerAction:
    """
    Main job scheduler action handler.
    
    Provides job scheduling with support for one-time,
    interval-based, and cron-based scheduling.
    """
    
    def __init__(self):
        self.scheduler = JobScheduler()
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._results: List[JobResult] = []
        self._middleware: List[Callable] = []
    
    def create_job(
        self,
        name: str,
        func: Callable,
        schedule_type: ScheduleType,
        interval_seconds: float = 0,
        cron_expression: Optional[str] = None,
        run_at: Optional[datetime] = None,
        max_runs: Optional[int] = None,
        metadata: Optional[Dict] = None
    ) -> str:
        """Create and register a new job."""
        job_id = str(uuid.uuid4())
        job = Job(
            id=job_id,
            name=name,
            func=func,
            schedule_type=schedule_type,
            interval_seconds=interval_seconds,
            cron_expression=cron_expression,
            run_at=run_at,
            max_runs=max_runs,
            metadata=metadata or {}
        )
        
        self.scheduler.add_job(job)
        return job_id
    
    def create_interval_job(
        self,
        name: str,
        func: Callable,
        interval_seconds: float,
        **kwargs
    ) -> str:
        """Create an interval-based job."""
        return self.create_job(
            name=name,
            func=func,
            schedule_type=ScheduleType.INTERVAL,
            interval_seconds=interval_seconds,
            **kwargs
        )
    
    def create_cron_job(
        self,
        name: str,
        func: Callable,
        cron_expression: str,
        **kwargs
    ) -> str:
        """Create a cron-based job."""
        return self.create_job(
            name=name,
            func=func,
            schedule_type=ScheduleType.CRON,
            cron_expression=cron_expression,
            **kwargs
        )
    
    def create_once_job(
        self,
        name: str,
        func: Callable,
        run_at: datetime,
        **kwargs
    ) -> str:
        """Create a one-time job."""
        return self.create_job(
            name=name,
            func=func,
            schedule_type=ScheduleType.ONCE,
            run_at=run_at,
            **kwargs
        )
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a pending job."""
        job = self.scheduler.get_job(job_id)
        if job:
            job.enabled = False
            job.status = JobStatus.CANCELLED
            return True
        return False
    
    def remove_job(self, job_id: str) -> bool:
        """Remove a job from the scheduler."""
        return self.scheduler.remove_job(job_id)
    
    async def run_job(self, job_id: str) -> Optional[JobResult]:
        """Manually run a job."""
        job = self.scheduler.get_job(job_id)
        if not job:
            return None
        
        result = await self.scheduler.execute_job(job)
        self._results.append(result)
        
        return result
    
    async def start(self) -> None:
        """Start the scheduler loop."""
        if self._running:
            return
        
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
    
    async def stop(self) -> None:
        """Stop the scheduler loop."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
    
    async def _run_loop(self) -> None:
        """Main scheduler loop."""
        while self._running:
            try:
                now = datetime.now()
                
                for job in list(self.scheduler._jobs.values()):
                    if not job.enabled:
                        continue
                    
                    if job.max_runs and job.runs >= job.max_runs:
                        continue
                    
                    next_run = self.scheduler.get_next_run(job)
                    if next_run and next_run <= now:
                        asyncio.create_task(self.scheduler.execute_job(job))
                
                await asyncio.sleep(1)
            
            except Exception as e:
                logger.error(f"Scheduler loop error: {e}")
                await asyncio.sleep(1)
    
    def get_results(self, limit: int = 100) -> List[Dict]:
        """Get recent job results."""
        results = self._results[-limit:]
        return [
            {
                "job_id": r.job_id,
                "job_name": r.job_name,
                "success": r.success,
                "duration_ms": r.duration_ms,
                "error": r.error,
                "started_at": datetime.fromtimestamp(r.started_at).isoformat()
            }
            for r in results
        ]
    
    def get_jobs_status(self) -> List[Dict]:
        """Get status of all jobs."""
        return [
            {
                "id": job.id,
                "name": job.name,
                "schedule_type": job.schedule_type.value,
                "status": job.status.value,
                "enabled": job.enabled,
                "runs": job.runs,
                "last_run": datetime.fromtimestamp(job.last_run).isoformat() if job.last_run else None,
                "next_run": self.scheduler.get_next_run(job).isoformat() if self.scheduler.get_next_run(job) else None
            }
            for job in self.scheduler._jobs.values()
        ]
