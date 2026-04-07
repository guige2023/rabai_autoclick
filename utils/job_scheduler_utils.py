"""
Job Scheduler and Cron Management Utilities.

Provides utilities for managing scheduled jobs, cron expressions,
job queues, and distributed task scheduling.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional


class JobStatus(Enum):
    """Status of a scheduled job."""
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    BLOCKED = "blocked"


class JobPriority(Enum):
    """Job priority levels."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


class TriggerType(Enum):
    """Types of job triggers."""
    CRON = "cron"
    INTERVAL = "interval"
    ONE_TIME = "one_time"
    MANUAL = "manual"


@dataclass
class CronSchedule:
    """Cron expression schedule."""
    expression: str
    minute: str = "*"
    hour: str = "*"
    day_of_month: str = "*"
    month: str = "*"
    day_of_week: str = "*"

    @classmethod
    def from_expression(cls, expression: str) -> "CronSchedule":
        """Parse a cron expression into components."""
        parts = expression.split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression: {expression}")

        return cls(
            expression=expression,
            minute=parts[0],
            hour=parts[1],
            day_of_month=parts[2],
            month=parts[3],
            day_of_week=parts[4],
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "minute": self.minute,
            "hour": self.hour,
            "day_of_month": self.day_of_month,
            "month": self.month,
            "day_of_week": self.day_of_week,
        }


@dataclass
class Job:
    """Scheduled job definition."""
    job_id: str
    name: str
    func_name: str
    func_args: tuple[Any, ...] = field(default_factory=tuple)
    func_kwargs: dict[str, Any] = field(default_factory=dict)
    trigger_type: TriggerType
    cron_schedule: Optional[CronSchedule] = None
    interval_seconds: Optional[int] = None
    run_at: Optional[datetime] = None
    priority: JobPriority = JobPriority.NORMAL
    max_retries: int = 3
    timeout_seconds: Optional[int] = None
    status: JobStatus = JobStatus.SCHEDULED
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    next_run_at: Optional[datetime] = None


@dataclass
class JobExecution:
    """Record of a job execution."""
    execution_id: str
    job_id: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: JobStatus
    result: Optional[Any] = None
    error: Optional[str] = None
    retry_count: int = 0
    duration_ms: float = 0.0
    worker_id: Optional[str] = None


class JobScheduler:
    """Scheduler for managing job execution."""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        max_workers: int = 4,
    ) -> None:
        self.db_path = db_path or Path("job_scheduler.db")
        self.max_workers = max_workers
        self._jobs: dict[str, Job] = {}
        self._running_jobs: dict[str, threading.Thread] = {}
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the scheduler database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                job_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS executions (
                execution_id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                execution_json TEXT NOT NULL,
                started_at TEXT NOT NULL,
                FOREIGN KEY (job_id) REFERENCES jobs(job_id)
            )
        """)
        conn.commit()
        conn.close()

    def schedule_job(
        self,
        name: str,
        func: Callable[..., Any],
        trigger_type: TriggerType,
        cron_expression: Optional[str] = None,
        interval_seconds: Optional[int] = None,
        run_at: Optional[datetime] = None,
        priority: JobPriority = JobPriority.NORMAL,
        max_retries: int = 3,
        timeout_seconds: Optional[int] = None,
        tags: Optional[list[str]] = None,
        **func_kwargs: Any,
    ) -> Job:
        """Schedule a new job."""
        job_id = f"job_{int(time.time())}_{hashlib.md5(name.encode()).hexdigest()[:8]}"

        cron_schedule = None
        if cron_expression:
            cron_schedule = CronSchedule.from_expression(cron_expression)

        job = Job(
            job_id=job_id,
            name=name,
            func_name=f"{func.__module__}.{func.__name__}",
            func_kwargs=func_kwargs,
            trigger_type=trigger_type,
            cron_schedule=cron_schedule,
            interval_seconds=interval_seconds,
            run_at=run_at,
            priority=priority,
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
            tags=tags or [],
            next_run_at=self._calculate_next_run(job, trigger_type, cron_schedule, interval_seconds, run_at),
        )

        with self._lock:
            self._jobs[job_id] = job

        self._save_job(job)
        return job

    def _calculate_next_run(
        self,
        job: Job,
        trigger_type: TriggerType,
        cron_schedule: Optional[CronSchedule],
        interval_seconds: Optional[int],
        run_at: Optional[datetime],
    ) -> Optional[datetime]:
        """Calculate the next run time for a job."""
        now = datetime.now()

        if trigger_type == TriggerType.ONE_TIME and run_at:
            return run_at if run_at > now else None

        if trigger_type == TriggerType.INTERVAL and interval_seconds:
            return now + timedelta(seconds=interval_seconds)

        if trigger_type == TriggerType.CRON and cron_schedule:
            return self._get_next_cron_run(cron_schedule)

        return None

    def _get_next_cron_run(self, schedule: CronSchedule) -> datetime:
        """Calculate next run time from cron expression."""
        now = datetime.now()
        minute_val = self._parse_cron_field(schedule.minute, 0, 59)
        hour_val = self._parse_cron_field(schedule.hour, 0, 23)

        if minute_val is not None and hour_val is not None:
            next_run = now.replace(minute=minute_val, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(hours=1)
            return next_run

        return now + timedelta(hours=1)

    def _parse_cron_field(
        self,
        field: str,
        min_val: int,
        max_val: int,
    ) -> Optional[int]:
        """Parse a single cron field."""
        if field == "*":
            return datetime.now().minute

        try:
            return int(field)
        except ValueError:
            return None

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a scheduled job."""
        with self._lock:
            if job_id in self._jobs:
                job = self._jobs[job_id]
                job.status = JobStatus.CANCELLED
                del self._jobs[job_id]
                self._save_job(job)
                return True
        return False

    def get_job(self, job_id: str) -> Optional[Job]:
        """Get a job by ID."""
        return self._jobs.get(job_id)

    def list_jobs(
        self,
        status: Optional[JobStatus] = None,
        tag: Optional[str] = None,
    ) -> list[Job]:
        """List all jobs with optional filtering."""
        jobs = list(self._jobs.values())

        if status:
            jobs = [j for j in jobs if j.status == status]

        if tag:
            jobs = [j for j in jobs if tag in j.tags]

        return sorted(jobs, key=lambda j: j.next_run_at or datetime.max)

    def get_pending_jobs(self) -> list[Job]:
        """Get all jobs that are due to run."""
        now = datetime.now()
        pending = []

        for job in self._jobs.values():
            if job.status != JobStatus.SCHEDULED:
                continue

            if job.next_run_at and job.next_run_at <= now:
                pending.append(job)

        return sorted(pending, key=lambda j: j.priority.value, reverse=True)

    def execute_job(
        self,
        job: Job,
        worker_id: Optional[str] = None,
    ) -> JobExecution:
        """Execute a job immediately."""
        execution_id = f"exec_{int(time.time())}_{hashlib.md5(job.job_id.encode()).hexdigest()[:8]}"

        execution = JobExecution(
            execution_id=execution_id,
            job_id=job.job_id,
            started_at=datetime.now(),
            status=JobStatus.RUNNING,
            worker_id=worker_id,
        )

        job.status = JobStatus.RUNNING
        self._save_job(job)

        try:
            result = self._run_job_function(job)
            execution.status = JobStatus.COMPLETED
            execution.result = result
            job.status = JobStatus.SCHEDULED

        except Exception as e:
            execution.status = JobStatus.FAILED
            execution.error = str(e)
            execution.retry_count += 1
            job.status = JobStatus.SCHEDULED if execution.retry_count < job.max_retries else JobStatus.FAILED

        execution.completed_at = datetime.now()
        execution.duration_ms = (execution.completed_at - execution.started_at).total_seconds() * 1000

        self._save_execution(execution)
        self._save_job(job)

        return execution

    def _run_job_function(self, job: Job) -> Any:
        """Run the actual job function."""
        import importlib

        module_name, func_name = job.func_name.rsplit(".", 1)
        module = importlib.import_module(module_name)
        func = getattr(module, func_name)

        if job.timeout_seconds:
            result = None
            exception_raised = False

            def target():
                nonlocal result, exception_raised
                try:
                    result = func(*job.func_args, **job.func_kwargs)
                except Exception:
                    exception_raised = True
                    raise

            thread = threading.Thread(target=target)
            thread.start()
            thread.join(timeout=job.timeout_seconds)

            if thread.is_alive():
                raise TimeoutError(f"Job timed out after {job.timeout_seconds} seconds")

            if exception_raised:
                raise

            return result
        else:
            return func(*job.func_args, **job.func_kwargs)

    def start_scheduler(self) -> None:
        """Start the scheduler loop."""
        self._stop_event.clear()

        while not self._stop_event.is_set():
            pending = self.get_pending_jobs()

            for job in pending[:self.max_workers]:
                thread = threading.Thread(
                    target=self._execute_job_thread,
                    args=(job,),
                )
                self._running_jobs[job.job_id] = thread
                thread.start()

            time.sleep(1)

    def _execute_job_thread(self, job: Job) -> None:
        """Execute job in a separate thread."""
        worker_id = threading.current_thread().name
        self.execute_job(job, worker_id)

        with self._lock:
            if job.job_id in self._running_jobs:
                del self._running_jobs[job.job_id]

    def stop_scheduler(self) -> None:
        """Stop the scheduler loop."""
        self._stop_event.set()

    def get_execution_history(
        self,
        job_id: str,
        limit: int = 50,
    ) -> list[JobExecution]:
        """Get execution history for a job."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM executions
            WHERE job_id = ?
            ORDER BY started_at DESC LIMIT ?
        """, (job_id, limit))
        rows = cursor.fetchall()
        conn.close()

        executions = []
        for row in rows:
            exec_data = json.loads(row["execution_json"])
            executions.append(JobExecution(
                execution_id=row["execution_id"],
                job_id=row["job_id"],
                started_at=datetime.fromisoformat(row["started_at"]),
                status=JobStatus(exec_data["status"]),
                result=exec_data.get("result"),
                error=exec_data.get("error"),
                retry_count=exec_data.get("retry_count", 0),
                duration_ms=exec_data.get("duration_ms", 0.0),
                worker_id=exec_data.get("worker_id"),
            ))

        return executions

    def _save_job(self, job: Job) -> None:
        """Save job to database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO jobs (job_id, job_json, created_at)
            VALUES (?, ?, ?)
        """, (
            job.job_id,
            json.dumps({
                "name": job.name,
                "func_name": job.func_name,
                "func_args": job.func_args,
                "func_kwargs": job.func_kwargs,
                "trigger_type": job.trigger_type.value,
                "cron_schedule": job.cron_schedule.to_dict() if job.cron_schedule else None,
                "interval_seconds": job.interval_seconds,
                "run_at": job.run_at.isoformat() if job.run_at else None,
                "priority": job.priority.value,
                "max_retries": job.max_retries,
                "timeout_seconds": job.timeout_seconds,
                "status": job.status.value,
                "tags": job.tags,
                "metadata": job.metadata,
                "next_run_at": job.next_run_at.isoformat() if job.next_run_at else None,
            }),
            job.created_at.isoformat(),
        ))
        conn.commit()
        conn.close()

    def _save_execution(self, execution: JobExecution) -> None:
        """Save execution record to database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO executions (execution_id, job_id, execution_json, started_at)
            VALUES (?, ?, ?, ?)
        """, (
            execution.execution_id,
            execution.job_id,
            json.dumps({
                "status": execution.status.value,
                "result": execution.result,
                "error": execution.error,
                "retry_count": execution.retry_count,
                "duration_ms": execution.duration_ms,
                "worker_id": execution.worker_id,
            }),
            execution.started_at.isoformat(),
        ))
        conn.commit()
        conn.close()
