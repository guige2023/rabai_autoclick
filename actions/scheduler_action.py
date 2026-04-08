"""Scheduler action module for RabAI AutoClick.

Provides job scheduling with cron expressions, interval-based
scheduling, priority queues, and job state management.
"""

import sys
import os
import json
import time
import uuid
import asyncio
import croniter
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import deque, defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ScheduleType(Enum):
    """Job schedule types."""
    CRON = "cron"
    INTERVAL = "interval"
    ONE_TIME = "one_time"
    MANUAL = "manual"


class JobStatus(Enum):
    """Job execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


@dataclass
class ScheduledJob:
    """Represents a scheduled job."""
    job_id: str
    name: str
    schedule_type: ScheduleType
    # For cron
    cron_expression: Optional[str] = None
    # For interval
    interval_seconds: Optional[float] = None
    # For one-time
    run_at: Optional[float] = None
    # Common
    priority: int = 5  # 1-10, higher = more urgent
    enabled: bool = True
    timeout_seconds: float = 300.0
    retry_on_failure: bool = True
    max_retries: int = 3
    description: str = ""
    
    # Runtime state
    last_run_time: Optional[float] = None
    next_run_time: Optional[float] = None
    run_count: int = 0
    consecutive_failures: int = 0


@dataclass
class JobExecution:
    """Record of a job execution."""
    execution_id: str
    job_id: str
    job_name: str
    status: JobStatus
    start_time: float
    end_time: Optional[float] = None
    result: Any = None
    error: Optional[str] = None
    attempt_number: int = 1
    execution_time_ms: float = 0.0


class JobScheduler:
    """Job scheduling and execution engine."""
    
    def __init__(self, persistence_path: Optional[str] = None):
        self._jobs: Dict[str, ScheduledJob] = {}
        self._executions: Dict[str, JobExecution] = {}
        self._job_functions: Dict[str, Callable] = {}
        self._execution_queue: deque = deque()
        self._running = False
        self._scheduler_task: Optional[asyncio.Task] = None
        self._persistence_path = persistence_path
        self._load()
    
    def _load(self) -> None:
        """Load scheduler data from persistence."""
        if self._persistence_path and os.path.exists(self._persistence_path):
            try:
                with open(self._persistence_path, 'r') as f:
                    data = json.load(f)
                    for job_data in data.get("jobs", []):
                        job = ScheduledJob(
                            job_id=job_data["job_id"],
                            name=job_data["name"],
                            schedule_type=ScheduleType(job_data["schedule_type"]),
                            cron_expression=job_data.get("cron_expression"),
                            interval_seconds=job_data.get("interval_seconds"),
                            run_at=job_data.get("run_at"),
                            priority=job_data.get("priority", 5),
                            enabled=job_data.get("enabled", True),
                            timeout_seconds=job_data.get("timeout_seconds", 300.0),
                            retry_on_failure=job_data.get("retry_on_failure", True),
                            max_retries=job_data.get("max_retries", 3),
                            description=job_data.get("description", ""),
                            last_run_time=job_data.get("last_run_time"),
                            next_run_time=job_data.get("next_run_time"),
                            run_count=job_data.get("run_count", 0)
                        )
                        self._jobs[job.job_id] = job
            except (json.JSONDecodeError, TypeError, KeyError):
                pass
    
    def _persist(self) -> None:
        """Persist scheduler data."""
        if self._persistence_path:
            try:
                data = {
                    "jobs": [
                        {
                            "job_id": j.job_id,
                            "name": j.name,
                            "schedule_type": j.schedule_type.value,
                            "cron_expression": j.cron_expression,
                            "interval_seconds": j.interval_seconds,
                            "run_at": j.run_at,
                            "priority": j.priority,
                            "enabled": j.enabled,
                            "timeout_seconds": j.timeout_seconds,
                            "retry_on_failure": j.retry_on_failure,
                            "max_retries": j.max_retries,
                            "description": j.description,
                            "last_run_time": j.last_run_time,
                            "next_run_time": j.next_run_time,
                            "run_count": j.run_count
                        }
                        for j in self._jobs.values()
                    ]
                }
                with open(self._persistence_path, 'w') as f:
                    json.dump(data, f, indent=2)
            except OSError:
                pass
    
    def _calculate_next_run(self, job: ScheduledJob) -> Optional[float]:
        """Calculate next run time for a job."""
        now = time.time()
        
        if job.schedule_type == ScheduleType.CRON and job.cron_expression:
            try:
                cron = croniter.croniter(job.cron_expression, now)
                return cron.get_next()
            except Exception:
                return None
        
        elif job.schedule_type == ScheduleType.INTERVAL and job.interval_seconds:
            if job.last_run_time:
                return job.last_run_time + job.interval_seconds
            return now
        
        elif job.schedule_type == ScheduleType.ONE_TIME and job.run_at:
            if job.run_at > now:
                return job.run_at
            return None
        
        return None
    
    def add_job(self, job: ScheduledJob) -> str:
        """Add a scheduled job."""
        job.next_run_time = self._calculate_next_run(job)
        self._jobs[job.job_id] = job
        self._persist()
        return job.job_id
    
    def remove_job(self, job_id: str) -> bool:
        """Remove a scheduled job."""
        if job_id in self._jobs:
            del self._jobs[job_id]
            self._persist()
            return True
        return False
    
    def get_job(self, job_id: str) -> Optional[ScheduledJob]:
        """Get a job by ID."""
        return self._jobs.get(job_id)
    
    def list_jobs(self, enabled_only: bool = False) -> List[ScheduledJob]:
        """List all jobs."""
        jobs = list(self._jobs.values())
        if enabled_only:
            jobs = [j for j in jobs if j.enabled]
        return sorted(jobs, key=lambda j: j.priority, reverse=True)
    
    def register_job_function(self, job_id: str, func: Callable) -> None:
        """Register an executable function for a job."""
        self._job_functions[job_id] = func
    
    async def execute_job(self, job: ScheduledJob) -> JobExecution:
        """Execute a single job."""
        execution_id = str(uuid.uuid4())
        execution = JobExecution(
            execution_id=execution_id,
            job_id=job.job_id,
            job_name=job.name,
            status=JobStatus.RUNNING,
            start_time=time.time()
        )
        self._executions[execution_id] = execution
        
        func = self._job_functions.get(job.job_id)
        
        try:
            if func:
                if asyncio.iscoroutinefunction(func):
                    result = await asyncio.wait_for(func(job), timeout=job.timeout_seconds)
                else:
                    result = await asyncio.wait_for(
                        asyncio.coroutine(func)(job),
                        timeout=job.timeout_seconds
                    )
            else:
                # Simulate job execution
                await asyncio.sleep(0.1)
                result = {"executed": True, "job_id": job.job_id}
            
            execution.status = JobStatus.COMPLETED
            execution.result = result
            job.consecutive_failures = 0
        
        except asyncio.TimeoutError:
            execution.status = JobStatus.FAILED
            execution.error = f"Job timed out after {job.timeout_seconds}s"
            job.consecutive_failures += 1
        
        except Exception as e:
            execution.status = JobStatus.FAILED
            execution.error = str(e)
            job.consecutive_failures += 1
            
            if job.retry_on_failure and job.consecutive_failures < job.max_retries:
                execution.status = JobStatus.PENDING
                execution.attempt_number += 1
        
        execution.end_time = time.time()
        execution.execution_time_ms = (execution.end_time - execution.start_time) * 1000
        
        if execution.status == JobStatus.COMPLETED:
            job.last_run_time = execution.end_time
            job.run_count += 1
            job.next_run_time = self._calculate_next_run(job)
        
        self._persist()
        return execution
    
    async def run_scheduler(self) -> None:
        """Run the scheduler loop."""
        self._running = True
        
        while self._running:
            now = time.time()
            
            # Find jobs ready to run
            for job in self._jobs.values():
                if not job.enabled:
                    continue
                if job.next_run_time and job.next_run_time <= now:
                    asyncio.create_task(self.execute_job(job))
            
            await asyncio.sleep(1)  # Check every second
    
    def stop_scheduler(self) -> None:
        """Stop the scheduler loop."""
        self._running = False
    
    def get_due_jobs(self) -> List[ScheduledJob]:
        """Get jobs that are due to run."""
        now = time.time()
        return [
            j for j in self._jobs.values()
            if j.enabled and j.next_run_time and j.next_run_time <= now
        ]
    
    def get_execution_history(
        self,
        job_id: Optional[str] = None,
        limit: int = 100
    ) -> List[JobExecution]:
        """Get execution history."""
        executions = list(self._executions.values())
        if job_id:
            executions = [e for e in executions if e.job_id == job_id]
        return sorted(executions, key=lambda e: e.start_time, reverse=True)[:limit]


class SchedulerAction(BaseAction):
    """Schedule and manage recurring jobs.
    
    Supports cron expressions, interval scheduling, priority queues,
    timeout handling, and execution history.
    """
    action_type = "scheduler"
    display_name = "任务调度器"
    description = "任务调度系统，支持Cron表达式和间隔调度"
    
    def __init__(self):
        super().__init__()
        self._scheduler: Optional[JobScheduler] = None
    
    def _get_scheduler(self, params: Dict[str, Any]) -> JobScheduler:
        """Get or create the scheduler."""
        if self._scheduler is None:
            persistence_path = params.get("persistence_path")
            self._scheduler = JobScheduler(persistence_path)
        return self._scheduler
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute scheduler operation."""
        operation = params.get("operation", "")
        
        try:
            if operation == "add_job":
                return self._add_job(params)
            elif operation == "remove_job":
                return self._remove_job(params)
            elif operation == "list_jobs":
                return self._list_jobs(params)
            elif operation == "get_job":
                return self._get_job(params)
            elif operation == "run_job":
                return self._run_job(params)
            elif operation == "enable_job":
                return self._enable_job(params)
            elif operation == "disable_job":
                return self._disable_job(params)
            elif operation == "get_due_jobs":
                return self._get_due_jobs(params)
            elif operation == "get_history":
                return self._get_history(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Scheduler error: {str(e)}")
    
    def _add_job(self, params: Dict[str, Any]) -> ActionResult:
        """Add a scheduled job."""
        scheduler = self._get_scheduler(params)
        job_id = params.get("job_id", str(uuid.uuid4()))
        name = params.get("name", "")
        schedule_type = ScheduleType(params.get("schedule_type", "interval"))
        
        if not name:
            return ActionResult(success=False, message="Job name is required")
        
        job = ScheduledJob(
            job_id=job_id,
            name=name,
            schedule_type=schedule_type,
            cron_expression=params.get("cron_expression"),
            interval_seconds=params.get("interval_seconds"),
            run_at=params.get("run_at"),
            priority=params.get("priority", 5),
            enabled=params.get("enabled", True),
            timeout_seconds=params.get("timeout_seconds", 300.0),
            description=params.get("description", "")
        )
        scheduler.add_job(job)
        return ActionResult(
            success=True,
            message=f"Job '{name}' added",
            data={"job_id": job_id, "name": name, "next_run_time": job.next_run_time}
        )
    
    def _remove_job(self, params: Dict[str, Any]) -> ActionResult:
        """Remove a job."""
        scheduler = self._get_scheduler(params)
        job_id = params.get("job_id", "")
        if not job_id:
            return ActionResult(success=False, message="job_id is required")
        
        removed = scheduler.remove_job(job_id)
        return ActionResult(success=removed, message=f"Job removed" if removed else "Job not found")
    
    def _list_jobs(self, params: Dict[str, Any]) -> ActionResult:
        """List all jobs."""
        scheduler = self._get_scheduler(params)
        enabled_only = params.get("enabled_only", False)
        jobs = scheduler.list_jobs(enabled_only)
        return ActionResult(
            success=True,
            message=f"Found {len(jobs)} jobs",
            data={"jobs": [{"job_id": j.job_id, "name": j.name, "enabled": j.enabled,
                          "next_run_time": j.next_run_time, "priority": j.priority} for j in jobs]}
        )
    
    def _get_job(self, params: Dict[str, Any]) -> ActionResult:
        """Get a job by ID."""
        scheduler = self._get_scheduler(params)
        job_id = params.get("job_id", "")
        if not job_id:
            return ActionResult(success=False, message="job_id is required")
        
        job = scheduler.get_job(job_id)
        if not job:
            return ActionResult(success=False, message="Job not found")
        
        return ActionResult(
            success=True,
            message=f"Job: {job.name}",
            data={"job_id": job.job_id, "name": job.name, "schedule_type": job.schedule_type.value,
                  "enabled": job.enabled, "next_run_time": job.next_run_time}
        )
    
    def _run_job(self, params: Dict[str, Any]) -> ActionResult:
        """Run a job immediately."""
        scheduler = self._get_scheduler(params)
        job_id = params.get("job_id", "")
        if not job_id:
            return ActionResult(success=False, message="job_id is required")
        
        job = scheduler.get_job(job_id)
        if not job:
            return ActionResult(success=False, message="Job not found")
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            execution = loop.run_until_complete(scheduler.execute_job(job))
            return ActionResult(
                success=execution.status == JobStatus.COMPLETED,
                message=f"Job {execution.status.value}: {execution.error or 'completed'}",
                data={"execution_id": execution.execution_id, "status": execution.status.value,
                      "execution_time_ms": execution.execution_time_ms}
            )
        finally:
            loop.close()
    
    def _enable_job(self, params: Dict[str, Any]) -> ActionResult:
        """Enable a job."""
        scheduler = self._get_scheduler(params)
        job_id = params.get("job_id", "")
        job = scheduler.get_job(job_id)
        if not job:
            return ActionResult(success=False, message="Job not found")
        job.enabled = True
        job.next_run_time = scheduler._calculate_next_run(job)
        scheduler._persist()
        return ActionResult(success=True, message=f"Job '{job.name}' enabled")
    
    def _disable_job(self, params: Dict[str, Any]) -> ActionResult:
        """Disable a job."""
        scheduler = self._get_scheduler(params)
        job_id = params.get("job_id", "")
        job = scheduler.get_job(job_id)
        if not job:
            return ActionResult(success=False, message="Job not found")
        job.enabled = False
        scheduler._persist()
        return ActionResult(success=True, message=f"Job '{job.name}' disabled")
    
    def _get_due_jobs(self, params: Dict[str, Any]) -> ActionResult:
        """Get jobs that are due to run."""
        scheduler = self._get_scheduler(params)
        jobs = scheduler.get_due_jobs()
        return ActionResult(
            success=True,
            message=f"{len(jobs)} jobs due",
            data={"jobs": [{"job_id": j.job_id, "name": j.name} for j in jobs]}
        )
    
    def _get_history(self, params: Dict[str, Any]) -> ActionResult:
        """Get execution history."""
        scheduler = self._get_scheduler(params)
        job_id = params.get("job_id")
        limit = params.get("limit", 100)
        history = scheduler.get_execution_history(job_id, limit)
        return ActionResult(
            success=True,
            message=f"Found {len(history)} executions",
            data={"executions": [{"execution_id": e.execution_id, "job_name": e.job_name,
                                  "status": e.status.value, "start_time": e.start_time,
                                  "execution_time_ms": e.execution_time_ms} for e in history]}
        )
