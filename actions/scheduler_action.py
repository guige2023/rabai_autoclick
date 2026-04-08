"""Scheduler action module for RabAI AutoClick.

Provides scheduling utilities:
- Scheduler: Task scheduler
- CronJob: Cron-based job
- IntervalJob: Interval-based job
"""

from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass
import threading
import time
import uuid
import croniter

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class ScheduledJob:
    """Scheduled job."""
    job_id: str
    name: str
    func: Callable
    schedule_type: str
    schedule_value: str
    enabled: bool = True
    last_run: Optional[float] = None
    next_run: Optional[float] = None
    metadata: Dict[str, Any] = None


class Scheduler:
    """Task scheduler."""

    def __init__(self):
        self._jobs: Dict[str, ScheduledJob] = {}
        self._lock = threading.RLock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def add_cron_job(self, name: str, func: Callable, cron_expression: str, metadata: Optional[Dict[str, Any]] = None) -> str:
        """Add a cron job."""
        job_id = str(uuid.uuid4())

        try:
            cron = croniter.croniter(cron_expression, time.time())
            next_run = cron.get_next(time.time)
        except Exception:
            next_run = None

        job = ScheduledJob(
            job_id=job_id,
            name=name,
            func=func,
            schedule_type="cron",
            schedule_value=cron_expression,
            next_run=next_run,
            metadata=metadata,
        )

        with self._lock:
            self._jobs[job_id] = job

        return job_id

    def add_interval_job(self, name: str, func: Callable, interval_seconds: float, metadata: Optional[Dict[str, Any]] = None) -> str:
        """Add an interval job."""
        job_id = str(uuid.uuid4())

        job = ScheduledJob(
            job_id=job_id,
            name=name,
            func=func,
            schedule_type="interval",
            schedule_value=str(interval_seconds),
            next_run=time.time() + interval_seconds,
            metadata=metadata,
        )

        with self._lock:
            self._jobs[job_id] = job

        return job_id

    def remove_job(self, job_id: str) -> bool:
        """Remove a job."""
        with self._lock:
            if job_id in self._jobs:
                del self._jobs[job_id]
                return True
            return False

    def enable_job(self, job_id: str) -> bool:
        """Enable a job."""
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id].enabled = True
                return True
            return False

    def disable_job(self, job_id: str) -> bool:
        """Disable a job."""
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id].enabled = False
                return True
            return False

    def get_due_jobs(self) -> List[ScheduledJob]:
        """Get jobs that are due."""
        now = time.time()
        due = []

        with self._lock:
            for job in self._jobs.values():
                if not job.enabled:
                    continue
                if job.next_run and job.next_run <= now:
                    due.append(job)

        return due

    def run_job(self, job: ScheduledJob) -> None:
        """Run a job."""
        try:
            job.func()
            job.last_run = time.time()
        except Exception:
            pass

        if job.schedule_type == "cron":
            try:
                cron = croniter.croniter(job.schedule_value, time.time())
                job.next_run = cron.get_next(time.time)
            except Exception:
                job.next_run = None
        else:
            try:
                interval = float(job.schedule_value)
                job.next_run = time.time() + interval
            except Exception:
                job.next_run = None

    def start(self) -> None:
        """Start the scheduler."""
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

    def _run_loop(self) -> None:
        """Main scheduler loop."""
        while self._running:
            due_jobs = self.get_due_jobs()
            for job in due_jobs:
                threading.Thread(target=self.run_job, args=(job,), daemon=True).start()
            time.sleep(0.5)

    def get_jobs(self) -> List[Dict[str, Any]]:
        """Get all jobs."""
        with self._lock:
            return [
                {
                    "job_id": j.job_id,
                    "name": j.name,
                    "schedule_type": j.schedule_type,
                    "schedule_value": j.schedule_value,
                    "enabled": j.enabled,
                    "last_run": j.last_run,
                    "next_run": j.next_run,
                }
                for j in self._jobs.values()
            ]


class SchedulerAction(BaseAction):
    """Scheduler management action."""
    action_type = "scheduler"
    display_name = "调度器"
    description = "任务调度"

    def __init__(self):
        super().__init__()
        self._scheduler = Scheduler()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "add_cron")

            if operation == "add_cron":
                return self._add_cron(params)
            elif operation == "add_interval":
                return self._add_interval(params)
            elif operation == "remove":
                return self._remove(params)
            elif operation == "enable":
                return self._enable(params)
            elif operation == "disable":
                return self._disable(params)
            elif operation == "list":
                return self._list(params)
            elif operation == "start":
                return self._start(params)
            elif operation == "stop":
                return self._stop(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Scheduler error: {str(e)}")

    def _add_cron(self, params: Dict[str, Any]) -> ActionResult:
        """Add cron job."""
        name = params.get("name")
        cron_expr = params.get("cron")

        if not name or not cron_expr:
            return ActionResult(success=False, message="name and cron are required")

        def job_func():
            return {"executed": True}

        job_id = self._scheduler.add_cron_job(name, job_func, cron_expr)

        return ActionResult(success=True, message=f"Cron job added: {name}", data={"job_id": job_id})

    def _add_interval(self, params: Dict[str, Any]) -> ActionResult:
        """Add interval job."""
        name = params.get("name")
        interval = params.get("interval")

        if not name or not interval:
            return ActionResult(success=False, message="name and interval are required")

        def job_func():
            return {"executed": True}

        job_id = self._scheduler.add_interval_job(name, job_func, interval)

        return ActionResult(success=True, message=f"Interval job added: {name}", data={"job_id": job_id})

    def _remove(self, params: Dict[str, Any]) -> ActionResult:
        """Remove job."""
        job_id = params.get("job_id")

        if not job_id:
            return ActionResult(success=False, message="job_id is required")

        success = self._scheduler.remove_job(job_id)

        return ActionResult(success=success, message="Removed" if success else "Job not found")

    def _enable(self, params: Dict[str, Any]) -> ActionResult:
        """Enable job."""
        job_id = params.get("job_id")

        if not job_id:
            return ActionResult(success=False, message="job_id is required")

        success = self._scheduler.enable_job(job_id)

        return ActionResult(success=success, message="Enabled" if success else "Job not found")

    def _disable(self, params: Dict[str, Any]) -> ActionResult:
        """Disable job."""
        job_id = params.get("job_id")

        if not job_id:
            return ActionResult(success=False, message="job_id is required")

        success = self._scheduler.disable_job(job_id)

        return ActionResult(success=success, message="Disabled" if success else "Job not found")

    def _list(self, params: Dict[str, Any]) -> ActionResult:
        """List all jobs."""
        jobs = self._scheduler.get_jobs()

        return ActionResult(success=True, message=f"{len(jobs)} jobs", data={"jobs": jobs})

    def _start(self, params: Dict[str, Any]) -> ActionResult:
        """Start scheduler."""
        self._scheduler.start()
        return ActionResult(success=True, message="Scheduler started")

    def _stop(self, params: Dict[str, Any]) -> ActionResult:
        """Stop scheduler."""
        self._scheduler.stop()
        return ActionResult(success=True, message="Scheduler stopped")
