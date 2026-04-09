"""
API scheduler action for priority queue and scheduled execution.

Provides job scheduling, priority handling, and rate limiting.
"""

from typing import Any, Callable, Dict, List, Optional
import time
import threading
import heapq
import uuid


class APISchedulerAction:
    """API request scheduler with priority queue and scheduling."""

    def __init__(
        self,
        max_concurrent: int = 10,
        default_priority: int = 5,
        enable_rate_limit: bool = True,
        rate_limit: int = 100,
        rate_window: float = 60.0,
    ) -> None:
        """
        Initialize API scheduler.

        Args:
            max_concurrent: Maximum concurrent executions
            default_priority: Default job priority (1-10)
            enable_rate_limit: Enable rate limiting
            rate_limit: Requests per window
            rate_window: Rate limit window in seconds
        """
        self.max_concurrent = max_concurrent
        self.default_priority = default_priority
        self.enable_rate_limit = enable_rate_limit
        self.rate_limit = rate_limit
        self.rate_window = rate_window

        self._job_queue: List[Dict[str, Any]] = []
        self._scheduled_jobs: Dict[str, Dict[str, Any]] = {}
        self._running_jobs: Dict[str, Dict[str, Any]] = {}
        self._completed_jobs: Dict[str, Dict[str, Any]] = {}
        self._rate_tracker: List[float] = []
        self._lock = threading.Lock()

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute scheduler operation.

        Args:
            params: Dictionary containing:
                - operation: 'schedule', 'execute', 'cancel', 'status', 'list'
                - job_id: Job identifier
                - job: Job definition
                - schedule_time: Time to execute (for schedule)
                - priority: Job priority (1-10)

        Returns:
            Dictionary with operation result
        """
        operation = params.get("operation", "execute")

        if operation == "schedule":
            return self._schedule_job(params)
        elif operation == "execute":
            return self._execute_job(params)
        elif operation == "cancel":
            return self._cancel_job(params)
        elif operation == "status":
            return self._get_status(params)
        elif operation == "list":
            return self._list_jobs(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _schedule_job(self, params: dict[str, Any]) -> dict[str, Any]:
        """Schedule job for future execution."""
        job = params.get("job", {})
        schedule_time = params.get("schedule_time")
        priority = params.get("priority", self.default_priority)
        job_id = params.get("job_id", str(uuid.uuid4()))

        if not job:
            return {"success": False, "error": "Job definition is required"}

        if isinstance(schedule_time, (int, float)):
            run_at = schedule_time
        elif isinstance(schedule_time, str):
            run_at = time.time() + self._parse_duration(schedule_time)
        else:
            run_at = time.time()

        scheduled_job = {
            "job_id": job_id,
            "job": job,
            "priority": priority,
            "schedule_time": run_at,
            "status": "scheduled",
            "created_at": time.time(),
        }

        with self._lock:
            self._scheduled_jobs[job_id] = scheduled_job
            heapq.heappush(
                self._job_queue,
                (run_at, priority, job_id),
            )

        return {
            "success": True,
            "job_id": job_id,
            "schedule_time": run_at,
            "scheduled": True,
        }

    def _parse_duration(self, duration: str) -> float:
        """Parse duration string to seconds."""
        units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
        if duration[-1] in units:
            return float(duration[:-1]) * units[duration[-1]]
        return float(duration)

    def _execute_job(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute job immediately or when scheduled."""
        job = params.get("job", {})
        job_id = params.get("job_id", str(uuid.uuid4()))
        priority = params.get("priority", self.default_priority)
        handler = params.get("handler")

        if self.enable_rate_limit and not self._check_rate_limit():
            return {"success": False, "error": "Rate limit exceeded"}

        if len(self._running_jobs) >= self.max_concurrent:
            return {
                "success": False,
                "error": "Max concurrent jobs reached",
                "queued": True,
                "job_id": job_id,
            }

        running_job = {
            "job_id": job_id,
            "job": job,
            "priority": priority,
            "status": "running",
            "started_at": time.time(),
        }

        with self._lock:
            self._running_jobs[job_id] = running_job

        try:
            if callable(handler):
                result = handler(job)
            else:
                result = {"success": True, "message": "Job executed"}

            running_job["status"] = "completed"
            running_job["completed_at"] = time.time()
            running_job["result"] = result

            with self._lock:
                self._completed_jobs[job_id] = running_job
                if job_id in self._running_jobs:
                    del self._running_jobs[job_id]

            return {"success": True, "job_id": job_id, "result": result}
        except Exception as e:
            running_job["status"] = "failed"
            running_job["error"] = str(e)
            running_job["completed_at"] = time.time()

            with self._lock:
                self._completed_jobs[job_id] = running_job
                if job_id in self._running_jobs:
                    del self._running_jobs[job_id]

            return {"success": False, "job_id": job_id, "error": str(e)}

    def _cancel_job(self, params: dict[str, Any]) -> dict[str, Any]:
        """Cancel scheduled or running job."""
        job_id = params.get("job_id", "")

        with self._lock:
            if job_id in self._scheduled_jobs:
                del self._scheduled_jobs[job_id]
                self._job_queue = [
                    (t, p, jid) for t, p, jid in self._job_queue if jid != job_id
                ]
                heapq.heapify(self._job_queue)
                return {"success": True, "job_id": job_id, "cancelled": "scheduled"}

            if job_id in self._running_jobs:
                self._running_jobs[job_id]["status"] = "cancelled"
                self._running_jobs[job_id]["completed_at"] = time.time()
                self._completed_jobs[job_id] = self._running_jobs[job_id]
                del self._running_jobs[job_id]
                return {"success": True, "job_id": job_id, "cancelled": "running"}

        return {"success": False, "error": "Job not found"}

    def _check_rate_limit(self) -> bool:
        """Check if request is within rate limit."""
        now = time.time()
        self._rate_tracker = [t for t in self._rate_tracker if now - t < self.rate_window]

        if len(self._rate_tracker) >= self.rate_limit:
            return False

        self._rate_tracker.append(now)
        return True

    def _get_status(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get scheduler status."""
        with self._lock:
            return {
                "success": True,
                "queued_jobs": len(self._job_queue),
                "running_jobs": len(self._running_jobs),
                "completed_jobs": len(self._completed_jobs),
                "max_concurrent": self.max_concurrent,
                "rate_limit": self.rate_limit,
                "rate_remaining": max(0, self.rate_limit - len(self._rate_tracker)),
            }

    def _list_jobs(self, params: dict[str, Any]) -> dict[str, Any]:
        """List jobs by status."""
        status_filter = params.get("status")
        limit = params.get("limit", 100)

        all_jobs = {}

        with self._lock:
            for jid, job in self._scheduled_jobs.items():
                if status_filter is None or job["status"] == status_filter:
                    all_jobs[jid] = job

            for jid, job in self._running_jobs.items():
                if status_filter is None or job["status"] == status_filter:
                    all_jobs[jid] = job

            for jid, job in self._completed_jobs.items():
                if status_filter is None or job["status"] == status_filter:
                    all_jobs[jid] = job

        job_list = list(all_jobs.values())[:limit]

        return {"success": True, "count": len(job_list), "jobs": job_list}
