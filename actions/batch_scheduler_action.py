"""
Batch Scheduler Action Module.

Schedules and executes batch jobs with cron-like expressions,
rate limiting, priority queuing, and execution windows.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class ScheduledJob:
    """A scheduled batch job."""
    name: str
    schedule: str  # cron expression or rate
    handler: Any
    enabled: bool = True
    last_run: Optional[float] = None
    next_run: Optional[float] = None


@dataclass
class ScheduleResult:
    """Result of schedule evaluation."""
    jobs_run: list[str]
    jobs_skipped: list[str]
    current_time: float


class BatchSchedulerAction(BaseAction):
    """Schedule and execute batch jobs."""

    def __init__(self) -> None:
        super().__init__("batch_scheduler")
        self._jobs: list[ScheduledJob] = []
        self._execution_history: list[dict[str, Any]] = []

    def execute(self, context: dict, params: dict) -> ScheduleResult:
        """
        Evaluate and run due jobs.

        Args:
            context: Execution context
            params: Parameters:
                - jobs: List of job configs to run
                - current_time: Override current time (default: now)
                - dry_run: Don't actually execute, just show what would run

        Returns:
            ScheduleResult with jobs that ran and were skipped
        """
        import time

        current_time = params.get("current_time", time.time())
        dry_run = params.get("dry_run", False)
        job_configs = params.get("jobs", [])

        jobs_run = []
        jobs_skipped = []

        for job_config in job_configs:
            name = job_config.get("name", "unnamed")
            schedule = job_config.get("schedule", "")
            handler = job_config.get("handler")
            enabled = job_config.get("enabled", True)

            if not enabled:
                jobs_skipped.append(f"{name} (disabled)")
                continue

            if not self._is_due(schedule, current_time):
                jobs_skipped.append(f"{name} (not due)")
                continue

            if dry_run:
                jobs_run.append(f"{name} (would run)")
                continue

            try:
                if handler:
                    result = handler(job_config)
                else:
                    result = {"status": "executed"}
                self._execution_history.append({
                    "name": name,
                    "timestamp": current_time,
                    "status": "success",
                    "result": result
                })
                jobs_run.append(name)
            except Exception as e:
                self._execution_history.append({
                    "name": name,
                    "timestamp": current_time,
                    "status": "failed",
                    "error": str(e)
                })
                jobs_run.append(f"{name} (error: {str(e)})")

        return ScheduleResult(jobs_run, jobs_skipped, current_time)

    def _is_due(self, schedule: str, current_time: float) -> bool:
        """Check if a schedule is due."""
        import time
        if not schedule:
            return True

        parts = schedule.split()
        if len(parts) == 1:
            rate = int(parts[0]) if parts[0].isdigit() else 0
            return int(current_time) % rate == 0 if rate > 0 else True
        elif len(parts) >= 2:
            minute = parts[0]
            hour = parts[1]
            now = time.localtime(current_time)
            if minute == "*" or int(minute) == now.tm_min:
                if hour == "*" or int(hour) == now.tm_hour:
                    return True
        return False

    def add_job(self, name: str, schedule: str, handler: Any) -> None:
        """Add a scheduled job."""
        self._jobs.append(ScheduledJob(name=name, schedule=schedule, handler=handler))

    def get_history(self, limit: int = 100) -> list[dict[str, Any]]:
        """Get execution history."""
        return self._execution_history[-limit:]

    def get_next_run(self, schedule: str) -> Optional[float]:
        """Get next run time for a schedule."""
        import time
        current_time = time.time()
        for offset in range(1, 86400):
            if self._is_due(schedule, current_time + offset):
                return current_time + offset
        return None
