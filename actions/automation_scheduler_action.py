"""Automation Scheduler Action Module.

Provides cron-like scheduling, workflow orchestration, and
task automation with dependency management.
"""

from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
import asyncio
import json
import hashlib
from croniter import croniter


class ScheduleType(Enum):
    """Types of scheduling mechanisms."""
    CRON = "cron"
    INTERVAL = "interval"
    ONE_TIME = "one_time"
    CALENDAR = "calendar"
    EVENT_DRIVEN = "event_driven"


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
    """Defines when a task should run."""
    schedule_type: ScheduleType
    expression: str
    timezone: str = "UTC"
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

    def is_due(self, last_run: Optional[datetime] = None) -> bool:
        """Check if the schedule is due to run."""
        now = datetime.now()
        if self.start_date and now < self.start_date:
            return False
        if self.end_date and now > self.end_date:
            return False

        if self.schedule_type == ScheduleType.CRON:
            return self._is_cron_due(last_run)
        elif self.schedule_type == ScheduleType.INTERVAL:
            return self._is_interval_due(last_run)
        elif self.schedule_type == ScheduleType.ONE_TIME:
            return self._is_one_time_due()
        return False

    def _is_cron_due(self, last_run: Optional[datetime] = None) -> bool:
        """Check cron schedule."""
        try:
            if last_run:
                cron = croniter(self.expression, last_run)
                next_run = cron.get_next(datetime)
            else:
                cron = croniter(self.expression, datetime.now())
                next_run = cron.get_next(datetime)
            return datetime.now() >= next_run
        except (ValueError, KeyError):
            return False

    def _is_interval_due(self, last_run: Optional[datetime] = None) -> bool:
        """Check interval schedule."""
        if not last_run:
            return True
        interval_seconds = self._parse_interval()
        elapsed = (datetime.now() - last_run).total_seconds()
        return elapsed >= interval_seconds

    def _is_one_time_due(self) -> bool:
        """Check one-time schedule."""
        try:
            run_time = datetime.fromisoformat(self.expression)
            return datetime.now() >= run_time
        except ValueError:
            return False

    def _parse_interval(self) -> float:
        """Parse interval expression to seconds."""
        if not self.expression:
            return 0
        parts = self.expression.lower()
        if "h" in parts:
            return float(parts.replace("h", "")) * 3600
        elif "m" in parts:
            return float(parts.replace("m", "")) * 60
        elif "s" in parts:
            return float(parts.replace("s", ""))
        return float(parts)


@dataclass
class AutomationTask:
    """Represents an automatable task."""
    id: str
    name: str
    handler: Callable
    schedule: Schedule
    dependencies: List[str] = field(default_factory=list)
    timeout: int = 300
    retry_count: int = 0
    retry_delay: int = 60
    max_retries: int = 3
    notification_config: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def generate_id(self) -> str:
        """Generate unique task ID."""
        content = f"{self.name}:{self.schedule.expression}:{datetime.now().isoformat()}"
        return hashlib.md5(content.encode()).hexdigest()[:12]


@dataclass
class TaskExecution:
    """Records a task execution."""
    task_id: str
    status: TaskStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    retries: int = 0
    log: List[str] = field(default_factory=list)


class AutomationScheduler:
    """Schedules and executes automated tasks."""

    def __init__(self, max_concurrent: int = 5, default_timezone: str = "UTC"):
        self._tasks: Dict[str, AutomationTask] = {}
        self._executions: Dict[str, TaskExecution] = {}
        self._running_tasks: Dict[str, asyncio.Task] = {}
        self._last_runs: Dict[str, datetime] = {}
        self._max_concurrent = max_concurrent
        self._default_timezone = default_timezone
        self._listeners: Dict[str, List[Callable]] = {}
        self._paused: bool = False

    def register_task(self, task: AutomationTask) -> str:
        """Register a new automation task."""
        if not task.id:
            task.id = task.generate_id()
        self._tasks[task.id] = task
        return task.id

    def unregister_task(self, task_id: str) -> bool:
        """Remove a task from the scheduler."""
        if task_id in self._tasks:
            del self._tasks[task_id]
            return True
        return False

    def get_task(self, task_id: str) -> Optional[AutomationTask]:
        """Get task by ID."""
        return self._tasks.get(task_id)

    def list_tasks(self, status: Optional[TaskStatus] = None) -> List[AutomationTask]:
        """List all registered tasks."""
        tasks = list(self._tasks.values())
        if status:
            exec_status = {
                t.task_id: t.status for t in self._executions.values()
            }
            tasks = [t for t in tasks if exec_status.get(t.id) == status]
        return tasks

    def add_dependency(self, task_id: str, depends_on: str):
        """Add a dependency to a task."""
        if task_id in self._tasks:
            self._tasks[task_id].dependencies.append(depends_on)

    async def execute_task(self, task_id: str, force: bool = False) -> TaskExecution:
        """Execute a task immediately or check if scheduled."""
        task = self._tasks.get(task_id)
        if not task:
            return TaskExecution(
                task_id=task_id,
                status=TaskStatus.FAILED,
                started_at=datetime.now(),
                error="Task not found",
            )

        if len(self._running_tasks) >= self._max_concurrent and not force:
            return TaskExecution(
                task_id=task_id,
                status=TaskStatus.PENDING,
                started_at=datetime.now(),
            )

        if not force and not task.schedule.is_due(self._last_runs.get(task_id)):
            return TaskExecution(
                task_id=task_id,
                status=TaskStatus.SKIPPED,
                started_at=datetime.now(),
            )

        if not self._check_dependencies(task):
            return TaskExecution(
                task_id=task_id,
                status=TaskStatus.SKIPPED,
                started_at=datetime.now(),
                log=["Dependencies not met"],
            )

        execution = TaskExecution(
            task_id=task_id,
            status=TaskStatus.RUNNING,
            started_at=datetime.now(),
        )
        self._executions[f"{task_id}:{execution.started_at.isoformat()}"] = execution

        try:
            async with asyncio.timeout(task.timeout):
                if asyncio.iscoroutinefunction(task.handler):
                    result = await task.handler()
                else:
                    result = task.handler()
                execution.status = TaskStatus.COMPLETED
                execution.result = result
                self._last_runs[task_id] = datetime.now()
        except asyncio.TimeoutError:
            execution.status = TaskStatus.FAILED
            execution.error = f"Task timed out after {task.timeout}s"
        except Exception as e:
            execution.status = TaskStatus.FAILED
            execution.error = str(e)
            if task.retry_count < task.max_retries:
                await self._schedule_retry(task, execution)

        execution.completed_at = datetime.now()
        self._emit_event(task_id, "task_completed", execution)
        return execution

    def _check_dependencies(self, task: AutomationTask) -> bool:
        """Check if all dependencies have completed successfully."""
        for dep_id in task.dependencies:
            dep_execution = self._get_latest_execution(dep_id)
            if not dep_execution:
                return False
            if dep_execution.status != TaskStatus.COMPLETED:
                return False
        return True

    def _get_latest_execution(self, task_id: str) -> Optional[TaskExecution]:
        """Get the most recent execution for a task."""
        task_execs = [
            (k, v) for k, v in self._executions.items() if v.task_id == task_id
        ]
        if not task_execs:
            return None
        return max(task_execs, key=lambda x: x[1].started_at)[1]

    async def _schedule_retry(self, task: AutomationTask, execution: TaskExecution):
        """Schedule a retry for failed task."""
        execution.retries += 1
        await asyncio.sleep(task.retry_delay)
        self._emit_event(task.id, "task_retry", execution)

    def pause(self):
        """Pause the scheduler."""
        self._paused = True

    def resume(self):
        """Resume the scheduler."""
        self._paused = False

    def on_event(self, event: str, callback: Callable):
        """Register event listener."""
        if event not in self._listeners:
            self._listeners[event] = []
        self._listeners[event].append(callback)

    def _emit_event(self, task_id: str, event: str, execution: TaskExecution):
        """Emit an event to registered listeners."""
        for callback in self._listeners.get(event, []):
            try:
                callback(task_id, execution)
            except Exception:
                pass

    def get_execution_history(
        self, task_id: Optional[str] = None, limit: int = 100
    ) -> List[TaskExecution]:
        """Get execution history for a task or all tasks."""
        if task_id:
            execs = [
                v for v in self._executions.values() if v.task_id == task_id
            ]
        else:
            execs = list(self._executions.values())
        return sorted(execs, key=lambda x: x.started_at, reverse=True)[:limit]

    def cancel_task(self, task_id: str) -> bool:
        """Cancel a running task."""
        if task_id in self._running_tasks:
            self._running_tasks[task_id].cancel()
            return True
        return False

    def get_metrics(self) -> Dict[str, Any]:
        """Get scheduler metrics."""
        total_executions = len(self._executions)
        completed = sum(
            1 for e in self._executions.values() if e.status == TaskStatus.COMPLETED
        )
        failed = sum(
            1 for e in self._executions.values() if e.status == TaskStatus.FAILED
        )
        return {
            "total_tasks": len(self._tasks),
            "running_tasks": len(self._running_tasks),
            "total_executions": total_executions,
            "completed": completed,
            "failed": failed,
            "success_rate": completed / total_executions if total_executions > 0 else 0,
            "paused": self._paused,
        }


# Module exports
__all__ = [
    "AutomationScheduler",
    "AutomationTask",
    "Schedule",
    "ScheduleType",
    "TaskExecution",
    "TaskStatus",
]
