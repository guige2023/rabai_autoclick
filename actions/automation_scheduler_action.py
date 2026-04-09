"""Automation Scheduler Action Module.

Provides scheduled automation execution with cron expressions,
interval-based triggers, event-driven scheduling, and workflow orchestration.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class ScheduleType(Enum):
    """Types of scheduling."""
    INTERVAL = "interval"
    CRON = "cron"
    ONCE = "once"
    DATETIME = "datetime"
    EVENT = "event"


class ScheduleStatus(Enum):
    """Status of a scheduled task."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


@dataclass
class CronExpression:
    """Parsed cron expression."""
    minute: str = "*"
    hour: str = "*"
    day_of_month: str = "*"
    month: str = "*"
    day_of_week: str = "*"

    @classmethod
    def parse(cls, expression: str) -> "CronExpression":
        """Parse a cron expression string."""
        parts = expression.split()
        while len(parts) < 5:
            parts.append("*")
        return cls(
            minute=parts[0],
            hour=parts[1],
            day_of_month=parts[2],
            month=parts[3],
            day_of_week=parts[4],
        )

    def matches(self, dt: datetime) -> bool:
        """Check if this expression matches the given datetime."""
        return (
            self._match_field(self.minute, dt.minute) and
            self._match_field(self.hour, dt.hour) and
            self._match_field(self.day_of_month, dt.day) and
            self._match_field(self.month, dt.month) and
            self._match_field(self.day_of_week, dt.weekday())
        )

    def _match_field(self, field: str, value: int) -> bool:
        """Match a cron field against a value."""
        if field == "*":
            return True

        # Handle lists (e.g., "1,2,3")
        if "," in field:
            return any(self._match_field(f.strip(), value) for f in field.split(","))

        # Handle ranges (e.g., "1-5")
        if "-" in field:
            start, end = field.split("-")
            return int(start) <= value <= int(end)

        # Handle step (e.g., "*/5")
        if "/" in field:
            step, divisor = field.split("/")
            if step == "*":
                return value % int(divisor) == 0
            base = int(step)
            return (value - base) % int(divisor) == 0

        # Direct match
        return int(field) == value


@dataclass
class ScheduledTask:
    """A scheduled automation task."""
    task_id: str
    name: str
    schedule_type: ScheduleType
    schedule_config: Dict[str, Any]
    action_config: Dict[str, Any]
    status: ScheduleStatus = ScheduleStatus.PENDING
    next_run: Optional[float] = None
    last_run: Optional[float] = None
    run_count: int = 0
    max_runs: Optional[int] = None
    enabled: bool = True
    timeout: float = 300.0


@dataclass
class ScheduleStats:
    """Statistics for scheduling operations."""
    total_scheduled: int = 0
    total_executed: int = 0
    total_failed: int = 0
    total_skipped: int = 0
    uptime_seconds: float = 0.0


class AutomationSchedulerAction(BaseAction):
    """Automation Scheduler Action for time-based automation.

    Supports interval, cron, one-time, and event-driven scheduling
    with comprehensive task management.

    Examples:
        >>> action = AutomationSchedulerAction()
        >>> result = action.execute(ctx, {
        ...     "command": "schedule",
        ...     "task_id": "daily_report",
        ...     "schedule_type": "cron",
        ...     "cron_expression": "0 8 * * *",
        ...     "action": {"type": "send_report"}
        ... })
    """

    action_type = "automation_scheduler"
    display_name = "自动化调度"
    description = "Cron/Interval/事件触发调度，支持工作流编排"

    _scheduled_tasks: Dict[str, ScheduledTask] = {}
    _scheduler_thread: Optional[threading.Thread] = None
    _stop_event: threading.Event = threading.Event()
    _lock: threading.RLock = threading.RLock()
    _stats = ScheduleStats()
    _start_time: float = time.time()

    def __init__(self):
        super().__init__()
        self._listeners: Dict[str, List[Callable]] = defaultdict(list)

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute scheduler command.

        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'schedule', 'cancel', 'pause', 'resume', 'list', 'status', 'trigger'
                - task_id: Unique task identifier
                - name: Task name
                - schedule_type: 'interval', 'cron', 'once', 'datetime'
                - interval_seconds: Seconds between runs (for interval)
                - cron_expression: Cron expression (for cron)
                - datetime: Specific datetime (for datetime type)
                - action_config: Action to execute
                - max_runs: Max number of executions
                - enabled: Task enabled state

        Returns:
            ActionResult with command result.
        """
        command = params.get("command", "schedule")

        try:
            if command == "schedule":
                return self._schedule_task(params)
            elif command == "cancel":
                return self._cancel_task(params)
            elif command == "pause":
                return self._pause_task(params)
            elif command == "resume":
                return self._resume_task(params)
            elif command == "list":
                return self._list_tasks(params)
            elif command == "status":
                return self._get_task_status(params)
            elif command == "trigger":
                return self._trigger_task(params)
            elif command == "start":
                return self._start_scheduler(params)
            elif command == "stop":
                return self._stop_scheduler(params)
            else:
                return ActionResult(success=False, message=f"Unknown command: {command}")

        except Exception as e:
            logger.exception("Scheduler command failed")
            return ActionResult(success=False, message=f"Scheduler error: {str(e)}")

    def _schedule_task(self, params: Dict[str, Any]) -> ActionResult:
        """Schedule a new task."""
        task_id = params.get("task_id", f"task_{int(time.time())}")
        name = params.get("name", task_id)
        schedule_type_str = params.get("schedule_type", "interval")

        try:
            schedule_type = ScheduleType(schedule_type_str)
        except ValueError:
            return ActionResult(success=False, message=f"Invalid schedule type: {schedule_type_str}")

        # Build schedule config
        schedule_config: Dict[str, Any] = {}
        if schedule_type == ScheduleType.INTERVAL:
            schedule_config["interval_seconds"] = params.get("interval_seconds", 60)
        elif schedule_type == ScheduleType.CRON:
            schedule_config["cron_expression"] = params.get("cron_expression", "* * * * *")
        elif schedule_type == ScheduleType.ONCE:
            schedule_config["run_time"] = params.get("run_time", time.time() + 60)
        elif schedule_type == ScheduleType.DATETIME:
            schedule_config["run_datetime"] = params.get("datetime")

        # Calculate next run time
        next_run = self._calculate_next_run(schedule_type, schedule_config)

        # Create task
        task = ScheduledTask(
            task_id=task_id,
            name=name,
            schedule_type=schedule_type,
            schedule_config=schedule_config,
            action_config=params.get("action_config", {}),
            next_run=next_run,
            max_runs=params.get("max_runs"),
            enabled=params.get("enabled", True),
            timeout=params.get("timeout", 300.0),
        )

        with self._lock:
            self._scheduled_tasks[task_id] = task
            self._stats.total_scheduled += 1

        return ActionResult(
            success=True,
            message=f"Scheduled task '{name}' ({task_id})",
            data={
                "task_id": task_id,
                "name": name,
                "schedule_type": schedule_type.value,
                "next_run": next_run,
                "status": task.status.value,
            }
        )

    def _cancel_task(self, params: Dict[str, Any]) -> ActionResult:
        """Cancel a scheduled task."""
        task_id = params.get("task_id")
        if not task_id:
            return ActionResult(success=False, message="task_id required")

        with self._lock:
            if task_id in self._scheduled_tasks:
                self._scheduled_tasks[task_id].status = ScheduleStatus.CANCELLED
                del self._scheduled_tasks[task_id]
                return ActionResult(success=True, message=f"Cancelled task: {task_id}")

        return ActionResult(success=False, message=f"Task not found: {task_id}")

    def _pause_task(self, params: Dict[str, Any]) -> ActionResult:
        """Pause a scheduled task."""
        task_id = params.get("task_id")
        if not task_id:
            return ActionResult(success=False, message="task_id required")

        with self._lock:
            if task_id in self._scheduled_tasks:
                self._scheduled_tasks[task_id].enabled = False
                self._scheduled_tasks[task_id].status = ScheduleStatus.PAUSED
                return ActionResult(success=True, message=f"Paused task: {task_id}")

        return ActionResult(success=False, message=f"Task not found: {task_id}")

    def _resume_task(self, params: Dict[str, Any]) -> ActionResult:
        """Resume a paused task."""
        task_id = params.get("task_id")
        if not task_id:
            return ActionResult(success=False, message="task_id required")

        with self._lock:
            if task_id in self._scheduled_tasks:
                task = self._scheduled_tasks[task_id]
                task.enabled = True
                task.status = ScheduleStatus.PENDING
                task.next_run = self._calculate_next_run(task.schedule_type, task.schedule_config)
                return ActionResult(success=True, message=f"Resumed task: {task_id}")

        return ActionResult(success=False, message=f"Task not found: {task_id}")

    def _list_tasks(self, params: Dict[str, Any]) -> ActionResult:
        """List all scheduled tasks."""
        with self._lock:
            tasks = []
            for task_id, task in self._scheduled_tasks.items():
                tasks.append({
                    "task_id": task.task_id,
                    "name": task.name,
                    "schedule_type": task.schedule_type.value,
                    "status": task.status.value,
                    "enabled": task.enabled,
                    "next_run": task.next_run,
                    "last_run": task.last_run,
                    "run_count": task.run_count,
                })

            return ActionResult(
                success=True,
                message=f"Listed {len(tasks)} tasks",
                data={"tasks": tasks, "total": len(tasks)}
            )

    def _get_task_status(self, params: Dict[str, Any]) -> ActionResult:
        """Get status of a specific task."""
        task_id = params.get("task_id")
        if not task_id:
            return ActionResult(success=False, message="task_id required")

        with self._lock:
            if task_id not in self._scheduled_tasks:
                return ActionResult(success=False, message=f"Task not found: {task_id}")

            task = self._scheduled_tasks[task_id]
            return ActionResult(
                success=True,
                message=f"Status for task: {task_id}",
                data={
                    "task_id": task.task_id,
                    "name": task.name,
                    "schedule_type": task.schedule_type.value,
                    "status": task.status.value,
                    "enabled": task.enabled,
                    "next_run": task.next_run,
                    "last_run": task.last_run,
                    "run_count": task.run_count,
                    "max_runs": task.max_runs,
                }
            )

    def _trigger_task(self, params: Dict[str, Any]) -> ActionResult:
        """Trigger a task to run immediately."""
        task_id = params.get("task_id")
        if not task_id:
            return ActionResult(success=False, message="task_id required")

        with self._lock:
            if task_id not in self._scheduled_tasks:
                return ActionResult(success=False, message=f"Task not found: {task_id}")

            task = self._scheduled_tasks[task_id]
            self._execute_task(task)

            return ActionResult(
                success=True,
                message=f"Triggered task: {task_id}",
                data={"task_id": task_id, "executed": True}
            )

    def _start_scheduler(self, params: Dict[str, Any]) -> ActionResult:
        """Start the scheduler background thread."""
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            return ActionResult(success=True, message="Scheduler already running")

        self._stop_event.clear()
        self._scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._scheduler_thread.start()

        return ActionResult(
            success=True,
            message="Scheduler started",
            data={"thread_id": self._scheduler_thread.name}
        )

    def _stop_scheduler(self, params: Dict[str, Any]) -> ActionResult:
        """Stop the scheduler background thread."""
        self._stop_event.set()
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5.0)

        return ActionResult(success=True, message="Scheduler stopped")

    def _scheduler_loop(self) -> None:
        """Main scheduler loop."""
        while not self._stop_event.is_set():
            now = time.time()

            with self._lock:
                for task_id, task in list(self._scheduled_tasks.items()):
                    if not task.enabled:
                        continue

                    if task.status == ScheduleStatus.RUNNING:
                        continue

                    if task.next_run and now >= task.next_run:
                        self._execute_task(task)

            self._stop_event.wait(1.0)

    def _execute_task(self, task: ScheduledTask) -> None:
        """Execute a scheduled task."""
        task.status = ScheduleStatus.RUNNING
        task.last_run = time.time()
        task.run_count += 1

        logger.info(f"Executing scheduled task: {task.task_id}")

        try:
            # Execute the action (in a real implementation, this would dispatch to action system)
            action_type = task.action_config.get("type", "unknown")
            logger.info(f"Would execute action: {action_type}")

            task.status = ScheduleStatus.COMPLETED
            self._stats.total_executed += 1

            # Schedule next run
            if task.max_runs and task.run_count >= task.max_runs:
                task.enabled = False
                task.status = ScheduleStatus.COMPLETED
            else:
                task.next_run = self._calculate_next_run(task.schedule_type, task.schedule_config)
                if task.enabled:
                    task.status = ScheduleStatus.PENDING

        except Exception as e:
            logger.error(f"Task {task.task_id} failed: {e}")
            task.status = ScheduleStatus.FAILED
            self._stats.total_failed += 1

            # Reschedule for retry
            if task.schedule_type == ScheduleType.INTERVAL:
                task.next_run = time.time() + task.schedule_config.get("retry_delay", 60)

        # Update uptime
        self._stats.uptime_seconds = time.time() - self._start_time

    def _calculate_next_run(self, schedule_type: ScheduleType, config: Dict[str, Any]) -> Optional[float]:
        """Calculate next run time for a schedule."""
        now = time.time()

        if schedule_type == ScheduleType.INTERVAL:
            interval = config.get("interval_seconds", 60)
            return now + interval

        elif schedule_type == ScheduleType.CRON:
            cron_expr = CronExpression.parse(config.get("cron_expression", "* * * * *"))
            # Find next match within 24 hours
            dt = datetime.now()
            for _ in range(8640):  # Check each minute for 60 days
                dt += timedelta(minutes=1)
                if cron_expr.matches(dt):
                    return dt.timestamp()

        elif schedule_type == ScheduleType.ONCE:
            run_time = config.get("run_time", now + 60)
            return run_time if run_time > now else None

        elif schedule_type == ScheduleType.DATETIME:
            dt_str = config.get("run_datetime")
            if dt_str:
                try:
                    dt = datetime.fromisoformat(dt_str)
                    return dt.timestamp() if dt.timestamp() > now else None
                except ValueError:
                    pass

        return None

    def add_event_listener(self, event: str, callback: Callable) -> None:
        """Add a listener for an event."""
        self._listeners[event].append(callback)

    def remove_event_listener(self, event: str, callback: Callable) -> None:
        """Remove an event listener."""
        if callback in self._listeners[event]:
            self._listeners[event].remove(callback)

    def emit_event(self, event: str, data: Any) -> None:
        """Emit an event to all listeners."""
        for callback in self._listeners.get(event, []):
            try:
                callback(data)
            except Exception as e:
                logger.error(f"Event listener error: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Get scheduler statistics."""
        return {
            "total_scheduled": self._stats.total_scheduled,
            "total_executed": self._stats.total_executed,
            "total_failed": self._stats.total_failed,
            "total_skipped": self._stats.total_skipped,
            "uptime_seconds": self._stats.uptime_seconds,
            "active_tasks": sum(1 for t in self._scheduled_tasks.values() if t.enabled),
            "paused_tasks": sum(1 for t in self._scheduled_tasks.values() if not t.enabled),
            "running_tasks": sum(1 for t in self._scheduled_tasks.values() if t.status == ScheduleStatus.RUNNING),
        }

    def get_required_params(self) -> List[str]:
        return ["command"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "task_id": None,
            "name": None,
            "schedule_type": "interval",
            "interval_seconds": 60,
            "cron_expression": "* * * * *",
            "datetime": None,
            "action_config": {},
            "max_runs": None,
            "enabled": True,
            "timeout": 300.0,
        }
