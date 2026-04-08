"""
Automation Scheduler Action Module.

Schedules and executes automation tasks with cron expressions,
intervals, calendar-based scheduling, and priority queuing.

Author: RabAi Team
"""

from __future__ import annotations

import json
import sys
import os
import time
import threading
import re
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ScheduleType(Enum):
    """Types of scheduling strategies."""
    INTERVAL = "interval"
    CRON = "cron"
    CALENDAR = "calendar"
    ONE_TIME = "one_time"
    FIXED_RATE = "fixed_rate"


class ScheduleState(Enum):
    """Schedule states."""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


@dataclass
class ScheduledTask:
    """A scheduled task definition."""
    id: str
    name: str
    action_type: str
    action_params: Dict[str, Any]
    schedule_type: ScheduleType
    schedule_config: Dict[str, Any]
    priority: int = 0
    enabled: bool = True
    max_runs: Optional[int] = None
    run_count: int = 0
    state: ScheduleState = ScheduleState.PENDING
    last_run: Optional[float] = None
    next_run: Optional[float] = None
    created_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CronExpression:
    """Parsed cron expression."""
    minutes: str
    hours: str
    day_of_month: str
    month: str
    day_of_week: str
    
    def get_next_run(self, from_time: Optional[datetime] = None) -> datetime:
        """Calculate next run time from cron expression."""
        if from_time is None:
            from_time = datetime.now()
        
        next_run = from_time + timedelta(minutes=1)
        
        for _ in range(366 * 24 * 60):
            if self._matches(next_run):
                return next_run
            next_run += timedelta(minutes=1)
        
        return next_run
    
    def _matches(self, dt: datetime) -> bool:
        """Check if datetime matches cron expression."""
        if not self._match_field(self.minutes, dt.minute):
            return False
        if not self._match_field(self.hours, dt.hour):
            return False
        if not self._match_field(self.day_of_month, dt.day):
            return False
        if not self._match_field(self.month, dt.month):
            return False
        if not self._match_field(self.day_of_week, dt.weekday()):
            return False
        return True
    
    def _match_field(self, field: str, value: int) -> bool:
        """Match a cron field against a value."""
        if field == "*":
            return True
        
        for part in field.split(","):
            if "/" in part:
                base, step = part.split("/")
                step = int(step)
                if base == "*":
                    return value % step == 0
                elif "-" in base:
                    start, end = map(int, base.split("-"))
                    return any((i - start) % step == 0 for i in range(start, end + 1))
            elif "-" in part:
                start, end = map(int, part.split("-"))
                if not (start <= value <= end):
                    return False
            elif part.isdigit():
                if int(part) != value:
                    return False
            else:
                return False
        return True


@dataclass
class SchedulerStats:
    """Scheduler statistics."""
    total_tasks: int = 0
    active_tasks: int = 0
    total_runs: int = 0
    successful_runs: int = 0
    failed_runs: int = 0
    skipped_runs: int = 0
    average_run_duration: float = 0.0
    last_update: float = field(default_factory=time.time)


class AutomationSchedulerAction(BaseAction):
    """Automation scheduler action.
    
    Schedules and manages automation tasks with support for
    multiple scheduling strategies and priority execution.
    """
    action_type = "automation_scheduler"
    display_name = "自动化调度器"
    description = "任务定时调度管理"
    
    def __init__(self):
        super().__init__()
        self._tasks: Dict[str, ScheduledTask] = {}
        self._task_queue: deque = deque()
        self._lock = threading.RLock()
        self._running = False
        self._worker_thread: Optional[threading.Thread] = None
        self._stats = SchedulerStats()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Schedule or manage automation tasks.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - operation: Operation to perform (schedule/list/cancel/pause/resume/stats)
                - task: Task definition for scheduling
                - task_id: Task ID for management operations
                
        Returns:
            ActionResult with operation results.
        """
        start_time = time.time()
        
        operation = params.get("operation", "schedule")
        
        if operation == "schedule":
            return self._schedule_task(params, start_time)
        elif operation == "list":
            return self._list_tasks(params, start_time)
        elif operation == "cancel":
            return self._cancel_task(params, start_time)
        elif operation == "pause":
            return self._pause_task(params, start_time)
        elif operation == "resume":
            return self._resume_task(params, start_time)
        elif operation == "stats":
            return self._get_stats(params, start_time)
        elif operation == "trigger":
            return self._trigger_task(params, start_time)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}",
                duration=time.time() - start_time
            )
    
    def _schedule_task(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Schedule a new task."""
        task_def = params.get("task", {})
        
        task_id = task_def.get("id", str(hash(str(time.time()))))
        name = task_def.get("name", f"Task-{task_id}")
        action_type = task_def.get("action_type", "")
        action_params = task_def.get("action_params", {})
        schedule_type = ScheduleType(task_def.get("schedule_type", "interval"))
        schedule_config = task_def.get("schedule_config", {})
        priority = task_def.get("priority", 0)
        
        if not action_type:
            return ActionResult(
                success=False,
                message="Missing required field: action_type",
                duration=time.time() - start_time
            )
        
        next_run = self._calculate_next_run(schedule_type, schedule_config)
        
        task = ScheduledTask(
            id=task_id,
            name=name,
            action_type=action_type,
            action_params=action_params,
            schedule_type=schedule_type,
            schedule_config=schedule_config,
            priority=priority,
            next_run=next_run,
            created_at=time.time()
        )
        
        with self._lock:
            self._tasks[task_id] = task
            self._stats.total_tasks += 1
            self._stats.active_tasks += 1
            self._resort_queue()
        
        return ActionResult(
            success=True,
            message=f"Task '{name}' scheduled (ID: {task_id})",
            data={
                "task_id": task_id,
                "name": name,
                "next_run": next_run,
                "schedule_type": schedule_type.value
            },
            duration=time.time() - start_time
        )
    
    def _calculate_next_run(self, schedule_type: ScheduleType, config: Dict[str, Any]) -> Optional[float]:
        """Calculate next run time for a schedule."""
        now = time.time()
        
        if schedule_type == ScheduleType.INTERVAL:
            interval_seconds = config.get("interval_seconds", 60)
            return now + interval_seconds
        
        elif schedule_type == ScheduleType.FIXED_RATE:
            interval_seconds = config.get("rate_seconds", 60)
            return now + interval_seconds
        
        elif schedule_type == ScheduleType.CRON:
            cron_expr = config.get("expression", "* * * * *")
            parts = cron_expr.split()
            if len(parts) == 5:
                cron = CronExpression(*parts)
                next_dt = cron.get_next_run()
                return next_dt.timestamp()
            return None
        
        elif schedule_type == ScheduleType.ONE_TIME:
            run_at = config.get("run_at")
            if run_at:
                if isinstance(run_at, (int, float)):
                    return run_at
                return None
            return None
        
        elif schedule_type == ScheduleType.CALENDAR:
            run_at = config.get("run_at")
            if run_at:
                if isinstance(run_at, (int, float)):
                    return run_at
            return None
        
        return None
    
    def _list_tasks(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List all scheduled tasks."""
        with self._lock:
            task_list = []
            for task_id, task in self._tasks.items():
                task_list.append({
                    "id": task.id,
                    "name": task.name,
                    "action_type": task.action_type,
                    "schedule_type": task.schedule_type.value,
                    "state": task.state.value,
                    "priority": task.priority,
                    "run_count": task.run_count,
                    "last_run": task.last_run,
                    "next_run": task.next_run,
                    "enabled": task.enabled
                })
            
            task_list.sort(key=lambda t: -t["priority"])
        
        return ActionResult(
            success=True,
            message=f"Found {len(task_list)} tasks",
            data={"tasks": task_list, "total": len(task_list)},
            duration=time.time() - start_time
        )
    
    def _cancel_task(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Cancel a scheduled task."""
        task_id = params.get("task_id")
        
        if not task_id:
            return ActionResult(
                success=False,
                message="Missing required parameter: task_id",
                duration=time.time() - start_time
            )
        
        with self._lock:
            if task_id not in self._tasks:
                return ActionResult(
                    success=False,
                    message=f"Task not found: {task_id}",
                    duration=time.time() - start_time
                )
            
            task = self._tasks[task_id]
            task.state = ScheduleState.CANCELLED
            task.enabled = False
            self._stats.active_tasks -= 1
        
        return ActionResult(
            success=True,
            message=f"Task cancelled: {task_id}",
            duration=time.time() - start_time
        )
    
    def _pause_task(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Pause a scheduled task."""
        task_id = params.get("task_id")
        
        if not task_id:
            return ActionResult(
                success=False,
                message="Missing required parameter: task_id",
                duration=time.time() - start_time
            )
        
        with self._lock:
            if task_id not in self._tasks:
                return ActionResult(
                    success=False,
                    message=f"Task not found: {task_id}",
                    duration=time.time() - start_time
                )
            
            task = self._tasks[task_id]
            if task.state == ScheduleState.RUNNING:
                return ActionResult(
                    success=False,
                    message=f"Cannot pause running task: {task_id}",
                    duration=time.time() - start_time
                )
            
            task.state = ScheduleState.PAUSED
            task.enabled = False
            self._stats.active_tasks -= 1
        
        return ActionResult(
            success=True,
            message=f"Task paused: {task_id}",
            duration=time.time() - start_time
        )
    
    def _resume_task(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Resume a paused task."""
        task_id = params.get("task_id")
        
        if not task_id:
            return ActionResult(
                success=False,
                message="Missing required parameter: task_id",
                duration=time.time() - start_time
            )
        
        with self._lock:
            if task_id not in self._tasks:
                return ActionResult(
                    success=False,
                    message=f"Task not found: {task_id}",
                    duration=time.time() - start_time
                )
            
            task = self._tasks[task_id]
            task.state = ScheduleState.PENDING
            task.enabled = True
            task.next_run = self._calculate_next_run(task.schedule_type, task.schedule_config)
            self._stats.active_tasks += 1
            self._resort_queue()
        
        return ActionResult(
            success=True,
            message=f"Task resumed: {task_id}",
            data={"next_run": task.next_run},
            duration=time.time() - start_time
        )
    
    def _trigger_task(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Manually trigger a task execution."""
        task_id = params.get("task_id")
        
        if not task_id:
            return ActionResult(
                success=False,
                message="Missing required parameter: task_id",
                duration=time.time() - start_time
            )
        
        with self._lock:
            if task_id not in self._tasks:
                return ActionResult(
                    success=False,
                    message=f"Task not found: {task_id}",
                    duration=time.time() - start_time
                )
            
            task = self._tasks[task_id]
            self._stats.total_runs += 1
        
        return ActionResult(
            success=True,
            message=f"Task triggered: {task_id}",
            data={
                "task_id": task.id,
                "action_type": task.action_type,
                "action_params": task.action_params
            },
            duration=time.time() - start_time
        )
    
    def _get_stats(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get scheduler statistics."""
        with self._lock:
            stats = {
                "total_tasks": self._stats.total_tasks,
                "active_tasks": self._stats.active_tasks,
                "total_runs": self._stats.total_runs,
                "successful_runs": self._stats.successful_runs,
                "failed_runs": self._stats.failed_runs,
                "skipped_runs": self._stats.skipped_runs,
                "average_run_duration": self._stats.average_run_duration,
                "last_update": self._stats.last_update
            }
        
        return ActionResult(
            success=True,
            message="Scheduler stats retrieved",
            data=stats,
            duration=time.time() - start_time
        )
    
    def _resort_queue(self) -> None:
        """Resort the task queue by priority and next run time."""
        tasks = [(t.id, t) for t in self._tasks.values() if t.enabled and t.next_run is not None]
        tasks.sort(key=lambda x: (-x[1].priority, x[1].next_run or float("inf")))
        self._task_queue = deque([t[0] for t in tasks])
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate scheduler parameters."""
        operation = params.get("operation", "schedule")
        if operation == "schedule":
            if "task" not in params:
                return False, "Missing required parameter: task"
        elif operation in ("cancel", "pause", "resume", "trigger"):
            if "task_id" not in params:
                return False, f"Missing required parameter: task_id for {operation}"
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return ["operation"]
