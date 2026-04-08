"""
Task Scheduler Action Module.

Schedules and manages recurring tasks with dependencies, priorities,
conflicts, and execution tracking.

Author: RabAi Team
"""

from __future__ import annotations

import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set


class ScheduleType(Enum):
    """Types of task schedules."""
    ONCE = "once"
    RECURRING = "recurring"
    CRON = "cron"
    DEPENDENT = "dependent"


class TaskPriority(Enum):
    """Task priority levels."""
    CRITICAL = 1
    HIGH = 2
    NORMAL = 3
    LOW = 4


@dataclass
class ScheduledTask:
    """A schedulable task."""
    id: str
    name: str
    fn: Callable
    schedule_type: ScheduleType
    priority: TaskPriority = TaskPriority.NORMAL
    depends_on: List[str] = field(default_factory=list)
    next_run: Optional[datetime] = None
    interval_seconds: Optional[float] = None
    cron_expression: Optional[str] = None
    enabled: bool = True
    timeout_seconds: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    max_retries: int = 3
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "schedule_type": self.schedule_type.value,
            "priority": self.priority.value,
            "depends_on": self.depends_on,
            "next_run": self.next_run.isoformat() if self.next_run else None,
            "interval_seconds": self.interval_seconds,
            "enabled": self.enabled,
        }


@dataclass
class TaskExecution:
    """Record of task execution."""
    task_id: str
    execution_id: str
    status: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    result: Any = None
    error: Optional[str] = None
    retry_count: int = 0


class TaskScheduler:
    """
    Task scheduling engine with dependency management.

    Supports one-time, recurring, cron-based, and dependent task scheduling
    with priority queuing and execution tracking.

    Example:
        >>> scheduler = TaskScheduler()
        >>> scheduler.schedule("daily_report", report_fn, interval_seconds=86400)
        >>> scheduler.run_pending()
    """

    def __init__(self):
        self._tasks: Dict[str, ScheduledTask] = {}
        self._executions: Dict[str, TaskExecution] = {}
        self._pending_queue: List[str] = []
        self._running: Set[str] = set()
        self._completed: Dict[str, List[str]] = defaultdict(list)
        self._failed: Dict[str, List[str]] = defaultdict(list)

    def schedule(
        self,
        name: str,
        fn: Callable,
        schedule_type: ScheduleType = ScheduleType.ONCE,
        priority: TaskPriority = TaskPriority.NORMAL,
        interval_seconds: Optional[float] = None,
        cron_expression: Optional[str] = None,
        depends_on: Optional[List[str]] = None,
        **kwargs,
    ) -> str:
        """Schedule a task."""
        task_id = str(uuid.uuid4())
        next_run = datetime.now() if schedule_type != ScheduleType.DEPENDENT else None

        task = ScheduledTask(
            id=task_id,
            name=name,
            fn=fn,
            schedule_type=schedule_type,
            priority=priority,
            depends_on=depends_on or [],
            next_run=next_run,
            interval_seconds=interval_seconds,
            cron_expression=cron_expression,
            **kwargs,
        )

        self._tasks[task_id] = task
        self._pending_queue.append(task_id)
        self._sort_queue()
        return task_id

    def cancel(self, task_id: str) -> bool:
        """Cancel a scheduled task."""
        if task_id in self._tasks:
            self._tasks[task_id].enabled = False
            if task_id in self._pending_queue:
                self._pending_queue.remove(task_id)
            return True
        return False

    def get_pending(self) -> List[ScheduledTask]:
        """Get pending tasks sorted by priority."""
        return [self._tasks[tid] for tid in self._pending_queue if tid in self._tasks]

    def run_pending(self) -> List[TaskExecution]:
        """Run all pending tasks whose dependencies are met."""
        executions = []
        to_remove = []

        for task_id in self._pending_queue:
            task = self._tasks[task_id]
            if not task.enabled:
                continue
            if not self._can_run(task_id):
                continue

            execution = self._execute_task(task)
            executions.append(execution)
            to_remove.append(task_id)

            if task.schedule_type == ScheduleType.ONCE:
                self._pending_queue.remove(task_id)
            elif task.schedule_type == ScheduleType.RECURRING and task.interval_seconds:
                task.next_run = datetime.now() + timedelta(seconds=task.interval_seconds)

        return executions

    def _can_run(self, task_id: str) -> bool:
        """Check if task dependencies are satisfied."""
        task = self._tasks[task_id]
        for dep_id in task.depends_on:
            if dep_id in self._failed and not self._completed.get(dep_id):
                return False
        return True

    def _execute_task(self, task: ScheduledTask) -> TaskExecution:
        """Execute a task."""
        execution_id = str(uuid.uuid4())
        execution = TaskExecution(
            task_id=task.id,
            execution_id=execution_id,
            status="running",
            started_at=datetime.now(),
        )

        self._running.add(task.id)
        self._executions[execution_id] = execution

        try:
            result = task.fn()
            execution.status = "completed"
            execution.result = result
            self._completed[task.id].append(execution_id)
        except Exception as e:
            execution.status = "failed"
            execution.error = str(e)
            execution.retry_count = task.max_retries
            self._failed[task.id].append(execution_id)

        execution.completed_at = datetime.now()
        self._running.discard(task.id)
        return execution

    def _sort_queue(self) -> None:
        """Sort pending queue by priority."""
        self._pending_queue.sort(
            key=lambda tid: (
                self._tasks[tid].priority.value,
                self._tasks[tid].next_run or datetime.max,
            )
        )


def create_scheduler() -> TaskScheduler:
    """Factory to create a task scheduler."""
    return TaskScheduler()
