"""Scheduler action module for RabAI AutoClick.

Provides scheduling operations:
- ScheduleTaskAction: Schedule a task for later execution
- ScheduleCronAction: Schedule tasks with cron expressions
- SchedulePeriodicAction: Schedule periodic tasks
- ScheduleCancelAction: Cancel scheduled tasks
"""

import time
import threading
import uuid
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import re


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class ScheduledTask:
    """Represents a scheduled task."""
    task_id: str
    name: str
    scheduled_time: Optional[datetime] = None
    interval_seconds: Optional[float] = None
    cron_expression: Optional[str] = None
    callback: Optional[Callable] = None
    params: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    run_count: int = 0


class Scheduler:
    """In-memory task scheduler."""
    def __init__(self):
        self._tasks: Dict[str, ScheduledTask] = {}
        self._lock = threading.RLock()
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def _run_loop(self):
        while self._running:
            with self._lock:
                now = datetime.utcnow()
                for task in list(self._tasks.values()):
                    if not task.enabled:
                        continue
                    if task.interval_seconds and task.next_run and now >= task.next_run:
                        try:
                            if task.callback:
                                task.callback(task.params)
                            task.last_run = now
                            task.next_run = now + timedelta(seconds=task.interval_seconds)
                            task.run_count += 1
                        except Exception:
                            pass
            time.sleep(1)

    def add_task(self, task: ScheduledTask) -> str:
        with self._lock:
            self._tasks[task.task_id] = task
            if task.interval_seconds:
                task.next_run = datetime.utcnow() + timedelta(seconds=task.interval_seconds)
        return task.task_id

    def remove_task(self, task_id: str) -> bool:
        with self._lock:
            if task_id in self._tasks:
                del self._tasks[task_id]
                return True
        return False

    def get_task(self, task_id: str) -> Optional[ScheduledTask]:
        with self._lock:
            return self._tasks.get(task_id)

    def list_tasks(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [
                {
                    "task_id": t.task_id,
                    "name": t.name,
                    "enabled": t.enabled,
                    "last_run": t.last_run.isoformat() if t.last_run else None,
                    "next_run": t.next_run.isoformat() if t.next_run else None,
                    "run_count": t.run_count
                }
                for t in self._tasks.values()
            ]

    def stop(self):
        self._running = False


_scheduler = Scheduler()


class ScheduleTaskAction(BaseAction):
    """Schedule a task for a specific time."""
    action_type = "schedule_task"
    display_name = "调度任务"
    description = "调度任务在指定时间执行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            delay_seconds = params.get("delay_seconds", None)
            scheduled_time = params.get("scheduled_time", None)
            callback_ref = params.get("callback_ref", None)
            task_params = params.get("params", {})

            if not name:
                return ActionResult(success=False, message="name is required")

            if delay_seconds is None and scheduled_time is None:
                return ActionResult(success=False, message="delay_seconds or scheduled_time is required")

            task_id = str(uuid.uuid4())

            if scheduled_time:
                from dateutil import parser
                parsed_time = parser.parse(scheduled_time)
                delay = (parsed_time - datetime.utcnow()).total_seconds()
                if delay < 0:
                    return ActionResult(success=False, message="scheduled_time is in the past")
            else:
                delay = delay_seconds

            task = ScheduledTask(
                task_id=task_id,
                name=name,
                scheduled_time=datetime.utcnow() + timedelta(seconds=delay),
                callback=callback_ref,
                params=task_params
            )

            _scheduler.add_task(task)

            return ActionResult(
                success=True,
                message=f"Task '{name}' scheduled to run in {delay} seconds",
                data={"task_id": task_id, "delay_seconds": delay, "name": name}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Schedule task failed: {str(e)}")


class ScheduleCronAction(BaseAction):
    """Schedule a task using a cron expression."""
    action_type = "schedule_cron"
    display_name = "Cron调度"
    description = "使用Cron表达式调度任务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            cron_expression = params.get("cron_expression", "")
            callback_ref = params.get("callback_ref", None)
            task_params = params.get("params", {})

            if not name:
                return ActionResult(success=False, message="name is required")
            if not cron_expression:
                return ActionResult(success=False, message="cron_expression is required")

            parts = cron_expression.split()
            if len(parts) < 5:
                return ActionResult(success=False, message="Invalid cron expression (need at least 5 fields)")

            task_id = str(uuid.uuid4())
            task = ScheduledTask(
                task_id=task_id,
                name=name,
                cron_expression=cron_expression,
                callback=callback_ref,
                params=task_params,
                enabled=True
            )

            _scheduler.add_task(task)

            return ActionResult(
                success=True,
                message=f"Cron task '{name}' scheduled: {cron_expression}",
                data={"task_id": task_id, "cron_expression": cron_expression, "name": name}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Schedule cron failed: {str(e)}")


class SchedulePeriodicAction(BaseAction):
    """Schedule a task to run periodically."""
    action_type = "schedule_periodic"
    display_name = "周期调度"
    description = "调度周期性任务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            interval_seconds = params.get("interval_seconds", 60)
            callback_ref = params.get("callback_ref", None)
            task_params = params.get("params", {})
            start_immediately = params.get("start_immediately", False)

            if not name:
                return ActionResult(success=False, message="name is required")
            if interval_seconds <= 0:
                return ActionResult(success=False, message="interval_seconds must be positive")

            task_id = str(uuid.uuid4())
            task = ScheduledTask(
                task_id=task_id,
                name=name,
                interval_seconds=interval_seconds,
                callback=callback_ref,
                params=task_params,
                enabled=True
            )

            if start_immediately:
                task.next_run = datetime.utcnow()
            else:
                task.next_run = datetime.utcnow() + timedelta(seconds=interval_seconds)

            _scheduler.add_task(task)

            return ActionResult(
                success=True,
                message=f"Periodic task '{name}' scheduled every {interval_seconds}s",
                data={
                    "task_id": task_id,
                    "interval_seconds": interval_seconds,
                    "name": name,
                    "start_immediately": start_immediately
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Schedule periodic failed: {str(e)}")


class ScheduleCancelAction(BaseAction):
    """Cancel a scheduled task."""
    action_type = "schedule_cancel"
    display_name = "取消调度"
    description = "取消已调度的任务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            task_id = params.get("task_id", None)
            name = params.get("name", None)

            if not task_id and not name:
                return ActionResult(success=False, message="task_id or name is required")

            if task_id:
                removed = _scheduler.remove_task(task_id)
                if removed:
                    return ActionResult(success=True, message=f"Task {task_id} cancelled")
                return ActionResult(success=False, message=f"Task {task_id} not found")

            tasks = _scheduler.list_tasks()
            if name:
                matching = [t for t in tasks if name in t.get("name", "")]
                if not matching:
                    return ActionResult(success=False, message=f"No tasks found matching '{name}'")
                for t in matching:
                    _scheduler.remove_task(t["task_id"])
                return ActionResult(
                    success=True,
                    message=f"Cancelled {len(matching)} tasks matching '{name}'"
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Schedule cancel failed: {str(e)}")
