"""Scheduler action module for RabAI AutoClick.

Provides task scheduling:
- Scheduler: Task scheduler
- ScheduledTask: Task with schedule
- CronExpression: Cron expression parser
- IntervalScheduler: Interval-based scheduler
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import threading
import time
import uuid
import re

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class ScheduledTask:
    """Scheduled task."""
    task_id: str
    name: str
    func: Callable
    interval: float
    last_run: float = 0.0
    next_run: float = 0.0
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


class CronExpression:
    """Cron expression parser."""

    def __init__(self, expression: str):
        self.expression = expression
        self._parse()

    def _parse(self) -> None:
        """Parse cron expression."""
        parts = self.expression.split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression: {self.expression}")

        self.minute = parts[0]
        self.hour = parts[1]
        self.day = parts[2]
        self.month = parts[3]
        self.weekday = parts[4]

    def get_next_run(self, from_time: Optional[datetime] = None) -> datetime:
        """Get next run time."""
        if from_time is None:
            from_time = datetime.now()

        next_run = from_time + timedelta(minutes=1)

        return next_run


class Scheduler:
    """Task scheduler."""

    def __init__(self):
        self._tasks: Dict[str, ScheduledTask] = {}
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()

    def add_task(
        self,
        name: str,
        func: Callable,
        interval: float,
        task_id: Optional[str] = None,
        immediate: bool = False,
    ) -> str:
        """Add a scheduled task."""
        with self._lock:
            tid = task_id or str(uuid.uuid4())
            now = time.time()

            task = ScheduledTask(
                task_id=tid,
                name=name,
                func=func,
                interval=interval,
                last_run=now if immediate else 0.0,
                next_run=now + interval if not immediate else now,
            )

            self._tasks[tid] = task
            return tid

    def remove_task(self, task_id: str) -> bool:
        """Remove a task."""
        with self._lock:
            if task_id in self._tasks:
                del self._tasks[task_id]
                return True
            return False

    def get_task(self, task_id: str) -> Optional[ScheduledTask]:
        """Get a task."""
        with self._lock:
            return self._tasks.get(task_id)

    def list_tasks(self) -> List[Dict]:
        """List all tasks."""
        with self._lock:
            return [
                {
                    "task_id": t.task_id,
                    "name": t.name,
                    "interval": t.interval,
                    "enabled": t.enabled,
                    "last_run": t.last_run,
                    "next_run": t.next_run,
                }
                for t in self._tasks.values()
            ]

    def enable_task(self, task_id: str) -> bool:
        """Enable a task."""
        with self._lock:
            task = self._tasks.get(task_id)
            if task:
                task.enabled = True
                return True
            return False

    def disable_task(self, task_id: str) -> bool:
        """Disable a task."""
        with self._lock:
            task = self._tasks.get(task_id)
            if task:
                task.enabled = False
                return True
            return False

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
            self._thread.join(timeout=2.0)

    def _run_loop(self) -> None:
        """Run loop."""
        while self._running:
            now = time.time()

            with self._lock:
                for task in self._tasks.values():
                    if task.enabled and now >= task.next_run:
                        try:
                            task.func()
                        except Exception:
                            pass
                        task.last_run = now
                        task.next_run = now + task.interval

            time.sleep(0.1)

    def run_once(self) -> None:
        """Run scheduler once (for testing)."""
        now = time.time()

        with self._lock:
            for task in self._tasks.values():
                if task.enabled and (task.last_run == 0 or now >= task.next_run):
                    try:
                        task.func()
                    except Exception:
                        pass
                    task.last_run = now
                    task.next_run = now + task.interval


class SchedulerAction(BaseAction):
    """Scheduler action."""
    action_type = "scheduler"
    display_name = "任务调度器"
    description = "定时任务调度"

    def __init__(self):
        super().__init__()
        self._scheduler = Scheduler()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "add")

            if operation == "add":
                return self._add_task(params)
            elif operation == "remove":
                return self._remove_task(params)
            elif operation == "list":
                return self._list_tasks()
            elif operation == "enable":
                return self._enable_task(params)
            elif operation == "disable":
                return self._disable_task(params)
            elif operation == "start":
                return self._start()
            elif operation == "stop":
                return self._stop()
            elif operation == "run_once":
                return self._run_once()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Scheduler error: {str(e)}")

    def _add_task(self, params: Dict[str, Any]) -> ActionResult:
        """Add a task."""
        name = params.get("name")
        interval = params.get("interval", 60.0)
        immediate = params.get("immediate", False)

        if not name:
            return ActionResult(success=False, message="name is required")

        def dummy_func():
            pass

        task_id = self._scheduler.add_task(name, dummy_func, interval, immediate=immediate)

        return ActionResult(success=True, message=f"Task added: {task_id}", data={"task_id": task_id})

    def _remove_task(self, params: Dict[str, Any]) -> ActionResult:
        """Remove a task."""
        task_id = params.get("task_id")

        if not task_id:
            return ActionResult(success=False, message="task_id is required")

        success = self._scheduler.remove_task(task_id)

        return ActionResult(success=success, message="Task removed" if success else "Task not found")

    def _list_tasks(self) -> ActionResult:
        """List all tasks."""
        tasks = self._scheduler.list_tasks()

        return ActionResult(success=True, message=f"{len(tasks)} tasks", data={"tasks": tasks})

    def _enable_task(self, params: Dict[str, Any]) -> ActionResult:
        """Enable a task."""
        task_id = params.get("task_id")

        if not task_id:
            return ActionResult(success=False, message="task_id is required")

        success = self._scheduler.enable_task(task_id)

        return ActionResult(success=success, message="Task enabled" if success else "Task not found")

    def _disable_task(self, params: Dict[str, Any]) -> ActionResult:
        """Disable a task."""
        task_id = params.get("task_id")

        if not task_id:
            return ActionResult(success=False, message="task_id is required")

        success = self._scheduler.disable_task(task_id)

        return ActionResult(success=success, message="Task disabled" if success else "Task not found")

    def _start(self) -> ActionResult:
        """Start the scheduler."""
        self._scheduler.start()
        return ActionResult(success=True, message="Scheduler started")

    def _stop(self) -> ActionResult:
        """Stop the scheduler."""
        self._scheduler.stop()
        return ActionResult(success=True, message="Scheduler stopped")

    def _run_once(self) -> ActionResult:
        """Run scheduler once."""
        self._scheduler.run_once()
        return ActionResult(success=True, message="Scheduler ran once")
