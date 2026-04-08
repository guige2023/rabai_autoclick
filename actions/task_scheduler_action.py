"""Task scheduler action module for RabAI AutoClick.

Provides task scheduling:
- TaskScheduler: Schedule and execute tasks
- CronScheduler: Cron-based scheduling
- IntervalScheduler: Interval-based scheduling
- OneTimeScheduler: One-time delayed tasks
- TaskGroup: Group related tasks
- TaskChain: Chain of dependent tasks
"""

import time
import threading
import schedule
from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ScheduledTask:
    """Represents a scheduled task."""
    id: str
    name: str
    func: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: Dict = field(default_factory=dict)
    status: TaskStatus = TaskStatus.PENDING
    scheduled_time: Optional[float] = None
    interval: Optional[float] = None
    cron_expr: Optional[str] = None
    max_runs: Optional[int] = None
    run_count: int = 0
    last_run: Optional[float] = None
    next_run: Optional[float] = None
    created_at: float = field(default_factory=time.time)
    tags: Set[str] = field(default_factory=set)


@dataclass
class TaskResult:
    """Result of task execution."""
    task_id: str
    status: TaskStatus
    started_at: float
    completed_at: Optional[float] = None
    result: Any = None
    error: Optional[str] = None
    duration: Optional[float] = None


class TaskScheduler:
    """Main task scheduler."""

    def __init__(self):
        self._tasks: Dict[str, ScheduledTask] = {}
        self._results: Dict[str, TaskResult] = {}
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()

    def add_task(
        self,
        name: str,
        func: Callable,
        args: tuple = None,
        kwargs: Dict = None,
        tags: Optional[Set[str]] = None,
    ) -> str:
        """Add a task to scheduler."""
        task_id = str(uuid.uuid4())[:8]
        task = ScheduledTask(
            id=task_id,
            name=name,
            func=func,
            args=args or (),
            kwargs=kwargs or {},
            tags=tags or set(),
        )
        with self._lock:
            self._tasks[task_id] = task
        return task_id

    def schedule_once(
        self,
        task_id: str,
        delay: float,
    ) -> bool:
        """Schedule task for one-time execution."""
        with self._lock:
            if task_id not in self._tasks:
                return False
            task = self._tasks[task_id]
            task.scheduled_time = time.time() + delay
            task.next_run = task.scheduled_time
        return True

    def schedule_interval(
        self,
        task_id: str,
        interval: float,
    ) -> bool:
        """Schedule task for interval execution."""
        with self._lock:
            if task_id not in self._tasks:
                return False
            task = self._tasks[task_id]
            task.interval = interval
            task.next_run = time.time() + interval
        return True

    def schedule_cron(
        self,
        task_id: str,
        cron_expr: str,
    ) -> bool:
        """Schedule task with cron expression."""
        with self._lock:
            if task_id not in self._tasks:
                return False
            task = self._tasks[task_id]
            task.cron_expr = cron_expr
        return True

    def cancel_task(self, task_id: str) -> bool:
        """Cancel a scheduled task."""
        with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id].status = TaskStatus.CANCELLED
                return True
            return False

    def remove_task(self, task_id: str) -> bool:
        """Remove task from scheduler."""
        with self._lock:
            if task_id in self._tasks:
                del self._tasks[task_id]
                return True
            return False

    def get_task(self, task_id: str) -> Optional[ScheduledTask]:
        """Get task by ID."""
        with self._lock:
            return self._tasks.get(task_id)

    def get_tasks(self, tags: Optional[Set[str]] = None) -> List[ScheduledTask]:
        """Get tasks, optionally filtered by tags."""
        with self._lock:
            if tags:
                return [t for t in self._tasks.values() if t.tags & tags]
            return list(self._tasks.values())

    def get_next_run_time(self, task_id: str) -> Optional[float]:
        """Get next run time for task."""
        with self._lock:
            task = self._tasks.get(task_id)
            return task.next_run if task else None

    def start(self):
        """Start the scheduler."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the scheduler."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)

    def _run_loop(self):
        """Main scheduler loop."""
        while self._running:
            now = time.time()

            with self._lock:
                tasks_to_run = [
                    task for task in self._tasks.values()
                    if task.status != TaskStatus.CANCELLED
                    and task.next_run is not None
                    and task.next_run <= now
                ]

            for task in tasks_to_run:
                self._execute_task(task)

            time.sleep(0.1)

    def _execute_task(self, task: ScheduledTask):
        """Execute a single task."""
        task.status = TaskStatus.RUNNING
        task.last_run = time.time()
        task.run_count += 1

        result = TaskResult(
            task_id=task.id,
            status=TaskStatus.RUNNING,
            started_at=time.time(),
        )

        try:
            task_result = task.func(*task.args, **task.kwargs)
            result.status = TaskStatus.COMPLETED
            result.result = task_result
        except Exception as e:
            result.status = TaskStatus.FAILED
            result.error = str(e)

        result.completed_at = time.time()
        result.duration = result.completed_at - result.started_at
        task.status = TaskStatus.COMPLETED if result.status == TaskStatus.COMPLETED else TaskStatus.FAILED

        with self._lock:
            self._results[task.id] = result

            if task.interval:
                task.next_run = time.time() + task.interval
                if task.max_runs and task.run_count >= task.max_runs:
                    task.status = TaskStatus.CANCELLED
            elif task.scheduled_time:
                task.status = TaskStatus.CANCELLED
            elif task.cron_expr:
                pass

    def get_result(self, task_id: str) -> Optional[TaskResult]:
        """Get task result."""
        with self._lock:
            return self._results.get(task_id)


class TaskChain:
    """Chain of dependent tasks."""

    def __init__(self, scheduler: TaskScheduler):
        self.scheduler = scheduler
        self._chain: List[str] = []

    def add(
        self,
        name: str,
        func: Callable,
        args: tuple = None,
        kwargs: Dict = None,
    ) -> "TaskChain":
        """Add task to chain."""
        task_id = self.scheduler.add_task(name, func, args, kwargs)
        self._chain.append(task_id)
        return self

    def execute(self) -> List[TaskResult]:
        """Execute chain sequentially."""
        results = []

        for task_id in self._chain:
            task = self.scheduler.get_task(task_id)
            if not task:
                continue

            self.scheduler._execute_task(task)
            result = self.scheduler.get_result(task_id)
            if result:
                results.append(result)
                if result.status == TaskStatus.FAILED:
                    break

        return results


class TaskGroup:
    """Group of parallel tasks."""

    def __init__(self, scheduler: TaskScheduler):
        self.scheduler = scheduler
        self._tasks: List[str] = []

    def add(
        self,
        name: str,
        func: Callable,
        args: tuple = None,
        kwargs: Dict = None,
    ) -> "TaskGroup":
        """Add task to group."""
        task_id = self.scheduler.add_task(name, func, args, kwargs)
        self._tasks.append(task_id)
        return self

    def execute_all(self) -> List[TaskResult]:
        """Execute all tasks in group."""
        threads = []
        results = []

        def run_task(task_id: str):
            task = self.scheduler.get_task(task_id)
            if task:
                self.scheduler._execute_task(task)
                results.append(self.scheduler.get_result(task_id))

        for task_id in self._tasks:
            t = threading.Thread(target=run_task, args=(task_id,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        return results


class TaskSchedulerAction(BaseAction):
    """Task scheduler action for automation."""
    action_type = "task_scheduler"
    display_name = "任务调度"
    description = "定时任务调度管理"

    def __init__(self):
        super().__init__()
        self._scheduler = TaskScheduler()
        self._scheduler.start()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "schedule")

            if operation == "add":
                return self._add_task(params)
            elif operation == "schedule_once":
                return self._schedule_once(params)
            elif operation == "schedule_interval":
                return self._schedule_interval(params)
            elif operation == "cancel":
                return self._cancel_task(params)
            elif operation == "list":
                return self._list_tasks(params)
            elif operation == "status":
                return self._get_status(params)
            elif operation == "result":
                return self._get_result(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Scheduler error: {str(e)}")

    def _add_task(self, params: Dict) -> ActionResult:
        """Add a new task."""
        name = params.get("name", "unnamed")
        func = params.get("func")
        args = params.get("args", [])
        kwargs = params.get("kwargs", {})
        tags = set(params.get("tags", []))

        if not func:
            return ActionResult(success=False, message="func is required")

        task_id = self._scheduler.add_task(name, func, tuple(args), kwargs, tags)
        return ActionResult(success=True, message=f"Task '{name}' added", data={"task_id": task_id})

    def _schedule_once(self, params: Dict) -> ActionResult:
        """Schedule task for one-time execution."""
        task_id = params.get("task_id")
        delay = params.get("delay", 1.0)

        if not task_id:
            return ActionResult(success=False, message="task_id is required")

        success = self._scheduler.schedule_once(task_id, delay)
        return ActionResult(success=success, message=f"Scheduled for {delay}s" if success else "Failed")

    def _schedule_interval(self, params: Dict) -> ActionResult:
        """Schedule task for interval execution."""
        task_id = params.get("task_id")
        interval = params.get("interval", 60.0)

        if not task_id:
            return ActionResult(success=False, message="task_id is required")

        success = self._scheduler.schedule_interval(task_id, interval)
        return ActionResult(success=success, message=f"Interval set to {interval}s" if success else "Failed")

    def _cancel_task(self, params: Dict) -> ActionResult:
        """Cancel a task."""
        task_id = params.get("task_id")
        if not task_id:
            return ActionResult(success=False, message="task_id is required")

        success = self._scheduler.cancel_task(task_id)
        return ActionResult(success=success, message="Task cancelled" if success else "Task not found")

    def _list_tasks(self, params: Dict) -> ActionResult:
        """List all tasks."""
        tags = set(params.get("tags", [])) or None
        tasks = self._scheduler.get_tasks(tags)

        task_list = [
            {
                "id": t.id,
                "name": t.name,
                "status": t.status.value,
                "next_run": t.next_run,
                "run_count": t.run_count,
                "tags": list(t.tags),
            }
            for t in tasks
        ]

        return ActionResult(success=True, message=f"{len(task_list)} tasks", data={"tasks": task_list})

    def _get_status(self, params: Dict) -> ActionResult:
        """Get task status."""
        task_id = params.get("task_id")
        if not task_id:
            return ActionResult(success=False, message="task_id is required")

        task = self._scheduler.get_task(task_id)
        if not task:
            return ActionResult(success=False, message="Task not found")

        return ActionResult(
            success=True,
            message=task.status.value,
            data={
                "id": task.id,
                "name": task.name,
                "status": task.status.value,
                "next_run": task.next_run,
                "last_run": task.last_run,
                "run_count": task.run_count,
            },
        )

    def _get_result(self, params: Dict) -> ActionResult:
        """Get task result."""
        task_id = params.get("task_id")
        if not task_id:
            return ActionResult(success=False, message="task_id is required")

        result = self._scheduler.get_result(task_id)
        if not result:
            return ActionResult(success=False, message="No result yet")

        return ActionResult(
            success=True,
            message=result.status.value,
            data={
                "task_id": result.task_id,
                "status": result.status.value,
                "duration": result.duration,
                "result": result.result,
                "error": result.error,
            },
        )
