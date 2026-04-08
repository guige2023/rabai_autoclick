"""Async Task Action Module.

Provides async task execution with
futures and callbacks.
"""

import time
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TaskStatus(Enum):
    """Task status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class AsyncTask:
    """Async task."""
    task_id: str
    func: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: Dict = field(default_factory=dict)
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None


class AsyncTaskManager:
    """Manages async tasks."""

    def __init__(self):
        self._tasks: Dict[str, AsyncTask] = {}
        self._lock = threading.Lock()
        self._executor = threading.Thread(target=self._process_queue, daemon=True)
        self._task_queue: List[str] = []
        self._queue_lock = threading.Lock()
        self._running = True
        self._executor.start()

    def submit(self, func: Callable, *args, **kwargs) -> str:
        """Submit a task."""
        task_id = f"task_{int(time.time() * 1000)}"

        task = AsyncTask(
            task_id=task_id,
            func=func,
            args=args,
            kwargs=kwargs
        )

        with self._lock:
            self._tasks[task_id] = task

        with self._queue_lock:
            self._task_queue.append(task_id)

        return task_id

    def _process_queue(self) -> None:
        """Process task queue."""
        while self._running:
            task_id = None

            with self._queue_lock:
                if self._task_queue:
                    task_id = self._task_queue.pop(0)

            if task_id:
                self._run_task(task_id)

            time.sleep(0.01)

    def _run_task(self, task_id: str) -> None:
        """Run a task."""
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return

            task.status = TaskStatus.RUNNING
            task.started_at = time.time()

        try:
            result = task.func(*task.args, **task.kwargs)

            with self._lock:
                task.result = result
                task.status = TaskStatus.COMPLETED
                task.completed_at = time.time()

        except Exception as e:
            with self._lock:
                task.error = str(e)
                task.status = TaskStatus.FAILED
                task.completed_at = time.time()

    def get_task(self, task_id: str) -> Optional[AsyncTask]:
        """Get task."""
        with self._lock:
            return self._tasks.get(task_id)

    def cancel(self, task_id: str) -> bool:
        """Cancel a task."""
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return False

            if task.status == TaskStatus.PENDING:
                task.status = TaskStatus.CANCELLED
                return True

            return False


class AsyncTaskAction(BaseAction):
    """Action for async task operations."""

    def __init__(self):
        super().__init__("async_task")
        self._manager = AsyncTaskManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute async task action."""
        try:
            operation = params.get("operation", "submit")

            if operation == "submit":
                return self._submit(params)
            elif operation == "get":
                return self._get(params)
            elif operation == "cancel":
                return self._cancel(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _submit(self, params: Dict) -> ActionResult:
        """Submit task."""
        def default_func():
            return {}

        task_id = self._manager.submit(
            params.get("func") or default_func
        )
        return ActionResult(success=True, data={"task_id": task_id})

    def _get(self, params: Dict) -> ActionResult:
        """Get task status."""
        task = self._manager.get_task(params.get("task_id", ""))
        if not task:
            return ActionResult(success=False, message="Task not found")

        return ActionResult(success=True, data={
            "task_id": task.task_id,
            "status": task.status.value,
            "result": task.result,
            "error": task.error
        })

    def _cancel(self, params: Dict) -> ActionResult:
        """Cancel task."""
        success = self._manager.cancel(params.get("task_id", ""))
        return ActionResult(success=success)
