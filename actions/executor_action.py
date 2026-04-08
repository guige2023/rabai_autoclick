"""Executor Action Module.

Provides task execution framework with priorities,
timeouts, retries, and execution tracking.
"""

import time
import hashlib
import asyncio
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import deque
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ExecutionStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"


class ExecutionPriority(Enum):
    """Task execution priority."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class ExecutableTask:
    """A task ready for execution."""
    task_id: str
    name: str
    handler: Callable
    params: Dict[str, Any]
    priority: ExecutionPriority = ExecutionPriority.NORMAL
    timeout_seconds: Optional[float] = None
    retry_count: int = 0
    retry_delay_seconds: float = 1.0
    created_at: float = field(default_factory=time.time)
    scheduled_at: Optional[float] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    result: Any = None
    error: Optional[str] = None
    status: ExecutionStatus = ExecutionStatus.PENDING
    attempts: int = 0
    dependencies: List[str] = field(default_factory=list)


@dataclass
class ExecutionResult:
    """Result of task execution."""
    task_id: str
    status: ExecutionStatus
    result: Any
    error: Optional[str]
    duration_ms: float
    attempts: int
    timestamp: float = field(default_factory=time.time)


@dataclass
class ExecutorMetrics:
    """Executor metrics."""
    total_submitted: int = 0
    total_completed: int = 0
    total_failed: int = 0
    total_cancelled: int = 0
    total_timeout: int = 0
    running_count: int = 0
    pending_count: int = 0
    average_duration_ms: float = 0.0


class TaskExecutor:
    """Executes tasks with priorities and timeouts."""

    def __init__(self, executor_id: str, name: str):
        self.executor_id = executor_id
        self.name = name
        self._tasks: Dict[str, ExecutableTask] = {}
        self._pending_queue: deque = deque()
        self._running_tasks: Dict[str, ExecutableTask] = {}
        self._completed_tasks: Dict[str, ExecutableTask] = {}
        self._lock = threading.RLock()
        self._metrics = ExecutorMetrics()
        self._execution_history: List[ExecutionResult] = []
        self._task_counter = 0

    def submit(
        self,
        name: str,
        handler: Callable,
        params: Optional[Dict[str, Any]] = None,
        priority: ExecutionPriority = ExecutionPriority.NORMAL,
        timeout_seconds: Optional[float] = None,
        retry_count: int = 0,
        retry_delay_seconds: float = 1.0,
        scheduled_at: Optional[float] = None,
        dependencies: Optional[List[str]] = None
    ) -> str:
        """Submit a task for execution."""
        with self._lock:
            self._task_counter += 1
            task_id = f"{self.executor_id}-{self._task_counter}"

            task = ExecutableTask(
                task_id=task_id,
                name=name,
                handler=handler,
                params=params or {},
                priority=priority,
                timeout_seconds=timeout_seconds,
                retry_count=retry_count,
                retry_delay_seconds=retry_delay_seconds,
                scheduled_at=scheduled_at,
                dependencies=dependencies or []
            )

            self._tasks[task_id] = task
            self._insert_pending(task)
            self._metrics.total_submitted += 1
            self._metrics.pending_count += 1

            return task_id

    def _insert_pending(self, task: ExecutableTask) -> None:
        """Insert task into pending queue by priority."""
        if task.scheduled_at and task.scheduled_at > time.time():
            return

        tasks_list = list(self._pending_queue)
        insert_pos = len(tasks_list)

        for i, t in enumerate(tasks_list):
            if task.priority.value > t.priority.value:
                insert_pos = i
                break
            elif task.priority.value == t.priority.value:
                if task.created_at < t.created_at:
                    insert_pos = i
                    break

        tasks_list.insert(insert_pos, task)
        self._pending_queue = deque(tasks_list)

    def execute_next(self) -> Optional[ExecutionResult]:
        """Execute the next pending task."""
        with self._lock:
            if not self._pending_queue:
                return None

            task = self._pending_queue.popleft()
            self._metrics.pending_count -= 1

            if task.task_id in self._running_tasks:
                return None

            task.status = ExecutionStatus.RUNNING
            task.started_at = time.time()
            task.attempts += 1

            self._running_tasks[task.task_id] = task
            self._metrics.running_count += 1

        try:
            result = self._execute_task(task)

            with self._lock:
                task.status = ExecutionStatus.COMPLETED
                task.completed_at = time.time()
                task.result = result

                del self._running_tasks[task.task_id]
                self._completed_tasks[task.task_id] = task
                self._metrics.running_count -= 1
                self._metrics.total_completed += 1

            return self._create_result(task)

        except TimeoutError:
            return self._handle_task_timeout(task)

        except Exception as e:
            return self._handle_task_error(task, str(e))

    def _execute_task(self, task: ExecutableTask) -> Any:
        """Execute a single task."""
        timeout = task.timeout_seconds

        if asyncio.iscoroutinefunction(task.handler):
            if timeout:
                return asyncio.run(
                    asyncio.wait_for(
                        task.handler(task.params),
                        timeout=timeout
                    )
                )
            return asyncio.run(task.handler(task.params))

        if timeout:
            result = [None]
            error = [None]

            def target():
                try:
                    result[0] = task.handler(task.params)
                except Exception as e:
                    error[0] = e

            thread = threading.Thread(target=target)
            thread.start()
            thread.join(timeout=timeout)

            if thread.is_alive():
                raise TimeoutError(f"Task timed out after {timeout}s")

            if error[0]:
                raise error[0]

            return result[0]

        return task.handler(task.params)

    def _handle_task_timeout(self, task: ExecutableTask) -> ExecutionResult:
        """Handle task timeout."""
        with self._lock:
            task.status = ExecutionStatus.TIMED_OUT
            task.completed_at = time.time()
            task.error = f"Task timed out after {task.timeout_seconds}s"

            if task.task_id in self._running_tasks:
                del self._running_tasks[task.task_id]

            self._completed_tasks[task.task_id] = task
            self._metrics.running_count -= 1
            self._metrics.total_timeout += 1

        return self._create_result(task)

    def _handle_task_error(
        self,
        task: ExecutableTask,
        error_message: str
    ) -> ExecutionResult:
        """Handle task execution error."""
        with self._lock:
            if task.attempts <= task.retry_count:
                task.status = ExecutionStatus.PENDING
                task.started_at = None

                if task.task_id in self._running_tasks:
                    del self._running_tasks[task.task_id]

                self._metrics.running_count -= 1

                time.sleep(task.retry_delay_seconds)
                self._insert_pending(task)
                self._metrics.pending_count += 1

                return None

            task.status = ExecutionStatus.FAILED
            task.completed_at = time.time()
            task.error = error_message

            if task.task_id in self._running_tasks:
                del self._running_tasks[task.task_id]

            self._completed_tasks[task.task_id] = task
            self._metrics.running_count -= 1
            self._metrics.total_failed += 1

        return self._create_result(task)

    def _create_result(self, task: ExecutableTask) -> ExecutionResult:
        """Create execution result from task."""
        duration_ms = 0.0
        if task.started_at and task.completed_at:
            duration_ms = (task.completed_at - task.started_at) * 1000

        result = ExecutionResult(
            task_id=task.task_id,
            status=task.status,
            result=task.result,
            error=task.error,
            duration_ms=duration_ms,
            attempts=task.attempts
        )

        self._execution_history.append(result)

        if len(self._execution_history) > 1000:
            self._execution_history = self._execution_history[-500:]

        return result

    def cancel(self, task_id: str) -> bool:
        """Cancel a pending or running task."""
        with self._lock:
            if task_id in self._running_tasks:
                task = self._running_tasks[task_id]
                task.status = ExecutionStatus.CANCELLED
                task.completed_at = time.time()

                del self._running_tasks[task_id]
                self._completed_tasks[task_id] = task

                self._metrics.running_count -= 1
                self._metrics.total_cancelled += 1

                return True

            for i, task in enumerate(self._pending_queue):
                if task.task_id == task_id:
                    task.status = ExecutionStatus.CANCELLED
                    task.completed_at = time.time()

                    self._pending_queue = deque(
                        t for t in self._pending_queue
                        if t.task_id != task_id
                    )
                    self._completed_tasks[task_id] = task

                    self._metrics.pending_count -= 1
                    self._metrics.total_cancelled += 1

                    return True

        return False

    def get_task(self, task_id: str) -> Optional[ExecutableTask]:
        """Get task by ID."""
        return self._tasks.get(task_id)

    def get_pending_tasks(self) -> List[Dict[str, Any]]:
        """Get pending tasks."""
        return [
            {
                "task_id": t.task_id,
                "name": t.name,
                "priority": t.priority.name,
                "created_at": t.created_at
            }
            for t in self._pending_queue
        ]

    def get_running_tasks(self) -> List[Dict[str, Any]]:
        """Get running tasks."""
        return [
            {
                "task_id": t.task_id,
                "name": t.name,
                "started_at": t.started_at,
                "attempts": t.attempts
            }
            for t in self._running_tasks.values()
        ]

    def get_completed_tasks(
        self,
        limit: int = 100,
        status: Optional[ExecutionStatus] = None
    ) -> List[Dict[str, Any]]:
        """Get completed tasks."""
        tasks = list(self._completed_tasks.values())

        if status:
            tasks = [t for t in tasks if t.status == status]

        tasks = tasks[-limit:]

        return [
            {
                "task_id": t.task_id,
                "name": t.name,
                "status": t.status.value,
                "result": str(t.result)[:100] if t.result else None,
                "error": t.error,
                "completed_at": t.completed_at,
                "duration_ms": (
                    (t.completed_at - t.started_at) * 1000
                    if t.started_at and t.completed_at else 0
                )
            }
            for t in tasks
        ]

    def get_metrics(self) -> ExecutorMetrics:
        """Get executor metrics."""
        return self._metrics

    def get_execution_history(
        self,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get execution history."""
        history = self._execution_history[-limit:]

        return [
            {
                "task_id": r.task_id,
                "status": r.status.value,
                "duration_ms": r.duration_ms,
                "attempts": r.attempts,
                "timestamp": r.timestamp
            }
            for r in history
        ]


class ExecutorAction(BaseAction):
    """Action for task execution operations."""

    def __init__(self):
        super().__init__("executor")
        self._executors: Dict[str, TaskExecutor] = {}

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute executor action."""
        try:
            operation = params.get("operation", "submit")

            if operation == "create":
                return self._create_executor(params)
            elif operation == "submit":
                return self._submit_task(params)
            elif operation == "execute":
                return self._execute_next(params)
            elif operation == "cancel":
                return self._cancel_task(params)
            elif operation == "get_task":
                return self._get_task(params)
            elif operation == "pending":
                return self._get_pending(params)
            elif operation == "running":
                return self._get_running(params)
            elif operation == "completed":
                return self._get_completed(params)
            elif operation == "metrics":
                return self._get_metrics(params)
            elif operation == "history":
                return self._get_history(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create_executor(self, params: Dict[str, Any]) -> ActionResult:
        """Create an executor."""
        executor_id = params.get("executor_id")
        name = params.get("name", "Unnamed Executor")

        if not executor_id:
            return ActionResult(success=False, message="executor_id required")

        executor = TaskExecutor(executor_id=executor_id, name=name)
        self._executors[executor_id] = executor

        return ActionResult(
            success=True,
            message=f"Executor created: {executor_id}"
        )

    def _submit_task(self, params: Dict[str, Any]) -> ActionResult:
        """Submit a task."""
        executor_id = params.get("executor_id")

        if not executor_id or executor_id not in self._executors:
            return ActionResult(success=False, message="Invalid executor_id")

        executor = self._executors[executor_id]

        def placeholder_handler(params):
            return {"status": "completed"}

        task_id = executor.submit(
            name=params.get("name", "task"),
            handler=params.get("handler") or placeholder_handler,
            params=params.get("params", {}),
            priority=ExecutionPriority(params.get("priority", "NORMAL")),
            timeout_seconds=params.get("timeout_seconds"),
            retry_count=params.get("retry_count", 0),
            retry_delay_seconds=params.get("retry_delay_seconds", 1.0),
            scheduled_at=params.get("scheduled_at"),
            dependencies=params.get("dependencies")
        )

        return ActionResult(
            success=True,
            data={"task_id": task_id}
        )

    def _execute_next(self, params: Dict[str, Any]) -> ActionResult:
        """Execute next pending task."""
        executor_id = params.get("executor_id")

        if not executor_id or executor_id not in self._executors:
            return ActionResult(success=False, message="Invalid executor_id")

        executor = self._executors[executor_id]
        result = executor.execute_next()

        if not result:
            return ActionResult(
                success=True,
                message="No tasks to execute"
            )

        return ActionResult(
            success=result.status == ExecutionStatus.COMPLETED,
            data={
                "task_id": result.task_id,
                "status": result.status.value,
                "result": result.result,
                "error": result.error,
                "duration_ms": result.duration_ms
            }
        )

    def _cancel_task(self, params: Dict[str, Any]) -> ActionResult:
        """Cancel a task."""
        executor_id = params.get("executor_id")
        task_id = params.get("task_id")

        if not executor_id or executor_id not in self._executors:
            return ActionResult(success=False, message="Invalid executor_id")

        executor = self._executors[executor_id]
        success = executor.cancel(task_id)

        return ActionResult(
            success=success,
            message="Task cancelled" if success else "Task not found"
        )

    def _get_task(self, params: Dict[str, Any]) -> ActionResult:
        """Get task details."""
        executor_id = params.get("executor_id")
        task_id = params.get("task_id")

        if not executor_id or executor_id not in self._executors:
            return ActionResult(success=False, message="Invalid executor_id")

        executor = self._executors[executor_id]
        task = executor.get_task(task_id)

        if not task:
            return ActionResult(success=False, message="Task not found")

        return ActionResult(
            success=True,
            data={
                "task_id": task.task_id,
                "name": task.name,
                "status": task.status.value,
                "priority": task.priority.name,
                "created_at": task.created_at,
                "started_at": task.started_at,
                "completed_at": task.completed_at
            }
        )

    def _get_pending(self, params: Dict[str, Any]) -> ActionResult:
        """Get pending tasks."""
        executor_id = params.get("executor_id")

        if not executor_id or executor_id not in self._executors:
            return ActionResult(success=False, message="Invalid executor_id")

        executor = self._executors[executor_id]
        tasks = executor.get_pending_tasks()

        return ActionResult(success=True, data={"tasks": tasks})

    def _get_running(self, params: Dict[str, Any]) -> ActionResult:
        """Get running tasks."""
        executor_id = params.get("executor_id")

        if not executor_id or executor_id not in self._executors:
            return ActionResult(success=False, message="Invalid executor_id")

        executor = self._executors[executor_id]
        tasks = executor.get_running_tasks()

        return ActionResult(success=True, data={"tasks": tasks})

    def _get_completed(self, params: Dict[str, Any]) -> ActionResult:
        """Get completed tasks."""
        executor_id = params.get("executor_id")

        if not executor_id or executor_id not in self._executors:
            return ActionResult(success=False, message="Invalid executor_id")

        executor = self._executors[executor_id]
        tasks = executor.get_completed_tasks(
            limit=params.get("limit", 100)
        )

        return ActionResult(success=True, data={"tasks": tasks})

    def _get_metrics(self, params: Dict[str, Any]) -> ActionResult:
        """Get executor metrics."""
        executor_id = params.get("executor_id")

        if executor_id and executor_id in self._executors:
            executor = self._executors[executor_id]
            metrics = executor.get_metrics()
        else:
            metrics = ExecutorMetrics()

        return ActionResult(
            success=True,
            data={
                "total_submitted": metrics.total_submitted,
                "total_completed": metrics.total_completed,
                "total_failed": metrics.total_failed,
                "total_cancelled": metrics.total_cancelled,
                "total_timeout": metrics.total_timeout,
                "running_count": metrics.running_count,
                "pending_count": metrics.pending_count
            }
        )

    def _get_history(self, params: Dict[str, Any]) -> ActionResult:
        """Get execution history."""
        executor_id = params.get("executor_id")

        if not executor_id or executor_id not in self._executors:
            return ActionResult(success=False, message="Invalid executor_id")

        executor = self._executors[executor_id]
        history = executor.get_execution_history(
            limit=params.get("limit", 100)
        )

        return ActionResult(success=True, data={"history": history})
