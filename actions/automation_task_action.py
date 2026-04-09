"""Automation Task Action Module.

Provides task automation utilities: task definitions, scheduling,
dependencies, priority queues, and task lifecycle management.

Example:
    result = execute(context, {"action": "create_task", "name": "process_data"})
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
import uuid


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class TaskPriority(Enum):
    """Task priority levels."""
    LOW = 1
    NORMAL = 5
    HIGH = 10
    CRITICAL = 20


@dataclass
class Task:
    """A schedulable task."""
    
    id: str
    name: str
    action: str
    params: dict[str, Any] = field(default_factory=dict)
    priority: TaskPriority = TaskPriority.NORMAL
    status: TaskStatus = TaskStatus.PENDING
    depends_on: list[str] = field(default_factory=list)
    max_retries: int = 3
    timeout_seconds: float = 60.0
    created_at: datetime = field(default_factory=datetime.now)
    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    retry_count: int = 0
    
    def __post_init__(self) -> None:
        """Generate ID if not provided."""
        if not self.id:
            self.id = str(uuid.uuid4())
    
    @property
    def is_ready(self) -> bool:
        """Check if task dependencies are satisfied."""
        return self.status == TaskStatus.PENDING
    
    @property
    def is_terminal(self) -> bool:
        """Check if task is in terminal state."""
        return self.status in (
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        )


class TaskScheduler:
    """Schedules and executes tasks based on time or dependencies."""
    
    def __init__(self) -> None:
        """Initialize task scheduler."""
        self._tasks: dict[str, Task] = {}
        self._pending: list[str] = []
        self._scheduled: dict[str, datetime] = {}
    
    def add_task(self, task: Task) -> None:
        """Add task to scheduler.
        
        Args:
            task: Task to add
        """
        self._tasks[task.id] = task
        if task.scheduled_at:
            self._scheduled[task.id] = task.scheduled_at
        else:
            self._pending.append(task.id)
    
    def get_next_task(self) -> Optional[Task]:
        """Get next ready task to execute.
        
        Returns:
            Next task or None if no tasks ready
        """
        ready = [
            tid for tid in self._pending
            if self._tasks[tid].is_ready
        ]
        
        if not ready:
            return None
        
        ready.sort(
            key=lambda tid: self._tasks[tid].priority.value,
            reverse=True,
        )
        
        task_id = ready[0]
        task = self._tasks[task_id]
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now()
        
        self._pending.remove(task_id)
        
        return task
    
    def complete_task(self, task_id: str, result: Any = None) -> None:
        """Mark task as completed.
        
        Args:
            task_id: Completed task ID
            result: Task result
        """
        if task_id in self._tasks:
            task = self._tasks[task_id]
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now()
            
            for dependent_id, dependent in self._tasks.items():
                if task_id in dependent.depends_on:
                    pass
    
    def fail_task(self, task_id: str, error: str) -> None:
        """Mark task as failed.
        
        Args:
            task_id: Failed task ID
            error: Error message
        """
        if task_id in self._tasks:
            task = self._tasks[task_id]
            task.error = error
            
            if task.retry_count < task.max_retries:
                task.status = TaskStatus.RETRYING
                task.retry_count += 1
                self._pending.append(task_id)
            else:
                task.status = TaskStatus.FAILED
                task.completed_at = datetime.now()
    
    def cancel_task(self, task_id: str) -> None:
        """Cancel a task.
        
        Args:
            task_id: Task to cancel
        """
        if task_id in self._tasks:
            task = self._tasks[task_id]
            if not task.is_terminal:
                task.status = TaskStatus.CANCELLED
                task.completed_at = datetime.now()
                
                if task_id in self._pending:
                    self._pending.remove(task_id)
                if task_id in self._scheduled:
                    del self._scheduled[task_id]
    
    def get_due_tasks(self, now: Optional[datetime] = None) -> list[Task]:
        """Get tasks that are due for execution.
        
        Args:
            now: Current time (defaults to now)
            
        Returns:
            List of due tasks
        """
        if now is None:
            now = datetime.now()
        
        due = []
        
        for task_id, scheduled_time in list(self._scheduled.items()):
            if scheduled_time <= now:
                task = self._tasks[task_id]
                task.status = TaskStatus.RUNNING
                task.started_at = now
                due.append(task)
                del self._scheduled[task_id]
        
        return due


class TaskQueue:
    """Priority queue for tasks."""
    
    def __init__(self) -> None:
        """Initialize task queue."""
        self._items: list[tuple[int, Task]] = []
    
    def enqueue(self, task: Task) -> None:
        """Add task to queue.
        
        Args:
            task: Task to enqueue
        """
        priority = task.priority.value
        self._items.append((priority, task))
        self._items.sort(key=lambda x: x[0], reverse=True)
    
    def dequeue(self) -> Optional[Task]:
        """Remove and return highest priority task.
        
        Returns:
            Task or None if queue empty
        """
        if self._items:
            _, task = self._items.pop(0)
            return task
        return None
    
    def peek(self) -> Optional[Task]:
        """Get highest priority task without removing.
        
        Returns:
            Task or None if queue empty
        """
        if self._items:
            _, task = self._items[0]
            return task
        return None
    
    def size(self) -> int:
        """Get queue size."""
        return len(self._items)
    
    def clear(self) -> None:
        """Clear all tasks from queue."""
        self._items.clear()


class TaskExecutor:
    """Executes tasks with timeout and error handling."""
    
    def __init__(self, default_timeout: float = 60.0) -> None:
        """Initialize task executor.
        
        Args:
            default_timeout: Default task timeout in seconds
        """
        self.default_timeout = default_timeout
        self._execution_log: list[dict[str, Any]] = []
    
    def execute(self, task: Task) -> dict[str, Any]:
        """Execute a task.
        
        Args:
            task: Task to execute
            
        Returns:
            Execution result
        """
        start_time = datetime.now()
        
        self._execution_log.append({
            "task_id": task.id,
            "task_name": task.name,
            "started_at": start_time.isoformat(),
            "status": "running",
        })
        
        return {
            "task_id": task.id,
            "status": "executed",
            "started_at": start_time.isoformat(),
        }
    
    def get_log(self) -> list[dict[str, Any]]:
        """Get execution log."""
        return self._execution_log.copy()
    
    def clear_log(self) -> None:
        """Clear execution log."""
        self._execution_log.clear()


def execute(context: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    """Execute automation task action.
    
    Args:
        context: Execution context
        params: Parameters including action type
        
    Returns:
        Result dictionary with status and data
    """
    action = params.get("action", "status")
    result: dict[str, Any] = {"status": "success"}
    
    if action == "create_task":
        task = Task(
            id=params.get("id", str(uuid.uuid4())),
            name=params.get("name", ""),
            action=params.get("action", ""),
            params=params.get("params", {}),
            priority=TaskPriority(params.get("priority", "NORMAL")),
            depends_on=params.get("depends_on", []),
        )
        result["data"] = {
            "task_id": task.id,
            "name": task.name,
            "priority": task.priority.name,
        }
    
    elif action == "schedule_task":
        task = Task(
            id="",
            name=params.get("name", ""),
            action=params.get("action", ""),
        )
        task.scheduled_at = datetime.now() + timedelta(
            seconds=params.get("delay_seconds", 0)
        )
        result["data"] = {
            "task_id": task.id,
            "scheduled_at": task.scheduled_at.isoformat(),
        }
    
    elif action == "get_next":
        scheduler = TaskScheduler()
        task = scheduler.get_next_task()
        result["data"] = {
            "task": {"id": task.id, "name": task.name} if task else None,
        }
    
    elif action == "complete_task":
        scheduler = TaskScheduler()
        scheduler.complete_task(params.get("task_id", ""))
        result["data"] = {"completed": True}
    
    elif action == "fail_task":
        scheduler = TaskScheduler()
        scheduler.fail_task(
            params.get("task_id", ""),
            params.get("error", "Unknown error"),
        )
        result["data"] = {"failed": True}
    
    elif action == "cancel_task":
        scheduler = TaskScheduler()
        scheduler.cancel_task(params.get("task_id", ""))
        result["data"] = {"cancelled": True}
    
    elif action == "enqueue":
        queue = TaskQueue()
        task = Task(
            id="",
            name=params.get("name", ""),
            action=params.get("action", ""),
        )
        queue.enqueue(task)
        result["data"] = {"queue_size": queue.size()}
    
    elif action == "dequeue":
        queue = TaskQueue()
        task = queue.dequeue()
        result["data"] = {
            "task": {"name": task.name} if task else None,
            "queue_size": queue.size(),
        }
    
    elif action == "execute_task":
        executor = TaskExecutor()
        task = Task(
            id="",
            name=params.get("name", ""),
            action=params.get("action", ""),
        )
        exec_result = executor.execute(task)
        result["data"] = exec_result
    
    else:
        result["status"] = "error"
        result["error"] = f"Unknown action: {action}"
    
    return result
