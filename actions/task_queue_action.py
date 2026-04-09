"""Task queue action for managing background task queues.

Provides task queuing, prioritization, retry logic,
and worker management.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskPriority(Enum):
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class Task:
    task_id: str
    func: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    priority: TaskPriority = TaskPriority.NORMAL
    status: TaskStatus = TaskStatus.PENDING
    retry_count: int = 0
    max_retries: int = 3
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    result: Any = None
    error: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


class TaskQueueAction:
    """Priority queue with retry and worker management.

    Args:
        max_queue_size: Maximum queue size.
        default_retry: Default retry count.
        enable_priority: Enable priority queueing.
    """

    def __init__(
        self,
        max_queue_size: int = 10000,
        default_retry: int = 3,
        enable_priority: bool = True,
    ) -> None:
        self._queue: list[Task] = []
        self._max_queue_size = max_queue_size
        self._default_retry = default_retry
        self._enable_priority = enable_priority
        self._running_tasks: dict[str, Task] = {}
        self._completed_tasks: list[Task] = []
        self._max_completed_history = 1000
        self._task_handlers: dict[str, list[Callable]] = {
            "on_complete": [],
            "on_failure": [],
            "on_retry": [],
        }

    def enqueue(
        self,
        func: Callable,
        *args: Any,
        priority: TaskPriority = TaskPriority.NORMAL,
        retry_count: int = 3,
        max_retries: Optional[int] = None,
        task_id: Optional[str] = None,
        **kwargs: Any,
    ) -> str:
        """Add a task to the queue.

        Args:
            func: Function to execute.
            args: Positional arguments.
            priority: Task priority.
            retry_count: Number of retries.
            max_retries: Override max retries.
            task_id: Optional task ID.
            kwargs: Keyword arguments.

        Returns:
            Task ID.
        """
        if len(self._queue) >= self._max_queue_size:
            raise Exception("Queue is full")

        tid = task_id or f"task_{int(time.time() * 1000)}"

        task = Task(
            task_id=tid,
            func=func,
            args=args,
            kwargs=kwargs,
            priority=priority,
            max_retries=max_retries if max_retries is not None else self._default_retry,
        )

        self._queue.append(task)

        if self._enable_priority:
            self._queue.sort(key=lambda t: t.priority.value, reverse=True)

        logger.debug(f"Enqueued task: {tid} (priority={priority.name})")
        return tid

    def dequeue(self) -> Optional[Task]:
        """Remove and return the next task from the queue.

        Returns:
            Next task or None if queue is empty.
        """
        if not self._queue:
            return None

        task = self._queue.pop(0)
        task.status = TaskStatus.RUNNING
        task.started_at = time.time()
        self._running_tasks[task.task_id] = task

        logger.debug(f"Dequeued task: {task.task_id}")
        return task

    def complete_task(
        self,
        task_id: str,
        result: Any,
    ) -> bool:
        """Mark a task as completed.

        Args:
            task_id: Task ID.
            result: Task result.

        Returns:
            True if task was found and completed.
        """
        task = self._running_tasks.pop(task_id, None)
        if not task:
            return False

        task.status = TaskStatus.COMPLETED
        task.completed_at = time.time()
        task.result = result

        self._completed_tasks.append(task)
        if len(self._completed_tasks) > self._max_completed_history:
            self._completed_tasks.pop(0)

        for handler in self._task_handlers["on_complete"]:
            try:
                handler(task)
            except Exception as e:
                logger.error(f"Complete handler error: {e}")

        logger.debug(f"Completed task: {task_id}")
        return True

    def fail_task(
        self,
        task_id: str,
        error: str,
    ) -> bool:
        """Mark a task as failed.

        Args:
            task_id: Task ID.
            error: Error message.

        Returns:
            True if task was found and marked failed.
        """
        task = self._running_tasks.pop(task_id, None)
        if not task:
            return False

        task.error = error
        task.retry_count += 1

        if task.retry_count < task.max_retries:
            task.status = TaskStatus.PENDING
            self._queue.insert(0, task)

            for handler in self._task_handlers["on_retry"]:
                try:
                    handler(task, task.retry_count)
                except Exception as e:
                    logger.error(f"Retry handler error: {e}")

            logger.debug(f"Retrying task: {task_id} (attempt {task.retry_count})")
        else:
            task.status = TaskStatus.FAILED
            task.completed_at = time.time()
            self._completed_tasks.append(task)

            if len(self._completed_tasks) > self._max_completed_history:
                self._completed_tasks.pop(0)

            for handler in self._task_handlers["on_failure"]:
                try:
                    handler(task)
                except Exception as e:
                    logger.error(f"Failure handler error: {e}")

            logger.warning(f"Task failed permanently: {task_id}")

        return True

    def cancel_task(self, task_id: str) -> bool:
        """Cancel a pending task.

        Args:
            task_id: Task ID.

        Returns:
            True if cancelled.
        """
        for i, task in enumerate(self._queue):
            if task.task_id == task_id:
                task.status = TaskStatus.CANCELLED
                self._queue.pop(i)
                logger.debug(f"Cancelled task: {task_id}")
                return True
        return False

    def get_task(self, task_id: str) -> Optional[Task]:
        """Get a task by ID.

        Args:
            task_id: Task ID.

        Returns:
            Task or None.
        """
        for task in self._queue:
            if task.task_id == task_id:
                return task

        if task_id in self._running_tasks:
            return self._running_tasks[task_id]

        for task in self._completed_tasks:
            if task.task_id == task_id:
                return task

        return None

    def get_queue_size(self) -> int:
        """Get current queue size.

        Returns:
            Number of pending tasks.
        """
        return len(self._queue)

    def get_running_count(self) -> int:
        """Get number of running tasks.

        Returns:
            Number of running tasks.
        """
        return len(self._running_tasks)

    def register_handler(self, event: str, handler: Callable) -> None:
        """Register a task event handler.

        Args:
            event: Event type ('on_complete', 'on_failure', 'on_retry').
            handler: Callback function.
        """
        if event in self._task_handlers:
            self._task_handlers[event].append(handler)

    def get_stats(self) -> dict[str, Any]:
        """Get task queue statistics.

        Returns:
            Dictionary with stats.
        """
        total_completed = sum(
            1 for t in self._completed_tasks if t.status == TaskStatus.COMPLETED
        )
        total_failed = sum(
            1 for t in self._completed_tasks if t.status == TaskStatus.FAILED
        )

        by_priority = {
            "low": sum(1 for t in self._queue if t.priority == TaskPriority.LOW),
            "normal": sum(1 for t in self._queue if t.priority == TaskPriority.NORMAL),
            "high": sum(1 for t in self._queue if t.priority == TaskPriority.HIGH),
            "critical": sum(1 for t in self._queue if t.priority == TaskPriority.CRITICAL),
        }

        return {
            "queue_size": len(self._queue),
            "running": len(self._running_tasks),
            "completed_total": len(self._completed_tasks),
            "completed_success": total_completed,
            "completed_failed": total_failed,
            "by_priority": by_priority,
            "max_queue_size": self._max_queue_size,
            "priority_enabled": self._enable_priority,
        }
