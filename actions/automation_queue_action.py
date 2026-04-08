"""
Automation Queue Action Module.

Priority-based task queue with async processing,
rate limiting, and dead-letter queue support.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, TypeVar
from dataclasses import dataclass, field
from enum import IntEnum
import logging
import asyncio
import time
from heapq import heappush, heappop
from concurrent.futures import Future

logger = logging.getLogger(__name__)

T = TypeVar("T")


class TaskPriority(IntEnum):
    """Task priority levels (lower number = higher priority)."""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4


@dataclass
class QueuedTask:
    """Task entry in the queue."""
    priority: int
    task_id: str
    func: Callable[..., Any]
    args: tuple = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    retries: int = 0
    max_retries: int = 3


class AutomationQueueAction:
    """
    Priority task queue for automation workflows.

    Supports priority scheduling, async processing,
    rate limiting, and dead-letter queue for failed tasks.

    Example:
        queue = AutomationQueueAction(max_workers=4)
        queue.enqueue(my_task, priority=TaskPriority.HIGH)
        queue.start()
    """

    def __init__(
        self,
        max_workers: int = 4,
        rate_limit: Optional[float] = None,
        max_retries: int = 3,
    ) -> None:
        self.max_workers = max_workers
        self.rate_limit = rate_limit
        self.max_retries = max_retries
        self._queue: list[QueuedTask] = []
        self._running: set[str] = set()
        self._completed: dict[str, Any] = {}
        self._dead_letter: list[QueuedTask] = []
        self._active = False
        self._lock = asyncio.Lock()

    def enqueue(
        self,
        func: Callable[..., T],
        *args: Any,
        priority: TaskPriority = TaskPriority.NORMAL,
        task_id: Optional[str] = None,
        max_retries: Optional[int] = None,
        **kwargs: Any,
    ) -> str:
        """Add a task to the queue."""
        task_id = task_id or f"task_{time.time()}_{id(func)}"
        task = QueuedTask(
            priority=priority,
            task_id=task_id,
            func=func,
            args=args,
            kwargs=kwargs,
            max_retries=max_retries or self.max_retries,
        )
        heappush(self._queue, task)
        logger.debug("Enqueued task %s with priority %s", task_id, priority.name)
        return task_id

    def dequeue(self) -> Optional[QueuedTask]:
        """Remove and return the highest priority task."""
        if not self._queue:
            return None
        return heappop(self._queue)

    def size(self) -> int:
        """Current queue size."""
        return len(self._queue)

    def get_task(self, task_id: str) -> Optional[QueuedTask]:
        """Find a task by ID."""
        for task in self._queue:
            if task.task_id == task_id:
                return task
        return None

    def remove(self, task_id: str) -> bool:
        """Remove a specific task from queue."""
        for i, task in enumerate(self._queue):
            if task.task_id == task_id:
                del self._queue[i]
                return True
        return False

    def get_dead_letter(self) -> list[QueuedTask]:
        """Get all failed tasks."""
        return list(self._dead_letter)

    def clear_dead_letter(self) -> None:
        """Clear dead letter queue."""
        self._dead_letter.clear()

    async def process_async(self) -> None:
        """Process tasks asynchronously."""
        self._active = True

        while self._active and (self._queue or self._running):
            if len(self._running) < self.max_workers:
                task = self.dequeue()
                if task:
                    asyncio.create_task(self._run_task(task))

            if self.rate_limit:
                await asyncio.sleep(self.rate_limit)
            else:
                await asyncio.sleep(0.01)

    async def _run_task(self, task: QueuedTask) -> None:
        """Run a single task."""
        self._running.add(task.task_id)
        logger.debug("Running task %s", task.task_id)

        try:
            if asyncio.iscoroutinefunction(task.func):
                result = await task.func(*task.args, **task.kwargs)
            else:
                result = task.func(*task.args, **task.kwargs)

            self._completed[task.task_id] = result
            logger.debug("Task %s completed successfully", task.task_id)

        except Exception as e:
            logger.error("Task %s failed: %s", task.task_id, e)
            task.retries += 1

            if task.retries < task.max_retries:
                task.retries += 1
                heappush(self._queue, task)
                logger.debug("Task %s re-enqueued (retry %d)", task.task_id, task.retries)
            else:
                self._dead_letter.append(task)
                logger.warning("Task %s moved to dead letter queue", task.task_id)

        finally:
            self._running.discard(task.task_id)

    def stop(self) -> None:
        """Stop the queue processor."""
        self._active = False

    def get_status(self) -> dict[str, Any]:
        """Get queue status."""
        return {
            "queue_size": len(self._queue),
            "running": len(self._running),
            "completed": len(self._completed),
            "dead_letter": len(self._dead_letter),
            "active": self._active,
        }
