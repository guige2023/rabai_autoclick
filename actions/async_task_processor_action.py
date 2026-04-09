"""Async Task Processor Action Module.

Process async tasks with priority, cancellation, and result tracking.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, TypeVar
from collections import heapq

T = TypeVar("T")
R = TypeVar("R")


class TaskPriority(Enum):
    """Task priority levels."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


class TaskStatus(Enum):
    """Task status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class AsyncTask(Generic[R]):
    """Async task wrapper."""
    task_id: str
    coroutine: Any
    priority: TaskPriority
    created_at: float
    status: TaskStatus = TaskStatus.PENDING
    result: R | None = None
    error: str | None = None
    started_at: float | None = None
    completed_at: float | None = None


@dataclass
class PriorityItem:
    """Priority queue item."""
    priority: int
    task_id: str
    task: AsyncTask

    def __lt__(self, other: PriorityItem) -> bool:
        if self.priority != other.priority:
            return self.priority > other.priority
        return self.task_id < other.task_id


class AsyncTaskProcessor:
    """Async task processor with priority queue."""

    def __init__(self, max_concurrent: int = 10, default_timeout: float = 60.0) -> None:
        self.max_concurrent = max_concurrent
        self.default_timeout = default_timeout
        self._tasks: dict[str, AsyncTask] = {}
        self._pending_queue: list[PriorityItem] = []
        self._running_tasks: dict[str, asyncio.Task] = {}
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._lock = asyncio.Lock()
        self._task_counter = 0

    def create_task(
        self,
        coroutine: Any,
        priority: TaskPriority = TaskPriority.NORMAL,
        timeout: float | None = None
    ) -> str:
        """Create a new async task."""
        self._task_counter += 1
        task_id = f"task_{self._task_counter}_{uuid.uuid4().hex[:8]}"
        task = AsyncTask(
            task_id=task_id,
            coroutine=coroutine,
            priority=priority,
            created_at=time.time()
        )
        self._tasks[task_id] = task
        heapq.heappush(
            self._pending_queue,
            PriorityItem(priority.value, task_id, task)
        )
        return task_id

    async def start(self) -> None:
        """Start processing tasks."""
        asyncio.create_task(self._process_loop())

    async def _process_loop(self) -> None:
        """Main processing loop."""
        while True:
            await asyncio.sleep(0.1)
            async with self._lock:
                while self._pending_queue and len(self._running_tasks) < self.max_concurrent:
                    item = heapq.heappop(self._pending_queue)
                    task = self._tasks.get(item.task_id)
                    if task and task.status == TaskStatus.PENDING:
                        asyncio.create_task(self._run_task(task))

    async def _run_task(self, task: AsyncTask) -> None:
        """Run a single task."""
        async with self._semaphore:
            task.status = TaskStatus.RUNNING
            task.started_at = time.time()
            async with self._lock:
                runner = asyncio.current_task()
                if runner:
                    self._running_tasks[task.task_id] = runner
            try:
                result = await asyncio.wait_for(task.coroutine, timeout=self.default_timeout)
                task.result = result
                task.status = TaskStatus.COMPLETED
            except asyncio.CancelledError:
                task.status = TaskStatus.CANCELLED
            except asyncio.TimeoutError:
                task.status = TaskStatus.FAILED
                task.error = "Task timed out"
            except Exception as e:
                task.status = TaskStatus.FAILED
                task.error = str(e)
            finally:
                task.completed_at = time.time()
                async with self._lock:
                    self._running_tasks.pop(task.task_id, None)

    def cancel_task(self, task_id: str) -> bool:
        """Cancel a task."""
        task = self._tasks.get(task_id)
        if not task:
            return False
        if task.status == TaskStatus.PENDING:
            task.status = TaskStatus.CANCELLED
            self._pending_queue = [
                p for p in self._pending_queue if p.task_id != task_id
            ]
            return True
        if task.task_id in self._running_tasks:
            self._running_tasks[task.task_id].cancel()
            return True
        return False

    def get_task(self, task_id: str) -> AsyncTask | None:
        """Get task by ID."""
        return self._tasks.get(task_id)

    def get_stats(self) -> dict[str, int]:
        """Get processor statistics."""
        return {
            "total": len(self._tasks),
            "pending": sum(1 for t in self._tasks.values() if t.status == TaskStatus.PENDING),
            "running": len(self._running_tasks),
            "completed": sum(1 for t in self._tasks.values() if t.status == TaskStatus.COMPLETED),
            "failed": sum(1 for t in self._tasks.values() if t.status == TaskStatus.FAILED),
        }
