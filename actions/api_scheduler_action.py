"""
API Scheduler Action Module.

Provides priority queue scheduling with rate limiting,
concurrency control, and delayed execution.
"""

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, TypeVar
import heapq

T = TypeVar("T")


class Priority(Enum):
    """Task priority levels."""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4


@dataclass
class ScheduledTask:
    """Scheduled task."""
    id: str
    func: Callable
    args: tuple = ()
    kwargs: dict = field(default_factory=dict)
    priority: Priority = Priority.NORMAL
    scheduled_at: float = field(default_factory=time.time)
    execute_at: float = field(default_factory=time.time)
    max_retries: int = 3
    retry_count: int = 0
    timeout: Optional[float] = None
    callback: Optional[Callable] = None
    error_callback: Optional[Callable] = None


@dataclass
class TaskResult:
    """Task execution result."""
    task_id: str
    success: bool
    result: Any = None
    error: Optional[Exception] = None
    execution_time: float = 0.0
    started_at: float = 0.0
    completed_at: float = 0.0


@dataclass
class SchedulerConfig:
    """Scheduler configuration."""
    max_concurrent: int = 10
    max_queue_size: int = 1000
    default_timeout: float = 60.0
    retry_delay: float = 1.0
    cleanup_interval: float = 60.0


class PriorityQueue:
    """Priority queue with delayed execution support."""

    def __init__(self):
        self._heap: list[tuple[float, int, ScheduledTask]] = []
        self._counter = 0
        self._lock = asyncio.Lock()

    async def enqueue(self, task: ScheduledTask) -> None:
        """Add task to queue."""
        async with self._lock:
            heapq.heappush(
                self._heap,
                (task.execute_at, self._counter, task)
            )
            self._counter += 1

    async def dequeue(self) -> Optional[ScheduledTask]:
        """Get next ready task."""
        async with self._lock:
            if not self._heap:
                return None

            earliest = self._heap[0]
            task = earliest[2]

            if task.execute_at <= time.time():
                heapq.heappop(self._heap)
                return task

            return None

    async def peek(self) -> Optional[ScheduledTask]:
        """Peek at next task without removing."""
        async with self._lock:
            if not self._heap:
                return None
            return self._heap[0][2]

    def size(self) -> int:
        """Get queue size."""
        return len(self._heap)

    async def remove(self, task_id: str) -> bool:
        """Remove task by ID."""
        async with self._lock:
            for i, (_, _, task) in enumerate(self._heap):
                if task.id == task_id:
                    del self._heap[i]
                    heapq.heapify(self._heap)
                    return True
        return False


class APISchedulerAction:
    """
    Priority-based task scheduler with rate limiting.

    Example:
        scheduler = APISchedulerAction(max_concurrent=5)

        task_id = await scheduler.schedule(
            func=api_call,
            args=(url,),
            priority=Priority.HIGH,
            delay=5.0
        )

        result = await scheduler.get_result(task_id)
    """

    def __init__(
        self,
        max_concurrent: int = 10,
        max_queue_size: int = 1000,
        default_timeout: float = 60.0
    ):
        self.config = SchedulerConfig(
            max_concurrent=max_concurrent,
            max_queue_size=max_queue_size,
            default_timeout=default_timeout
        )
        self._queue = PriorityQueue()
        self._results: dict[str, TaskResult] = {}
        self._running_tasks: set[str] = set()
        self._semaphore = asyncio.Semaphore(self.config.max_concurrent)
        self._running = False
        self._processor_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    async def schedule(
        self,
        func: Callable,
        args: tuple = (),
        kwargs: Optional[dict] = None,
        priority: Priority = Priority.NORMAL,
        delay: float = 0.0,
        at_time: Optional[float] = None,
        timeout: Optional[float] = None,
        max_retries: int = 3,
        callback: Optional[Callable] = None,
        error_callback: Optional[Callable] = None
    ) -> str:
        """Schedule a task for execution."""
        task_id = str(uuid.uuid4())

        task = ScheduledTask(
            id=task_id,
            func=func,
            args=args,
            kwargs=kwargs or {},
            priority=priority,
            execute_at=at_time if at_time else time.time() + delay,
            timeout=timeout or self.config.default_timeout,
            max_retries=max_retries,
            callback=callback,
            error_callback=error_callback
        )

        await self._queue.enqueue(task)
        return task_id

    async def cancel(self, task_id: str) -> bool:
        """Cancel scheduled task."""
        return await self._queue.remove(task_id)

    async def get_result(self, task_id: str) -> Optional[TaskResult]:
        """Get task result."""
        return self._results.get(task_id)

    async def _process_task(self, task: ScheduledTask) -> TaskResult:
        """Process single task."""
        start_time = time.time()
        result = TaskResult(
            task_id=task.id,
            success=False,
            started_at=start_time
        )

        async with self._semaphore:
            self._running_tasks.add(task.id)

            try:
                if asyncio.iscoroutinefunction(task.func):
                    result.result = await asyncio.wait_for(
                        task.func(*task.args, **task.kwargs),
                        timeout=task.timeout
                    )
                else:
                    result.result = await asyncio.wait_for(
                        asyncio.to_thread(task.func, *task.args, **task.kwargs),
                        timeout=task.timeout
                    )

                result.success = True

                if task.callback:
                    try:
                        callback_result = task.callback(result)
                        if asyncio.iscoroutinefunction(task.callback):
                            await callback_result
                    except Exception:
                        pass

            except asyncio.TimeoutError:
                result.error = TimeoutError(f"Task {task.id} timed out")

            except Exception as e:
                result.error = e

                if task.retry_count < task.max_retries:
                    task.retry_count += 1
                    task.execute_at = time.time() + self.config.retry_delay
                    await self._queue.enqueue(task)
                    return result

                if task.error_callback:
                    try:
                        task.error_callback(result)
                    except Exception:
                        pass

            finally:
                self._running_tasks.discard(task.id)
                result.completed_at = time.time()
                result.execution_time = result.completed_at - result.started_at
                self._results[task.id] = result

        return result

    async def _process_queue(self) -> None:
        """Process tasks from queue."""
        while self._running:
            try:
                task = await self._queue.dequeue()
                if task:
                    asyncio.create_task(self._process_task(task))
                else:
                    await asyncio.sleep(0.1)
            except Exception:
                pass

    async def start(self) -> None:
        """Start scheduler."""
        self._running = True
        self._processor_task = asyncio.create_task(self._process_queue())

    async def stop(self) -> None:
        """Stop scheduler."""
        self._running = False
        if self._processor_task:
            self._processor_task.cancel()

    async def get_stats(self) -> dict:
        """Get scheduler statistics."""
        return {
            "queue_size": self._queue.size(),
            "running_tasks": len(self._running_tasks),
            "completed_tasks": len(self._results),
            "max_concurrent": self.config.max_concurrent
        }

    @property
    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._running
