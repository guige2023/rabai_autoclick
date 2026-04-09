"""
Task Scheduler Action Module.

Provides cron-style task scheduling with dependencies,
priority queuing, and execution tracking.

Author: rabai_autoclick team
"""

import time
import asyncio
import logging
from typing import Optional, Dict, Any, List, Callable, Set
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
from heapq import heappush, heappop

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Priority(Enum):
    """Task priority levels."""
    LOW = 3
    NORMAL = 2
    HIGH = 1
    CRITICAL = 0


@dataclass
class ScheduledTask:
    """A scheduled task definition."""
    task_id: str
    func: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    priority: Priority = Priority.NORMAL
    scheduled_at: Optional[float] = None
    max_retries: int = 3
    timeout: Optional[float] = None
    dependencies: Set[str] = field(default_factory=set)
    tags: Set[str] = field(default_factory=set)

    def __lt__(self, other: "ScheduledTask") -> bool:
        """Compare tasks by scheduled time and priority."""
        if self.scheduled_at == other.scheduled_at:
            return self.priority.value < other.priority.value
        return self.scheduled_at < other.scheduled_at


@dataclass
class TaskResult:
    """Result of a task execution."""
    task_id: str
    status: TaskStatus
    result: Any = None
    error: Optional[str] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    retries: int = 0

    @property
    def duration(self) -> Optional[float]:
        """Get execution duration in seconds."""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return None


class TaskSchedulerAction:
    """
    Advanced Task Scheduler.

    Supports cron expressions, task dependencies, priority queuing,
    retries, and comprehensive execution tracking.

    Example:
        >>> async def my_task(x, y):
        ...     return x + y
        >>> scheduler = TaskSchedulerAction()
        >>> task = scheduler.schedule_task("add", my_task, args=(1, 2), when=time.time() + 60)
        >>> scheduler.start()
    """

    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
        self._tasks: List[ScheduledTask] = []
        self._running_tasks: Dict[str, ScheduledTask] = {}
        self._results: Dict[str, TaskResult] = {}
        self._completed_task_ids: Set[str] = set()
        self._cancelled_task_ids: Set[str] = set()
        self._running = False
        self._lock = asyncio.Lock()

    def schedule_task(
        self,
        task_id: str,
        func: Callable,
        args: tuple = (),
        kwargs: Optional[Dict[str, Any]] = None,
        when: Optional[float] = None,
        priority: Priority = Priority.NORMAL,
        max_retries: int = 3,
        timeout: Optional[float] = None,
        dependencies: Optional[Set[str]] = None,
        tags: Optional[Set[str]] = None,
    ) -> ScheduledTask:
        """
        Schedule a task for execution.

        Args:
            task_id: Unique task identifier
            func: Async callable to execute
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
            when: Unix timestamp to execute at (None = ASAP)
            priority: Task priority (lower = higher priority)
            max_retries: Maximum retry attempts on failure
            timeout: Task timeout in seconds
            dependencies: Set of task IDs that must complete first
            tags: Set of tags for categorization

        Returns:
            ScheduledTask object
        """
        scheduled_at = when if when is not None else time.time()

        task = ScheduledTask(
            task_id=task_id,
            func=func,
            args=args,
            kwargs=kwargs or {},
            priority=priority,
            scheduled_at=scheduled_at,
            max_retries=max_retries,
            timeout=timeout,
            dependencies=dependencies or set(),
            tags=tags or set(),
        )

        heappush(self._tasks, task)
        logger.info(f"Scheduled task {task_id} at {datetime.fromtimestamp(scheduled_at)}")
        return task

    def schedule_interval(
        self,
        task_id: str,
        func: Callable,
        interval: float,
        args: tuple = (),
        kwargs: Optional[Dict[str, Any]] = None,
        priority: Priority = Priority.NORMAL,
        tags: Optional[Set[str]] = None,
    ) -> ScheduledTask:
        """
        Schedule a recurring task.

        Args:
            task_id: Unique task identifier
            func: Async callable to execute
            interval: Interval in seconds between executions
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
            priority: Task priority
            tags: Set of tags for categorization

        Returns:
            ScheduledTask object
        """
        scheduled_at = time.time() + interval
        task = self.schedule_task(
            task_id=task_id,
            func=func,
            args=args,
            kwargs=kwargs,
            when=scheduled_at,
            priority=priority,
            tags=tags,
        )
        task.interval = interval
        return task

    def get_task(self, task_id: str) -> Optional[ScheduledTask]:
        """Get task by ID from the queue."""
        for task in self._tasks:
            if task.task_id == task_id:
                return task
        return None

    def cancel_task(self, task_id: str) -> bool:
        """
        Cancel a pending task.

        Args:
            task_id: Task identifier

        Returns:
            True if task was cancelled
        """
        self._cancelled_task_ids.add(task_id)
        self._tasks = [t for t in self._tasks if t.task_id != task_id]
        logger.info(f"Cancelled task {task_id}")
        return True

    def get_result(self, task_id: str) -> Optional[TaskResult]:
        """Get result of a completed task."""
        return self._results.get(task_id)

    def get_pending_tasks(self) -> List[ScheduledTask]:
        """Get all pending tasks sorted by scheduled time."""
        return sorted(self._tasks, key=lambda t: (t.scheduled_at, t.priority.value))

    def get_task_stats(self) -> Dict[str, Any]:
        """Get scheduler statistics."""
        return {
            "pending": len(self._tasks),
            "running": len(self._running_tasks),
            "completed": len([r for r in self._results.values() if r.status == TaskStatus.COMPLETED]),
            "failed": len([r for r in self._results.values() if r.status == TaskStatus.FAILED]),
        }

    async def _check_dependencies(self, task: ScheduledTask) -> bool:
        """Check if all dependencies are satisfied."""
        for dep_id in task.dependencies:
            if dep_id in self._cancelled_task_ids:
                return False
            if dep_id not in self._completed_task_ids:
                result = self._results.get(dep_id)
                if result is None or result.status != TaskStatus.COMPLETED:
                    return False
        return True

    async def _execute_task(self, task: ScheduledTask) -> TaskResult:
        """Execute a single task with timeout and retries."""
        result = TaskResult(
            task_id=task.task_id,
            status=TaskStatus.RUNNING,
            started_at=time.time(),
        )

        retries = 0
        last_error = None

        while retries <= task.max_retries:
            try:
                if task.timeout:
                    result.result = await asyncio.wait_for(
                        task.func(*task.args, **task.kwargs),
                        timeout=task.timeout
                    )
                else:
                    result.result = await task.func(*task.args, **task.kwargs)

                result.status = TaskStatus.COMPLETED
                result.completed_at = time.time()
                self._completed_task_ids.add(task.task_id)
                logger.info(f"Task {task.task_id} completed successfully")
                return result

            except asyncio.TimeoutError:
                last_error = f"Task {task.task_id} timed out after {task.timeout}s"
                logger.warning(last_error)
            except Exception as e:
                last_error = str(e)
                logger.warning(f"Task {task.task_id} failed (attempt {retries + 1}): {e}")

            retries += 1
            if retries <= task.max_retries:
                await asyncio.sleep(2 ** retries)

        result.status = TaskStatus.FAILED
        result.error = last_error
        result.completed_at = time.time()
        result.retries = retries
        logger.error(f"Task {task.task_id} failed after {retries} attempts: {last_error}")
        return result

    async def _worker(self, worker_id: int) -> None:
        """Worker coroutine to process tasks."""
        while self._running:
            task = None

            async with self._lock:
                current_time = time.time()
                while self._tasks:
                    next_task = heappop(self._tasks)
                    if next_task.task_id in self._cancelled_task_ids:
                        continue
                    if next_task.scheduled_at > current_time:
                        heappush(self._tasks, next_task)
                        break
                    if await self._check_dependencies(next_task):
                        task = next_task
                        break
                    else:
                        logger.warning(f"Dependencies not met for task {next_task.task_id}")

                if task is None and self._tasks:
                    next_task = self._tasks[0]
                    wait_time = max(0, next_task.scheduled_at - current_time)
                    if wait_time > 0:
                        await asyncio.sleep(min(wait_time, 1.0))

            if task:
                async with self._lock:
                    self._running_tasks[task.task_id] = task
                result = await self._execute_task(task)
                async with self._lock:
                    self._running_tasks.pop(task.task_id, None)
                    self._results[task.task_id] = result

                    if hasattr(task, "interval") and task.interval:
                        task.scheduled_at = time.time() + task.interval
                        heappush(self._tasks, task)
            else:
                await asyncio.sleep(0.1)

    async def start(self) -> None:
        """Start the scheduler."""
        self._running = True
        logger.info(f"Starting task scheduler with {self.max_workers} workers")
        workers = [self._worker(i) for i in range(self.max_workers)]
        await asyncio.gather(*workers)

    def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        logger.info("Task scheduler stopped")
