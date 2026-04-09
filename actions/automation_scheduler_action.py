"""Automation scheduler action module.

Provides advanced task scheduling:
- TaskScheduler: Schedule and manage tasks
- RateLimiter: Rate limiting for scheduled tasks
- PriorityQueue: Priority queue for task execution
- SchedulePolicy: Execution policies for schedules
"""

from __future__ import annotations

import time
import heapq
import threading
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import uuid

logger = logging.getLogger(__name__)


class SchedulePolicy(Enum):
    """Execution policy for scheduled tasks."""
    FIRE_ONCE = "fire_once"
    FIRE_PERIODIC = "fire_periodic"
    FIRE_DAILY = "fire_daily"
    FIRE_WEEKLY = "fire_weekly"
    CUSTOM = "custom"


class TaskPriority(Enum):
    """Priority levels for tasks."""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4


@dataclass
class ScheduledTask:
    """A scheduled task."""
    id: str
    name: str
    handler: Callable[..., Any]
    schedule_policy: SchedulePolicy
    priority: TaskPriority = TaskPriority.NORMAL
    interval_seconds: Optional[float] = None
    cron_expression: Optional[str] = None
    next_run_time: Optional[float] = None
    last_run_time: Optional[float] = None
    max_runs: Optional[int] = None
    run_count: int = 0
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __lt__(self, other: "ScheduledTask") -> bool:
        if self.next_run_time is None and other.next_run_time is None:
            return self.priority.value < other.priority.value
        if self.next_run_time is None:
            return False
        if other.next_run_time is None:
            return True
        return self.next_run_time < other.next_run_time


@dataclass
class TaskResult:
    """Result of a task execution."""
    task_id: str
    success: bool
    output: Any
    duration: float
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


class RateLimiter:
    """Token bucket rate limiter."""

    def __init__(
        self,
        rate: float,
        capacity: Optional[float] = None,
    ):
        self.rate = rate
        self.capacity = capacity or rate
        self._tokens = self.capacity
        self._last_update = time.time()
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Acquire tokens from the rate limiter."""
        start_time = time.time()

        while True:
            with self._lock:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return True

            if not blocking:
                return False

            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    return False

            wait_time = (tokens - self._tokens) / self.rate
            if timeout is not None:
                wait_time = min(wait_time, timeout - elapsed)
            time.sleep(min(wait_time, 0.1))

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_update
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last_update = now

    @property
    def available_tokens(self) -> float:
        """Get current available tokens."""
        with self._lock:
            self._refill()
            return self._tokens


class PriorityQueue:
    """Thread-safe priority queue for tasks."""

    def __init__(self):
        self._heap: List[Tuple[float, int, ScheduledTask]] = []
        self._counter = 0
        self._lock = threading.Lock()

    def push(self, task: ScheduledTask) -> None:
        """Add a task to the queue."""
        priority = task.next_run_time or float("inf")
        with self._lock:
            heapq.heappush(self._heap, (priority, self._counter, task))
            self._counter += 1

    def pop(self) -> Optional[ScheduledTask]:
        """Pop the highest priority task."""
        with self._lock:
            if not self._heap:
                return None
            _, _, task = heapq.heappop(self._heap)
            return task

    def peek(self) -> Optional[ScheduledTask]:
        """Peek at the highest priority task without removing it."""
        with self._lock:
            if not self._heap:
                return None
            _, _, task = self._heap[0]
            return task

    def remove(self, task_id: str) -> bool:
        """Remove a task by ID."""
        with self._lock:
            new_heap = [
                (p, c, t) for p, c, t in self._heap
                if t.id != task_id
            ]
            if len(new_heap) == len(self._heap):
                return False
            self._heap = new_heap
            heapq.heapify(self._heap)
            return True

    def __len__(self) -> int:
        with self._lock:
            return len(self._heap)


class TaskScheduler:
    """Advanced task scheduler."""

    def __init__(
        self,
        max_concurrent: int = 10,
        default_rate_limit: Optional[float] = None,
    ):
        self._tasks: Dict[str, ScheduledTask] = {}
        self._queue = PriorityQueue()
        self._rate_limiter = RateLimiter(default_rate_limit or 100.0) if default_rate_limit else None
        self._max_concurrent = max_concurrent
        self._running_tasks: int = 0
        self._lock = threading.Lock()
        self._running = False
        self._worker_thread: Optional[threading.Thread] = None
        self._results: Dict[str, TaskResult] = {}
        self._result_callbacks: Dict[str, List[Callable[[TaskResult], None]]] = {}

    def schedule(
        self,
        name: str,
        handler: Callable[..., Any],
        policy: SchedulePolicy,
        interval_seconds: Optional[float] = None,
        cron_expression: Optional[str] = None,
        priority: TaskPriority = TaskPriority.NORMAL,
        max_runs: Optional[int] = None,
        enabled: bool = True,
    ) -> str:
        """Schedule a new task."""
        task_id = str(uuid.uuid4())[:8]
        task = ScheduledTask(
            id=task_id,
            name=name,
            handler=handler,
            schedule_policy=policy,
            priority=priority,
            interval_seconds=interval_seconds,
            cron_expression=cron_expression,
            max_runs=max_runs,
            enabled=enabled,
        )
        task.next_run_time = self._compute_next_run(task)

        with self._lock:
            self._tasks[task_id] = task
            self._queue.push(task)

        logger.info(f"Scheduled task: {name} ({task_id})")
        return task_id

    def _compute_next_run(self, task: ScheduledTask) -> float:
        """Compute next run time for a task."""
        now = time.time()
        if task.interval_seconds:
            return now + task.interval_seconds
        return now

    def unschedule(self, task_id: str) -> bool:
        """Remove a scheduled task."""
        with self._lock:
            if task_id in self._tasks:
                del self._tasks[task_id]
                return self._queue.remove(task_id)
        return False

    def execute_now(self, task_id: str) -> Optional[TaskResult]:
        """Execute a task immediately."""
        task = self._tasks.get(task_id)
        if not task:
            return None
        return self._run_task(task)

    def _run_task(self, task: ScheduledTask) -> TaskResult:
        """Run a single task."""
        start_time = time.time()
        try:
            result = task.handler()
            duration = time.time() - start_time
            task_result = TaskResult(
                task_id=task.id,
                success=True,
                output=result,
                duration=duration,
            )
        except Exception as e:
            duration = time.time() - start_time
            task_result = TaskResult(
                task_id=task.id,
                success=False,
                output=None,
                duration=duration,
                error=str(e),
            )

        task.last_run_time = time.time()
        task.run_count += 1

        with self._lock:
            self._results[task.id] = task_result
            callbacks = self._result_callbacks.get(task.id, [])
            for cb in callbacks:
                try:
                    cb(task_result)
                except Exception as e:
                    logger.error(f"Result callback error: {e}")

        if (
            task.schedule_policy == SchedulePolicy.FIRE_PERIODIC
            and task.enabled
            and (not task.max_runs or task.run_count < task.max_runs)
        ):
            task.next_run_time = self._compute_next_run(task)
            self._queue.push(task)

        return task_result

    def on_result(
        self,
        task_id: str,
        callback: Callable[[TaskResult], None],
    ) -> None:
        """Register a callback for task results."""
        if task_id not in self._result_callbacks:
            self._result_callbacks[task_id] = []
        self._result_callbacks[task_id].append(callback)

    def start(self) -> None:
        """Start the scheduler."""
        self._running = True
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()
        logger.info("Task scheduler started")

    def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        if self._worker_thread:
            self._worker_thread.join(timeout=5.0)
        logger.info("Task scheduler stopped")

    def _worker_loop(self) -> None:
        """Main worker loop."""
        while self._running:
            task = self._queue.pop()
            if task is None:
                time.sleep(0.1)
                continue

            if task.next_run_time and time.time() < task.next_run_time:
                self._queue.push(task)
                time.sleep(min(0.1, task.next_run_time - time.time()))
                continue

            if self._rate_limiter and not self._rate_limiter.acquire(blocking=False):
                self._queue.push(task)
                time.sleep(0.1)
                continue

            self._run_task(task)


def schedule_task(
    scheduler: TaskScheduler,
    name: str,
    handler: Callable[..., Any],
    interval_seconds: float,
) -> str:
    """Convenience function to schedule a periodic task."""
    return scheduler.schedule(
        name=name,
        handler=handler,
        policy=SchedulePolicy.FIRE_PERIODIC,
        interval_seconds=interval_seconds,
    )
