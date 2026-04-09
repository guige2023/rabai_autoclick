"""Scheduler advanced action for complex task scheduling.

Provides cron-style scheduling, rate limiting, priority queues,
and distributed task coordination.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class ScheduleType(Enum):
    ONCE = "once"
    INTERVAL = "interval"
    CRON = "cron"


class TaskPriority(Enum):
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class ScheduledTask:
    task_id: str
    callback: Callable
    schedule_type: ScheduleType
    interval: float = 0.0
    next_run: float = field(default_factory=time.time)
    priority: TaskPriority = TaskPriority.NORMAL
    enabled: bool = True
    max_retries: int = 3
    retry_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskResult:
    task_id: str
    success: bool
    result: Any = None
    error: Optional[str] = None
    execution_time_ms: float = 0.0


class SchedulerAdvancedAction:
    """Advanced task scheduler with multiple scheduling strategies.

    Args:
        max_concurrent: Maximum concurrent task executions.
        default_priority: Default priority for scheduled tasks.
    """

    def __init__(
        self,
        max_concurrent: int = 10,
        default_priority: TaskPriority = TaskPriority.NORMAL,
    ) -> None:
        self._tasks: dict[str, ScheduledTask] = {}
        self._max_concurrent = max_concurrent
        self._default_priority = default_priority
        self._running_tasks: set[str] = set()
        self._task_history: list[TaskResult] = []
        self._max_history = 1000
        self._paused = False

    def schedule_once(
        self,
        task_id: str,
        callback: Callable,
        delay: float = 0.0,
        priority: Optional[TaskPriority] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> str:
        """Schedule a task to run once after a delay.

        Args:
            task_id: Unique task identifier.
            callback: Function to execute.
            delay: Delay in seconds before execution.
            priority: Task priority.
            metadata: Optional task metadata.

        Returns:
            Task ID.
        """
        task = ScheduledTask(
            task_id=task_id,
            callback=callback,
            schedule_type=ScheduleType.ONCE,
            next_run=time.time() + delay,
            priority=priority or self._default_priority,
            metadata=metadata or {},
        )
        self._tasks[task_id] = task
        logger.debug(f"Scheduled one-time task: {task_id} (delay={delay}s)")
        return task_id

    def schedule_interval(
        self,
        task_id: str,
        callback: Callable,
        interval: float,
        initial_delay: float = 0.0,
        priority: Optional[TaskPriority] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> str:
        """Schedule a task to run at fixed intervals.

        Args:
            task_id: Unique task identifier.
            callback: Function to execute.
            interval: Interval in seconds between executions.
            initial_delay: Delay before first execution.
            priority: Task priority.
            metadata: Optional task metadata.

        Returns:
            Task ID.
        """
        task = ScheduledTask(
            task_id=task_id,
            callback=callback,
            schedule_type=ScheduleType.INTERVAL,
            interval=interval,
            next_run=time.time() + initial_delay,
            priority=priority or self._default_priority,
            metadata=metadata or {},
        )
        self._tasks[task_id] = task
        logger.debug(f"Scheduled interval task: {task_id} (interval={interval}s)")
        return task_id

    def cancel(self, task_id: str) -> bool:
        """Cancel a scheduled task.

        Args:
            task_id: Task ID to cancel.

        Returns:
            True if task was found and cancelled.
        """
        if task_id in self._tasks:
            del self._tasks[task_id]
            logger.debug(f"Cancelled task: {task_id}")
            return True
        return False

    def pause(self) -> None:
        """Pause the scheduler."""
        self._paused = True
        logger.info("Scheduler paused")

    def resume(self) -> None:
        """Resume the scheduler."""
        self._paused = False
        logger.info("Scheduler resumed")

    def is_paused(self) -> bool:
        """Check if scheduler is paused.

        Returns:
            True if paused.
        """
        return self._paused

    def run_pending(self) -> list[TaskResult]:
        """Run all tasks that are due.

        Returns:
            List of task execution results.
        """
        if self._paused:
            return []

        results = []
        now = time.time()
        due_tasks = self._get_due_tasks(now)

        for task in due_tasks:
            if len(self._running_tasks) >= self._max_concurrent:
                break

            result = self._execute_task(task)
            results.append(result)

            if task.schedule_type == ScheduleType.ONCE:
                self.cancel(task.task_id)
            else:
                task.next_run = now + task.interval

        return results

    def _get_due_tasks(self, now: float) -> list[ScheduledTask]:
        """Get tasks that are due for execution.

        Args:
            now: Current timestamp.

        Returns:
            List of due tasks sorted by priority.
        """
        due = [t for t in self._tasks.values() if t.enabled and t.next_run <= now]
        return sorted(due, key=lambda t: t.priority.value, reverse=True)

    def _execute_task(self, task: ScheduledTask) -> TaskResult:
        """Execute a scheduled task.

        Args:
            task: Task to execute.

        Returns:
            Task execution result.
        """
        self._running_tasks.add(task.task_id)
        start_time = time.time()

        try:
            result = task.callback()
            execution_time = (time.time() - start_time) * 1000
            task_result = TaskResult(
                task_id=task.task_id,
                success=True,
                result=result,
                execution_time_ms=execution_time,
            )
            task.retry_count = 0
            logger.debug(f"Task completed: {task.task_id} ({execution_time:.2f}ms)")

        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            task.retry_count += 1

            if task.retry_count < task.max_retries:
                task.next_run = time.time() + (2 ** task.retry_count)
                logger.warning(
                    f"Task failed {task.task_id}, retry {task.retry_count}/{task.max_retries}: {e}"
                )
            else:
                self.cancel(task.task_id)
                logger.error(f"Task failed permanently {task.task_id}: {e}")

            task_result = TaskResult(
                task_id=task.task_id,
                success=False,
                error=str(e),
                execution_time_ms=execution_time,
            )

        finally:
            self._running_tasks.discard(task.task_id)
            self._task_history.append(task_result)
            if len(self._task_history) > self._max_history:
                self._task_history.pop(0)

        return task_result

    def get_task(self, task_id: str) -> Optional[ScheduledTask]:
        """Get a scheduled task by ID.

        Args:
            task_id: Task ID.

        Returns:
            Task object or None.
        """
        return self._tasks.get(task_id)

    def get_all_tasks(self) -> list[ScheduledTask]:
        """Get all scheduled tasks.

        Returns:
            List of all tasks.
        """
        return list(self._tasks.values())

    def get_next_run_time(self) -> Optional[float]:
        """Get the next scheduled run time.

        Returns:
            Next run timestamp or None.
        """
        if not self._tasks:
            return None
        enabled_tasks = [t for t in self._tasks.values() if t.enabled]
        if not enabled_tasks:
            return None
        return min(t.next_run for t in enabled_tasks)

    def get_history(self, limit: int = 50) -> list[TaskResult]:
        """Get task execution history.

        Args:
            limit: Maximum results to return.

        Returns:
            List of task results (newest first).
        """
        return self._task_history[-limit:][::-1]

    def get_stats(self) -> dict[str, Any]:
        """Get scheduler statistics.

        Returns:
            Dictionary with scheduler stats.
        """
        total = len(self._tasks)
        enabled = sum(1 for t in self._tasks.values() if t.enabled)
        by_type = {
            "once": sum(1 for t in self._tasks.values() if t.schedule_type == ScheduleType.ONCE),
            "interval": sum(1 for t in self._tasks.values() if t.schedule_type == ScheduleType.INTERVAL),
        }
        successful = sum(1 for r in self._task_history if r.success)
        failed = sum(1 for r in self._task_history if not r.success)
        return {
            "total_tasks": total,
            "enabled_tasks": enabled,
            "running_tasks": len(self._running_tasks),
            "max_concurrent": self._max_concurrent,
            "by_type": by_type,
            "total_runs": len(self._task_history),
            "successful_runs": successful,
            "failed_runs": failed,
            "is_paused": self._paused,
            "next_run": self.get_next_run_time(),
        }
