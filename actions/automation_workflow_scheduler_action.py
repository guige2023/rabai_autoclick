"""Automation workflow scheduler action for timed task execution.

Schedules and manages automated workflows with cron-style
timing, priority queuing, and execution tracking.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class ScheduleType(Enum):
    """Types of scheduling supported."""
    ONCE = "once"
    INTERVAL = "interval"
    CRON = "cron"


@dataclass
class ScheduledTask:
    """A task scheduled for execution."""
    id: str
    name: str
    func: Callable[..., Any]
    schedule_type: ScheduleType
    interval_seconds: float = 0.0
    next_run: float = field(default_factory=time.time)
    last_run: Optional[float] = None
    enabled: bool = True
    priority: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ScheduleStats:
    """Execution statistics for the scheduler."""
    tasks_scheduled: int = 0
    tasks_executed: int = 0
    tasks_failed: int = 0
    total_execution_time_ms: float = 0.0


class AutomationWorkflowSchedulerAction:
    """Schedule and manage automated workflow execution.

    Args:
        timezone: Timezone for schedule calculations.

    Example:
        >>> scheduler = AutomationWorkflowSchedulerAction()
        >>> scheduler.schedule_interval("heartbeat", heartbeat_fn, 60)
        >>> await scheduler.start()
    """

    def __init__(self, timezone: str = "UTC") -> None:
        self.timezone = timezone
        self._tasks: dict[str, ScheduledTask] = {}
        self._running = False
        self._stats = ScheduleStats()

    def schedule_interval(
        self,
        task_id: str,
        name: str,
        func: Callable[..., Any],
        interval_seconds: float,
        priority: int = 0,
    ) -> "AutomationWorkflowSchedulerAction":
        """Schedule a task to run at intervals.

        Args:
            task_id: Unique identifier for the task.
            name: Human-readable name.
            func: Function to execute.
            interval_seconds: Interval between executions.
            priority: Higher priority tasks run first.

        Returns:
            Self for method chaining.
        """
        task = ScheduledTask(
            id=task_id,
            name=name,
            func=func,
            schedule_type=ScheduleType.INTERVAL,
            interval_seconds=interval_seconds,
            next_run=time.time() + interval_seconds,
            priority=priority,
        )
        self._tasks[task_id] = task
        self._stats.tasks_scheduled += 1
        return self

    def schedule_once(
        self,
        task_id: str,
        name: str,
        func: Callable[..., Any],
        delay_seconds: float,
        priority: int = 0,
    ) -> "AutomationWorkflowSchedulerAction":
        """Schedule a task to run once after a delay.

        Args:
            task_id: Unique identifier for the task.
            name: Human-readable name.
            func: Function to execute.
            delay_seconds: Delay before execution.
            priority: Higher priority tasks run first.

        Returns:
            Self for method chaining.
        """
        task = ScheduledTask(
            id=task_id,
            name=name,
            func=func,
            schedule_type=ScheduleType.ONCE,
            next_run=time.time() + delay_seconds,
            priority=priority,
        )
        self._tasks[task_id] = task
        self._stats.tasks_scheduled += 1
        return self

    def unschedule(self, task_id: str) -> bool:
        """Remove a scheduled task.

        Args:
            task_id: ID of task to remove.

        Returns:
            True if task was removed.
        """
        if task_id in self._tasks:
            del self._tasks[task_id]
            return True
        return False

    def enable(self, task_id: str) -> bool:
        """Enable a scheduled task.

        Args:
            task_id: ID of task to enable.

        Returns:
            True if task was found and enabled.
        """
        if task_id in self._tasks:
            self._tasks[task_id].enabled = True
            return True
        return False

    def disable(self, task_id: str) -> bool:
        """Disable a scheduled task.

        Args:
            task_id: ID of task to disable.

        Returns:
            True if task was found and disabled.
        """
        if task_id in self._tasks:
            self._tasks[task_id].enabled = False
            return True
        return False

    async def start(self, tick_interval: float = 1.0) -> None:
        """Start the scheduler loop.

        Args:
            tick_interval: Seconds between schedule checks.
        """
        self._running = True
        logger.info(f"Scheduler started with {len(self._tasks)} tasks")

        while self._running:
            await self._tick()
            await asyncio.sleep(tick_interval)

    def stop(self) -> None:
        """Stop the scheduler loop."""
        self._running = False
        logger.info("Scheduler stopped")

    async def _tick(self) -> None:
        """Process one scheduler tick."""
        now = time.time()
        due_tasks = [
            task for task in self._tasks.values()
            if task.enabled and task.next_run <= now
        ]
        due_tasks.sort(key=lambda t: (-t.priority, t.next_run))

        for task in due_tasks:
            await self._execute_task(task)

            if task.schedule_type == ScheduleType.ONCE:
                self._tasks.pop(task.id, None)
            else:
                task.last_run = now
                task.next_run = now + task.interval_seconds

    async def _execute_task(self, task: ScheduledTask) -> None:
        """Execute a scheduled task.

        Args:
            task: Task to execute.
        """
        start_time = time.time()
        logger.info(f"Executing scheduled task: {task.name}")

        try:
            if asyncio.iscoroutinefunction(task.func):
                await task.func()
            else:
                task.func()
            self._stats.tasks_executed += 1
        except Exception as e:
            logger.error(f"Task {task.name} failed: {e}")
            self._stats.tasks_failed += 1

        self._stats.total_execution_time_ms += (time.time() - start_time) * 1000

    def get_task(self, task_id: str) -> Optional[ScheduledTask]:
        """Get a scheduled task by ID.

        Args:
            task_id: Task identifier.

        Returns:
            Task if found, None otherwise.
        """
        return self._tasks.get(task_id)

    def get_stats(self) -> ScheduleStats:
        """Get scheduler statistics.

        Returns:
            Current scheduler statistics.
        """
        return self._stats

    def get_pending_tasks(self) -> list[ScheduledTask]:
        """Get list of pending tasks.

        Returns:
            List of tasks sorted by next run time.
        """
        return sorted(
            [t for t in self._tasks.values() if t.enabled],
            key=lambda t: t.next_run,
        )
