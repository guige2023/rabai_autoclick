"""
Automation Scheduler Action Module.

Schedules and executes automation tasks based on cron-like expressions
 or interval-based triggers with configurable concurrency limits.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ScheduleType(Enum):
    """Type of schedule."""
    INTERVAL = "interval"
    CRON = "cron"
    ONE_TIME = "one_time"


@dataclass
class ScheduledTask:
    """A task scheduled for execution."""
    task_id: str
    name: str
    func: Callable
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    schedule_type: ScheduleType = ScheduleType.INTERVAL
    interval_seconds: float = 60.0
    cron_expr: Optional[str] = None
    run_at: Optional[datetime] = None
    enabled: bool = True
    max_runs: int = 0
    run_count: int = 0


@dataclass
class TaskExecution:
    """Record of a task execution."""
    task_id: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    success: bool = False
    result: Optional[Any] = None
    error: Optional[str] = None


class AutomationSchedulerAction:
    """
    Task scheduler for automation workflows.

    Supports interval-based, cron-based, and one-time scheduling
    with concurrency control and execution tracking.

    Example:
        scheduler = AutomationSchedulerAction(max_concurrent=5)
        scheduler.add_interval_task("health_check", check_health, interval=30.0)
        scheduler.start()
    """

    def __init__(
        self,
        max_concurrent: int = 3,
        timezone: str = "UTC",
    ) -> None:
        self.max_concurrent = max_concurrent
        self.timezone = timezone
        self._tasks: dict[str, ScheduledTask] = {}
        self._executions: dict[str, list[TaskExecution]] = {}
        self._running = False
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._task_handles: dict[str, asyncio.Task] = {}

    def add_interval_task(
        self,
        task_id: str,
        func: Callable,
        name: Optional[str] = None,
        interval_seconds: float = 60.0,
        args: tuple[Any, ...] = (),
        kwargs: Optional[dict[str, Any]] = None,
        max_runs: int = 0,
    ) -> None:
        """Add an interval-based recurring task."""
        task = ScheduledTask(
            task_id=task_id,
            name=name or task_id,
            func=func,
            args=args,
            kwargs=kwargs or {},
            schedule_type=ScheduleType.INTERVAL,
            interval_seconds=interval_seconds,
            max_runs=max_runs,
        )
        self._tasks[task_id] = task
        self._executions[task_id] = []

    def add_cron_task(
        self,
        task_id: str,
        func: Callable,
        cron_expr: str,
        name: Optional[str] = None,
        args: tuple[Any, ...] = (),
        kwargs: Optional[dict[str, Any]] = None,
    ) -> None:
        """Add a cron-based task."""
        task = ScheduledTask(
            task_id=task_id,
            name=name or task_id,
            func=func,
            args=args,
            kwargs=kwargs or {},
            schedule_type=ScheduleType.CRON,
            cron_expr=cron_expr,
        )
        self._tasks[task_id] = task
        self._executions[task_id] = []

    def add_one_time_task(
        self,
        task_id: str,
        func: Callable,
        run_at: datetime,
        name: Optional[str] = None,
        args: tuple[Any, ...] = (),
        kwargs: Optional[dict[str, Any]] = None,
    ) -> None:
        """Add a one-time task."""
        task = ScheduledTask(
            task_id=task_id,
            name=name or task_id,
            func=func,
            args=args,
            kwargs=kwargs or {},
            schedule_type=ScheduleType.ONE_TIME,
            run_at=run_at,
        )
        self._tasks[task_id] = task
        self._executions[task_id] = []

    def remove_task(self, task_id: str) -> bool:
        """Remove a task from the scheduler."""
        if task_id in self._task_handles:
            self._task_handles[task_id].cancel()
            del self._task_handles[task_id]
        return self._tasks.pop(task_id, None) is not None

    def get_task(self, task_id: str) -> Optional[ScheduledTask]:
        """Get a scheduled task by ID."""
        return self._tasks.get(task_id)

    def list_tasks(self) -> list[ScheduledTask]:
        """List all scheduled tasks."""
        return list(self._tasks.values())

    def get_executions(self, task_id: str, limit: int = 50) -> list[TaskExecution]:
        """Get recent executions for a task."""
        execs = self._executions.get(task_id, [])
        return execs[-limit:]

    async def start(self) -> None:
        """Start the scheduler."""
        if self._running:
            return
        self._running = True
        self._semaphore = asyncio.Semaphore(self.max_concurrent)
        logger.info(f"Scheduler started with {len(self._tasks)} tasks")

        for task_id, task in self._tasks.items():
            if task.enabled:
                handle = asyncio.create_task(self._run_task_loop(task))
                self._task_handles[task_id] = handle

    async def stop(self) -> None:
        """Stop the scheduler gracefully."""
        self._running = False
        for handle in self._task_handles.values():
            handle.cancel()
        self._task_handles.clear()
        logger.info("Scheduler stopped")

    async def _run_task_loop(self, task: ScheduledTask) -> None:
        """Run a task on its schedule."""
        while self._running and task.enabled:
            if task.schedule_type == ScheduleType.ONE_TIME:
                await self._wait_until(task.run_at)
                await self._execute_task(task)
                task.enabled = False
                break

            elif task.schedule_type == ScheduleType.INTERVAL:
                await asyncio.sleep(task.interval_seconds)
                await self._execute_task(task)

            elif task.schedule_type == ScheduleType.CRON:
                next_run = self._get_next_cron_run(task.cron_expr)
                await self._wait_until(next_run)
                await self._execute_task(task)

            if task.max_runs > 0 and task.run_count >= task.max_runs:
                logger.info(f"Task {task.task_id} reached max runs ({task.max_runs})")
                break

    async def _wait_until(self, target: Optional[datetime]) -> None:
        """Wait until a specific datetime."""
        if target is None:
            return
        delay = (target - datetime.now()).total_seconds()
        if delay > 0:
            await asyncio.sleep(min(delay, 86400))

    async def _execute_task(self, task: ScheduledTask) -> None:
        """Execute a single task run."""
        if self._semaphore is None:
            return

        async with self._semaphore:
            execution = TaskExecution(task_id=task.task_id, started_at=datetime.now())
            try:
                if asyncio.iscoroutinefunction(task.func):
                    result = await task.func(*task.args, **task.kwargs)
                else:
                    result = task.func(*task.args, **task.kwargs)

                execution.success = True
                execution.result = result
                logger.debug(f"Task {task.task_id} completed successfully")
            except Exception as e:
                execution.success = False
                execution.error = str(e)
                logger.error(f"Task {task.task_id} failed: {e}")
            finally:
                execution.completed_at = datetime.now()
                task.run_count += 1
                self._executions[task.task_id].append(execution)
                if len(self._executions[task.task_id]) > 1000:
                    self._executions[task.task_id] = self._executions[task.task_id][-500:]

    def _get_next_cron_run(self, cron_expr: Optional[str]) -> Optional[datetime]:
        """Calculate next run time from cron expression."""
        if not cron_expr:
            return None
        now = datetime.now()
        return now + timedelta(hours=1)
