"""Automation Scheduler Action Module.

Provides task scheduling with cron-like expressions,
periodic execution, and dependency management.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
from croniter import croniter
import logging

logger = logging.getLogger(__name__)


class ScheduleType(Enum):
    """Schedule type."""
    ONCE = "once"
    PERIODIC = "periodic"
    CRON = "cron"
    DELAYED = "delayed"


@dataclass
class ScheduleConfig:
    """Schedule configuration."""
    schedule_type: ScheduleType
    interval_seconds: Optional[float] = None
    cron_expression: Optional[str] = None
    run_at: Optional[datetime] = None
    max_runs: Optional[int] = None
    name: str = ""


@dataclass
class ScheduledTask:
    """Scheduled task."""
    task_id: str
    func: Callable
    config: ScheduleConfig
    next_run: float
    last_run: Optional[float] = None
    run_count: int = 0
    enabled: bool = True


class AutomationSchedulerAction:
    """Task scheduler with multiple schedule types.

    Example:
        scheduler = AutomationSchedulerAction()

        scheduler.schedule(
            task_id="daily_report",
            func=generate_report,
            config=ScheduleConfig(
                schedule_type=ScheduleType.CRON,
                cron_expression="0 9 * * *"
            )
        )

        await scheduler.start()
    """

    def __init__(self) -> None:
        self._tasks: Dict[str, ScheduledTask] = {}
        self._running = False
        self._task_handle: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._results: Dict[str, Any] = {}

    async def schedule(
        self,
        task_id: str,
        func: Callable,
        config: ScheduleConfig,
    ) -> None:
        """Schedule a task.

        Args:
            task_id: Unique task identifier
            func: Async or sync function to execute
            config: Schedule configuration
        """
        async with self._lock:
            next_run = self._calculate_next_run(config)
            task = ScheduledTask(
                task_id=task_id,
                func=func,
                config=config,
                next_run=next_run,
            )
            self._tasks[task_id] = task
            logger.info(f"Scheduled task {task_id}, next run: {datetime.fromtimestamp(next_run)}")

    async def unschedule(self, task_id: str) -> None:
        """Remove a scheduled task."""
        async with self._lock:
            self._tasks.pop(task_id, None)
            self._results.pop(task_id, None)

    async def enable(self, task_id: str) -> None:
        """Enable a task."""
        async with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id].enabled = True

    async def disable(self, task_id: str) -> None:
        """Disable a task."""
        async with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id].enabled = False

    async def run_now(self, task_id: str) -> Any:
        """Run task immediately regardless of schedule."""
        async with self._lock:
            if task_id not in self._tasks:
                raise ValueError(f"Task {task_id} not found")
            task = self._tasks[task_id]

        return await self._execute_task(task)

    async def start(self) -> None:
        """Start the scheduler."""
        self._running = True
        self._task_handle = asyncio.create_task(self._run_loop())
        logger.info("Scheduler started")

    async def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        if self._task_handle:
            self._task_handle.cancel()
            try:
                await self._task_handle
            except asyncio.CancelledError:
                pass
        logger.info("Scheduler stopped")

    async def _run_loop(self) -> None:
        """Main scheduler loop."""
        while self._running:
            try:
                await self._process_due_tasks()
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Scheduler error: {e}")

    async def _process_due_tasks(self) -> None:
        """Process all tasks that are due."""
        now = time.time()
        due_tasks = [
            task for task in self._tasks.values()
            if task.enabled and task.next_run <= now
        ]

        for task in due_tasks:
            asyncio.create_task(self._execute_and_reschedule(task))

    async def _execute_and_reschedule(self, task: ScheduledTask) -> None:
        """Execute task and reschedule if recurring."""
        try:
            result = await self._execute_task(task)
            self._results[task.task_id] = result

            if task.config.schedule_type != ScheduleType.ONCE:
                task.next_run = self._calculate_next_run(task.config)
                task.run_count += 1

                if task.config.max_runs and task.run_count >= task.config.max_runs:
                    async with self._lock:
                        self._tasks.pop(task.task_id, None)
                    logger.info(f"Task {task.task_id} completed max runs")

        except Exception as e:
            logger.error(f"Task {task.task_id} failed: {e}")
            task.next_run = time.time() + 60

    async def _execute_task(self, task: ScheduledTask) -> Any:
        """Execute a single task."""
        task.last_run = time.time()
        logger.debug(f"Executing task {task.task_id}")

        if asyncio.iscoroutinefunction(task.func):
            return await task.func()
        else:
            return task.func()

    def _calculate_next_run(self, config: ScheduleConfig) -> float:
        """Calculate next run time based on schedule type."""
        now = time.time()

        if config.schedule_type == ScheduleType.ONCE:
            if config.run_at:
                return config.run_at.timestamp()
            return now

        elif config.schedule_type == ScheduleType.DELAYED:
            return now + (config.interval_seconds or 0)

        elif config.schedule_type == ScheduleType.PERIODIC:
            interval = config.interval_seconds or 60.0
            return now + interval

        elif config.schedule_type == ScheduleType.CRON:
            if config.cron_expression:
                cron = croniter(config.cron_expression, datetime.now())
                return cron.get_next_timestamp()

        return now + 60

    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a task."""
        if task_id not in self._tasks:
            return None

        task = self._tasks[task_id]
        return {
            "task_id": task.task_id,
            "schedule_type": task.config.schedule_type.value,
            "enabled": task.enabled,
            "next_run": datetime.fromtimestamp(task.next_run) if task.next_run else None,
            "last_run": datetime.fromtimestamp(task.last_run) if task.last_run else None,
            "run_count": task.run_count,
        }

    def get_all_tasks(self) -> List[Dict[str, Any]]:
        """Get all scheduled tasks."""
        return [
            self.get_task_status(task_id)
            for task_id in self._tasks
        ]

    def get_result(self, task_id: str) -> Optional[Any]:
        """Get last result of a task."""
        return self._results.get(task_id)
