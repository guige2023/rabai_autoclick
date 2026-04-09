"""Task scheduler action module.

Provides task scheduling functionality with cron-like scheduling,
periodic tasks, delayed execution, and task prioritization.
"""

from __future__ import annotations

import time
import threading
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
import logging
import sched
import uuid

logger = logging.getLogger(__name__)


class ScheduleType(Enum):
    """Task schedule type."""
    ONCE = "once"
    INTERVAL = "interval"
    CRON = "cron"
    DELAYED = "delayed"


@dataclass
class ScheduledTask:
    """Represents a scheduled task."""
    id: str
    name: str
    func: Callable[..., Any]
    args: tuple = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    schedule_type: ScheduleType = ScheduleType.ONCE
    interval: float = 0.0
    next_run: Optional[float] = None
    last_run: Optional[float] = None
    enabled: bool = True
    max_runs: Optional[int] = None
    run_count: int = 0
    on_error: Optional[Callable[[Exception], None]] = None


class TaskScheduler:
    """Task scheduler with multiple schedule types."""

    def __init__(self daemon: bool = True):
        """Initialize task scheduler.

        Args:
            daemon: Run scheduler as daemon thread
        """
        self.daemon = daemon
        self._tasks: dict[str, ScheduledTask] = {}
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._scheduler = sched.scheduler(time.time, time.sleep)
        self._task_results: dict[str, Any] = {}

    def add_task(
        self,
        name: str,
        func: Callable[..., Any],
        args: tuple = (),
        kwargs: Optional[dict[str, Any]] = None,
        schedule_type: ScheduleType = ScheduleType.ONCE,
        interval: float = 0.0,
        delay: float = 0.0,
        cron_expr: Optional[str] = None,
        max_runs: Optional[int] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
    ) -> str:
        """Add task to scheduler.

        Args:
            name: Task name
            func: Function to execute
            args: Positional arguments
            kwargs: Keyword arguments
            schedule_type: Schedule type
            interval: Interval for periodic tasks (seconds)
            delay: Delay before first execution (seconds)
            cron_expr: Cron expression (not implemented)
            max_runs: Maximum number of runs
            on_error: Error callback

        Returns:
            Task ID
        """
        task_id = str(uuid.uuid4())

        next_run = None
        if delay > 0:
            next_run = time.time() + delay
        elif schedule_type == ScheduleType.INTERVAL and interval > 0:
            next_run = time.time() + interval

        task = ScheduledTask(
            id=task_id,
            name=name,
            func=func,
            args=args,
            kwargs=kwargs or {},
            schedule_type=schedule_type,
            interval=interval,
            next_run=next_run,
            max_runs=max_runs,
            on_error=on_error,
        )

        with self._lock:
            self._tasks[task_id] = task

        if self._running:
            self._schedule_task(task)

        logger.info(f"Added task: {name} ({task_id})")
        return task_id

    def remove_task(self, task_id: str) -> bool:
        """Remove task from scheduler.

        Args:
            task_id: Task ID to remove

        Returns:
            True if task was removed
        """
        with self._lock:
            if task_id in self._tasks:
                del self._tasks[task_id]
                logger.info(f"Removed task: {task_id}")
                return True
        return False

    def get_task(self, task_id: str) -> Optional[ScheduledTask]:
        """Get task by ID."""
        with self._lock:
            return self._tasks.get(task_id)

    def list_tasks(self) -> list[ScheduledTask]:
        """List all tasks."""
        with self._lock:
            return list(self._tasks.values())

    def enable_task(self, task_id: str) -> bool:
        """Enable a task."""
        with self._lock:
            task = self._tasks.get(task_id)
            if task:
                task.enabled = True
                if task.schedule_type == ScheduleType.INTERVAL:
                    task.next_run = time.time() + task.interval
                return True
        return False

    def disable_task(self, task_id: str) -> bool:
        """Disable a task."""
        with self._lock:
            task = self._tasks.get(task_id)
            if task:
                task.enabled = False
                return True
        return False

    def start(self) -> None:
        """Start scheduler."""
        with self._lock:
            if self._running:
                return
            self._running = True

        self._thread = threading.Thread(target=self._run_loop, daemon=self.daemon)
        self._thread.start()
        logger.info("Task scheduler started")

    def stop(self) -> None:
        """Stop scheduler."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        logger.info("Task scheduler stopped")

    def _run_loop(self) -> None:
        """Main scheduler loop."""
        while self._running:
            try:
                self._process_due_tasks()
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"Scheduler error: {e}")

    def _process_due_tasks(self) -> None:
        """Process tasks that are due."""
        current_time = time.time()

        with self._lock:
            due_tasks = [
                task for task in self._tasks.values()
                if task.enabled and task.next_run and task.next_run <= current_time
            ]

        for task in due_tasks:
            self._execute_task(task)

    def _execute_task(self, task: ScheduledTask) -> None:
        """Execute a task."""
        try:
            logger.debug(f"Executing task: {task.name}")
            result = task.func(*task.args, **task.kwargs)
            self._task_results[task.id] = result
            task.last_run = time.time()
            task.run_count += 1

            if task.schedule_type == ScheduleType.INTERVAL:
                if task.max_runs and task.run_count >= task.max_runs:
                    task.enabled = False
                    logger.info(f"Task {task.name} reached max runs")
                else:
                    task.next_run = time.time() + task.interval

            elif task.schedule_type == ScheduleType.DELAYED:
                task.enabled = False

            logger.debug(f"Task completed: {task.name}")

        except Exception as e:
            logger.error(f"Task execution error: {task.name} - {e}")
            if task.on_error:
                task.on_error(e)

    def _schedule_task(self, task: ScheduledTask) -> None:
        """Schedule a task in the internal scheduler."""
        if task.next_run:
            delay = max(0, task.next_run - time.time())
            self._scheduler.enter(delay, 0, self._execute_task, argument=(task,))

    def get_next_run_time(self, task_id: str) -> Optional[datetime]:
        """Get next run time for task."""
        task = self.get_task(task_id)
        if task and task.next_run:
            return datetime.fromtimestamp(task.next_run)
        return None

    def get_task_result(self, task_id: str) -> Optional[Any]:
        """Get task execution result."""
        return self._task_results.get(task_id)


class PeriodicTask:
    """Simple periodic task wrapper."""

    def __init__(
        self,
        interval: float,
        func: Callable[..., Any],
        args: tuple = (),
        kwargs: Optional[dict[str, Any]] = None,
    ):
        """Initialize periodic task.

        Args:
            interval: Execution interval (seconds)
            func: Function to execute
            args: Positional arguments
            kwargs: Keyword arguments
        """
        self.interval = interval
        self.func = func
        self.args = args
        self.kwargs = kwargs or {}
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start periodic execution."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop periodic execution."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        """Run loop."""
        while self._running:
            try:
                self.func(*self.args, **self.kwargs)
            except Exception as e:
                logger.error(f"Periodic task error: {e}")
            time.sleep(self.interval)


def create_scheduler(daemon: bool = True) -> TaskScheduler:
    """Create task scheduler instance.

    Args:
        daemon: Run as daemon thread

    Returns:
        TaskScheduler instance
    """
    return TaskScheduler(daemon=daemon)
