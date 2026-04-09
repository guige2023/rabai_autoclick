"""Automation Timer Action Module.

Provides timer, scheduling, and timeout management for automation workflows
with support for periodic tasks, delays, and deadline tracking.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Awaitable

logger = logging.getLogger(__name__)


class TimerType(Enum):
    """Timer types."""
    DELAY = "delay"
    INTERVAL = "interval"
    DEADLINE = "deadline"
    CRON = "cron"
    ONE_SHOT = "one_shot"


@dataclass
class TimerTask:
    """Represents a scheduled timer task."""
    task_id: str
    name: str
    timer_type: TimerType
    interval_seconds: float = 0.0
    deadline: Optional[float] = None
    callback: Optional[Callable[[], Awaitable[Any]]] = None
    callback_args: tuple = ()
    callback_kwargs: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    last_run: Optional[float] = None
    next_run: Optional[float] = None
    run_count: int = 0
    max_runs: int = 0
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    def elapsed_ms(self) -> float:
        """Return elapsed time since creation in milliseconds."""
        return (time.time() - self.created_at) * 1000

    def is_expired(self) -> bool:
        """Check if the timer has expired."""
        if self.max_runs > 0 and self.run_count >= self.max_runs:
            return True
        if self.deadline and time.time() > self.deadline:
            return True
        return False


class AutomationTimerManager:
    """Manages multiple timer tasks."""

    def __init__(self):
        self._tasks: Dict[str, TimerTask] = {}
        self._running_tasks: Dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()

    async def create_delay(
        self,
        name: str,
        delay_seconds: float,
        callback: Optional[Callable] = None,
        callback_args: tuple = (),
        callback_kwargs: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> TimerTask:
        """Create a one-shot delay task."""
        task_id = str(uuid.uuid4())[:8]
        task = TimerTask(
            task_id=task_id,
            name=name,
            timer_type=TimerType.DELAY,
            interval_seconds=delay_seconds,
            next_run=time.time() + delay_seconds,
            callback=callback,
            callback_args=callback_args,
            callback_kwargs=callback_kwargs or {},
            metadata=metadata or {}
        )
        async with self._lock:
            self._tasks[task_id] = task
        return task

    async def create_interval(
        self,
        name: str,
        interval_seconds: float,
        callback: Optional[Callable] = None,
        callback_args: tuple = (),
        callback_kwargs: Optional[Dict[str, Any]] = None,
        max_runs: int = 0,
        metadata: Optional[Dict[str, Any]] = None
    ) -> TimerTask:
        """Create a recurring interval task."""
        task_id = str(uuid.uuid4())[:8]
        task = TimerTask(
            task_id=task_id,
            name=name,
            timer_type=TimerType.INTERVAL,
            interval_seconds=interval_seconds,
            next_run=time.time() + interval_seconds,
            callback=callback,
            callback_args=callback_args,
            callback_kwargs=callback_kwargs or {},
            max_runs=max_runs,
            metadata=metadata or {}
        )
        async with self._lock:
            self._tasks[task_id] = task
        return task

    async def create_deadline(
        self,
        name: str,
        deadline_seconds: float,
        callback: Optional[Callable] = None,
        callback_args: tuple = (),
        callback_kwargs: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> TimerTask:
        """Create a deadline task."""
        task_id = str(uuid.uuid4())[:8]
        task = TimerTask(
            task_id=task_id,
            name=name,
            timer_type=TimerType.DEADLINE,
            deadline=time.time() + deadline_seconds,
            callback=callback,
            callback_args=callback_args,
            callback_kwargs=callback_kwargs or {},
            metadata=metadata or {}
        )
        async with self._lock:
            self._tasks[task_id] = task
        return task

    async def get_task(self, task_id: str) -> Optional[TimerTask]:
        """Get a task by ID."""
        async with self._lock:
            task = self._tasks.get(task_id)
            return task.copy() if task else None

    async def cancel_task(self, task_id: str) -> bool:
        """Cancel and remove a task."""
        async with self._lock:
            task = self._tasks.pop(task_id, None)
            if task and task_id in self._running_tasks:
                running = self._running_tasks.pop(task_id)
                running.cancel()
            return task is not None

    async def enable_task(self, task_id: str) -> bool:
        """Enable a task."""
        async with self._lock:
            task = self._tasks.get(task_id)
            if task:
                task.enabled = True
                task.next_run = time.time() + task.interval_seconds
                return True
        return False

    async def disable_task(self, task_id: str) -> bool:
        """Disable a task."""
        async with self._lock:
            task = self._tasks.get(task_id)
            if task:
                task.enabled = False
                return True
        return False

    async def list_tasks(
        self,
        timer_type: Optional[TimerType] = None,
        enabled_only: bool = False
    ) -> List[Dict[str, Any]]:
        """List all tasks."""
        async with self._lock:
            tasks = list(self._tasks.values())
            if timer_type:
                tasks = [t for t in tasks if t.timer_type == timer_type]
            if enabled_only:
                tasks = [t for t in tasks if t.enabled]

            return [
                {
                    "task_id": t.task_id,
                    "name": t.name,
                    "timer_type": t.timer_type.value,
                    "enabled": t.enabled,
                    "run_count": t.run_count,
                    "last_run": t.last_run,
                    "next_run": t.next_run,
                    "is_expired": t.is_expired()
                }
                for t in tasks
            ]

    async def run_loop(self) -> None:
        """Run the timer manager loop."""
        while True:
            try:
                async with self._lock:
                    current_time = time.time()
                    due_tasks = [
                        t for t in self._tasks.values()
                        if t.enabled and t.next_run and t.next_run <= current_time
                    ]

                for task in due_tasks:
                    await self._execute_task(task)

                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Timer loop error: {e}")

    async def _execute_task(self, task: TimerTask) -> None:
        """Execute a single task."""
        task.last_run = time.time()
        task.run_count += 1

        if task.callback:
            try:
                if asyncio.iscoroutinefunction(task.callback):
                    await task.callback(*task.callback_args, **task.callback_kwargs)
                else:
                    task.callback(*task.callback_args, **task.callback_kwargs)
            except Exception as e:
                logger.exception(f"Task {task.name} callback error: {e}")

        # Update next run time
        if task.timer_type == TimerType.INTERVAL:
            if task.is_expired():
                task.enabled = False
            else:
                task.next_run = time.time() + task.interval_seconds
        elif task.timer_type == TimerType.DEADLINE:
            task.enabled = False


class TimeoutManager:
    """Manages timeouts for async operations."""

    def __init__(self):
        self._timeouts: Dict[str, asyncio.Future] = {}

    async def with_timeout(
        self,
        timeout_seconds: float,
        coro: Awaitable,
        timeout_message: str = "Operation timed out"
    ) -> Any:
        """Execute a coroutine with a timeout."""
        timeout_id = str(uuid.uuid4())[:8]
        try:
            result = await asyncio.wait_for(coro, timeout=timeout_seconds)
            return result
        except asyncio.TimeoutError:
            raise TimeoutError(f"{timeout_message} ({timeout_seconds}s)")

    def create_deadline(
        self,
        deadline_seconds: float,
        coro: Awaitable,
        on_timeout: Optional[Callable] = None
    ) -> asyncio.Task:
        """Create a deadline for a coroutine."""
        task = asyncio.create_task(coro)
        return task

    async def wait_with_timeout(
        self,
        waitable: Awaitable,
        timeout_seconds: float
    ) -> Any:
        """Wait for a waitable with timeout."""
        return await asyncio.wait_for(waitable, timeout=timeout_seconds)


class AutomationTimerAction:
    """Main action class for automation timer management."""

    def __init__(self):
        self._manager = AutomationTimerManager()
        self._loop_task: Optional[asyncio.Task] = None

    async def start_loop(self) -> None:
        """Start the timer loop."""
        if self._loop_task is None:
            self._loop_task = asyncio.create_task(self._manager.run_loop())

    async def stop_loop(self) -> None:
        """Stop the timer loop."""
        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
            self._loop_task = None

    async def execute(
        self,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the automation timer action.

        Args:
            context: Dictionary containing:
                - operation: Operation to perform
                - Other operation-specific fields

        Returns:
            Dictionary with operation results.
        """
        operation = context.get("operation", "create_delay")

        if operation == "create_delay":
            task = await self._manager.create_delay(
                name=context.get("name", "delay_task"),
                delay_seconds=context.get("delay_seconds", 1.0),
                callback=None
            )
            return {
                "success": True,
                "task": {
                    "task_id": task.task_id,
                    "name": task.name,
                    "timer_type": task.timer_type.value,
                    "next_run": task.next_run
                }
            }

        elif operation == "create_interval":
            task = await self._manager.create_interval(
                name=context.get("name", "interval_task"),
                interval_seconds=context.get("interval_seconds", 1.0),
                max_runs=context.get("max_runs", 0)
            )
            return {
                "success": True,
                "task": {
                    "task_id": task.task_id,
                    "name": task.name,
                    "timer_type": task.timer_type.value,
                    "interval_seconds": task.interval_seconds
                }
            }

        elif operation == "create_deadline":
            task = await self._manager.create_deadline(
                name=context.get("name", "deadline_task"),
                deadline_seconds=context.get("deadline_seconds", 60.0)
            )
            return {
                "success": True,
                "task": {
                    "task_id": task.task_id,
                    "name": task.name,
                    "timer_type": task.timer_type.value,
                    "deadline": task.deadline
                }
            }

        elif operation == "cancel":
            success = await self._manager.cancel_task(context.get("task_id", ""))
            return {"success": success}

        elif operation == "enable":
            success = await self._manager.enable_task(context.get("task_id", ""))
            return {"success": success}

        elif operation == "disable":
            success = await self._manager.disable_task(context.get("task_id", ""))
            return {"success": success}

        elif operation == "list":
            tasks = await self._manager.list_tasks(
                enabled_only=context.get("enabled_only", False)
            )
            return {"success": True, "tasks": tasks}

        elif operation == "start_loop":
            await self.start_loop()
            return {"success": True, "message": "Timer loop started"}

        elif operation == "stop_loop":
            await self.stop_loop()
            return {"success": True, "message": "Timer loop stopped"}

        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
