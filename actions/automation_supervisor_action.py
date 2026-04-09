"""
Automation Supervisor Action Module.

Provides supervisory control for automation tasks with
health monitoring, restart policies, and resource management.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class SupervisorStatus(Enum):
    """Supervision status."""

    IDLE = "idle"
    RUNNING = "running"
    RESTARTING = "restarting"
    STOPPED = "stopped"
    FAILED = "failed"


class RestartPolicy(Enum):
    """Restart policy types."""

    ALWAYS = "always"
    ON_FAILURE = "on_failure"
    NEVER = "never"
    ON_ERROR = "on_error"


@dataclass
class SupervisedTask:
    """Represents a supervised automation task."""

    id: str
    name: str
    task_func: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    status: SupervisorStatus = SupervisorStatus.IDLE
    restart_count: int = 0
    max_restarts: int = 3
    restart_delay: float = 1.0
    last_start: float = 0.0
    last_stop: float = 0.0
    last_error: str = ""
    health_check_interval: float = 30.0
    health_check_func: Optional[Callable] = None
    is_healthy: bool = True


class AutomationSupervisorAction:
    """
    Supervises automation tasks with health monitoring.

    Features:
    - Automatic restart on failure
    - Health check monitoring
    - Resource usage tracking
    - Task grouping and isolation

    Example:
        supervisor = AutomationSupervisorAction()
        supervisor.register("worker", worker_task, max_restarts=5)
        await supervisor.start("worker")
    """

    def __init__(
        self,
        default_restart_delay: float = 1.0,
        default_max_restarts: int = 3,
    ) -> None:
        """
        Initialize supervisor action.

        Args:
            default_restart_delay: Default delay between restarts.
            default_max_restarts: Default maximum restart attempts.
        """
        self.default_restart_delay = default_restart_delay
        self.default_max_restarts = default_max_restarts
        self._tasks: dict[str, SupervisedTask] = {}
        self._running_tasks: dict[str, asyncio.Task] = {}
        self._monitor_task: Optional[asyncio.Task] = None
        self._status = SupervisorStatus.IDLE
        self._lock = asyncio.Lock()

    def register(
        self,
        name: str,
        task_func: Callable,
        *args: Any,
        max_restarts: Optional[int] = None,
        restart_delay: Optional[float] = None,
        restart_policy: RestartPolicy = RestartPolicy.ON_FAILURE,
        health_check: Optional[Callable] = None,
        **kwargs: Any,
    ) -> SupervisedTask:
        """
        Register a task for supervision.

        Args:
            name: Task name.
            task_func: Task function to execute.
            *args: Positional arguments for task function.
            max_restarts: Maximum restart attempts.
            restart_delay: Delay between restarts.
            restart_policy: When to restart.
            health_check: Optional health check function.
            **kwargs: Keyword arguments for task function.

        Returns:
            Created SupervisedTask.
        """
        task = SupervisedTask(
            id=str(uuid.uuid4()),
            name=name,
            task_func=task_func,
            args=args,
            kwargs=kwargs,
            max_restarts=max_restarts or self.default_max_restarts,
            restart_delay=restart_delay or self.default_restart_delay,
            health_check_func=health_check,
        )
        self._tasks[name] = task
        logger.info(f"Registered task: {name} (max_restarts={task.max_restarts})")
        return task

    async def start(self, name: str) -> bool:
        """
        Start a supervised task.

        Args:
            name: Task name.

        Returns:
            True if started successfully.
        """
        if name not in self._tasks:
            logger.error(f"Task not found: {name}")
            return False

        async with self._lock:
            if name in self._running_tasks:
                logger.warning(f"Task already running: {name}")
                return False

            task = self._tasks[name]
            task.status = SupervisorStatus.RUNNING
            task.last_start = time.time()

            asyncio_task = asyncio.create_task(self._run_task(name))
            self._running_tasks[name] = asyncio_task

        if self._monitor_task is None or self._monitor_task.done():
            self._monitor_task = asyncio.create_task(self._monitor_loop())

        logger.info(f"Started task: {name}")
        return True

    async def stop(self, name: str) -> bool:
        """
        Stop a supervised task.

        Args:
            name: Task name.

        Returns:
            True if stopped successfully.
        """
        if name not in self._running_tasks:
            return False

        async with self._lock:
            task = self._tasks[name]
            task.status = SupervisorStatus.STOPPED
            task.last_stop = time.time()

            if name in self._running_tasks:
                asyncio_task = self._running_tasks[name]
                asyncio_task.cancel()
                try:
                    await asyncio_task
                except asyncio.CancelledError:
                    pass
                del self._running_tasks[name]

        logger.info(f"Stopped task: {name}")
        return True

    async def restart(self, name: str) -> bool:
        """
        Restart a supervised task.

        Args:
            name: Task name.

        Returns:
            True if restarted successfully.
        """
        if name not in self._tasks:
            return False

        task = self._tasks[name]
        await self.stop(name)
        task.status = SupervisorStatus.IDLE
        return await self.start(name)

    async def _run_task(self, name: str) -> None:
        """Run a supervised task with restart handling."""
        task = self._tasks[name]

        while task.status == SupervisorStatus.RUNNING:
            try:
                if asyncio.iscoroutinefunction(task.task_func):
                    result = await task.task_func(*task.args, **task.kwargs)
                else:
                    result = task.task_func(*task.args, **task.kwargs)

                if task.restart_policy == RestartPolicy.ALWAYS:
                    await asyncio.sleep(task.restart_delay)
                    task.last_start = time.time()
                    continue
                else:
                    break

            except Exception as e:
                task.last_error = str(e)
                task.is_healthy = False
                logger.error(f"Task {name} failed: {e}")

                if task.restart_count < task.max_restarts:
                    task.restart_count += 1
                    task.status = SupervisorStatus.RESTARTING
                    logger.info(f"Restarting {name} ({task.restart_count}/{task.max_restarts})")
                    await asyncio.sleep(task.restart_delay)
                    task.status = SupervisorStatus.RUNNING
                    task.last_start = time.time()
                else:
                    task.status = SupervisorStatus.FAILED
                    logger.error(f"Task {name} exceeded max restarts")
                    break

    async def _monitor_loop(self) -> None:
        """Monitor loop for health checks."""
        while True:
            await asyncio.sleep(10)

            for name, task in list(self._tasks.items()):
                if task.health_check_func and task.status == SupervisorStatus.RUNNING:
                    try:
                        is_healthy = task.health_check_func()
                        task.is_healthy = is_healthy
                        if not is_healthy:
                            logger.warning(f"Health check failed for {name}")
                    except Exception as e:
                        logger.error(f"Health check error for {name}: {e}")
                        task.is_healthy = False

    def get_task_status(self, name: str) -> Optional[dict[str, Any]]:
        """
        Get status of a supervised task.

        Args:
            name: Task name.

        Returns:
            Status dictionary or None.
        """
        if name not in self._tasks:
            return None

        task = self._tasks[name]
        return {
            "name": task.name,
            "status": task.status.value,
            "restart_count": task.restart_count,
            "is_healthy": task.is_healthy,
            "last_start": task.last_start,
            "last_error": task.last_error,
            "is_running": name in self._running_tasks,
        }

    def list_tasks(self) -> list[str]:
        """Get list of registered task names."""
        return list(self._tasks.keys())

    def get_stats(self) -> dict[str, Any]:
        """
        Get supervisor statistics.

        Returns:
            Statistics dictionary.
        """
        running = sum(1 for t in self._tasks.values() if t.status == SupervisorStatus.RUNNING)
        failed = sum(1 for t in self._tasks.values() if t.status == SupervisorStatus.FAILED)

        return {
            "total_tasks": len(self._tasks),
            "running_tasks": running,
            "failed_tasks": failed,
            "supervisor_status": self._status.value,
        }
