"""Automation Coordinator Action Module.

Provides task coordination with dependencies, parallel execution,
barriers, and semaphore-based concurrency control.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class TaskStatus(Enum):
    """Task status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class Task:
    """Coordinated task."""
    task_id: str
    func: Callable
    dependencies: List[str] = field(default_factory=list)
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: Optional[Exception] = None


class AutomationCoordinatorAction:
    """Task coordinator with dependency management.

    Example:
        coordinator = AutomationCoordinatorAction()

        coordinator.add_task("a", lambda: fetch_a())
        coordinator.add_task("b", lambda: fetch_b())
        coordinator.add_task("c", lambda: combine(), dependencies=["a", "b"])

        results = await coordinator.execute_all()
    """

    def __init__(self, max_concurrency: int = 10) -> None:
        self._tasks: Dict[str, Task] = {}
        self._max_concurrency = max_concurrency
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._running: Set[str] = set()
        self._completed: Set[str] = set()
        self._results: Dict[str, Any] = {}
        self._errors: Dict[str, Exception] = {}

    def add_task(
        self,
        task_id: str,
        func: Callable,
        dependencies: Optional[List[str]] = None,
    ) -> "AutomationCoordinatorAction":
        """Add task to coordinator.

        Returns self for chaining.
        """
        self._tasks[task_id] = Task(
            task_id=task_id,
            func=func,
            dependencies=dependencies or [],
        )
        return self

    async def execute_all(self) -> Dict[str, Any]:
        """Execute all tasks respecting dependencies."""
        self._completed.clear()
        self._results.clear()
        self._errors.clear()

        pending = [
            task_id for task_id, task in self._tasks.items()
            if not task.dependencies
        ]

        running_tasks = [
            asyncio.create_task(self._execute_task(task_id))
            for task_id in pending
        ]

        if running_tasks:
            await asyncio.gather(*running_tasks, return_exceptions=True)

        return self._results

    async def _execute_task(self, task_id: str) -> Any:
        """Execute single task with semaphore."""
        async with self._semaphore:
            task = self._tasks.get(task_id)
            if not task:
                return

            task.status = TaskStatus.RUNNING
            self._running.add(task_id)

            logger.debug(f"Executing task: {task_id}")

            try:
                if asyncio.iscoroutinefunction(task.func):
                    result = await task.func()
                else:
                    result = task.func()

                task.status = TaskStatus.COMPLETED
                task.result = result
                self._results[task_id] = result
                self._completed.add(task_id)

                await self._execute_dependents(task_id)

            except Exception as e:
                logger.error(f"Task {task_id} failed: {e}")
                task.status = TaskStatus.FAILED
                task.error = e
                self._errors[task_id] = e

            finally:
                self._running.discard(task_id)

    async def _execute_dependents(self, completed_task_id: str) -> None:
        """Execute tasks that depend on completed task."""
        newly_ready = []

        for task_id, task in self._tasks.items():
            if task_id in self._completed or task_id in self._running:
                continue

            if completed_task_id in task.dependencies:
                remaining_deps = [
                    dep for dep in task.dependencies
                    if dep not in self._completed
                ]
                if not remaining_deps:
                    newly_ready.append(task_id)

        if newly_ready:
            tasks = [
                asyncio.create_task(self._execute_task(task_id))
                for task_id in newly_ready
            ]
            await asyncio.gather(*tasks, return_exceptions=True)

    def get_status(self, task_id: str) -> Optional[TaskStatus]:
        """Get status of task."""
        task = self._tasks.get(task_id)
        return task.status if task else None

    def get_results(self) -> Dict[str, Any]:
        """Get all task results."""
        return dict(self._results)

    def get_errors(self) -> Dict[str, str]:
        """Get all task errors."""
        return {k: str(v) for k, v in self._errors.items()}

    def clear(self) -> None:
        """Clear all tasks and results."""
        self._tasks.clear()
        self._completed.clear()
        self._running.clear()
        self._results.clear()
        self._errors.clear()
