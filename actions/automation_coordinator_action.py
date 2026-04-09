"""
Automation Coordinator Action Module.

Coordinates multiple automation agents/tasks with dependency
management, resource locking, and execution orchestration.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import Enum
import logging
import uuid

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    BLOCKED = "blocked"


@dataclass
class CoordinatorTask:
    """
    Task definition for the coordinator.

    Attributes:
        task_id: Unique task identifier.
        name: Task display name.
        func: Async function to execute.
        args: Positional arguments.
        kwargs: Keyword arguments.
        dependencies: List of task IDs that must complete first.
        priority: Task priority (higher = sooner).
        timeout: Maximum execution time in seconds.
        retries: Number of retries on failure.
    """
    task_id: str
    name: str
    func: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    dependencies: list[str] = field(default_factory=list)
    priority: int = 0
    timeout: float = 300.0
    retries: int = 0
    status: TaskStatus = field(default=TaskStatus.PENDING, init=False)
    result: Any = field(default=None, init=False)
    error: Optional[Exception] = field(default=None, init=False)
    is_running: bool = field(default=False, init=False)


@dataclass
class ResourceLock:
    """Resource lock for coordination."""
    resource_id: str
    holder: Optional[str] = None
    waiters: list[str] = field(default_factory=list)


class AutomationCoordinatorAction:
    """
    Coordinates execution of multiple automation tasks with dependencies.

    Example:
        coordinator = AutomationCoordinatorAction()
        coordinator.add_task("task1", func1, priority=10)
        coordinator.add_task("task2", func2, dependencies=["task1"])
        coordinator.add_task("task3", func3, dependencies=["task1"])
        await coordinator.execute_all()
    """

    def __init__(self, max_concurrent: int = 5):
        """
        Initialize automation coordinator.

        Args:
            max_concurrent: Maximum parallel task executions.
        """
        self.tasks: dict[str, CoordinatorTask] = {}
        self.max_concurrent = max_concurrent
        self._running_tasks: set[str] = set()
        self._completed_tasks: set[str] = set()
        self._locks: dict[str, ResourceLock] = {}
        self._semaphore: asyncio.Semaphore = asyncio.Semaphore(max_concurrent)
        self._cancelled = False

    def add_task(
        self,
        name: str,
        func: Callable,
        *args: Any,
        task_id: Optional[str] = None,
        dependencies: Optional[list[str]] = None,
        priority: int = 0,
        timeout: float = 300.0,
        retries: int = 0,
        **kwargs: Any
    ) -> str:
        """
        Add a task to the coordinator.

        Args:
            name: Task display name.
            func: Async function to execute.
            *args: Positional arguments.
            task_id: Optional task ID (auto-generated if None).
            dependencies: List of task IDs that must complete first.
            priority: Task priority.
            timeout: Maximum execution time.
            retries: Number of retries on failure.
            **kwargs: Keyword arguments.

        Returns:
            Task ID string.
        """
        tid = task_id or str(uuid.uuid4())[:8]

        task = CoordinatorTask(
            task_id=tid,
            name=name,
            func=func,
            args=args,
            kwargs=kwargs,
            dependencies=dependencies or [],
            priority=priority,
            timeout=timeout,
            retries=retries
        )

        self.tasks[tid] = task
        logger.info(f"Added task '{name}' with ID {tid}, dependencies: {task.dependencies}")
        return tid

    def add_task_dependency(self, task_id: str, depends_on: str) -> None:
        """Add a dependency to an existing task."""
        if task_id in self.tasks:
            if depends_on not in self.tasks[task_id].dependencies:
                self.tasks[task_id].dependencies.append(depends_on)

    def remove_task(self, task_id: str) -> bool:
        """Remove a task from the coordinator."""
        if task_id in self.tasks:
            del self.tasks[task_id]
            return True
        return False

    def _get_ready_tasks(self) -> list[CoordinatorTask]:
        """Get tasks that are ready to run (all dependencies met)."""
        ready = []

        for task in self.tasks.values():
            if task.status != TaskStatus.PENDING:
                continue

            if task.is_running or task.task_id in self._running_tasks:
                continue

            deps_met = all(
                dep in self._completed_tasks
                for dep in task.dependencies
            )

            if deps_met:
                ready.append(task)

        ready.sort(key=lambda t: t.priority, reverse=True)
        return ready

    async def execute_all(self) -> dict[str, Any]:
        """
        Execute all tasks respecting dependencies and concurrency.

        Returns:
            Dict mapping task IDs to results.
        """
        self._cancelled = False
        self._completed_tasks.clear()
        results = {}

        while not self._cancelled:
            ready = self._get_ready_tasks()

            if not ready and len(self._running_tasks) == 0:
                break

            if not ready:
                await asyncio.sleep(0.1)
                continue

            task = ready[0]
            asyncio.create_task(self._run_task(task))

            await asyncio.sleep(0.01)

        for task_id, task in self.tasks.items():
            results[task_id] = {
                "name": task.name,
                "status": task.status.value,
                "result": task.result,
                "error": str(task.error) if task.error else None
            }

        return results

    async def _run_task(self, task: CoordinatorTask) -> None:
        """Execute a single task with semaphore and retries."""
        async with self._semaphore:
            if self._cancelled:
                task.status = TaskStatus.CANCELLED
                return

            task.is_running = True
            task.status = TaskStatus.RUNNING
            self._running_tasks.add(task.task_id)

            logger.info(f"Running task: {task.name} ({task.task_id})")

            attempts = task.retries + 1

            for attempt in range(attempts):
                try:
                    if asyncio.iscoroutinefunction(task.func):
                        result = await asyncio.wait_for(
                            task.func(*task.args, **task.kwargs),
                            timeout=task.timeout
                        )
                    else:
                        result = task.func(*task.args, **task.kwargs)

                    task.result = result
                    task.status = TaskStatus.COMPLETED
                    self._completed_tasks.add(task.task_id)
                    logger.info(f"Task completed: {task.name} ({task.task_id})")
                    break

                except asyncio.TimeoutError:
                    task.error = TimeoutError(f"Task timed out after {task.timeout}s")
                    logger.error(f"Task timeout: {task.name} ({task.task_id})")

                except Exception as e:
                    task.error = e
                    logger.error(f"Task failed: {task.name} ({task.task_id}): {e}")

                    if attempt == attempts - 1:
                        task.status = TaskStatus.FAILED
                    else:
                        await asyncio.sleep(2 ** attempt)

                if task.status == TaskStatus.FAILED:
                    break

            task.is_running = False
            self._running_tasks.discard(task.task_id)

    async def cancel_all(self) -> None:
        """Cancel all running and pending tasks."""
        self._cancelled = True

        for task in self.tasks.values():
            if task.status == TaskStatus.RUNNING:
                task.status = TaskStatus.CANCELLED

        logger.info("All tasks cancelled")

    def get_task_status(self, task_id: str) -> Optional[dict]:
        """Get status of a specific task."""
        if task_id not in self.tasks:
            return None

        task = self.tasks[task_id]
        return {
            "task_id": task.task_id,
            "name": task.name,
            "status": task.status.value,
            "result": task.result,
            "error": str(task.error) if task.error else None,
            "is_running": task.is_running
        }

    def get_execution_graph(self) -> dict:
        """Get the task dependency graph."""
        graph = {}

        for task_id, task in self.tasks.items():
            graph[task_id] = {
                "name": task.name,
                "dependencies": task.dependencies,
                "dependents": [
                    t.task_id for t in self.tasks.values()
                    if task_id in t.dependencies
                ],
                "status": task.status.value,
                "priority": task.priority
            }

        return graph

    def acquire_lock(self, resource_id: str, task_id: str) -> bool:
        """
        Acquire a lock on a resource.

        Args:
            resource_id: Resource identifier.
            task_id: Task requesting the lock.

        Returns:
            True if lock acquired, False if held by another.
        """
        if resource_id not in self._locks:
            self._locks[resource_id] = ResourceLock(resource_id=resource_id)

        lock = self._locks[resource_id]

        if lock.holder is None:
            lock.holder = task_id
            return True

        if lock.holder == task_id:
            return True

        if task_id not in lock.waiters:
            lock.waiters.append(task_id)

        return False

    def release_lock(self, resource_id: str, task_id: str) -> bool:
        """Release a lock on a resource."""
        if resource_id not in self._locks:
            return False

        lock = self._locks[resource_id]

        if lock.holder == task_id:
            if lock.waiters:
                lock.holder = lock.waiters.pop(0)
            else:
                lock.holder = None
            return True

        return False
