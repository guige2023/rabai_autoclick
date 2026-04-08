"""Task chain execution action module.

Provides sequential and parallel task chain execution with dependencies,
cancellation support, and result aggregation.
"""

from __future__ import annotations

import time
import logging
import threading
from typing import Optional, Dict, Any, Callable, List
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    """Status of a task."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


@dataclass
class Task:
    """A callable task with metadata."""
    name: str
    func: Callable[[], Any]
    dependencies: List[str] = field(default_factory=list)
    timeout_seconds: float = 60.0
    retry_count: int = 0
    result: Any = field(default=None, repr=False)
    error: Optional[str] = field(default=None, repr=False)
    status: TaskStatus = TaskStatus.PENDING
    started_at: Optional[float] = None
    completed_at: Optional[float] = None

    @property
    def duration_ms(self) -> Optional[float]:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at) * 1000
        return None


class TaskChain:
    """Task chain executor with dependency resolution.

    Executes tasks in dependency order, supporting parallel execution
    of independent tasks.

    Example:
        chain = TaskChain()
        chain.add_task("fetch", fetch_data)
        chain.add_task("process", process_data, deps=["fetch"])
        chain.add_task("save", save_data, deps=["process"])

        results = chain.execute()
    """

    def __init__(self, max_workers: int = 4) -> None:
        """Initialize task chain.

        Args:
            max_workers: Maximum parallel task executors.
        """
        self.max_workers = max_workers
        self._tasks: Dict[str, Task] = {}
        self._cancelled = threading.Event()
        self._lock = threading.Lock()

    def add_task(
        self,
        name: str,
        func: Callable[[], Any],
        dependencies: Optional[List[str]] = None,
        timeout_seconds: float = 60.0,
        retry_count: int = 0,
    ) -> "TaskChain":
        """Add a task to the chain.

        Args:
            name: Unique task name.
            func: Callable to execute.
            dependencies: List of task names that must complete first.
            timeout_seconds: Task timeout.
            retry_count: Number of retries on failure.

        Returns:
            Self for chaining.
        """
        with self._lock:
            self._tasks[name] = Task(
                name=name,
                func=func,
                dependencies=dependencies or [],
                timeout_seconds=timeout_seconds,
                retry_count=retry_count,
            )
        return self

    def execute(self) -> Dict[str, Task]:
        """Execute all tasks in dependency order.

        Returns:
            Dict of task_name -> Task with results.
        """
        completed: Dict[str, Task] = {}
        pending = set(self._tasks.keys())

        with self._lock:
            for task in self._tasks.values():
                task.status = TaskStatus.PENDING

        while pending and not self._cancelled.is_set():
            ready = self._get_ready_tasks(completed, pending)
            if not ready:
                if pending:
                    logger.error("Task dependency cycle or missing dependencies: %s", pending)
                    for name in pending:
                        self._tasks[name].status = TaskStatus.FAILED
                        self._tasks[name].error = "Dependency not satisfiable"
                break

            executed = self._execute_batch(ready)
            completed.update(executed)
            pending -= set(executed.keys())

        return dict(self._tasks)

    def cancel(self) -> None:
        """Cancel all pending tasks."""
        self._cancelled.set()

    def get_result(self, name: str) -> Any:
        """Get the result of a completed task."""
        task = self._tasks.get(name)
        return task.result if task else None

    def _get_ready_tasks(
        self,
        completed: Dict[str, Task],
        pending: set,
    ) -> List[str]:
        """Get tasks whose dependencies are all satisfied."""
        ready = []
        for name in pending:
            task = self._tasks[name]
            deps_satisfied = all(dep in completed for dep in task.dependencies)
            if deps_satisfied:
                ready.append(name)
        return ready

    def _execute_batch(self, task_names: List[str]) -> Dict[str, Task]:
        """Execute a batch of tasks in parallel."""
        if len(task_names) == 1:
            name = task_names[0]
            self._execute_single(name)
            return {name: self._tasks[name]}

        threads = []
        with threading.Lock():
            for name in task_names:
                t = threading.Thread(target=self._execute_single, args=(name,), daemon=True)
                t.start()
                threads.append(t)

        for t in threads:
            t.join()

        return {name: self._tasks[name] for name in task_names}

    def _execute_single(self, name: str) -> None:
        """Execute a single task with retry."""
        task = self._tasks[name]

        if self._cancelled.is_set():
            task.status = TaskStatus.CANCELLED
            return

        task.status = TaskStatus.RUNNING
        task.started_at = time.time()

        last_error: Optional[Exception] = None
        for attempt in range(task.retry_count + 1):
            try:
                task.result = self._run_with_timeout(task)
                task.status = TaskStatus.SUCCESS
                task.completed_at = time.time()
                logger.debug("Task '%s' completed successfully", name)
                return
            except TimeoutError:
                task.status = TaskStatus.TIMEOUT
                task.error = f"Timed out after {task.timeout_seconds}s"
                last_error = TimeoutError(task.error)
            except Exception as e:
                last_error = e
                logger.debug("Task '%s' attempt %d failed: %s", name, attempt + 1, e)

        task.status = TaskStatus.FAILED
        task.error = str(last_error)
        task.completed_at = time.time()

    def _run_with_timeout(self, task: Task) -> Any:
        """Run task with timeout using polling."""
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(task.func)
            try:
                return future.result(timeout=task.timeout_seconds)
            except concurrent.futures.TimeoutError:
                raise TimeoutError(f"Task '{task.name}' timed out after {task.timeout_seconds}s")


class TaskChainAction:
    """High-level task chain action.

    Example:
        chain = TaskChainAction(max_workers=4)
        chain.add("fetch", lambda: fetch_data(url))
        chain.add("process", lambda: process(data), deps=["fetch"])
        results = chain.run()
    """

    def __init__(self, max_workers: int = 4) -> None:
        self.chain = TaskChain(max_workers=max_workers)

    def add(
        self,
        name: str,
        func: Callable[[], Any],
        deps: Optional[List[str]] = None,
        **kwargs,
    ) -> None:
        """Add a task to the chain."""
        self.chain.add_task(name, func, dependencies=deps, **kwargs)

    def run(self) -> Dict[str, Any]:
        """Execute the task chain.

        Returns:
            Dict with task results indexed by name.
        """
        self.chain.execute()
        return {name: task.result for name, task in self.chain._tasks.items()}

    def cancel(self) -> None:
        """Cancel execution."""
        self.chain.cancel()
