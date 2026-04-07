"""Task management utilities for RabAI AutoClick.

Provides:
- Task definitions
- Task execution
- Task tracking
"""

import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


@dataclass
class TaskResult:
    """Result of task execution."""
    status: TaskStatus
    value: Any = None
    error: Optional[Exception] = None
    duration: float = 0


class Task(ABC):
    """Base task class."""

    def __init__(self, name: str) -> None:
        """Initialize task.

        Args:
            name: Task name.
        """
        self.name = name
        self._status = TaskStatus.PENDING
        self._result: Optional[TaskResult] = None

    @abstractmethod
    def execute(self) -> Any:
        """Execute task.

        Returns:
            Task result.
        """
        pass

    @property
    def status(self) -> TaskStatus:
        """Get task status."""
        return self._status

    @property
    def result(self) -> Optional[TaskResult]:
        """Get task result."""
        return self._result

    def run(self) -> TaskResult:
        """Run task and return result.

        Returns:
            Task result.
        """
        self._status = TaskStatus.RUNNING
        start = time.time()

        try:
            value = self.execute()
            duration = time.time() - start
            self._result = TaskResult(
                status=TaskStatus.COMPLETED,
                value=value,
                duration=duration,
            )
            self._status = TaskStatus.COMPLETED
        except Exception as e:
            duration = time.time() - start
            self._result = TaskResult(
                status=TaskStatus.FAILED,
                error=e,
                duration=duration,
            )
            self._status = TaskStatus.FAILED

        return self._result


class CallableTask(Task):
    """Task wrapping a callable."""

    def __init__(
        self,
        name: str,
        func: Callable[[], Any],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Initialize callable task.

        Args:
            name: Task name.
            func: Function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        """
        super().__init__(name)
        self._func = func
        self._args = args
        self._kwargs = kwargs

    def execute(self) -> Any:
        """Execute wrapped callable."""
        return self._func(*self._args, **self._kwargs)


class TaskGroup:
    """Group of tasks for batch execution."""

    def __init__(self, name: str) -> None:
        """Initialize task group.

        Args:
            name: Group name.
        """
        self.name = name
        self._tasks: List[Task] = []

    def add(self, task: Task) -> "TaskGroup":
        """Add task to group.

        Args:
            task: Task to add.

        Returns:
            Self for chaining.
        """
        self._tasks.append(task)
        return self

    def run_all(self) -> List[TaskResult]:
        """Run all tasks.

        Returns:
            List of results.
        """
        return [task.run() for task in self._tasks]

    def run_parallel(self, num_workers: int = 4) -> List[TaskResult]:
        """Run tasks in parallel.

        Args:
            num_workers: Number of worker threads.

        Returns:
            List of results.
        """
        results: Dict[int, TaskResult] = {}
        threads: List[threading.Thread] = []
        lock = threading.Lock()

        def run_task(index: int, task: Task) -> None:
            result = task.run()
            with lock:
                results[index] = result

        for i, task in enumerate(self._tasks):
            thread = threading.Thread(target=run_task, args=(i, task))
            thread.start()
            threads.append(thread)

            if len(threads) >= num_workers:
                threads[0].join()
                threads.pop(0)

        for thread in threads:
            thread.join()

        return [results[i] for i in range(len(self._tasks))]


class TaskQueue:
    """Queue of tasks for sequential execution."""

    def __init__(self) -> None:
        """Initialize task queue."""
        self._queue: List[Task] = []
        self._results: List[TaskResult] = []
        self._running = False
        self._cancelled = False

    def add(self, task: Task) -> "TaskQueue":
        """Add task to queue.

        Args:
            task: Task to add.

        Returns:
            Self for chaining.
        """
        self._queue.append(task)
        return self

    def run(self) -> List[TaskResult]:
        """Run all queued tasks.

        Returns:
            List of results.
        """
        self._running = True
        self._results.clear()

        while self._queue and not self._cancelled:
            task = self._queue.pop(0)
            result = task.run()
            self._results.append(result)

            if result.status == TaskStatus.FAILED:
                break

        self._running = False
        return self._results

    def cancel(self) -> None:
        """Cancel execution."""
        self._cancelled = True

    @property
    def pending(self) -> int:
        """Get number of pending tasks."""
        return len(self._queue)


class TaskTracker:
    """Track task execution over time."""

    def __init__(self) -> None:
        """Initialize tracker."""
        self._tasks: Dict[str, Task] = {}
        self._history: List[TaskResult] = []
        self._lock = threading.Lock()

    def register(self, task: Task) -> None:
        """Register a task.

        Args:
            task: Task to register.
        """
        with self._lock:
            self._tasks[task.name] = task

    def unregister(self, name: str) -> bool:
        """Unregister a task.

        Args:
            name: Task name.

        Returns:
            True if task was registered.
        """
        with self._lock:
            if name in self._tasks:
                del self._tasks[name]
                return True
            return False

    def track(self, result: TaskResult) -> None:
        """Track task result.

        Args:
            result: Task result to track.
        """
        with self._lock:
            self._history.append(result)

    def get_task(self, name: str) -> Optional[Task]:
        """Get task by name.

        Args:
            name: Task name.

        Returns:
            Task or None.
        """
        with self._lock:
            return self._tasks.get(name)

    def get_history(self, limit: int = 50) -> List[TaskResult]:
        """Get task history.

        Args:
            limit: Maximum entries.

        Returns:
            List of results.
        """
        with self._lock:
            return self._history[-limit:]

    def get_stats(self) -> Dict[str, int]:
        """Get task statistics.

        Returns:
            Dict of status counts.
        """
        with self._lock:
            stats = {status: 0 for status in TaskStatus}
            for result in self._history:
                stats[result.status] += 1
            return stats


class TaskExecutor:
    """Execute tasks with timeout and retry."""

    def __init__(
        self,
        timeout: float = 30.0,
        max_retries: int = 3,
    ) -> None:
        """Initialize executor.

        Args:
            timeout: Default timeout in seconds.
            max_retries: Default max retries.
        """
        self._timeout = timeout
        self._max_retries = max_retries

    def execute(
        self,
        func: Callable[[], Any],
        timeout: Optional[float] = None,
        max_retries: Optional[int] = None,
    ) -> TaskResult:
        """Execute function with timeout and retry.

        Args:
            func: Function to execute.
            timeout: Optional timeout override.
            max_retries: Optional retries override.

        Returns:
            Task result.
        """
        timeout = timeout or self._timeout
        max_retries = max_retries or self._max_retries
        last_error: Optional[Exception] = None

        for attempt in range(max_retries + 1):
            thread = threading.current_thread()
            result = [None]
            error = [None]
            finished = threading.Event()

            def target():
                try:
                    result[0] = func()
                except Exception as e:
                    error[0] = e
                finally:
                    finished.set()

            t = threading.Thread(target=target)

            if attempt == 0:
                # First attempt - can timeout
                t.start()
                if not finished.wait(timeout):
                    t.join(timeout=0.1)
                    return TaskResult(
                        status=TaskStatus.TIMEOUT,
                        error=TimeoutError(f"Task timed out after {timeout}s"),
                    )
            else:
                # Retry - no timeout
                t.start()
                t.join()

            if error[0] is None:
                return TaskResult(
                    status=TaskStatus.COMPLETED,
                    value=result[0],
                )
            last_error = error[0]

        return TaskResult(
            status=TaskStatus.FAILED,
            error=last_error,
        )
