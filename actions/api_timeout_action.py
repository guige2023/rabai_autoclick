"""API timeout and deadline handling utilities.

This module provides timeout management:
- Request timeout with cancellation
- Deadline propagation
- Timeout retry logic
- Resource cleanup

Example:
    >>> from actions.api_timeout_action import with_timeout, Deadline
    >>> result = with_timeout(fetch_data, timeout=5.0)
"""

from __future__ import annotations

import time
import threading
import logging
from typing import Any, Callable, Optional
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class TimeoutError(Exception):
    """Raised when an operation times out."""
    pass


class DeadlineExceeded(Exception):
    """Raised when deadline is exceeded."""
    pass


@dataclass
class Deadline:
    """Represents a deadline for an operation."""
    start_time: float
    duration: float

    @property
    def end_time(self) -> float:
        return self.start_time + self.duration

    @property
    def remaining(self) -> float:
        return max(0, self.end_time - time.time())

    @property
    def is_expired(self) -> bool:
        return time.time() >= self.end_time


def with_timeout(
    func: Callable[..., Any],
    timeout: float,
    *args: Any,
    default: Any = None,
    **kwargs: Any,
) -> Any:
    """Execute a function with a timeout.

    Args:
        func: Function to execute.
        timeout: Timeout in seconds.
        *args: Positional arguments for the function.
        default: Default value to return on timeout.
        **kwargs: Keyword arguments for the function.

    Returns:
        Function result or default value on timeout.
    """
    result = [None]
    error = [None]

    def worker() -> None:
        try:
            result[0] = func(*args, **kwargs)
        except Exception as e:
            error[0] = e

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join(timeout=timeout)
    if thread.is_alive():
        if default is not None:
            return default
        raise TimeoutError(f"Function '{func.__name__}' timed out after {timeout}s")
    if error[0]:
        raise error[0]
    return result[0]


async def with_deadline(
    func: Callable[..., Any],
    deadline: Deadline,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Execute a function within a deadline.

    Args:
        func: Function to execute.
        deadline: Deadline to observe.
        *args: Positional arguments.
        **kwargs: Keyword arguments.

    Returns:
        Function result.

    Raises:
        DeadlineExceeded: If deadline is exceeded.
    """
    remaining = deadline.remaining
    if remaining <= 0:
        raise DeadlineExceeded("Deadline already exceeded")
    return with_timeout(func, remaining, *args, **kwargs)


class TimeoutManager:
    """Manage multiple timeouts.

    Example:
        >>> manager = TimeoutManager()
        >>> task_id = manager.add_task(long_running_func, timeout=10.0)
        >>> manager.cancel(task_id)
    """

    def __init__(self) -> None:
        self._tasks: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def add_task(
        self,
        func: Callable[..., Any],
        timeout: float,
        *args: Any,
        **kwargs: Any,
    ) -> str:
        """Add a task with timeout.

        Args:
            func: Function to execute.
            timeout: Timeout in seconds.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Task ID.
        """
        import uuid
        task_id = str(uuid.uuid4())[:8]
        thread = threading.Thread(target=self._run_task, args=(task_id, func, timeout, args, kwargs))
        thread.daemon = True
        with self._lock:
            self._tasks[task_id] = {
                "thread": thread,
                "running": False,
                "completed": False,
                "result": None,
                "error": None,
            }
        thread.start()
        return task_id

    def _run_task(
        self,
        task_id: str,
        func: Callable[..., Any],
        timeout: float,
        args: tuple,
        kwargs: dict,
    ) -> None:
        """Run a task with timeout tracking."""
        with self._lock:
            self._tasks[task_id]["running"] = True
        result = None
        error = None
        try:
            result = with_timeout(func, timeout, *args, **kwargs)
        except TimeoutError as e:
            error = e
        except Exception as e:
            error = e
        with self._lock:
            self._tasks[task_id]["completed"] = True
            self._tasks[task_id]["running"] = False
            self._tasks[task_id]["result"] = result
            self._tasks[task_id]["error"] = error

    def cancel(self, task_id: str) -> bool:
        """Cancel a running task.

        Note: This only marks the task for cancellation;
        the thread must check periodically.
        """
        with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id]["cancelled"] = True
                return True
        return False

    def get_status(self, task_id: str) -> Optional[dict[str, Any]]:
        """Get task status."""
        with self._lock:
            return self._tasks.get(task_id)

    def wait_for_completion(self, task_id: str, timeout: Optional[float] = None) -> Any:
        """Wait for task completion and return result."""
        start = time.time()
        with self._lock:
            while task_id not in self._tasks or not self._tasks[task_id].get("completed"):
                if timeout and (time.time() - start) >= timeout:
                    raise TimeoutError(f"Task {task_id} did not complete within {timeout}s")
                time.sleep(0.1)
        status = self._tasks[task_id]
        if status.get("error"):
            raise status["error"]
        return status.get("result")


class RetryWithTimeout:
    """Retry a function with timeout per attempt.

    Example:
        >>> retry = RetryWithTimeout(max_attempts=3, timeout_per_attempt=5.0)
        >>> result = retry.execute(flaky_api_call)
    """

    def __init__(
        self,
        max_attempts: int = 3,
        timeout_per_attempt: float = 5.0,
        backoff_factor: float = 1.0,
    ) -> None:
        self.max_attempts = max_attempts
        self.timeout_per_attempt = timeout_per_attempt
        self.backoff_factor = backoff_factor

    def execute(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Execute with retries.

        Args:
            func: Function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Function result.

        Raises:
            TimeoutError: If all attempts timeout.
        """
        last_error = None
        for attempt in range(self.max_attempts):
            try:
                timeout = self.timeout_per_attempt * (self.backoff_factor ** attempt)
                return with_timeout(func, timeout, *args, **kwargs)
            except TimeoutError as e:
                last_error = e
                logger.warning(f"Attempt {attempt + 1} timed out: {e}")
        raise TimeoutError(f"All {self.max_attempts} attempts timed out") from last_error
