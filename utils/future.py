"""Async/future utilities for RabAI AutoClick.

Provides:
- Future results
- Async execution
- Promise patterns
"""

import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Optional


class FutureStatus(Enum):
    """Future status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class FutureResult:
    """Result of async operation."""
    status: FutureStatus
    value: Any = None
    error: Optional[Exception] = None


class Future(ABC):
    """Base future class."""

    def __init__(self) -> None:
        """Initialize future."""
        self._status = FutureStatus.PENDING
        self._result: Any = None
        self._error: Optional[Exception] = None
        self._callbacks: list = []
        self._lock = threading.Lock()

    @property
    def status(self) -> FutureStatus:
        """Get future status."""
        return self._status

    @property
    def is_done(self) -> bool:
        """Check if done."""
        return self._status in (
            FutureStatus.COMPLETED,
            FutureStatus.FAILED,
            FutureStatus.CANCELLED,
        )

    @property
    def is_success(self) -> bool:
        """Check if completed successfully."""
        return self._status == FutureStatus.COMPLETED

    @abstractmethod
    def get(self, timeout: Optional[float] = None) -> Any:
        """Get result.

        Args:
            timeout: Optional timeout.

        Returns:
            Result value.

        Raises:
            TimeoutError: If timeout exceeded.
            Exception: If operation failed.
        """
        pass

    def on_complete(self, callback: Callable[[Any], None]) -> None:
        """Register completion callback.

        Args:
            callback: Function to call on completion.
        """
        with self._lock:
            if self.is_done:
                callback(self._result)
            else:
                self._callbacks.append(callback)

    def _notify_complete(self) -> None:
        """Notify callbacks of completion."""
        with self._lock:
            for callback in self._callbacks:
                try:
                    callback(self._result)
                except Exception:
                    pass
            self._callbacks.clear()


class ThreadFuture(Future):
    """Future running in a thread."""

    def __init__(
        self,
        func: Callable[[], Any],
        args: tuple = (),
        kwargs: dict = None,
    ) -> None:
        """Initialize thread future.

        Args:
            func: Function to execute.
            args: Positional arguments.
            kwargs: Keyword arguments.
        """
        super().__init__()
        self._func = func
        self._args = args
        self._kwargs = kwargs or {}
        self._thread: Optional[threading.Thread] = None

    def start(self) -> "ThreadFuture":
        """Start execution.

        Returns:
            Self for chaining.
        """
        self._status = FutureStatus.RUNNING
        self._thread = threading.Thread(target=self._execute)
        self._thread.start()
        return self

    def _execute(self) -> None:
        """Execute function."""
        try:
            result = self._func(*self._args, **self._kwargs)
            with self._lock:
                self._result = result
                self._status = FutureStatus.COMPLETED
            self._notify_complete()
        except Exception as e:
            with self._lock:
                self._error = e
                self._status = FutureStatus.FAILED
            self._notify_complete()

    def get(self, timeout: Optional[float] = None) -> Any:
        """Get result."""
        if self._thread is None:
            self.start()

        self._thread.join(timeout=timeout)

        if self._thread.is_alive():
            raise TimeoutError("Future timed out")

        if self._status == FutureStatus.FAILED:
            raise self._error

        return self._result

    def cancel(self) -> bool:
        """Cancel execution.

        Returns:
            True if cancelled.
        """
        if self._status != FutureStatus.RUNNING:
            return False
        self._status = FutureStatus.CANCELLED
        return True


class Promise:
    """Promise for async operations."""

    def __init__(self) -> None:
        """Initialize promise."""
        self._future = FutureStub()

    @property
    def future(self) -> "FutureStub":
        """Get associated future."""
        return self._future

    def resolve(self, value: Any) -> None:
        """Resolve promise with value.

        Args:
            value: Value to resolve with.
        """
        self._future._set_result(value)

    def reject(self, error: Exception) -> None:
        """Reject promise with error.

        Args:
            error: Error to reject with.
        """
        self._future._set_error(error)


class FutureStub(Future):
    """Future that can be resolved externally."""

    def __init__(self) -> None:
        """Initialize stub."""
        super().__init__()
        self._event = threading.Event()

    def _set_result(self, value: Any) -> None:
        """Set result externally.

        Args:
            value: Result value.
        """
        with self._lock:
            self._result = value
            self._status = FutureStatus.COMPLETED
        self._event.set()
        self._notify_complete()

    def _set_error(self, error: Exception) -> None:
        """Set error externally.

        Args:
            error: Error.
        """
        with self._lock:
            self._error = error
            self._status = FutureStatus.FAILED
        self._event.set()
        self._notify_complete()

    def get(self, timeout: Optional[float] = None) -> Any:
        """Get result."""
        if not self._event.wait(timeout=timeout):
            raise TimeoutError("Future timed out")

        if self._status == FutureStatus.FAILED:
            raise self._error

        return self._result


class AsyncBatch:
    """Run multiple async operations."""

    def __init__(self) -> None:
        """Initialize batch."""
        self._futures: list = []

    def add(
        self,
        func: Callable[[], Any],
        *args: Any,
        **kwargs: Any,
    ) -> ThreadFuture:
        """Add operation to batch.

        Args:
            func: Function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Future for this operation.
        """
        future = ThreadFuture(func, args, kwargs)
        self._futures.append(future)
        return future

    def start_all(self) -> None:
        """Start all operations."""
        for future in self._futures:
            future.start()

    def wait_all(self, timeout: Optional[float] = None) -> list:
        """Wait for all operations.

        Args:
            timeout: Optional timeout.

        Returns:
            List of results.
        """
        results = []
        for future in self._futures:
            try:
                results.append(future.get(timeout=timeout))
            except Exception as e:
                results.append(e)
        return results

    @property
    def futures(self) -> list:
        """Get all futures."""
        return self._futures.copy()


def async_call(
    func: Callable[[], Any],
    *args: Any,
    **kwargs: Any,
) -> ThreadFuture:
    """Execute function asynchronously.

    Args:
        func: Function to execute.
        *args: Positional arguments.
        **kwargs: Keyword arguments.

    Returns:
        Future for the operation.
    """
    return ThreadFuture(func, args, kwargs).start()
