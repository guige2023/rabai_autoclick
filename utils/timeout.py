"""Timeout utilities for RabAI AutoClick.

Provides:
- Timeout contexts
- Timeout decorators
- Timeout management
"""

import functools
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional, TypeVar


T = TypeVar("T")


class TimeoutError(Exception):
    """Timeout error."""
    pass


@dataclass
class TimeoutResult:
    """Result of a timed operation."""
    success: bool
    value: Any = None
    elapsed: float = 0
    error: Optional[str] = None


class Timeout:
    """Timeout context manager."""

    def __init__(self, seconds: float, message: Optional[str] = None) -> None:
        """Initialize timeout.

        Args:
            seconds: Timeout in seconds.
            message: Optional error message.
        """
        self._seconds = seconds
        self._message = message or f"Operation timed out after {seconds}s"
        self._elapsed = 0

    @property
    def elapsed(self) -> float:
        """Get elapsed time."""
        return self._elapsed

    def __enter__(self) -> "Timeout":
        """Enter context."""
        self._start = time.time()
        return self

    def __exit__(self, *args: Any) -> None:
        """Exit context."""
        self._elapsed = time.time() - self._start

    def check(self) -> None:
        """Check if timeout exceeded."""
        if self._elapsed >= self._seconds:
            raise TimeoutError(self._message)


def timeout_decorator(seconds: float, default: Any = None) -> Callable:
    """Decorator to add timeout to function.

    Args:
        seconds: Timeout in seconds.
        default: Default value on timeout.

    Returns:
        Decorated function.
    """
    def decorator(func: Callable[..., T]) -> Callable[..., Optional[T]]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Optional[T]:
            result = [default]
            error = [None]
            finished = threading.Event()

            def target():
                try:
                    result[0] = func(*args, **kwargs)
                except Exception as e:
                    error[0] = e
                finally:
                    finished.set()

            thread = threading.Thread(target=target)
            thread.start()

            if not finished.wait(seconds):
                thread.join(timeout=0.1)
                return default

            if error[0]:
                raise error[0]

            return result[0]

        return wrapper
    return decorator


def timeout_call(
    func: Callable[..., T],
    seconds: float,
    *args: Any,
    **kwargs: Any,
) -> TimeoutResult:
    """Call function with timeout.

    Args:
        func: Function to call.
        seconds: Timeout in seconds.
        *args: Positional arguments.
        **kwargs: Keyword arguments.

    Returns:
        TimeoutResult with success status and value/error.
    """
    start = time.time()
    result = [None]
    error = [None]
    finished = threading.Event()

    def target():
        try:
            result[0] = func(*args, **kwargs)
        except Exception as e:
            error[0] = e
        finally:
            finished.set()

    thread = threading.Thread(target=target)
    thread.start()

    if not finished.wait(seconds):
        thread.join(timeout=0.1)
        elapsed = time.time() - start
        return TimeoutResult(
            success=False,
            elapsed=elapsed,
            error=f"Timed out after {seconds}s",
        )

    elapsed = time.time() - start

    if error[0]:
        return TimeoutResult(
            success=False,
            elapsed=elapsed,
            error=str(error[0]),
        )

    return TimeoutResult(
        success=True,
        value=result[0],
        elapsed=elapsed,
    )


class TimeoutManager:
    """Manage multiple timeouts."""

    def __init__(self) -> None:
        """Initialize manager."""
        self._timeouts: Dict[str, threading.Timer] = {}
        self._callbacks: Dict[str, Callable] = {}
        self._lock = threading.Lock()

    def set_timeout(
        self,
        name: str,
        seconds: float,
        callback: Callable[[], None],
    ) -> None:
        """Set a named timeout.

        Args:
            name: Timeout name.
            seconds: Seconds until timeout.
            callback: Function to call on timeout.
        """
        self.cancel(name)

        with self._lock:
            timer = threading.Timer(seconds, self._execute, args=(name,))
            self._timeouts[name] = timer
            self._callbacks[name] = callback
            timer.start()

    def _execute(self, name: str) -> None:
        """Execute timeout callback."""
        callback = None
        with self._lock:
            callback = self._callbacks.get(name)
            if name in self._timeouts:
                del self._timeouts[name]
            if name in self._callbacks:
                del self._callbacks[name]

        if callback:
            try:
                callback()
            except Exception:
                pass

    def cancel(self, name: str) -> bool:
        """Cancel a named timeout.

        Args:
            name: Timeout name.

        Returns:
            True if cancelled.
        """
        with self._lock:
            if name in self._timeouts:
                self._timeouts[name].cancel()
                del self._timeouts[name]
                if name in self._callbacks:
                    del self._callbacks[name]
                return True
        return False

    def cancel_all(self) -> None:
        """Cancel all timeouts."""
        with self._lock:
            for timer in self._timeouts.values():
                timer.cancel()
            self._timeouts.clear()
            self._callbacks.clear()


class RetryTimeout:
    """Timeout with retry support."""

    def __init__(
        self,
        max_attempts: int = 3,
        timeout_seconds: float = 5.0,
        backoff: float = 1.0,
    ) -> None:
        """Initialize retry timeout.

        Args:
            max_attempts: Maximum retry attempts.
            timeout_seconds: Timeout per attempt.
            backoff: Backoff multiplier between attempts.
        """
        self._max_attempts = max_attempts
        self._timeout = timeout_seconds
        self._backoff = backoff

    def execute(self, func: Callable[..., T]) -> TimeoutResult:
        """Execute function with retries.

        Args:
            func: Function to execute.

        Returns:
            TimeoutResult.
        """
        last_error = None
        attempt = 0
        wait_time = self._timeout

        while attempt < self._max_attempts:
            result = timeout_call(func, wait_time)
            if result.success:
                return result

            last_error = result.error
            attempt += 1

            if attempt < self._max_attempts:
                time.sleep(wait_time * self._backoff)
                wait_time *= self._backoff

        return TimeoutResult(
            success=False,
            error=f"Failed after {self._max_attempts} attempts: {last_error}",
        )


from typing import Dict
