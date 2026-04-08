"""
Timeout guard utilities for operation timeouts.

Provides context managers and decorators for
enforcing operation timeouts with cleanup.
"""

from __future__ import annotations

import signal
import threading
import time
from contextlib import contextmanager
from typing import Callable, TypeVar


T = TypeVar("T")


class TimeoutError(Exception):
    """Raised when operation exceeds timeout."""
    pass


class TimeoutGuard:
    """
    Guard that enforces timeout on a block of code.

    Uses threading for cancellation.
    """

    def __init__(self, timeout_seconds: float):
        self.timeout_seconds = timeout_seconds
        self._cancelled = False
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "TimeoutGuard":
        self._thread = threading.current_thread()
        self._cancelled = False
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass

    def is_cancelled(self) -> bool:
        """Check if timeout was triggered."""
        return self._cancelled

    def cancel(self) -> None:
        """Manually trigger timeout."""
        self._cancelled = True


@contextmanager
def timeout(seconds: float, error_message: str = "Operation timed out"):
    """
    Context manager for timeout.

    Raises TimeoutError if block exceeds timeout.

    Args:
        seconds: Timeout in seconds
        error_message: Error message on timeout

    Example:
        with timeout(5.0):
            # code that must complete within 5 seconds
            pass
    """
    def raise_timeout():
        raise TimeoutError(error_message)

    timer = threading.Timer(seconds, raise_timeout)
    timer.daemon = True
    timer.start()
    try:
        yield
    finally:
        timer.cancel()


@contextmanager
def timeout_with_cleanup(
    seconds: float,
    on_timeout: Callable[[], None] | None = None,
):
    """
    Timeout with cleanup callback.

    Args:
        seconds: Timeout in seconds
        on_timeout: Cleanup function to call on timeout
    """
    def timeout_handler():
        if on_timeout:
            on_timeout()
        raise TimeoutError(f"Timed out after {seconds} seconds")

    timer = threading.Timer(seconds, timeout_handler)
    timer.daemon = True
    timer.start()
    try:
        yield
    finally:
        timer.cancel()


class AlarmTimeout:
    """
    Signal-based timeout (Unix only).

    More reliable than threading.Timer for subprocess calls.
    """

    def __init__(self, timeout_seconds: float):
        self.timeout_seconds = timeout_seconds
        self._old_handler: signal.Handler | None = None

    def __enter__(self) -> "AlarmTimeout":
        def handler(signum, frame):
            raise TimeoutError(f"Timed out after {self.timeout_seconds} seconds")
        self._old_handler = signal.signal(signal.SIGALRM, handler)
        signal.alarm(int(self.timeout_seconds))
        return self

    def __exit__(self, *args: object) -> None:
        signal.alarm(0)
        if self._old_handler is not None:
            signal.signal(signal.SIGALRM, self._old_handler)


def with_timeout(
    timeout_seconds: float,
    default: T | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T | None]]:
    """
    Decorator to add timeout to a function.

    Args:
        timeout_seconds: Timeout in seconds
        default: Default value to return on timeout

    Returns:
        Decorated function
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T | None]:
        def wrapper(*args, **kwargs) -> T | None:
            result = default
            def run():
                nonlocal result
                result = func(*args, **kwargs)
            thread = threading.Thread(target=run)
            thread.daemon = True
            thread.start()
            thread.join(timeout_seconds)
            if thread.is_alive():
                return default
            return result
        return wrapper
    return decorator


class Deadline:
    """
    Deadline tracker for operations.

    Tracks remaining time and checks if deadline exceeded.
    """

    def __init__(self, timeout_seconds: float | None = None):
        self._start = time.monotonic()
        self._deadline = self._start + timeout_seconds if timeout_seconds else None

    def remaining(self) -> float | None:
        """Get remaining seconds until deadline."""
        if self._deadline is None:
            return None
        return max(0.0, self._deadline - time.monotonic())

    def exceeded(self) -> bool:
        """Check if deadline exceeded."""
        if self._deadline is None:
            return False
        return time.monotonic() > self._deadline

    def check(self) -> None:
        """Raise TimeoutError if exceeded."""
        if self.exceeded():
            raise TimeoutError("Deadline exceeded")

    def wait(self, poll_interval: float = 0.1) -> bool:
        """
        Wait until deadline or return False if exceeded.

        Args:
            poll_interval: Sleep interval between checks

        Returns:
            True if deadline not exceeded, False otherwise
        """
        while not self.exceeded():
            remaining = self.remaining()
            if remaining is None or remaining <= 0:
                return True
            time.sleep(min(poll_interval, remaining))
        return False
