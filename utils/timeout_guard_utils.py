"""
Timeout Guard Utilities

Provides utilities for timeout enforcement
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable
import time


class TimeoutGuard:
    """
    Enforces timeouts on operations.
    
    Provides context manager and decorator
    for timeout enforcement.
    """

    def __init__(self, timeout_seconds: float) -> None:
        self._timeout = timeout_seconds
        self._start_time: float | None = None

    def __enter__(self) -> TimeoutGuard:
        """Enter context manager."""
        self._start_time = time.time()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        """Exit context manager, raise if timeout exceeded."""
        if self._start_time is not None:
            elapsed = time.time() - self._start_time
            if elapsed > self._timeout:
                raise TimeoutError(f"Operation exceeded {self._timeout}s timeout")
        return False

    def check(self) -> None:
        """Check if timeout exceeded."""
        if self._start_time is not None:
            elapsed = time.time() - self._start_time
            if elapsed > self._timeout:
                raise TimeoutError(f"Operation exceeded {self._timeout}s timeout")

    def remaining(self) -> float:
        """Get remaining time before timeout."""
        if self._start_time is None:
            return self._timeout
        elapsed = time.time() - self._start_time
        return max(0.0, self._timeout - elapsed)


def with_timeout(timeout_seconds: float) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to enforce timeout on a function."""
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        def wrapper(*args: Any, **kwargs: Any) -> T:
            guard = TimeoutGuard(timeout_seconds)
            with guard:
                return func(*args, **kwargs)
        return wrapper
    return decorator
