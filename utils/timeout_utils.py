"""Timeout utilities: function timeout enforcement, deadline tracking, and context managers."""

from __future__ import annotations

import signal
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable

__all__ = [
    "TimeoutError",
    "with_timeout",
    "deadline",
    "timeout_context",
]


class TimeoutError(Exception):
    """Raised when a function times out."""
    pass


def with_timeout(
    seconds: float,
    default: Any = None,
    suppress_exceptions: tuple[type[Exception], ...] = (),
) -> Callable[[Callable[[], Any]], Callable[[], Any]]:
    """Decorator to enforce a timeout on a function."""

    def decorator(func: Callable[[], Any]) -> Callable[[], Any]:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            result = default
            exception_info: Exception | None = None

            def target():
                nonlocal result, exception_info
                try:
                    result = func(*args, **kwargs)
                except Exception as e:
                    if not isinstance(e, suppress_exceptions):
                        exception_info = e

            thread = threading.Thread(target=target, daemon=True)
            thread.start()
            thread.join(timeout=seconds)

            if thread.is_alive():
                raise TimeoutError(f"Function '{func.__name__}' timed out after {seconds}s")

            if exception_info:
                raise exception_info

            return result
        return wrapper
    return decorator


@dataclass
class Deadline:
    """Tracks a deadline for operations."""

    deadline: float

    @classmethod
    def from_timeout(cls, seconds: float) -> "Deadline":
        return cls(deadline=time.time() + seconds)

    @property
    def remaining(self) -> float:
        return max(0, self.deadline - time.time())

    @property
    def expired(self) -> bool:
        return time.time() >= self.deadline

    def __enter__(self) -> "Deadline":
        return self

    def __exit__(self, *args: Any) -> None:
        pass


@contextmanager
def timeout_context(seconds: float):
    """Context manager for timeout enforcement."""
    deadline = time.time() + seconds
    try:
        yield Deadline(deadline)
    finally:
        pass


def enforce_timeout(seconds: float) -> bool:
    """Enforce a soft timeout using signal (Unix only)."""
    if not hasattr(signal, "SIGALRM"):
        return False

    def handler(signum: int, frame: Any) -> None:
        raise TimeoutError("Operation timed out")

    old_handler = signal.signal(signal.SIGALRM, handler)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old_handler)
