"""Timeout decorator utilities: function timeout enforcement with multiple strategies."""

from __future__ import annotations

import signal
import threading
import time
from functools import wraps
from typing import Any, Callable

__all__ = [
    "TimeoutException",
    "timeout",
    "with_timeout",
]


class TimeoutException(Exception):
    """Raised when a function call times out."""
    pass


def timeout(seconds: float, use_signal: bool = False):
    """Decorator to enforce a timeout on a function call.

    Uses threading by default (works on all platforms).
    Set use_signal=True for Unix signals (SIGALRM).
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if use_signal and hasattr(signal, "SIGALRM"):
                return _timeout_signal(func, seconds, *args, **kwargs)
            return _timeout_thread(func, seconds, *args, **kwargs)
        wrapper._timeout_seconds = seconds
        return wrapper
    return decorator


def _timeout_signal(func: Callable[..., Any], seconds: float, *args: Any, **kwargs: Any) -> Any:
    """Timeout using SIGALRM (Unix only)."""
    def handler(signum: int, frame: Any) -> None:
        raise TimeoutException(f"Function '{func.__name__}' timed out after {seconds}s")

    old_handler = signal.signal(signal.SIGALRM, handler)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        return func(*args, **kwargs)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old_handler)


def _timeout_thread(func: Callable[..., Any], seconds: float, *args: Any, **kwargs: Any) -> Any:
    """Timeout using a background thread."""
    result: list[Any] = [None]
    exception: list[Any] = [None]

    def target():
        try:
            result[0] = func(*args, **kwargs)
        except Exception as e:
            exception[0] = e

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout=seconds)

    if thread.is_alive():
        raise TimeoutException(f"Function '{func.__name__}' timed out after {seconds}s")

    if exception[0]:
        raise exception[0]

    return result[0]


def with_timeout(seconds: float, default: Any = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator that returns a default value on timeout instead of raising."""
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return _timeout_thread(func, seconds, *args, **kwargs)
            except TimeoutException:
                return default
        return wrapper
    return decorator
