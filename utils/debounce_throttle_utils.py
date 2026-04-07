"""Debounce and throttle utilities: rate limiting function calls."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable

__all__ = [
    "debounce",
    "throttle",
    "Debouncer",
    "Throttler",
]


def debounce(wait_seconds: float):
    """Decorator that debounces a function: waits for silence before calling."""
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        timer: threading.Timer | None = None
        lock = threading.Lock()

        def debounced(*args: Any, **kwargs: Any) -> Any:
            nonlocal timer
            with lock:
                if timer:
                    timer.cancel()
                timer = threading.Timer(wait_seconds, lambda: func(*args, **kwargs))
                timer.start()

        debounced._debounce_wait = wait_seconds
        return debounced
    return decorator


def throttle(wait_seconds: float):
    """Decorator that throttles a function: allows one call per wait period."""
    last_called: float = 0.0
    lock = threading.Lock()

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        def throttled(*args: Any, **kwargs: Any) -> Any:
            nonlocal last_called
            with lock:
                now = time.time()
                if now - last_called >= wait_seconds:
                    last_called = now
                    return func(*args, **kwargs)
            return None

        throttled._throttle_wait = wait_seconds
        return throttled
    return decorator


class Debouncer:
    """Programmatic debouncer with cancel support."""

    def __init__(self, wait_seconds: float) -> None:
        self.wait_seconds = wait_seconds
        self._timer: threading.Timer | None = None
        self._lock = threading.Lock()

    def call(self, func: Callable[[], Any]) -> None:
        with self._lock:
            if self._timer:
                self._timer.cancel()
            self._timer = threading.Timer(self.wait_seconds, func)
            self._timer.start()

    def cancel(self) -> None:
        with self._lock:
            if self._timer:
                self._timer.cancel()
                self._timer = None

    def flush(self) -> None:
        with self._lock:
            if self._timer:
                self._timer.cancel()
                self._timer = None


class Throttler:
    """Programmatic throttler."""

    def __init__(self, wait_seconds: float) -> None:
        self.wait_seconds = wait_seconds
        self._last_called: float = 0.0
        self._lock = threading.Lock()

    def call(self, func: Callable[[], Any]) -> Any:
        with self._lock:
            now = time.time()
            if now - self._last_called >= self.wait_seconds:
                self._last_called = now
                return func()
        return None

    def can_call(self) -> bool:
        with self._lock:
            return (time.time() - self._last_called) >= self.wait_seconds

    def reset(self) -> None:
        with self._lock:
            self._last_called = 0.0
