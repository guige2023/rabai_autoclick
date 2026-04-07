"""
Timeout Utilities

Provides various timeout mechanisms including
context managers, decorators, and async support.
"""

from __future__ import annotations

import asyncio
import copy
import signal
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class TimeoutError(Exception):
    """Raised when an operation times out."""
    pass


class TimeoutStrategy(Enum):
    """Strategy for handling timeouts."""
    THREAD = auto()      # Use a thread
    SIGNAL = auto()      # Use SIGALRM (Unix only)
    EXECUTOR = auto()    # Use a thread pool executor


@dataclass
class TimeoutResult(Generic[T]):
    """Result of a timed operation."""
    success: bool
    value: T | None = None
    error: TimeoutError | None = None
    duration_ms: float = 0.0


def timeout(
    seconds: float,
    strategy: TimeoutStrategy = TimeoutStrategy.THREAD,
    default: T | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T | None]]:
    """
    Decorator to add timeout to a function.

    Args:
        seconds: Timeout in seconds.
        strategy: Strategy for implementing timeout.
        default: Default value to return on timeout.

    Usage:
        @timeout(5.0)
        def my_function():
            ...
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T | None]:
        def wrapper(*args: Any, **kwargs: Any) -> T | None:
            if strategy == TimeoutStrategy.THREAD:
                return _timeout_thread(func, seconds, default, *args, **kwargs)
            elif strategy == TimeoutStrategy.SIGNAL:
                return _timeout_signal(func, seconds, default, *args, **kwargs)
            else:
                raise ValueError(f"Unknown strategy: {strategy}")

        wrapper.__name__ = func.__name__
        return wrapper

    return decorator


def _timeout_thread(
    func: Callable[..., T],
    seconds: float,
    default: T,
    *args: Any,
    **kwargs: Any,
) -> T | None:
    """Implement timeout using a thread."""
    result: dict[str, Any] = {"value": default, "exception": None}

    def target():
        try:
            result["value"] = func(*args, **kwargs)
        except Exception as e:
            result["exception"] = e

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout=seconds)

    if thread.is_alive():
        return default

    if result["exception"]:
        raise result["exception"]

    return result["value"]


def _timeout_signal(
    func: Callable[..., T],
    seconds: float,
    default: T,
    *args: Any,
    **kwargs: Any,
) -> T | None:
    """Implement timeout using SIGALRM (Unix only)."""
    def handler(signum, frame):
        raise TimeoutError(f"Function '{func.__name__}' timed out after {seconds}s")

    old_handler = signal.signal(signal.SIGALRM, handler)
    signal.alarm(int(seconds))

    try:
        result = func(*args, **kwargs)
    except TimeoutError:
        return default
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)

    return result


@contextmanager
def timeout_context(seconds: float, default: Any = None):
    """
    Context manager for timeout.

    Usage:
        with timeout_context(5.0):
            # code that should timeout
            ...
    """
    result: dict[str, Any] = {"timed_out": False}

    def target():
        try:
            yield
        except TimeoutError:
            result["timed_out"] = True

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout=seconds)

    if thread.is_alive():
        result["timed_out"] = True

    if result["timed_out"]:
        raise TimeoutError(f"Operation timed out after {seconds}s")


class Timer(ABC):
    """Abstract timer interface."""

    @abstractmethod
    def start(self) -> None:
        """Start the timer."""
        pass

    @abstractmethod
    def stop(self) -> float:
        """Stop the timer and return elapsed time."""
        pass

    @abstractmethod
    def reset(self) -> None:
        """Reset the timer."""
        pass

    @abstractmethod
    def elapsed(self) -> float:
        """Get elapsed time without stopping."""
        pass


class Stopwatch(Timer):
    """
    Simple stopwatch for measuring elapsed time.
    """

    def __init__(self, auto_start: bool = False):
        self._start_time: float | None = None
        self._stop_time: float | None = None
        self._elapsed: float = 0.0

        if auto_start:
            self.start()

    def start(self) -> None:
        """Start the stopwatch."""
        if self._start_time is None:
            self._start_time = time.time()

    def stop(self) -> float:
        """Stop the stopwatch and return elapsed time."""
        if self._start_time is None:
            return 0.0

        self._stop_time = time.time()
        self._elapsed = self._stop_time - self._start_time
        return self._elapsed * 1000  # Return in milliseconds

    def reset(self) -> None:
        """Reset the stopwatch."""
        self._start_time = None
        self._stop_time = None
        self._elapsed = 0.0

    def elapsed(self) -> float:
        """Get elapsed time in milliseconds."""
        if self._start_time is None:
            return 0.0

        if self._stop_time is None:
            return (time.time() - self._start_time) * 1000

        return self._elapsed * 1000

    def __enter__(self) -> Stopwatch:
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, *args: Any) -> None:
        """Context manager exit."""
        self.stop()


class CountdownTimer(Timer):
    """
    Timer that counts down from a specified duration.
    """

    def __init__(self, duration_seconds: float, auto_start: bool = False):
        self._duration = duration_seconds
        self._start_time: float | None = None
        self._remaining: float = duration_seconds

        if auto_start:
            self.start()

    def start(self) -> None:
        """Start the countdown."""
        self._start_time = time.time()
        self._remaining = self._duration

    def stop(self) -> float:
        """Stop and return remaining time."""
        if self._start_time is None:
            return self._remaining

        elapsed = time.time() - self._start_time
        self._remaining = max(0, self._duration - elapsed)
        self._start_time = None
        return self._remaining * 1000

    def reset(self) -> None:
        """Reset the timer."""
        self._start_time = None
        self._remaining = self._duration

    def elapsed(self) -> float:
        """Get remaining time in milliseconds."""
        if self._start_time is None:
            return self._remaining * 1000

        elapsed = time.time() - self._start_time
        return max(0, self._duration - elapsed) * 1000

    @property
    def is_expired(self) -> bool:
        """Check if timer has expired."""
        return self.elapsed() <= 0

    def __bool__(self) -> bool:
        """Check if timer is still running."""
        return self._start_time is not None and not self.is_expired


class TimeoutManager:
    """
    Manager for multiple timers with callbacks.
    """

    def __init__(self):
        self._timers: dict[str, CountdownTimer] = {}
        self._callbacks: dict[str, list[Callable[[], None]]] = {}
        self._lock = threading.RLock()
        self._check_thread: threading.Thread | None = None
        self._running = False

    def create_timer(
        self,
        name: str,
        duration_seconds: float,
        callback: Callable[[], None] | None = None,
    ) -> CountdownTimer:
        """Create a named timer."""
        with self._lock:
            timer = CountdownTimer(duration_seconds)
            self._timers[name] = timer
            if callback:
                if name not in self._callbacks:
                    self._callbacks[name] = []
                self._callbacks[name].append(callback)
            return timer

    def start_timer(self, name: str) -> bool:
        """Start a named timer."""
        with self._lock:
            if name in self._timers:
                self._timers[name].start()
                return True
            return False

    def cancel_timer(self, name: str) -> bool:
        """Cancel a named timer."""
        with self._lock:
            if name in self._timers:
                self._timers[name].reset()
                return True
            return False

    def get_remaining(self, name: str) -> float | None:
        """Get remaining time for a timer."""
        with self._lock:
            if name in self._timers:
                return self._timers[name].elapsed()
            return None

    def list_timers(self) -> list[str]:
        """List all timer names."""
        with self._lock:
            return list(self._timers.keys())

    def delete_timer(self, name: str) -> bool:
        """Delete a named timer."""
        with self._lock:
            if name in self._timers:
                del self._timers[name]
                self._callbacks.pop(name, None)
                return True
            return False


async def timeout_async(
    coro: Any,
    seconds: float,
) -> Any:
    """
    Await with timeout for async operations.

    Usage:
        result = await timeout_async(my_coro(), 5.0)
    """
    try:
        return await asyncio.wait_for(coro, timeout=seconds)
    except asyncio.TimeoutError:
        raise TimeoutError(f"Operation timed out after {seconds}s")


class Debouncer:
    """
    Debounces rapid calls, only executing after
    a quiet period.
    """

    def __init__(self, delay_seconds: float):
        self._delay = delay_seconds
        self._timer: CountdownTimer | None = None
        self._pending_func: Callable | None = None
        self._pending_args: tuple = ()
        self._pending_kwargs: dict[str, Any] = {}
        self._lock = threading.Lock()

    def call(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Call a function with debouncing.

        The function will be executed after the delay
        unless another call comes in first.
        """
        with self._lock:
            self._pending_func = func
            self._pending_args = args
            self._pending_kwargs = kwargs

            if self._timer and self._timer:
                self._timer.reset()

            self._timer = CountdownTimer(self._delay)
            self._timer.start()

    def execute_if_ready(self) -> bool:
        """
        Execute pending function if timer has expired.

        Returns:
            True if function was executed.
        """
        with self._lock:
            if self._timer and self._timer.is_expired:
                if self._pending_func:
                    self._pending_func(*self._pending_args, **self._pending_kwargs)
                    self._pending_func = None
                    return True

        return False


class Throttler:
    """
    Throttles calls to a maximum rate.
    """

    def __init__(self, max_calls: int, period_seconds: float):
        self._max_calls = max_calls
        self._period = period_seconds
        self._calls: list[float] = []
        self._lock = threading.Lock()

    def can_proceed(self) -> bool:
        """Check if a call can proceed immediately."""
        with self._lock:
            now = time.time()
            cutoff = now - self._period
            self._calls = [t for t in self._calls if t > cutoff]

            if len(self._calls) < self._max_calls:
                self._calls.append(now)
                return True

            return False

    def wait_and_proceed(self, timeout: float | None = None) -> bool:
        """
        Wait until a call can proceed or timeout.

        Returns:
            True if call proceeded, False if timed out.
        """
        start = time.time()

        while True:
            if self.can_proceed():
                return True

            if timeout is not None:
                elapsed = time.time() - start
                if elapsed >= timeout:
                    return False

            time.sleep(0.01)

    @contextmanager
    def throttle(self):
        """Context manager for throttling."""
        self.wait_and_proceed()
        yield
