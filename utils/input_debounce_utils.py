"""
Input debouncing and throttling utilities for UI automation.

This module provides utilities for controlling the rate of
input events and preventing rapid-fire actions.
"""

from __future__ import annotations

import time
import threading
from dataclasses import dataclass, field
from typing import Callable, Optional, Any, Dict, List
from enum import Enum, auto
from abc import ABC, abstractmethod


class RateLimitStrategy(Enum):
    """Rate limiting strategies."""
    DEBOUNCE = auto()
    THROTTLE = auto()
    QUEUE = auto()


@dataclass
class RateLimitConfig:
    """
    Configuration for rate limiting.

    Attributes:
        max_calls: Maximum calls allowed.
        time_window: Time window in seconds.
        strategy: Which strategy to use.
        leading_edge: Execute on first call (vs trailing).
    """
    max_calls: int = 10
    time_window: float = 1.0
    strategy: RateLimitStrategy = RateLimitStrategy.DEBOUNCE
    leading_edge: bool = True


class RateLimiter(ABC):
    """Abstract base class for rate limiters."""

    @abstractmethod
    def execute(self, func: Callable[[], Any]) -> Any:
        """Execute function with rate limiting."""
        pass

    @abstractmethod
    def reset(self) -> None:
        """Reset the rate limiter state."""
        pass


class Debouncer(RateLimiter):
    """
    Debouncer that waits for a period of inactivity before executing.

    Useful for waiting for user input to settle before taking action.
    """

    def __init__(self, wait_time: float = 0.3) -> None:
        self._wait_time = wait_time
        self._timer: Optional[threading.Timer] = None
        self._last_args: Any = None
        self._lock = threading.Lock()

    def execute(self, func: Callable[[], Any], *args: Any, **kwargs: Any) -> Any:
        """Execute function after wait period, cancelling previous calls."""
        with self._lock:
            if self._timer:
                self._timer.cancel()

            def wrapper() -> Any:
                return func(*args, **kwargs)

            self._last_args = (args, kwargs)
            self._timer = threading.Timer(self._wait_time, wrapper)
            self._timer.start()

    def execute_and_wait(self, func: Callable[[], Any], *args: Any, **kwargs: Any) -> Any:
        """Execute function immediately if not already pending."""
        with self._lock:
            if self._timer:
                return None  # Already pending
            result = func(*args, **kwargs)
            return result

    def reset(self) -> None:
        """Cancel any pending execution."""
        with self._lock:
            if self._timer:
                self._timer.cancel()
                self._timer = None


class Throttler(RateLimiter):
    """
    Throttler that limits execution rate.

    Ensures function is not called more than once per time window.
    """

    def __init__(self, max_calls: int = 10, time_window: float = 1.0) -> None:
        self._max_calls = max_calls
        self._time_window = time_window
        self._calls: List[float] = []
        self._lock = threading.Lock()
        self._last_result: Any = None

    def execute(self, func: Callable[[], Any], *args: Any, **kwargs: Any) -> Any:
        """Execute function if within rate limit."""
        with self._lock:
            now = time.time()
            self._cleanup(now)

            if len(self._calls) < self._max_calls:
                self._last_result = func(*args, **kwargs)
                self._calls.append(now)
                return self._last_result

            return self._last_result

    def _cleanup(self, now: float) -> None:
        """Remove calls outside time window."""
        cutoff = now - self._time_window
        self._calls = [t for t in self._calls if t > cutoff]

    def reset(self) -> None:
        """Clear all recorded calls."""
        with self._lock:
            self._calls.clear()
            self._last_result = None

    @property
    def remaining_calls(self) -> int:
        """Get number of remaining calls in current window."""
        with self._lock:
            self._cleanup(time.time())
            return self._max_calls - len(self._calls)


class QueueRateLimiter(RateLimiter):
    """
    Queue-based rate limiter that processes calls sequentially.

    Ensures minimum interval between executions.
    """

    def __init__(self, min_interval: float = 0.1) -> None:
        self._min_interval = min_interval
        self._last_execution: float = 0.0
        self._lock = threading.Lock()
        self._queue: List[Callable[[], Any]] = []
        self._running: bool = False

    def execute(self, func: Callable[[], Any]) -> Any:
        """Queue function for execution."""
        with self._lock:
            self._queue.append(func)
            if not self._running:
                self._process_queue()

    def _process_queue(self) -> None:
        """Process queued functions."""
        if not self._queue:
            self._running = False
            return

        self._running = True
        func = self._queue.pop(0)

        now = time.time()
        elapsed = now - self._last_execution
        if elapsed < self._min_interval:
            threading.Timer(self._min_interval - elapsed, self._execute_func, [func]).start()
        else:
            self._execute_func(func)

    def _execute_func(self, func: Callable[[], Any]) -> None:
        """Execute a single function."""
        func()
        self._last_execution = time.time()
        with self._lock:
            self._process_queue()

    def reset(self) -> None:
        """Clear the queue."""
        with self._lock:
            self._queue.clear()
            self._running = False


class AdaptiveRateLimiter(RateLimiter):
    """
    Adaptive rate limiter that adjusts based on success/failure.

    Increases rate on success, decreases on errors.
    """

    def __init__(
        self,
        initial_rate: float = 10.0,
        min_rate: float = 1.0,
        max_rate: float = 100.0,
        increase_factor: float = 1.2,
        decrease_factor: float = 0.5,
    ) -> None:
        self._current_rate = initial_rate
        self._min_rate = min_rate
        self._max_rate = max_rate
        self._increase_factor = increase_factor
        self._decrease_factor = decrease_factor
        self._throttler = Throttler(int(initial_rate), 1.0)
        self._lock = threading.Lock()

    def execute(self, func: Callable[[], Any], *args: Any, **kwargs: Any) -> Any:
        """Execute with adaptive rate limiting."""
        return self._throttler.execute(func, *args, **kwargs)

    def report_success(self) -> None:
        """Call after successful execution to increase rate."""
        with self._lock:
            new_rate = min(self._current_rate * self._increase_factor, self._max_rate)
            if int(new_rate) != int(self._current_rate):
                self._throttler = Throttler(int(new_rate), 1.0)
            self._current_rate = new_rate

    def report_failure(self) -> None:
        """Call after failed execution to decrease rate."""
        with self._lock:
            new_rate = max(self._current_rate * self._decrease_factor, self._min_rate)
            if int(new_rate) != int(self._current_rate):
                self._throttler = Throttler(int(new_rate), 1.0)
            self._current_rate = new_rate

    def reset(self) -> None:
        """Reset to initial rate."""
        with self._lock:
            self._throttler = Throttler(int(self._current_rate), 1.0)

    @property
    def current_rate(self) -> float:
        """Get current rate limit."""
        return self._current_rate


def debounce(wait_time: float = 0.3) -> Callable[[Callable], Callable]:
    """
    Decorator for debouncing function calls.

    Usage:
        @debounce(0.5)
        def my_function():
            ...
    """
    debouncer = Debouncer(wait_time)

    def decorator(func: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> None:
            debouncer.execute(func, *args, **kwargs)
        return wrapper
    return decorator


def throttle(max_calls: int = 10, time_window: float = 1.0) -> Callable[[Callable], Callable]:
    """
    Decorator for throttling function calls.

    Usage:
        @throttle(5, 1.0)
        def my_function():
            ...
    """
    throttler = Throttler(max_calls, time_window)

    def decorator(func: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return throttler.execute(func, *args, **kwargs)
        return wrapper
    return decorator
