"""throttle_action module for rabai_autoclick.

Provides throttling utilities: call throttling, rate limiting,
token bucket implementation, and adaptive throttling.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

__all__ = [
    "Throttle",
    "AdaptiveThrottle",
    "LeakyBucket",
    "TokenBucket",
    "WindowedRateLimiter",
    "CallThrottle",
    "throttle_calls",
    "throttle_rate",
    "throttle_period",
    "CallCounter",
    "CallStats",
]


@dataclass
class CallStats:
    """Statistics for a throttled function."""
    total_calls: int = 0
    throttled_calls: int = 0
    last_call_time: float = 0.0
    last_throttled_time: float = 0.0
    avg_interval: float = 0.0

    @property
    def pass_rate(self) -> float:
        if self.total_calls == 0:
            return 0.0
        return (self.total_calls - self.throttled_calls) / self.total_calls


class Throttle:
    """Throttle function calls to at most once per interval."""

    def __init__(self, min_interval: float) -> None:
        self.min_interval = min_interval
        self._last_call = 0.0
        self._lock = threading.Lock()

    def can_call(self) -> bool:
        """Check if call can proceed now."""
        now = time.monotonic()
        return (now - self._last_call) >= self.min_interval

    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Acquire permission to call.

        Args:
            blocking: Wait if throttled.
            timeout: Max wait time.

        Returns:
            True if acquired.
        """
        deadline = None if timeout is None else time.monotonic() + timeout

        with self._lock:
            while True:
                now = time.monotonic()
                elapsed = now - self._last_call
                if elapsed >= self.min_interval:
                    self._last_call = now
                    return True
                if not blocking:
                    return False
                wait_time = self.min_interval - elapsed
                if timeout is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    wait_time = min(wait_time, remaining)
                ev = threading.Event()
                ev.wait(wait_time)

    def __call__(self, func: Callable) -> Callable:
        """Decorator form."""
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if self.acquire():
                return func(*args, **kwargs)
        return wrapper


class AdaptiveThrottle:
    """Adaptive throttle that adjusts rate based on success."""

    def __init__(
        self,
        initial_rate: float = 10.0,
        min_rate: float = 0.1,
        max_rate: float = 100.0,
        increase_factor: float = 1.1,
        decrease_factor: float = 0.5,
    ) -> None:
        self.current_rate = initial_rate
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.increase_factor = increase_factor
        self.decrease_factor = decrease_factor
        self._lock = threading.Lock()
        self._last_adjust = time.monotonic()
        self._interval = 1.0 / initial_rate
        self._last_call = 0.0

    def _adjust_rate(self) -> None:
        """Adjust rate based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_adjust
        if elapsed >= 1.0:
            if elapsed > self._interval * 2:
                self.current_rate = min(self.max_rate, self.current_rate * self.increase_factor)
            else:
                self.current_rate = max(self.min_rate, self.current_rate * self.decrease_factor)
            self._interval = 1.0 / self.current_rate
            self._last_adjust = now

    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Acquire permission to call."""
        deadline = None if timeout is None else time.monotonic() + timeout

        with self._lock:
            self._adjust_rate()
            while True:
                now = time.monotonic()
                elapsed = now - self._last_call
                if elapsed >= self._interval:
                    self._last_call = now
                    return True
                if not blocking:
                    return False
                wait_time = self._interval - elapsed
                if timeout is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    wait_time = min(wait_time, remaining)
                ev = threading.Event()
                ev.wait(wait_time)
                self._adjust_rate()


class LeakyBucket:
    """Leaky bucket algorithm for rate limiting.

    Drops requests if bucket is full.
    """

    def __init__(self, capacity: int, leak_rate: float) -> None:
        self.capacity = capacity
        self.leak_rate = leak_rate
        self._level = 0.0
        self._last_leak = time.monotonic()
        self._lock = threading.Lock()

    def _leak(self) -> None:
        """Leak water from bucket."""
        now = time.monotonic()
        elapsed = now - self._last_leak
        leaked = elapsed * self.leak_rate
        self._level = max(0.0, self._level - leaked)
        self._last_leak = now

    def add(self) -> bool:
        """Try to add one drop to bucket.

        Returns:
            True if added, False if bucket full.
        """
        with self._lock:
            self._leak()
            if self._level < self.capacity:
                self._level += 1
                return True
            return False

    def acquire(self, blocking: bool = False, timeout: Optional[float] = None) -> bool:
        """Acquire permission to proceed."""
        deadline = None if timeout is None else time.monotonic() + timeout

        while True:
            if self.add():
                return True
            if not blocking:
                return False
            remaining = None if timeout is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                return False
            ev = threading.Event()
            wait_time = 1.0 / self.leak_rate if self.leak_rate > 0 else 1.0
            if remaining is not None:
                wait_time = min(wait_time, remaining)
            ev.wait(wait_time)


class TokenBucket:
    """Token bucket for rate limiting with burst support."""

    def __init__(self, rate: float, capacity: int) -> None:
        self.rate = rate
        self.capacity = capacity
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        """Add tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last_refill = now

    def acquire(self, tokens: int = 1, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Acquire tokens.

        Args:
            tokens: Number of tokens to acquire.
            blocking: Wait if not enough tokens.
            timeout: Max wait time.

        Returns:
            True if acquired.
        """
        deadline = None if timeout is None else time.monotonic() + timeout

        with self._lock:
            while True:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return True
                if not blocking:
                    return False
                wait_time = (tokens - self._tokens) / self.rate
                if timeout is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    wait_time = min(wait_time, remaining)
                ev = threading.Event()
                ev.wait(wait_time)


class WindowedRateLimiter:
    """Rate limiter using sliding window algorithm."""

    def __init__(self, max_calls: int, window_seconds: float) -> None:
        self.max_calls = max_calls
        self.window_seconds = window_seconds
        self._calls: deque = deque()
        self._lock = threading.Lock()

    def _cleanup(self) -> None:
        """Remove calls outside the window."""
        cutoff = time.monotonic() - self.window_seconds
        while self._calls and self._calls[0] < cutoff:
            self._calls.popleft()

    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Acquire permission to make a call."""
        deadline = None if timeout is None else time.monotonic() + timeout

        with self._lock:
            while True:
                self._cleanup()
                if len(self._calls) < self.max_calls:
                    self._calls.append(time.monotonic())
                    return True
                if not blocking:
                    return False
                oldest = self._calls[0]
                wait_time = oldest + self.window_seconds - time.monotonic()
                if timeout is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    wait_time = min(wait_time, remaining)
                ev = threading.Event()
                ev.wait(max(0, wait_time))


class CallThrottle:
    """Decorator-style throttle with statistics."""

    def __init__(
        self,
        max_calls: int,
        period: float,
        stats: Optional[CallStats] = None,
    ) -> None:
        self.max_calls = max_calls
        self.period = period
        self.stats = stats or CallStats()
        self._limiter = WindowedRateLimiter(max_calls, period)

    def __call__(self, func: Callable) -> Callable:
        """Decorate a function."""
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            self.stats.total_calls += 1
            if self._limiter.acquire(blocking=False):
                return func(*args, **kwargs)
            else:
                self.stats.throttled_calls += 1
                self.stats.last_throttled_time = time.time()
        return wrapper


def throttle_calls(max_calls: int, period: float) -> Callable:
    """Decorator to throttle function calls.

    Args:
        max_calls: Maximum calls per period.
        period: Time period in seconds.

    Returns:
        Throttle decorator.
    """
    limiter = WindowedRateLimiter(max_calls, period)
    stats = CallStats()

    def decorator(func: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            stats.total_calls += 1
            if limiter.acquire(blocking=False):
                stats.last_call_time = time.time()
                return func(*args, **kwargs)
            else:
                stats.throttled_calls += 1
                stats.last_throttled_time = time.time()
        wrapper._throttle_stats = stats
        return wrapper
    return decorator


def throttle_rate(rate: float, burst: int = 1) -> Callable:
    """Decorator for token bucket rate limiting.

    Args:
        rate: Calls per second.
        burst: Burst capacity.

    Returns:
        Throttle decorator.
    """
    bucket = TokenBucket(rate, burst)

    def decorator(func: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if bucket.acquire(blocking=False):
                return func(*args, **kwargs)
        return wrapper
    return decorator


def throttle_period(min_interval: float) -> Callable:
    """Decorator to enforce minimum interval between calls.

    Args:
        min_interval: Minimum seconds between calls.

    Returns:
        Throttle decorator.
    """
    throttle = Throttle(min_interval)

    def decorator(func: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if throttle.acquire(blocking=False):
                return func(*args, **kwargs)
        return wrapper
    return decorator


class CallCounter:
    """Track call frequency over sliding windows."""

    def __init__(self, window_seconds: float = 60.0) -> None:
        self.window_seconds = window_seconds
        self._calls: deque = deque()
        self._lock = threading.Lock()

    def record(self) -> None:
        """Record a call."""
        with self._lock:
            now = time.monotonic()
            self._calls.append(now)
            cutoff = now - self.window_seconds
            while self._calls and self._calls[0] < cutoff:
                self._calls.popleft()

    def count(self) -> int:
        """Get call count in window."""
        with self._lock:
            now = time.monotonic()
            cutoff = now - self.window_seconds
            while self._calls and self._calls[0] < cutoff:
                self._calls.popleft()
            return len(self._calls)

    def rate(self) -> float:
        """Get calls per second in window."""
        count = self.count()
        return count / self.window_seconds if self.window_seconds > 0 else 0.0
