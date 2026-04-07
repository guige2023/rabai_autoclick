"""ratelimit_action module for rabai_autoclick.

Provides rate limiting primitives: sliding window, token bucket,
leaky bucket, fixed window, and adaptive rate limiting.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Optional

__all__ = [
    "RateLimiter",
    "SlidingWindowLimiter",
    "TokenBucketLimiter",
    "LeakyBucketLimiter",
    "FixedWindowLimiter",
    "AdaptiveRateLimiter",
    "MultiLimiter",
    "RateLimitExceeded",
    "is_rate_limited",
    "check_rate_limit",
]


class RateLimitExceeded(Exception):
    """Raised when rate limit is exceeded."""
    pass


@dataclass
class RateLimitConfig:
    """Configuration for a rate limiter."""
    max_calls: int
    period_seconds: float
    burst: int = 1


class SlidingWindowLimiter:
    """Sliding window rate limiter."""

    def __init__(self, max_calls: int, window_seconds: float) -> None:
        self.max_calls = max_calls
        self.window_seconds = window_seconds
        self._calls: deque = deque()
        self._lock = threading.Lock()

    def _cleanup(self) -> None:
        """Remove expired calls from window."""
        cutoff = time.monotonic() - self.window_seconds
        while self._calls and self._calls[0] < cutoff:
            self._calls.popleft()

    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Acquire permission to proceed.

        Args:
            blocking: Wait if rate limited.
            timeout: Max wait time.

        Returns:
            True if acquired.
        """
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

    def try_acquire(self) -> bool:
        """Try to acquire without blocking."""
        return self.acquire(blocking=False)

    def reset(self) -> None:
        """Reset all calls."""
        with self._lock:
            self._calls.clear()

    def get_remaining(self) -> int:
        """Get remaining calls in current window."""
        with self._lock:
            self._cleanup()
            return max(0, self.max_calls - len(self._calls))


class TokenBucketLimiter:
    """Token bucket rate limiter with burst support."""

    def __init__(self, rate: float, capacity: int) -> None:
        self.rate = rate
        self.capacity = capacity
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last_refill = now

    def acquire(self, tokens: int = 1, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Acquire tokens.

        Args:
            tokens: Number of tokens needed.
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

    def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire without blocking."""
        return self.acquire(tokens=tokens, blocking=False)

    def reset(self) -> None:
        """Reset tokens to full capacity."""
        with self._lock:
            self._tokens = float(self.capacity)
            self._last_refill = time.monotonic()

    def get_available(self) -> float:
        """Get available tokens."""
        with self._lock:
            self._refill()
            return self._tokens


class LeakyBucketLimiter:
    """Leaky bucket rate limiter."""

    def __init__(self, capacity: int, leak_rate: float) -> None:
        self.capacity = capacity
        self.leak_rate = leak_rate
        self._level = 0.0
        self._last_leak = time.monotonic()
        self._lock = threading.Lock()

    def _leak(self) -> None:
        """Leak from bucket."""
        now = time.monotonic()
        elapsed = now - self._last_leak
        self._level = max(0.0, self._level - elapsed * self.leak_rate)
        self._last_leak = now

    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Try to add one drop to bucket.

        Args:
            blocking: Wait if bucket full.
            timeout: Max wait time.

        Returns:
            True if acquired.
        """
        deadline = None if timeout is None else time.monotonic() + timeout

        while True:
            with self._lock:
                self._leak()
                if self._level < self.capacity:
                    self._level += 1
                    return True
            if not blocking:
                return False
            wait_time = 1.0 / self.leak_rate if self.leak_rate > 0 else 1.0
            if timeout is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                wait_time = min(wait_time, remaining)
            ev = threading.Event()
            ev.wait(wait_time)

    def try_acquire(self) -> bool:
        """Try without blocking."""
        return self.acquire(blocking=False)


class FixedWindowLimiter:
    """Fixed window rate limiter."""

    def __init__(self, max_calls: int, window_seconds: float) -> None:
        self.max_calls = max_calls
        self.window_seconds = window_seconds
        self._window_start = time.monotonic()
        self._count = 0
        self._lock = threading.Lock()

    def _check_window(self) -> None:
        """Check if window needs reset."""
        now = time.monotonic()
        if now - self._window_start >= self.window_seconds:
            self._window_start = now
            self._count = 0

    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Acquire permission to proceed."""
        deadline = None if timeout is None else time.monotonic() + timeout

        with self._lock:
            while True:
                self._check_window()
                if self._count < self.max_calls:
                    self._count += 1
                    return True
                if not blocking:
                    return False
                wait_time = self.window_seconds - (time.monotonic() - self._window_start)
                if timeout is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    wait_time = min(wait_time, remaining)
                ev = threading.Event()
                ev.wait(max(0, wait_time))

    def try_acquire(self) -> bool:
        """Try without blocking."""
        return self.acquire(blocking=False)

    def reset(self) -> None:
        """Reset window."""
        with self._lock:
            self._window_start = time.monotonic()
            self._count = 0


class AdaptiveRateLimiter:
    """Adaptive rate limiter that adjusts based on success."""

    def __init__(
        self,
        initial_rate: float = 10.0,
        min_rate: float = 0.1,
        max_rate: float = 1000.0,
        increase_factor: float = 1.2,
        decrease_factor: float = 0.5,
    ) -> None:
        self.current_rate = initial_rate
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.increase_factor = increase_factor
        self.decrease_factor = decrease_factor
        self._lock = threading.Lock()
        self._limiter = TokenBucketLimiter(initial_rate, int(initial_rate))

    def record_success(self) -> None:
        """Record successful call, increase rate."""
        with self._lock:
            new_rate = min(self.max_rate, self.current_rate * self.increase_factor)
            if new_rate != self.current_rate:
                self.current_rate = new_rate
                self._limiter = TokenBucketLimiter(new_rate, int(new_rate))

    def record_failure(self) -> None:
        """Record failed call, decrease rate."""
        with self._lock:
            new_rate = max(self.min_rate, self.current_rate * self.decrease_factor)
            if new_rate != self.current_rate:
                self.current_rate = new_rate
                self._limiter = TokenBucketLimiter(new_rate, int(new_rate))

    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Acquire permission to proceed."""
        return self._limiter.acquire(blocking=blocking, timeout=timeout)

    def try_acquire(self) -> bool:
        """Try without blocking."""
        return self.acquire(blocking=False)


class MultiLimiter:
    """Combine multiple rate limiters (AND logic)."""

    def __init__(self, limiters: list) -> None:
        self.limiters = limiters

    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Acquire from all limiters."""
        deadline = None if timeout is None else time.monotonic() + timeout
        for limiter in self.limiters:
            remaining = None if timeout is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                return False
            if not limiter.acquire(blocking=blocking, timeout=remaining):
                return False
        return True

    def try_acquire(self) -> bool:
        """Try all limiters without blocking."""
        return all(limiter.try_acquire() for limiter in self.limiters)


def is_rate_limited(limiter: SlidingWindowLimiter) -> bool:
    """Check if rate limited without consuming."""
    return not limiter.try_acquire()


def check_rate_limit(
    limiter: SlidingWindowLimiter,
    raise_on_limit: bool = False,
) -> bool:
    """Check rate limit and optionally raise exception."""
    if limiter.try_acquire():
        return True
    if raise_on_limit:
        raise RateLimitExceeded("Rate limit exceeded")
    return False
