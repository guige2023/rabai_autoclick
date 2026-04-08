"""
Rate limiter utilities with multiple algorithms.

Provides token bucket, sliding window, and leaky bucket
rate limiters with thread-safety.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Literal


class TokenBucketRateLimiter:
    """
    Token bucket rate limiter.

    Allows bursts up to bucket capacity, then limits
    to sustained rate determined by refill rate.
    """

    def __init__(
        self,
        capacity: float,
        refill_rate: float,
        tokens: float | None = None,
    ):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._tokens = tokens if tokens is not None else capacity
        self._last_refill = time.time()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = time.time()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
        self._last_refill = now

    def try_acquire(self, tokens: float = 1.0) -> bool:
        """Try to acquire tokens without blocking."""
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def acquire(self, tokens: float = 1.0, timeout: float | None = None) -> bool:
        """Acquire tokens, waiting if necessary."""
        deadline = time.time() + timeout if timeout else None
        while True:
            if self.try_acquire(tokens):
                return True
            if deadline and time.time() >= deadline:
                return False
            time.sleep(0.01)

    def wait_and_acquire(self, tokens: float = 1.0) -> None:
        """Block until tokens are acquired."""
        while not self.try_acquire(tokens):
            time.sleep(0.01)

    @property
    def available_tokens(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens


class SlidingWindowRateLimiter:
    """
    Sliding window rate limiter.

    Tracks requests in a sliding time window for
    more accurate rate limiting than fixed window.
    """

    def __init__(self, max_requests: int, window_seconds: float):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._events: deque[float] = deque()
        self._lock = threading.Lock()

    def _prune(self) -> None:
        cutoff = time.time() - self.window_seconds
        while self._events and self._events[0] < cutoff:
            self._events.popleft()

    def try_acquire(self, requests: int = 1) -> bool:
        """Try to acquire without blocking."""
        with self._lock:
            self._prune()
            if len(self._events) + requests <= self.max_requests:
                now = time.time()
                for _ in range(requests):
                    self._events.append(now)
                return True
            return False

    def acquire(self, requests: int = 1, timeout: float | None = None) -> bool:
        """Acquire with blocking."""
        deadline = time.time() + timeout if timeout else None
        while True:
            if self.try_acquire(requests):
                return True
            if deadline and time.time() >= deadline:
                return False
            time.sleep(0.01)

    @property
    def current_count(self) -> int:
        with self._lock:
            self._prune()
            return len(self._events)


class LeakyBucketRateLimiter:
    """
    Leaky bucket rate limiter.

    Processes requests at a constant rate, rejecting
    requests that would overflow the bucket.
    """

    def __init__(self, capacity: int, leak_rate: float):
        self.capacity = capacity
        self.leak_rate = leak_rate
        self._level: float = 0.0
        self._last_leak = time.time()
        self._lock = threading.Lock()

    def _leak(self) -> None:
        now = time.time()
        elapsed = now - self._last_leak
        self._level = max(0.0, self._level - elapsed * self.leak_rate)
        self._last_leak = now

    def try_acquire(self, amount: int = 1) -> bool:
        """Try to add to bucket without blocking."""
        with self._lock:
            self._leak()
            if self._level + amount <= self.capacity:
                self._level += amount
                return True
            return False

    def acquire(self, amount: int = 1, timeout: float | None = None) -> bool:
        """Acquire with blocking."""
        deadline = time.time() + timeout if timeout else None
        while True:
            if self.try_acquire(amount):
                return True
            if deadline and time.time() >= deadline:
                return False
            time.sleep(0.01)

    @property
    def current_level(self) -> int:
        with self._lock:
            self._leak()
            return int(self._level)


class MultiLimiter:
    """Apply multiple rate limiters (AND logic)."""

    def __init__(self, limiters: list):
        self.limiters = limiters

    def try_acquire(self) -> bool:
        return all(limiter.try_acquire() for limiter in self.limiters)

    def acquire(self, timeout: float | None = None) -> bool:
        deadline = time.time() + timeout if timeout else None
        while True:
            if self.try_acquire():
                return True
            if deadline and time.time() >= deadline:
                return False
            time.sleep(0.01)


class AdaptiveRateLimiter:
    """
    Adaptive rate limiter that adjusts based on success/failure.

    Starts conservative, increases rate on success,
    backs off on failures.
    """

    def __init__(
        self,
        initial_rate: float = 1.0,
        min_rate: float = 0.1,
        max_rate: float = 100.0,
        increase_factor: float = 1.1,
        decrease_factor: float = 0.5,
    ):
        self.rate = initial_rate
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.increase_factor = increase_factor
        self.decrease_factor = decrease_factor
        self._limiter: TokenBucketRateLimiter | None = None
        self._lock = threading.Lock()

    def _get_limiter(self) -> TokenBucketRateLimiter:
        if self._limiter is None or abs(self._limiter.refill_rate - self.rate) > 0.01:
            self._limiter = TokenBucketRateLimiter(
                capacity=self.rate,
                refill_rate=self.rate,
                tokens=self.rate,
            )
        return self._limiter

    def record_success(self) -> None:
        with self._lock:
            self.rate = min(self.rate * self.increase_factor, self.max_rate)
            self._limiter = None

    def record_failure(self) -> None:
        with self._lock:
            self.rate = max(self.rate * self.decrease_factor, self.min_rate)
            self._limiter = None

    def try_acquire(self) -> bool:
        return self._get_limiter().try_acquire()

    def acquire(self, timeout: float | None = None) -> bool:
        return self._get_limiter().acquire(timeout=timeout)
