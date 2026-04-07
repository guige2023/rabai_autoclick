"""
Rate Limiter Utilities

Provides various rate limiting algorithms including token bucket,
sliding window, and fixed window rate limiting.
"""

from __future__ import annotations

import threading
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class RateLimitExceeded(Exception):
    """Raised when rate limit is exceeded."""
    pass


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    requests_per_second: float = 10.0
    requests_per_minute: float | None = None
    requests_per_hour: float | None = None
    burst_size: int = 1


class RateLimiter(ABC):
    """Abstract rate limiter interface."""

    @abstractmethod
    def acquire(self, key: str = "default") -> bool:
        """
        Attempt to acquire a permit.

        Returns:
            True if permit was acquired, False if rate limited.
        """
        pass

    @abstractmethod
    def wait_and_acquire(self, key: str = "default", timeout: float | None = None) -> bool:
        """
        Wait until a permit is available and acquire it.

        Returns:
            True if acquired, False if timed out.
        """
        pass

    @abstractmethod
    def get_wait_time(self, key: str = "default") -> float:
        """
        Get estimated wait time until next permit.

        Returns:
            Wait time in seconds.
        """
        pass


class TokenBucketRateLimiter(RateLimiter):
    """
    Token bucket rate limiter.

    Allows bursty traffic up to burst_size while maintaining
    the average rate over time.
    """

    def __init__(self, config: RateLimitConfig | None = None):
        self._config = config or RateLimitConfig()
        self._buckets: dict[str, TokenBucket] = {}
        self._lock = threading.RLock()

        # Convert rates to tokens per second
        rate = self._config.requests_per_second
        if self._config.requests_per_minute:
            rate = max(rate, self._config.requests_per_minute / 60.0)
        if self._config.requests_per_hour:
            rate = max(rate, self._config.requests_per_hour / 3600.0)

        self._refill_rate = rate
        self._burst_size = self._config.burst_size

    def acquire(self, key: str = "default") -> bool:
        """Try to acquire a permit without blocking."""
        with self._lock:
            bucket = self._get_bucket(key)
            return bucket.try_consume()

    def wait_and_acquire(self, key: str = "default", timeout: float | None = None) -> bool:
        """Wait until a permit is available."""
        start = time.time()

        while True:
            if self.acquire(key):
                return True

            wait_time = self.get_wait_time(key)

            if timeout is not None and time.time() - start + wait_time > timeout:
                return False

            time.sleep(min(wait_time, 0.1))

    def get_wait_time(self, key: str = "default") -> float:
        """Get time until next permit is available."""
        with self._lock:
            bucket = self._get_bucket(key)
            return bucket.time_until_token()

    def _get_bucket(self, key: str) -> TokenBucket:
        """Get or create a bucket for a key."""
        if key not in self._buckets:
            self._buckets[key] = TokenBucket(
                capacity=self._burst_size,
                refill_rate=self._refill_rate,
            )
        return self._buckets[key]


class TokenBucket:
    """Individual token bucket state."""

    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._tokens = float(capacity)
        self._last_refill = time.time()
        self._lock = threading.Lock()

    def try_consume(self, tokens: int = 1) -> bool:
        """Try to consume tokens without blocking."""
        with self._lock:
            self._refill()

            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def time_until_token(self) -> float:
        """Get time until a token is available."""
        with self._lock:
            self._refill()

            if self._tokens >= 1:
                return 0.0

            tokens_needed = 1 - self._tokens
            return tokens_needed / self.refill_rate

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_refill

        tokens_to_add = elapsed * self.refill_rate
        self._tokens = min(self.capacity, self._tokens + tokens_to_add)
        self._last_refill = now


class SlidingWindowRateLimiter(RateLimiter):
    """
    Sliding window rate limiter.

    Uses a sliding window algorithm for smoother rate limiting.
    """

    def __init__(self, config: RateLimitConfig | None = None):
        self._config = config or RateLimitConfig()
        self._requests: dict[str, list[float]] = defaultdict(list)
        self._window_seconds = 1.0  # 1 second window
        self._max_requests = int(self._config.requests_per_second)
        self._lock = threading.RLock()

    def acquire(self, key: str = "default") -> bool:
        """Try to acquire without blocking."""
        with self._lock:
            self._cleanup_old_requests(key)

            if len(self._requests[key]) < self._max_requests:
                self._requests[key].append(time.time())
                return True

            return False

    def wait_and_acquire(self, key: str = "default", timeout: float | None = None) -> bool:
        """Wait until a permit is available."""
        start = time.time()

        while True:
            if self.acquire(key):
                return True

            wait_time = self.get_wait_time(key)

            if timeout is not None and time.time() - start + wait_time > timeout:
                return False

            time.sleep(min(wait_time, 0.1))

    def get_wait_time(self, key: str = "default") -> float:
        """Get time until next permit."""
        with self._lock:
            self._cleanup_old_requests(key)

            if len(self._requests[key]) < self._max_requests:
                return 0.0

            # Time until oldest request expires
            oldest = self._requests[key][0]
            return max(0.0, self._window_seconds - (time.time() - oldest))

    def _cleanup_old_requests(self, key: str) -> None:
        """Remove requests outside the window."""
        cutoff = time.time() - self._window_seconds
        self._requests[key] = [t for t in self._requests[key] if t > cutoff]


class FixedWindowRateLimiter(RateLimiter):
    """
    Fixed window rate limiter.

    Simple and memory-efficient but may allow burst at window boundaries.
    """

    def __init__(self, config: RateLimitConfig | None = None):
        self._config = config or RateLimitConfig()
        self._counters: dict[str, tuple[int, float]] = {}  # (count, window_start)
        self._window_seconds = 1.0
        self._max_requests = int(self._config.requests_per_second)
        self._lock = threading.RLock()

    def acquire(self, key: str = "default") -> bool:
        """Try to acquire without blocking."""
        with self._lock:
            self._check_window(key)

            count, _ = self._counters[key]
            if count < self._max_requests:
                self._counters[key] = (count + 1, time.time())
                return True

            return False

    def wait_and_acquire(self, key: str = "default", timeout: float | None = None) -> bool:
        """Wait until a permit is available."""
        start = time.time()

        while True:
            if self.acquire(key):
                return True

            wait_time = self.get_wait_time(key)

            if timeout is not None and time.time() - start + wait_time > timeout:
                return False

            time.sleep(min(wait_time, 0.1))

    def get_wait_time(self, key: str = "default") -> float:
        """Get time until next permit."""
        with self._lock:
            self._check_window(key)

            count, _ = self._counters[key]
            if count < self._max_requests:
                return 0.0

            return self._window_seconds

    def _check_window(self, key: str) -> None:
        """Reset counter if window has passed."""
        now = time.time()
        if key in self._counters:
            count, window_start = self._counters[key]
            if now - window_start >= self._window_seconds:
                self._counters[key] = (0, now)
        else:
            self._counters[key] = (0, now)


def create_rate_limiter(
    algorithm: str = "token_bucket",
    config: RateLimitConfig | None = None,
) -> RateLimiter:
    """
    Create a rate limiter with the specified algorithm.

    Args:
        algorithm: One of "token_bucket", "sliding_window", "fixed_window"
        config: Rate limit configuration.

    Returns:
        Configured rate limiter instance.
    """
    config = config or RateLimitConfig()

    if algorithm == "token_bucket":
        return TokenBucketRateLimiter(config)
    elif algorithm == "sliding_window":
        return SlidingWindowRateLimiter(config)
    elif algorithm == "fixed_window":
        return FixedWindowRateLimiter(config)
    else:
        raise ValueError(f"Unknown algorithm: {algorithm}")


class RateLimiterContext:
    """Context manager for rate-limited operations."""

    def __init__(
        self,
        limiter: RateLimiter,
        key: str = "default",
        blocking: bool = True,
        timeout: float | None = None,
    ):
        self._limiter = limiter
        self._key = key
        self._blocking = blocking
        self._timeout = timeout

    def __enter__(self) -> bool:
        if self._blocking:
            return self._limiter.wait_and_acquire(self._key, self._timeout)
        return self._limiter.acquire(self._key)

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass


def rate_limit(
    limiter: RateLimiter,
    key: str = "default",
    blocking: bool = True,
    timeout: float | None = None,
) -> RateLimiterContext:
    """
    Create a rate limit context.

    Usage:
        limiter = TokenBucketRateLimiter()
        with rate_limit(limiter):
            # do something rate-limited
            pass
    """
    return RateLimiterContext(limiter, key, blocking, timeout)
