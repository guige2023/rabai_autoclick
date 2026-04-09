"""Rate limiter with token bucket and sliding window algorithms.

This module provides rate limiting functionality using multiple algorithms:
- Token bucket: Allows burst traffic while maintaining average rate
- Sliding window: Smooth rate limiting over a rolling time window
- Fixed window: Simple rate limiting with fixed time windows

Example:
    >>> from actions.rate_limiter_action import RateLimiter, TokenBucketLimiter
    >>> limiter = TokenBucketLimiter(rate=10, capacity=20)
    >>> if limiter.allow():
    ...     execute_operation()
"""

from __future__ import annotations

import time
import threading
import logging
from dataclasses import dataclass
from typing import Optional
from enum import Enum

logger = logging.getLogger(__name__)


class RateLimitAlgorithm(Enum):
    """Available rate limiting algorithms."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"


@dataclass
class RateLimitResult:
    """Result of a rate limit check."""
    allowed: bool
    remaining: int
    retry_after: Optional[float]
    total: int


class RateLimiter:
    """Base class for rate limiters."""

    def __init__(self, rate: int, window: float = 1.0) -> None:
        self.rate = rate
        self.window = window

    def allow(self, tokens: int = 1) -> RateLimitResult:
        """Check if the operation should be allowed."""
        raise NotImplementedError

    def reset(self) -> None:
        """Reset the rate limiter state."""
        raise NotImplementedError


class TokenBucketLimiter(RateLimiter):
    """Token bucket rate limiter.

    Allows burst traffic up to the bucket capacity while maintaining
    the specified average rate.

    Attributes:
        rate: Token refill rate per second.
        capacity: Maximum token bucket capacity.
    """

    def __init__(self, rate: int, capacity: int) -> None:
        super().__init__(rate=rate)
        self.capacity = capacity
        self._tokens = float(capacity)
        self._last_refill = time.time()
        self._lock = threading.Lock()

    def allow(self, tokens: int = 1) -> RateLimitResult:
        """Check if the operation should be allowed.

        Args:
            tokens: Number of tokens to consume.

        Returns:
            RateLimitResult indicating if the operation is allowed.
        """
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return RateLimitResult(
                    allowed=True,
                    remaining=int(self._tokens),
                    retry_after=None,
                    total=self.capacity,
                )
            else:
                retry_after = (tokens - self._tokens) / self.rate
                return RateLimitResult(
                    allowed=False,
                    remaining=0,
                    retry_after=retry_after,
                    total=self.capacity,
                )

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last_refill = now

    def reset(self) -> None:
        """Reset the bucket to full capacity."""
        with self._lock:
            self._tokens = float(self.capacity)
            self._last_refill = time.time()


class SlidingWindowLimiter(RateLimiter):
    """Sliding window rate limiter.

    Provides smooth rate limiting over a rolling time window.

    Attributes:
        rate: Maximum requests per window.
        window: Window size in seconds.
    """

    def __init__(self, rate: int, window: float = 1.0) -> None:
        super().__init__(rate=rate, window=window)
        self._requests: list[float] = []
        self._lock = threading.Lock()

    def allow(self, tokens: int = 1) -> RateLimitResult:
        """Check if the operation should be allowed.

        Args:
            tokens: Number of requests to count (default 1).

        Returns:
            RateLimitResult indicating if the operation is allowed.
        """
        with self._lock:
            now = time.time()
            window_start = now - self.window
            self._requests = [t for t in self._requests if t > window_start]
            if len(self._requests) + tokens <= self.rate:
                self._requests.extend([now] * tokens)
                remaining = self.rate - len(self._requests)
                return RateLimitResult(
                    allowed=True,
                    remaining=remaining,
                    retry_after=None,
                    total=self.rate,
                )
            else:
                oldest = min(self._requests) if self._requests else now
                retry_after = oldest + self.window - now
                return RateLimitResult(
                    allowed=False,
                    remaining=0,
                    retry_after=max(0, retry_after),
                    total=self.rate,
                )

    def reset(self) -> None:
        """Reset the request history."""
        with self._lock:
            self._requests.clear()


class FixedWindowLimiter(RateLimiter):
    """Fixed window rate limiter.

    Simple rate limiting with fixed time windows.

    Attributes:
        rate: Maximum requests per window.
        window: Window size in seconds.
    """

    def __init__(self, rate: int, window: float = 1.0) -> None:
        super().__init__(rate=rate, window=window)
        self._count = 0
        self._window_start = time.time()
        self._lock = threading.Lock()

    def allow(self, tokens: int = 1) -> RateLimitResult:
        """Check if the operation should be allowed.

        Args:
            tokens: Number of requests to count (default 1).

        Returns:
            RateLimitResult indicating if the operation is allowed.
        """
        with self._lock:
            now = time.time()
            if now - self._window_start >= self.window:
                self._count = 0
                self._window_start = now

            if self._count + tokens <= self.rate:
                self._count += tokens
                remaining = self.rate - self._count
                return RateLimitResult(
                    allowed=True,
                    remaining=remaining,
                    retry_after=None,
                    total=self.rate,
                )
            else:
                retry_after = self._window_start + self.window - now
                return RateLimitResult(
                    allowed=False,
                    remaining=0,
                    retry_after=max(0, retry_after),
                    total=self.rate,
                )

    def reset(self) -> None:
        """Reset the window and count."""
        with self._lock:
            self._count = 0
            self._window_start = time.time()


class MultiLimiter:
    """Combine multiple rate limiters with AND logic.

    An operation is allowed only if all limiters allow it.
    """

    def __init__(self, limiters: list[RateLimiter]) -> None:
        self.limiters = limiters

    def allow(self, tokens: int = 1) -> RateLimitResult:
        """Check if the operation should be allowed by all limiters."""
        results = [limiter.allow(tokens) for limiter in self.limiters]
        allowed = all(r.allowed for r in results)
        retry_after = max(
            (r.retry_after for r in results if r.retry_after is not None),
            default=None,
        )
        remaining = min(r.remaining for r in results)
        total = results[0].total if results else 0
        return RateLimitResult(
            allowed=allowed,
            remaining=remaining,
            retry_after=retry_after,
            total=total,
        )

    def reset(self) -> None:
        """Reset all limiters."""
        for limiter in self.limiters:
            limiter.reset()


def create_limiter(
    algorithm: RateLimitAlgorithm,
    rate: int,
    **kwargs: int,
) -> RateLimiter:
    """Factory function to create a rate limiter.

    Args:
        algorithm: The rate limiting algorithm to use.
        rate: Maximum requests per window.
        **kwargs: Additional arguments specific to the algorithm.

    Returns:
        A RateLimiter instance.

    Raises:
        ValueError: If an unknown algorithm is specified.
    """
    if algorithm == RateLimitAlgorithm.TOKEN_BUCKET:
        capacity = kwargs.get("capacity", rate)
        return TokenBucketLimiter(rate=rate, capacity=capacity)
    elif algorithm == RateLimitAlgorithm.SLIDING_WINDOW:
        window = kwargs.get("window", 1.0)
        return SlidingWindowLimiter(rate=rate, window=window)
    elif algorithm == RateLimitAlgorithm.FIXED_WINDOW:
        window = kwargs.get("window", 1.0)
        return FixedWindowLimiter(rate=rate, window=window)
    else:
        raise ValueError(f"Unknown algorithm: {algorithm}")
