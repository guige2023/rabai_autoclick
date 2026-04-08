"""Rate limiting utilities for controlling action frequency.

Provides token bucket and sliding window rate limiters,
 decorators for rate-limiting function calls, and
integration helpers for API call throttling.

Example:
    >>> from utils.rate_limit_utils import rate_limit, TokenBucket
    >>> @rate_limit(calls=10, period=1.0)
    ... def api_call():
    ...     return do_api_request()
    >>> bucket = TokenBucket(capacity=5, refill_rate=1.0)
    >>> if bucket.consume():
    ...     do_action()
"""

from __future__ import annotations

import threading
import time
from typing import Callable, Optional

__all__ = [
    "rate_limit",
    "TokenBucket",
    "SlidingWindowRateLimiter",
    "RateLimiter",
]


def rate_limit(calls: int = 10, period: float = 1.0) -> Callable:
    """Decorator to rate-limit a function.

    Args:
        calls: Maximum number of calls allowed.
        period: Time window in seconds.

    Returns:
        Decorated function.

    Example:
        >>> @rate_limit(calls=5, period=1.0)
        ... def throttled_func():
        ...     ...
    """
    limiter = TokenBucket(capacity=calls, refill_rate=float(calls) / period)

    def decorator(fn: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            if limiter.consume():
                return fn(*args, **kwargs)
            else:
                raise Exception(f"Rate limit exceeded: {calls} calls per {period}s")
        return wrapper
    return decorator


class TokenBucket:
    """Token bucket rate limiter.

    Tokens are added at a constant rate up to the capacity.
    Each consume() removes one token.

    Example:
        >>> bucket = TokenBucket(capacity=10, refill_rate=1.0)
        >>> if bucket.consume():
        ...     do_action()
    """

    def __init__(self, capacity: float, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def consume(self, tokens: float = 1.0) -> bool:
        """Try to consume tokens.

        Args:
            tokens: Number of tokens to consume.

        Returns:
            True if tokens were available and consumed.
        """
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def wait_and_consume(self, tokens: float = 1.0, timeout: Optional[float] = None) -> bool:
        """Wait until tokens are available and consume them.

        Args:
            tokens: Number of tokens to consume.
            timeout: Maximum time to wait (None = wait forever).

        Returns:
            True if consumed, False if timeout.
        """
        start = time.monotonic()
        while True:
            if self.consume(tokens):
                return True
            if timeout is not None and time.monotonic() - start >= timeout:
                return False
            time.sleep(0.05)

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
        self._last_refill = now

    @property
    def available_tokens(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens


class SlidingWindowRateLimiter:
    """Sliding window rate limiter.

    Tracks the last N calls and enforces a maximum rate
    over a rolling time window.

    Example:
        >>> limiter = SlidingWindowRateLimiter(max_calls=10, window=60.0)
        >>> if limiter.allow():
        ...     do_action()
    """

    def __init__(self, max_calls: int, window: float):
        self.max_calls = max_calls
        self.window = window
        self._calls: list[float] = []
        self._lock = threading.Lock()

    def allow(self) -> bool:
        """Check if a call is allowed under the rate limit.

        Returns:
            True if the call is allowed.
        """
        with self._lock:
            now = time.monotonic()
            cutoff = now - self.window

            # Remove old calls
            self._calls = [t for t in self._calls if t > cutoff]

            if len(self._calls) < self.max_calls:
                self._calls.append(now)
                return True
            return False

    def wait_and_allow(self, timeout: Optional[float] = None) -> bool:
        """Wait until a call is allowed.

        Returns:
            True if allowed, False if timeout.
        """
        start = time.monotonic()
        while True:
            if self.allow():
                return True
            if timeout is not None and time.monotonic() - start >= timeout:
                return False
            time.sleep(0.1)

    @property
    def remaining(self) -> int:
        with self._lock:
            now = time.monotonic()
            cutoff = now - self.window
            self._calls = [t for t in self._calls if t > cutoff]
            return max(0, self.max_calls - len(self._calls))


class RateLimiter:
    """Combined rate limiter with both capacity and rate constraints.

    Useful for API rate limiting with both burst capacity and sustained rate.
    """

    def __init__(self, capacity: int, refill_rate: float):
        self._bucket = TokenBucket(float(capacity), refill_rate)

    def allow(self) -> bool:
        """Check if an action is allowed."""
        return self._bucket.consume()

    def wait(self, timeout: Optional[float] = None) -> bool:
        """Wait until an action is allowed."""
        return self._bucket.wait_and_consume(timeout=timeout)
