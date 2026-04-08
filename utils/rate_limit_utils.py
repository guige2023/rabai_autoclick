"""Rate limit utilities for RabAI AutoClick.

Provides:
- Token bucket rate limiter
- Sliding window rate limiter
- Rate-limited decorator
"""

from __future__ import annotations

import threading
import time
from typing import (
    Any,
    Callable,
    Optional,
    TypeVar,
)


T = TypeVar("T")


class TokenBucket:
    """Token bucket rate limiter.

    Args:
        rate: Tokens per second.
        capacity: Maximum tokens in bucket.
    """

    def __init__(
        self,
        rate: float,
        capacity: float,
    ) -> None:
        if rate <= 0 or capacity <= 0:
            raise ValueError("rate and capacity must be positive")
        self._rate = rate
        self._capacity = capacity
        self._tokens = capacity
        self._last_update = time.monotonic()
        self._lock = threading.Lock()

    def consume(self, tokens: float = 1.0) -> bool:
        """Try to consume tokens.

        Args:
            tokens: Number of tokens to consume.

        Returns:
            True if tokens were consumed.
        """
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_update
            self._tokens = min(
                self._capacity,
                self._tokens + elapsed * self._rate,
            )
            self._last_update = now

            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def wait_for_token(self, tokens: float = 1.0) -> float:
        """Wait until tokens are available.

        Args:
            tokens: Number of tokens needed.

        Returns:
            Seconds waited.
        """
        start = time.monotonic()
        while True:
            if self.consume(tokens):
                return time.monotonic() - start
            time.sleep(0.01)


class SlidingWindowRateLimit:
    """Sliding window rate limiter.

    Args:
        max_calls: Maximum calls per window.
        window_seconds: Window size in seconds.
    """

    def __init__(
        self,
        max_calls: int,
        window_seconds: float,
    ) -> None:
        if max_calls <= 0 or window_seconds <= 0:
            raise ValueError("max_calls and window_seconds must be positive")
        self._max_calls = max_calls
        self._window = window_seconds
        self._calls: list = []
        self._lock = threading.Lock()

    def is_allowed(self) -> bool:
        """Check if a call is allowed now.

        Returns:
            True if call is allowed.
        """
        with self._lock:
            now = time.monotonic()
            cutoff = now - self._window
            self._calls = [t for t in self._calls if t > cutoff]
            if len(self._calls) < self._max_calls:
                self._calls.append(now)
                return True
            return False

    def wait_and_call(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Wait for rate limit window then call function.

        Args:
            func: Function to call.
            *args: Positional args.
            **kwargs: Keyword args.

        Returns:
            Function result.
        """
        while not self.is_allowed():
            time.sleep(0.01)
        return func(*args, **kwargs)

    @property
    def remaining(self) -> int:
        """Remaining calls in current window."""
        with self._lock:
            now = time.monotonic()
            cutoff = now - self._window
            self._calls = [t for t in self._calls if t > cutoff]
            return max(0, self._max_calls - len(self._calls))


def rate_limit(
    max_calls: int,
    period: float = 1.0,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to rate-limit a function.

    Args:
        max_calls: Maximum calls per period.
        period: Time period in seconds.

    Returns:
        Decorated function.
    """
    limiter = SlidingWindowRateLimit(max_calls, period)

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        def wrapper(*args: Any, **kwargs: Any) -> T:
            return limiter.wait_and_call(func, *args, **kwargs)
        return wrapper
    return decorator


__all__ = [
    "TokenBucket",
    "SlidingWindowRateLimit",
    "rate_limit",
]
