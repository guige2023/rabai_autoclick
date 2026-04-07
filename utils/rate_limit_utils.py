"""Rate limiting and throttling utilities for RabAI AutoClick.

Provides:
- Token bucket rate limiter
- Sliding window rate limiter
- Fixed window rate limiter
- Async rate limiter
- Decorators for rate limiting functions
"""

import asyncio
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Optional, TypeVar
import functools


T = TypeVar("T")


class RateLimitExceeded(Exception):
    """Raised when rate limit is exceeded."""

    def __init__(self, message: str, retry_after: float) -> None:
        self.retry_after = retry_after
        super().__init__(message)


@dataclass
class TokenBucket:
    """Token bucket rate limiter.

    Tokens are added at a constant rate. Each operation consumes
    a token. If no tokens are available, operations must wait.

    Attributes:
        capacity: Maximum number of tokens in the bucket.
        refill_rate: Tokens added per second.
        tokens: Current number of tokens available.
        last_refill: Timestamp of last refill.
    """

    capacity: float
    refill_rate: float
    tokens: float = field(init=False)
    last_refill: float = field(init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def __post_init__(self) -> None:
        self.tokens = self.capacity
        self.last_refill = time.monotonic()

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now

    def consume(self, tokens: float = 1.0, blocking: bool = False, timeout: Optional[float] = None) -> bool:
        """Attempt to consume tokens.

        Args:
            tokens: Number of tokens to consume.
            blocking: Whether to block until tokens are available.
            timeout: Maximum time to wait if blocking.

        Returns:
            True if tokens were consumed, False otherwise.

        Raises:
            RateLimitExceeded: If blocking timeout is exceeded.
        """
        start = time.monotonic()
        while True:
            with self._lock:
                self._refill()
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return True

            if not blocking:
                return False

            if timeout is not None and time.monotonic() - start >= timeout:
                raise RateLimitExceeded(
                    f"Rate limit exceeded, retry after {timeout}s",
                    retry_after=timeout
                )

            wait_time = (tokens - self.tokens) / self.refill_rate
            if timeout is not None:
                wait_time = min(wait_time, timeout - (time.monotonic() - start))
            time.sleep(max(0.001, wait_time))

    def available_tokens(self) -> float:
        """Get number of available tokens."""
        with self._lock:
            self._refill()
            return self.tokens


@dataclass
class SlidingWindowRateLimiter:
    """Sliding window rate limiter with high accuracy.

    Tracks requests in a time window and enforces a maximum
    count within that window.

    Attributes:
        max_requests: Maximum requests allowed in the window.
        window_seconds: Size of the sliding window in seconds.
    """

    max_requests: int
    window_seconds: float
    _requests: deque = field(init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def __post_init__(self) -> None:
        self._requests = deque()

    def _cleanup(self) -> None:
        """Remove expired request timestamps."""
        cutoff = time.monotonic() - self.window_seconds
        while self._requests and self._requests[0] < cutoff:
            self._requests.popleft()

    def acquire(self, blocking: bool = False, timeout: Optional[float] = None) -> bool:
        """Attempt to acquire a request slot.

        Args:
            blocking: Whether to block until a slot is available.
            timeout: Maximum time to wait if blocking.

        Returns:
            True if slot was acquired, False otherwise.

        Raises:
            RateLimitExceeded: If blocking timeout is exceeded.
        """
        start = time.monotonic()
        while True:
            with self._lock:
                self._cleanup()
                if len(self._requests) < self.max_requests:
                    self._requests.append(time.monotonic())
                    return True

            if not blocking:
                return False

            if timeout is not None and time.monotonic() - start >= timeout:
                raise RateLimitExceeded(
                    f"Rate limit exceeded, retry after {timeout}s",
                    retry_after=timeout
                )

            time.sleep(0.01)

    def try_acquire(self) -> bool:
        """Try to acquire a slot without blocking."""
        return self.acquire(blocking=False)

    def reset(self) -> None:
        """Reset the rate limiter, clearing all request history."""
        with self._lock:
            self._requests.clear()

    def remaining(self) -> int:
        """Get number of remaining slots in current window."""
        with self._lock:
            self._cleanup()
            return max(0, self.max_requests - len(self._requests))


@dataclass
class FixedWindowRateLimiter:
    """Fixed window rate limiter (simpler but has boundary edge).

    Divides time into fixed windows and enforces a maximum
    count per window.

    Attributes:
        max_requests: Maximum requests allowed per window.
        window_seconds: Size of each window in seconds.
    """

    max_requests: int
    window_seconds: float
    _count: int = field(init=False, default=0)
    _window_start: float = field(init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def __post_init__(self) -> None:
        self._window_start = time.monotonic()

    def _ensure_window(self) -> None:
        """Reset counter if window has rolled over."""
        now = time.monotonic()
        if now - self._window_start >= self.window_seconds:
            self._count = 0
            self._window_start = now

    def acquire(self, blocking: bool = False, timeout: Optional[float] = None) -> bool:
        """Attempt to acquire a request slot."""
        start = time.monotonic()
        while True:
            with self._lock:
                self._ensure_window()
                if self._count < self.max_requests:
                    self._count += 1
                    return True

            if not blocking:
                return False

            if timeout is not None and time.monotonic() - start >= timeout:
                raise RateLimitExceeded(
                    f"Rate limit exceeded, retry after {timeout}s",
                    retry_after=timeout
                )

            remaining = self.window_seconds - (time.monotonic() - self._window_start)
            time.sleep(max(0.001, min(remaining, timeout or remaining) if timeout else remaining))

    def try_acquire(self) -> bool:
        """Try to acquire without blocking."""
        return self.acquire(blocking=False)

    def reset(self) -> None:
        """Reset the rate limiter."""
        with self._lock:
            self._count = 0
            self._window_start = time.monotonic()


class AsyncTokenBucket:
    """Async token bucket rate limiter."""

    def __init__(self, capacity: float, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_refill = asyncio.get_event_loop().time()
        self._lock = asyncio.Lock()

    async def _refill(self) -> None:
        now = asyncio.get_event_loop().time()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now

    async def acquire(self, tokens: float = 1.0) -> bool:
        async with self._lock:
            await self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
        return False

    async def consume(self, tokens: float = 1.0) -> None:
        while True:
            async with self._lock:
                await self._refill()
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return
            await asyncio.sleep(0.01)


def rate_limit(max_calls: int, period: float, blocking: bool = True):
    """Decorator to rate limit a function.

    Args:
        max_calls: Maximum number of calls allowed in the period.
        period: Time period in seconds.
        blocking: Whether to block until rate limit clears.

    Returns:
        Decorated function.
    """
    limiter = SlidingWindowRateLimiter(max_requests=max_calls, window_seconds=period)

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            limiter.acquire(blocking=blocking)
            return func(*args, **kwargs)
        return wrapper
    return decorator


def rate_limit_async(max_calls: int, period: float):
    """Async decorator to rate limit a function.

    Args:
        max_calls: Maximum number of calls allowed in the period.
        period: Time period in seconds.

    Returns:
        Decorated async function.
    """
    limiter = AsyncTokenBucket(capacity=max_calls, refill_rate=max_calls / period)

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            await limiter.consume()
            return func(*args, **kwargs)
        return wrapper
    return decorator
