"""API Rate Limiter Action Module.

Provides token bucket and sliding window rate limiting for API calls.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Generic, TypeVar

T = TypeVar("T")


class RateLimitStrategy(Enum):
    """Rate limiting strategies."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"


@dataclass
class TokenBucketConfig:
    """Configuration for token bucket algorithm."""
    capacity: int = 100
    refill_rate: float = 10.0
    initial_tokens: float | None = None


@dataclass
class SlidingWindowConfig:
    """Configuration for sliding window algorithm."""
    window_size_seconds: float = 60.0
    max_requests: int = 100


class TokenBucket:
    """Token bucket rate limiter implementation."""

    def __init__(self, config: TokenBucketConfig) -> None:
        self.capacity = config.capacity
        self.refill_rate = config.refill_rate
        self.tokens = config.initial_tokens if config.initial_tokens is not None else config.capacity
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens from the bucket."""
        async with self._lock:
            self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    async def wait_for_tokens(self, tokens: int = 1, timeout: float | None = None) -> bool:
        """Wait until tokens are available."""
        start = time.monotonic()
        while True:
            if await self.acquire(tokens):
                return True
            if timeout and (time.monotonic() - start) >= timeout:
                return False
            await asyncio.sleep(0.05)

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now


class SlidingWindowRateLimiter:
    """Sliding window rate limiter implementation."""

    def __init__(self, config: SlidingWindowConfig) -> None:
        self.window_size = config.window_size_seconds
        self.max_requests = config.max_requests
        self.requests: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        """Check if request is allowed under the limit."""
        async with self._lock:
            now = time.monotonic()
            cutoff = now - self.window_size
            while self.requests and self.requests[0] < cutoff:
                self.requests.popleft()
            if len(self.requests) < self.max_requests:
                self.requests.append(now)
                return True
            return False

    async def wait_for_slot(self, timeout: float | None = None) -> bool:
        """Wait until a request slot becomes available."""
        start = time.monotonic()
        while True:
            if await self.acquire():
                return True
            if timeout and (time.monotonic() - start) >= timeout:
                return False
            await asyncio.sleep(0.05)

    def get_current_count(self) -> int:
        """Get current request count in window."""
        now = time.monotonic()
        cutoff = now - self.window_size
        while self.requests and self.requests[0] < cutoff:
            self.requests.popleft()
        return len(self.requests)


@dataclass
class RateLimiterStats:
    """Statistics for rate limiter."""
    total_requests: int = 0
    allowed_requests: int = 0
    rejected_requests: int = 0
    total_wait_time: float = 0.0


class APICallThrottle(Generic[T]):
    """Throttled API call wrapper with rate limiting."""

    def __init__(
        self,
        func: Callable[..., T],
        strategy: RateLimitStrategy = RateLimitStrategy.TOKEN_BUCKET,
        token_config: TokenBucketConfig | None = None,
        sliding_config: SlidingWindowConfig | None = None,
    ) -> None:
        self.func = func
        self.strategy = strategy
        self.stats = RateLimiterStats()
        if strategy == RateLimitStrategy.TOKEN_BUCKET:
            config = token_config or TokenBucketConfig()
            self._limiter: TokenBucket | SlidingWindowRateLimiter = TokenBucket(config)
        else:
            config = sliding_config or SlidingWindowConfig()
            self._limiter = SlidingWindowRateLimiter(config)

    async def call_async(self, *args, timeout: float | None = None, **kwargs) -> T | None:
        """Make a throttled async API call."""
        self.stats.total_requests += 1
        wait_start = time.monotonic()
        if isinstance(self._limiter, TokenBucket):
            acquired = await self._limiter.wait_for_tokens(timeout=timeout)
        else:
            acquired = await self._limiter.wait_for_slot(timeout=timeout)
        self.stats.total_wait_time += time.monotonic() - wait_start
        if not acquired:
            self.stats.rejected_requests += 1
            return None
        self.stats.allowed_requests += 1
        return await self.func(*args, **kwargs)

    def call_sync(self, *args, **kwargs) -> T | None:
        """Make a throttled sync API call (blocking)."""
        self.stats.total_requests += 1
        if isinstance(self._limiter, TokenBucket):
            self._limiter._lock.lock()
            self._limiter._refill()
            while self._limiter.tokens < 1:
                self._limiter._lock.unlock()
                time.sleep(0.05)
                self._limiter._lock.lock()
                self._limiter._refill()
            self._limiter.tokens -= 1
            self._limiter._lock.unlock()
        else:
            raise NotImplementedError("Sync calls not supported for sliding window")
        self.stats.allowed_requests += 1
        return self.func(*args, **kwargs)

    def get_stats(self) -> RateLimiterStats:
        """Get rate limiter statistics."""
        return self.stats


class RateLimitExceededError(Exception):
    """Raised when rate limit is exceeded and timeout expires."""
    pass
