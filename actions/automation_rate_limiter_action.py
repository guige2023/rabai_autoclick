"""Rate limiter for controlling execution frequency.

Supports token bucket, sliding window, and fixed window algorithms.
"""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable

logger = logging.getLogger(__name__)


class RateLimitExceeded(Exception):
    """Raised when rate limit is exceeded."""

    def __init__(self, retry_after: float) -> None:
        self.retry_after = retry_after
        super().__init__(f"Rate limit exceeded. Retry after {retry_after:.2f}s")


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""

    requests_per_second: float = 10.0
    requests_per_minute: int = 100
    requests_per_hour: int = 1000
    burst_size: int | None = None
    initial_tokens: float | None = None


class RateLimiter(ABC):
    """Abstract base for rate limiters."""

    @abstractmethod
    async def acquire(self, tokens: int = 1) -> None:
        """Acquire permission to proceed."""
        pass

    @abstractmethod
    async def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire without blocking."""
        pass

    @abstractmethod
    def available(self) -> float:
        """Get available tokens."""
        pass

    @abstractmethod
    def reset(self) -> None:
        """Reset the rate limiter."""
        pass


class TokenBucketLimiter(RateLimiter):
    """Token bucket rate limiter.

    Allows burst traffic up to bucket size, then refills at steady rate.
    """

    def __init__(self, config: RateLimitConfig) -> None:
        self.config = config
        self.capacity = config.burst_size or int(config.requests_per_second)
        self.tokens = float(self.capacity if config.initial_tokens is None else config.initial_tokens)
        self.refill_rate = config.requests_per_second
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now

    async def acquire(self, tokens: int = 1) -> None:
        """Acquire tokens, waiting if necessary."""
        async with self._lock:
            self._refill()
            while self.tokens < tokens:
                wait_time = (tokens - self.tokens) / self.refill_rate
                logger.debug("Rate limit reached, waiting %.2fs", wait_time)
                await asyncio.sleep(wait_time)
                self._refill()
            self.tokens -= tokens

    async def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire tokens without blocking."""
        async with self._lock:
            self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    def available(self) -> float:
        """Get available tokens."""
        self._refill()
        return self.tokens

    def reset(self) -> None:
        """Reset the bucket."""
        self.tokens = float(self.capacity)
        self.last_refill = time.monotonic()


class SlidingWindowLimiter(RateLimiter):
    """Sliding window rate limiter with precise limiting."""

    def __init__(self, config: RateLimitConfig) -> None:
        self.config = config
        self.max_requests = config.requests_per_second
        self.window_seconds = 1.0
        self.requests: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> None:
        """Acquire permission, waiting if window is full."""
        for _ in range(tokens):
            async with self._lock:
                now = time.monotonic()
                cutoff = now - self.window_seconds
                self.requests = [t for t in self.requests if t > cutoff]

                while len(self.requests) >= self.max_requests:
                    sleep_time = self.requests[0] - cutoff
                    await asyncio.sleep(sleep_time)
                    now = time.monotonic()
                    cutoff = now - self.window_seconds
                    self.requests = [t for t in self.requests if t > cutoff]

                self.requests.append(now)

    async def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire without blocking."""
        async with self._lock:
            now = time.monotonic()
            cutoff = now - self.window_seconds
            self.requests = [t for t in self.requests if t > cutoff]

            if len(self.requests) + tokens <= self.max_requests:
                for _ in range(tokens):
                    self.requests.append(now)
                return True
            return False

    def available(self) -> float:
        """Get available requests in current window."""
        now = time.monotonic()
        cutoff = now - self.window_seconds
        self.requests = [t for t in self.requests if t > cutoff]
        return max(0, self.max_requests - len(self.requests))

    def reset(self) -> None:
        """Reset the window."""
        self.requests = []


class FixedWindowLimiter(RateLimiter):
    """Fixed window rate limiter with simple implementation."""

    def __init__(self, config: RateLimitConfig) -> None:
        self.config = config
        self.max_requests = config.requests_per_second
        self.window_seconds = 1.0
        self.current_requests = 0
        self.window_start = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> None:
        """Acquire permission, waiting if window limit reached."""
        for _ in range(tokens):
            async with self._lock:
                self._advance_window()
                while self.current_requests >= self.max_requests:
                    sleep_time = self.window_start + self.window_seconds - time.monotonic()
                    await asyncio.sleep(sleep_time)
                    self._advance_window()
                self.current_requests += 1

    async def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire without blocking."""
        async with self._lock:
            self._advance_window()
            if self.current_requests + tokens <= self.max_requests:
                self.current_requests += tokens
                return True
            return False

    def _advance_window(self) -> None:
        """Advance window if expired."""
        now = time.monotonic()
        if now >= self.window_start + self.window_seconds:
            self.current_requests = 0
            self.window_start = now

    def available(self) -> float:
        """Get available requests in current window."""
        self._advance_window()
        return max(0, self.max_requests - self.current_requests)

    def reset(self) -> None:
        """Reset the limiter."""
        self.current_requests = 0
        self.window_start = time.monotonic()


class MultiTierRateLimiter(RateLimiter):
    """Rate limiter combining multiple time windows."""

    def __init__(self, config: RateLimitConfig) -> None:
        self.config = config
        self.limits: list[RateLimiter] = []

        if config.requests_per_second > 0:
            self.limits.append(TokenBucketLimiter(config))
        if config.requests_per_minute > 0:
            self.limits.append(SlidingWindowLimiter(RateLimitConfig(requests_per_second=config.requests_per_minute / 60)))
        if config.requests_per_hour > 0:
            self.limits.append(FixedWindowLimiter(RateLimitConfig(requests_per_second=config.requests_per_hour / 3600)))

        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> None:
        """Acquire permission from all limiters."""
        for limiter in self.limits:
            await limiter.acquire(tokens)

    async def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire from all limiters without blocking."""
        async with self._lock:
            for limiter in self.limits:
                if not await limiter.try_acquire(tokens):
                    return False
            return True

    def available(self) -> float:
        """Get minimum available across all tiers."""
        if not self.limits:
            return float("inf")
        return min(limiter.available() for limiter in self.limits)

    def reset(self) -> None:
        """Reset all limiters."""
        for limiter in self.limits:
            limiter.reset()


def create_rate_limiter(
    config: RateLimitConfig | None = None,
    algorithm: str = "token_bucket",
) -> RateLimiter:
    """Create a rate limiter with specified algorithm.

    Args:
        config: Rate limit configuration.
        algorithm: One of 'token_bucket', 'sliding_window', 'fixed_window', 'multi'.

    Returns:
        Configured rate limiter instance.
    """
    config = config or RateLimitConfig()
    if algorithm == "token_bucket":
        return TokenBucketLimiter(config)
    elif algorithm == "sliding_window":
        return SlidingWindowLimiter(config)
    elif algorithm == "fixed_window":
        return FixedWindowLimiter(config)
    elif algorithm == "multi":
        return MultiTierRateLimiter(config)
    else:
        raise ValueError(f"Unknown algorithm: {algorithm}")


class RateLimitedExecutor:
    """Execute functions with rate limiting applied."""

    def __init__(self, limiter: RateLimiter) -> None:
        self.limiter = limiter

    async def execute(self, fn: Callable, *args, **kwargs) -> Any:
        """Execute function with rate limiting."""
        await self.limiter.acquire()
        return await fn(*args, **kwargs)

    def sync_execute(self, fn: Callable, *args, **kwargs) -> Any:
        """Synchronous execute with rate limiting."""
        asyncio.get_event_loop().run_until_complete(self.execute(fn, *args, **kwargs))
