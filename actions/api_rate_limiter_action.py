"""
API Rate Limiter Action Module.

Provides token bucket and sliding window rate limiting for API calls.
"""

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class RateLimitStrategy(Enum):
    """Rate limiting strategies."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"
    LEAKY_BUCKET = "leaky_bucket"


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    requests_per_second: float = 10.0
    burst_size: int = 20
    strategy: RateLimitStrategy = RateLimitStrategy.TOKEN_BUCKET
    block_duration: float = 1.0


class TokenBucket:
    """Token bucket algorithm implementation."""

    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens, return True if successful."""
        async with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(
            self.capacity,
            self._tokens + elapsed * self.refill_rate
        )
        self._last_refill = now


class SlidingWindow:
    """Sliding window algorithm implementation."""

    def __init__(self, max_requests: int, window_size: float):
        self.max_requests = max_requests
        self.window_size = window_size
        self._requests: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        """Acquire slot, return True if successful."""
        async with self._lock:
            now = time.monotonic()
            cutoff = now - self.window_size

            self._requests = [r for r in self._requests if r > cutoff]

            if len(self._requests) < self.max_requests:
                self._requests.append(now)
                return True
            return False

    def get_wait_time(self) -> float:
        """Get time to wait before next available slot."""
        if len(self._requests) < self.max_requests:
            return 0.0
        oldest = min(self._requests)
        return max(0.0, oldest + self.window_size - time.monotonic())


class LeakyBucket:
    """Leaky bucket algorithm implementation."""

    def __init__(self, capacity: int, leak_rate: float):
        self.capacity = capacity
        self.leak_rate = leak_rate
        self._level = 0.0
        self._last_leak = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        """Acquire slot, return True if successful."""
        async with self._lock:
            self._leak()
            if self._level < self.capacity:
                self._level += 1
                return True
            return False

    def _leak(self) -> None:
        """Leak water based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_leak
        self._level = max(0.0, self._level - elapsed * self.leak_rate)
        self._last_leak = now


class RateLimiter:
    """Main rate limiter class."""

    def __init__(self, config: RateLimitConfig):
        self.config = config

        if config.strategy == RateLimitStrategy.TOKEN_BUCKET:
            self._limiter = TokenBucket(
                capacity=config.burst_size,
                refill_rate=config.requests_per_second
            )
        elif config.strategy == RateLimitStrategy.SLIDING_WINDOW:
            self._limiter = SlidingWindow(
                max_requests=int(config.requests_per_second * config.block_duration),
                window_size=config.block_duration
            )
        elif config.strategy == RateLimitStrategy.LEAKY_BUCKET:
            self._limiter = LeakyBucket(
                capacity=config.burst_size,
                leak_rate=config.requests_per_second
            )
        else:
            self._limiter = TokenBucket(
                capacity=config.burst_size,
                refill_rate=config.requests_per_second
            )

    async def acquire(self) -> bool:
        """Attempt to acquire rate limit slot."""
        return await self._limiter.acquire()

    async def wait_for_slot(self, timeout: Optional[float] = None) -> bool:
        """Wait for available slot."""
        start = time.monotonic()
        while True:
            if await self.acquire():
                return True
            if timeout and (time.monotonic() - start) >= timeout:
                return False

            wait_time = 0.01
            if hasattr(self._limiter, "get_wait_time"):
                wait_time = self._limiter.get_wait_time()
            await asyncio.sleep(min(wait_time, 0.1))


class APIRateLimiterAction:
    """
    Rate limiter for API calls with multiple strategies.

    Example:
        limiter = APIRateLimiterAction(
            requests_per_second=10,
            burst_size=20,
            strategy=RateLimitStrategy.TOKEN_BUCKET
        )
        await limiter.acquire()
        result = api.call()
    """

    def __init__(
        self,
        requests_per_second: float = 10.0,
        burst_size: int = 20,
        strategy: RateLimitStrategy = RateLimitStrategy.TOKEN_BUCKET,
        block_duration: float = 1.0
    ):
        config = RateLimitConfig(
            requests_per_second=requests_per_second,
            burst_size=burst_size,
            strategy=strategy,
            block_duration=block_duration
        )
        self._limiter = RateLimiter(config)

    async def acquire(self) -> bool:
        """Acquire rate limit slot."""
        return await self._limiter.acquire()

    async def execute(
        self,
        func: callable,
        *args: Any,
        **kwargs: Any
    ) -> Any:
        """Execute function with rate limiting."""
        await self._limiter.wait_for_slot()
        return func(*args, **kwargs)

    async def execute_async(
        self,
        func: callable,
        *args: Any,
        **kwargs: Any
    ) -> Any:
        """Execute async function with rate limiting."""
        await self._limiter.wait_for_slot()
        return await func(*args, **kwargs)
