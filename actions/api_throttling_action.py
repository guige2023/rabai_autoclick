"""
API Throttling Action Module.

Provides API throttling and rate limiting with
token bucket, sliding window, and fixed window algorithms.
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging
import time

logger = logging.getLogger(__name__)


class ThrottleAlgorithm(Enum):
    """Throttling algorithms."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"
    LEAKY_BUCKET = "leaky_bucket"


@dataclass
class ThrottleConfig:
    """Throttle configuration."""
    algorithm: ThrottleAlgorithm
    rate: float
    capacity: float
    window_size: Optional[float] = None


@dataclass
class ThrottleResult:
    """Result of throttle check."""
    allowed: bool
    remaining: float
    reset_at: Optional[datetime] = None
    retry_after: Optional[float] = None


class TokenBucket:
    """Token bucket algorithm."""

    def __init__(self, rate: float, capacity: float):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def consume(self, tokens: float = 1.0) -> ThrottleResult:
        """Try to consume tokens."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.last_update = now

            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

            if self.tokens >= tokens:
                self.tokens -= tokens
                return ThrottleResult(
                    allowed=True,
                    remaining=self.tokens
                )
            else:
                wait_time = (tokens - self.tokens) / self.rate
                return ThrottleResult(
                    allowed=False,
                    remaining=self.tokens,
                    retry_after=wait_time
                )


class SlidingWindow:
    """Sliding window algorithm."""

    def __init__(self, rate: float, window_size: float):
        self.rate = rate
        self.window_size = window_size
        self.requests: List[float] = []
        self._lock = asyncio.Lock()

    async def check(self) -> ThrottleResult:
        """Check if request is allowed."""
        async with self._lock:
            now = time.monotonic()
            cutoff = now - self.window_size

            self.requests = [r for r in self.requests if r > cutoff]

            if len(self.requests) < self.rate:
                self.requests.append(now)
                return ThrottleResult(
                    allowed=True,
                    remaining=self.rate - len(self.requests)
                )
            else:
                oldest = min(self.requests)
                retry_after = oldest + self.window_size - now
                return ThrottleResult(
                    allowed=False,
                    remaining=0,
                    retry_after=retry_after
                )


class FixedWindow:
    """Fixed window algorithm."""

    def __init__(self, rate: float, window_size: float):
        self.rate = rate
        self.window_size = window_size
        self.count = 0
        self.window_start = time.monotonic()
        self._lock = asyncio.Lock()

    async def check(self) -> ThrottleResult:
        """Check if request is allowed."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.window_start

            if elapsed >= self.window_size:
                self.count = 0
                self.window_start = now

            if self.count < self.rate:
                self.count += 1
                reset_at = datetime.now() + timedelta(
                    seconds=self.window_size - elapsed
                )
                return ThrottleResult(
                    allowed=True,
                    remaining=self.rate - self.count,
                    reset_at=reset_at
                )
            else:
                reset_at = datetime.now() + timedelta(
                    seconds=self.window_size - elapsed
                )
                return ThrottleResult(
                    allowed=False,
                    remaining=0,
                    reset_at=reset_at,
                    retry_after=self.window_size - elapsed
                )


class LeakyBucket:
    """Leaky bucket algorithm."""

    def __init__(self, rate: float, capacity: float):
        self.rate = rate
        self.capacity = capacity
        self.level = 0.0
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def check(self) -> ThrottleResult:
        """Check if request is allowed."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.last_update = now

            self.level = max(0, self.level - elapsed * self.rate)

            if self.level < self.capacity:
                self.level += 1
                return ThrottleResult(
                    allowed=True,
                    remaining=self.capacity - self.level
                )
            else:
                wait_time = (self.level - self.capacity + 1) / self.rate
                return ThrottleResult(
                    allowed=False,
                    remaining=0,
                    retry_after=wait_time
                )


class ThrottleManager:
    """Manages throttling for multiple clients."""

    def __init__(self):
        self.limiters: Dict[str, Any] = {}
        self.default_config = ThrottleConfig(
            algorithm=ThrottleAlgorithm.TOKEN_BUCKET,
            rate=100,
            capacity=100
        )

    def set_limiter(self, client_id: str, config: ThrottleConfig):
        """Set limiter for client."""
        if config.algorithm == ThrottleAlgorithm.TOKEN_BUCKET:
            limiter = TokenBucket(config.rate, config.capacity)
        elif config.algorithm == ThrottleAlgorithm.SLIDING_WINDOW:
            limiter = SlidingWindow(config.rate, config.window_size or 60)
        elif config.algorithm == ThrottleAlgorithm.FIXED_WINDOW:
            limiter = FixedWindow(config.rate, config.window_size or 60)
        elif config.algorithm == ThrottleAlgorithm.LEAKY_BUCKET:
            limiter = LeakyBucket(config.rate, config.capacity)
        else:
            limiter = TokenBucket(config.rate, config.capacity)

        self.limiters[client_id] = limiter

    async def check(self, client_id: str, tokens: float = 1.0) -> ThrottleResult:
        """Check if request is allowed."""
        if client_id not in self.limiters:
            self.set_limiter(client_id, self.default_config)

        limiter = self.limiters[client_id]

        if isinstance(limiter, TokenBucket):
            return await limiter.consume(tokens)
        elif isinstance(limiter, (SlidingWindow, FixedWindow)):
            return await limiter.check()
        elif isinstance(limiter, LeakyBucket):
            return await limiter.check()

        return ThrottleResult(allowed=True, remaining=0)

    def remove_limiter(self, client_id: str) -> bool:
        """Remove limiter for client."""
        if client_id in self.limiters:
            del self.limiters[client_id]
            return True
        return False


async def main():
    """Demonstrate API throttling."""
    manager = ThrottleManager()

    manager.set_limiter("client1", ThrottleConfig(
        algorithm=ThrottleAlgorithm.TOKEN_BUCKET,
        rate=5,
        capacity=10
    ))

    for i in range(12):
        result = await manager.check("client1")
        print(f"Request {i+1}: allowed={result.allowed}, remaining={result.remaining:.1f}")


if __name__ == "__main__":
    asyncio.run(main())
