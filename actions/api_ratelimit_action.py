"""
API Rate Limit Action Module.

Provides token bucket and sliding window rate limiting for API calls.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple


class RateLimitExceeded(Exception):
    """Raised when rate limit is exceeded."""
    pass


@dataclass
class TokenBucket:
    """Token bucket algorithm implementation."""
    capacity: int
    refill_rate: float  # tokens per second
    tokens: float = field(init=False)
    last_refill: float = field(init=False)

    def __post_init__(self) -> None:
        self.tokens = float(self.capacity)
        self.last_refill = time.monotonic()

    def consume(self, tokens: int = 1) -> bool:
        """Try to consume tokens. Returns True if successful."""
        self._refill()
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now

    def wait_time(self, tokens: int = 1) -> float:
        """Calculate wait time to acquire tokens."""
        self._refill()
        if self.tokens >= tokens:
            return 0.0
        return (tokens - self.tokens) / self.refill_rate


@dataclass
class SlidingWindowCounter:
    """Sliding window counter for rate limiting."""
    max_requests: int
    window_seconds: float
    requests: Dict[str, list] = field(default_factory=dict)

    def allow(self, key: str) -> bool:
        """Check if request is allowed."""
        now = time.monotonic()
        if key not in self.requests:
            self.requests[key] = []

        # Remove expired entries
        cutoff = now - self.window_seconds
        self.requests[key] = [t for t in self.requests[key] if t > cutoff]

        if len(self.requests[key]) < self.max_requests:
            self.requests[key].append(now)
            return True
        return False

    def reset(self, key: str) -> None:
        """Reset counter for a key."""
        if key in self.requests:
            del self.requests[key]


class RateLimiter:
    """Main rate limiter class supporting multiple strategies."""

    def __init__(
        self,
        strategy: str = "token_bucket",
        requests_per_second: float = 10.0,
        burst_size: int = 20,
    ) -> None:
        self.strategy = strategy
        self.requests_per_second = requests_per_second
        self.burst_size = burst_size

        if strategy == "token_bucket":
            self.bucket = TokenBucket(burst_size, requests_per_second)
        elif strategy == "sliding_window":
            self.counter = SlidingWindowCounter(burst_size, 1.0)
        else:
            raise ValueError(f"Unknown strategy: {strategy}")

    async def acquire(self, tokens: int = 1) -> None:
        """Acquire permission to make a request."""
        if self.strategy == "token_bucket":
            wait_time = self.bucket.wait_time(tokens)
            if wait_time > 0:
                await asyncio.sleep(wait_time)
        elif self.strategy == "sliding_window":
            while not self.counter.allow("global"):
                await asyncio.sleep(0.01)

    def acquire_sync(self, tokens: int = 1) -> None:
        """Synchronous acquire."""
        if self.strategy == "token_bucket":
            while not self.bucket.consume(tokens):
                time.sleep(0.01)
        elif self.strategy == "sliding_window":
            while not self.counter.allow("global"):
                time.sleep(0.01)

    def get_limit_info(self) -> Dict[str, float]:
        """Get current rate limit information."""
        if self.strategy == "token_bucket":
            return {
                "tokens": self.bucket.tokens,
                "capacity": self.bucket.capacity,
                "refill_rate": self.bucket.refill_rate,
            }
        return {}


class PerKeyRateLimiter:
    """Rate limiter with per-key limits."""

    def __init__(
        self,
        default_rps: float = 10.0,
        default_burst: int = 20,
    ) -> None:
        self.default_rps = default_rps
        self.default_burst = default_burst
        self.limiters: Dict[str, RateLimiter] = {}
        self.limits: Dict[str, Tuple[float, int]] = {}

    def set_limit(self, key: str, rps: float, burst: int) -> None:
        """Set custom limit for a key."""
        self.limits[key] = (rps, burst)
        self.limiters[key] = RateLimiter(
            strategy="token_bucket",
            requests_per_second=rps,
            burst_size=burst,
        )

    async def acquire(self, key: str, tokens: int = 1) -> None:
        """Acquire permission for a specific key."""
        if key not in self.limiters:
            self.set_limit(key, self.default_rps, self.default_burst)
        await self.limiters[key].acquire(tokens)

    def allow(self, key: str) -> bool:
        """Synchronous allow check."""
        if key not in self.limiters:
            self.set_limit(key, self.default_rps, self.default_burst)
        return self.limiters[key].counter.allow(key)


async def with_rate_limit(
    func,
    limiter: RateLimiter,
    *args,
    **kwargs
):
    """Decorator pattern for rate-limited function calls."""
    await limiter.acquire()
    return await func(*args, **kwargs)
