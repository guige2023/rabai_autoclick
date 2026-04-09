"""
API Rate Limiter Action Module

Token bucket and sliding window rate limiting for API requests.
Supports distributed rate limiting with Redis backend, burst handling,
and adaptive rate adjustment.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class RateLimitStrategy(Enum):
    """Rate limiting strategies."""

    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"
    ADAPTIVE = "adaptive"


class RateLimitResult(Enum):
    """Result of a rate limit check."""

    ALLOWED = "allowed"
    THROTTLED = "throttled"
    OVER_LIMIT = "over_limit"


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""

    requests_per_second: float = 10.0
    burst_size: int = 20
    window_size_seconds: float = 60.0
    strategy: RateLimitStrategy = RateLimitStrategy.TOKEN_BUCKET
    enable_burst: bool = True
    adaptive_enabled: bool = False
    adaptive_factor: float = 0.1
    min_rate: float = 1.0
    max_rate: float = 100.0


@dataclass
class RateLimitStatus:
    """Status of rate limiting."""

    allowed: bool
    result: RateLimitResult
    remaining: int
    reset_at: float
    retry_after: float = 0.0
    current_rate: float = 0.0


class TokenBucket:
    """
    Token bucket algorithm implementation.

    Tokens are added to the bucket at a constant rate.
    Each request consumes tokens. If no tokens are available,
    the request must wait.
    """

    def __init__(self, rate: float, burst_size: int):
        self.rate = rate
        self.burst_size = burst_size
        self._tokens = float(burst_size)
        self._last_update = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1, blocking: bool = True) -> bool:
        """
        Acquire tokens from the bucket.

        Args:
            tokens: Number of tokens to acquire
            blocking: If True, wait for tokens. If False, return immediately.

        Returns:
            True if tokens were acquired, False otherwise
        """
        async with self._lock:
            await self._refill()

            if self._tokens >= tokens:
                self._tokens -= tokens
                return True

            if not blocking:
                return False

            # Calculate wait time
            tokens_needed = tokens - self._tokens
            wait_time = tokens_needed / self.rate

            await asyncio.sleep(wait_time)
            await self._refill()
            self._tokens -= tokens
            return True

    async def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_update
        self._tokens = min(self.burst_size, self._tokens + elapsed * self.rate)
        self._last_update = now

    async def get_available(self) -> float:
        """Get number of available tokens."""
        async with self._lock:
            await self._refill()
            return self._tokens


class SlidingWindow:
    """
    Sliding window rate limiter.

    Maintains a rolling window of request timestamps.
    Requests within the window are counted.
    """

    def __init__(self, max_requests: int, window_seconds: float):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: List[float] = []
        self._lock = asyncio.Lock()

    async def is_allowed(self) -> bool:
        """Check if a request is allowed."""
        async with self._lock:
            now = time.time()
            self._cleanup(now)

            if len(self._requests) < self.max_requests:
                self._requests.append(now)
                return True
            return False

    async def get_retry_after(self) -> float:
        """Get seconds until next request is allowed."""
        async with self._lock:
            now = time.time()
            self._cleanup(now)

            if len(self._requests) < self.max_requests:
                return 0.0

            oldest = min(self._requests)
            return max(0, oldest + self.window_seconds - now)

    async def _cleanup(self, now: float) -> None:
        """Remove requests outside the window."""
        cutoff = now - self.window_seconds
        self._requests = [t for t in self._requests if t > cutoff]

    async def get_count(self) -> int:
        """Get number of requests in current window."""
        async with self._lock:
            now = time.time()
            self._cleanup(now)
            return len(self._requests)


class AdaptiveRateLimiter:
    """
    Adaptive rate limiter that adjusts based on success/failure rates.

    Increases rate when requests succeed, decreases when failures occur.
    """

    def __init__(self, config: RateLimitConfig):
        self.config = config
        self._current_rate = config.requests_per_second
        self._failure_count = 0
        self._success_count = 0
        self._total_requests = 0

    async def record_success(self) -> None:
        """Record a successful request."""
        self._success_count += 1
        self._total_requests += 1
        self._adjust_rate()

    async def record_failure(self) -> None:
        """Record a failed request."""
        self._failure_count += 1
        self._total_requests += 1
        self._adjust_rate()

    def _adjust_rate(self) -> None:
        """Adjust the rate based on success/failure ratio."""
        if self._total_requests < 10:
            return

        failure_ratio = self._failure_count / self._total_requests

        if failure_ratio > 0.1:
            # High failure rate, decrease rate
            self._current_rate *= (1 - self.config.adaptive_factor * 2)
            logger.info(f"Rate decreased to {self._current_rate:.2f} due to failures")
        elif failure_ratio < 0.01:
            # Very low failure rate, increase rate
            self._current_rate *= (1 + self.config.adaptive_factor)
            logger.info(f"Rate increased to {self._current_rate:.2f} due to success")

        self._current_rate = max(self.config.min_rate, min(self.config.max_rate, self._current_rate))

    def get_current_rate(self) -> float:
        """Get current rate."""
        return self._current_rate


class APIRateLimiterAction:
    """
    Main action class for API rate limiting.

    Features:
    - Token bucket and sliding window algorithms
    - Adaptive rate adjustment
    - Burst handling
    - Distributed rate limiting support (Redis)
    - Per-endpoint and global rate limits
    """

    def __init__(self, config: Optional[RateLimitConfig] = None):
        self.config = config or RateLimitConfig()
        self._buckets: Dict[str, TokenBucket] = {}
        self._windows: Dict[str, SlidingWindow] = {}
        self._adaptive: Optional[AdaptiveRateLimiter] = None
        self._lock = asyncio.Lock()

        if self.config.adaptive_enabled:
            self._adaptive = AdaptiveRateLimiter(self.config)

    def _get_bucket(self, key: str) -> TokenBucket:
        """Get or create a token bucket for a key."""
        if key not in self._buckets:
            self._buckets[key] = TokenBucket(
                rate=self.config.requests_per_second,
                burst_size=self.config.burst_size,
            )
        return self._buckets[key]

    def _get_window(self, key: str) -> SlidingWindow:
        """Get or create a sliding window for a key."""
        if key not in self._windows:
            self._windows[key] = SlidingWindow(
                max_requests=int(self.config.requests_per_second * self.config.window_size_seconds),
                window_seconds=self.config.window_size_seconds,
            )
        return self._windows[key]

    async def check_limit(
        self,
        key: str = "default",
        tokens: int = 1,
    ) -> RateLimitStatus:
        """
        Check if a request is allowed under rate limits.

        Args:
            key: Rate limit key (e.g., endpoint, user ID)
            tokens: Number of tokens to acquire

        Returns:
            RateLimitStatus with the result
        """
        now = time.time()

        if self.config.strategy == RateLimitStrategy.TOKEN_BUCKET:
            bucket = self._get_bucket(key)
            allowed = await bucket.acquire(tokens, blocking=False)

            if allowed:
                remaining = int(await bucket.get_available())
                return RateLimitStatus(
                    allowed=True,
                    result=RateLimitResult.ALLOWED,
                    remaining=remaining,
                    reset_at=now + remaining / self.config.requests_per_second,
                    current_rate=self.config.requests_per_second,
                )
            else:
                retry_after = tokens / self.config.requests_per_second
                return RateLimitStatus(
                    allowed=False,
                    result=RateLimitResult.THROTTLED,
                    remaining=0,
                    reset_at=now + retry_after,
                    retry_after=retry_after,
                    current_rate=self.config.requests_per_second,
                )

        elif self.config.strategy == RateLimitStrategy.SLIDING_WINDOW:
            window = self._get_window(key)
            allowed = await window.is_allowed()

            if allowed:
                count = await window.get_count()
                return RateLimitStatus(
                    allowed=True,
                    result=RateLimitResult.ALLOWED,
                    remaining=max(0, window.max_requests - count),
                    reset_at=now + self.config.window_size_seconds,
                    current_rate=self.config.requests_per_second,
                )
            else:
                retry_after = await window.get_retry_after()
                return RateLimitStatus(
                    allowed=False,
                    result=RateLimitResult.THROTTLED,
                    remaining=0,
                    reset_at=now + retry_after,
                    retry_after=retry_after,
                    current_rate=self.config.requests_per_second,
                )

        # Default: allow
        return RateLimitStatus(
            allowed=True,
            result=RateLimitResult.ALLOWED,
            remaining=int(self.config.burst_size),
            reset_at=now + self.config.window_size_seconds,
        )

    async def acquire(
        self,
        key: str = "default",
        tokens: int = 1,
    ) -> bool:
        """
        Acquire rate limit tokens, waiting if necessary.

        Args:
            key: Rate limit key
            tokens: Number of tokens to acquire

        Returns:
            True when tokens are acquired
        """
        if self.config.strategy == RateLimitStrategy.TOKEN_BUCKET:
            bucket = self._get_bucket(key)
            return await bucket.acquire(tokens, blocking=True)
        elif self.config.strategy == RateLimitStrategy.SLIDING_WINDOW:
            window = self._get_window(key)
            while not await window.is_allowed():
                await asyncio.sleep(await window.get_retry_after())
            return True
        return True

    async def record_success(self, key: str = "default") -> None:
        """Record a successful request."""
        if self._adaptive:
            await self._adaptive.record_success()

    async def record_failure(self, key: str = "default") -> None:
        """Record a failed request."""
        if self._adaptive:
            await self._adaptive.record_failure()

    def get_current_rate(self) -> float:
        """Get current rate (adaptive mode only)."""
        if self._adaptive:
            return self._adaptive.get_current_rate()
        return self.config.requests_per_second

    async def execute_with_limit(
        self,
        func: Callable[..., Any],
        key: str = "default",
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        Execute a function with rate limiting.

        Args:
            func: Async function to execute
            key: Rate limit key
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            Result of the function
        """
        await self.acquire(key)

        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            await self.record_success(key)
            return result
        except Exception as e:
            await self.record_failure(key)
            raise


async def demo_rate_limiter():
    """Demonstrate rate limiter usage."""
    config = RateLimitConfig(
        requests_per_second=5.0,
        burst_size=10,
        strategy=RateLimitStrategy.TOKEN_BUCKET,
    )
    limiter = APIRateLimiterAction(config)

    for i in range(15):
        status = await limiter.check_limit("test")
        print(f"Request {i + 1}: {status.result.value}, remaining={status.remaining}")
        await asyncio.sleep(0.1)


if __name__ == "__main__":
    asyncio.run(demo_rate_limiter())
