"""
Rate Limiter V2 Action Module

Provides advanced rate limiting with multiple algorithms for UI automation
workflows. Supports token bucket, sliding window, leaky bucket, and adaptive
rate limiting.

Author: AI Agent
Version: 1.0.0
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class RateLimitAlgorithm(Enum):
    """Rate limiting algorithm types."""
    TOKEN_BUCKET = auto()
    SLIDING_WINDOW = auto()
    LEAKY_BUCKET = auto()
    FIXED_WINDOW = auto()
    ADAPTIVE = auto()


@dataclass
class RateLimitConfig:
    """Rate limiter configuration."""
    algorithm: RateLimitAlgorithm = RateLimitAlgorithm.TOKEN_BUCKET
    rate: float = 100.0
    capacity: float = 100.0
    refill_rate: float = 10.0
    window_size: float = 60.0
    burst_size: float = 20.0
    adaptive_target: float = 0.9
    adaptive_increment: float = 0.1
    adaptive_decrement: float = 0.5


@dataclass
class RateLimitResult:
    """Result of rate limit check."""
    allowed: bool
    remaining: float
    reset_at: float
    retry_after: Optional[float] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class RateLimitMetrics:
    """Rate limiter metrics."""
    total_requests: int = 0
    allowed_requests: int = 0
    rejected_requests: int = 0
    total_tokens_consumed: float = 0.0
    current_rate: float = 0.0
    last_update_time: float = field(default_factory=lambda: time.time())


class TokenBucketRateLimiter:
    """
    Token bucket rate limiter.

    Example:
        >>> limiter = TokenBucketRateLimiter(RateLimitConfig(rate=100, capacity=100))
        >>> result = await limiter.acquire(1)
    """

    def __init__(self, config: RateLimitConfig) -> None:
        self.config = config
        self._tokens = config.capacity
        self._last_refill = time.time()
        self._lock = asyncio.Lock()
        self._metrics = RateLimitMetrics()

    async def acquire(self, tokens: float = 1.0) -> RateLimitResult:
        """Acquire tokens from bucket."""
        async with self._lock:
            await self._refill()

            if self._tokens >= tokens:
                self._tokens -= tokens
                self._metrics.total_requests += 1
                self._metrics.allowed_requests += 1
                self._metrics.total_tokens_consumed += tokens

                return RateLimitResult(
                    allowed=True,
                    remaining=self._tokens,
                    reset_at=self._calculate_reset_time(),
                )
            else:
                self._metrics.total_requests += 1
                self._metrics.rejected_requests += 1
                retry_after = (tokens - self._tokens) / self.config.refill_rate

                return RateLimitResult(
                    allowed=False,
                    remaining=self._tokens,
                    reset_at=time.time() + retry_after,
                    retry_after=retry_after,
                )

    async def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_refill
        refill_amount = elapsed * self.config.refill_rate

        self._tokens = min(self.config.capacity, self._tokens + refill_amount)
        self._last_refill = now

    def _calculate_reset_time(self) -> float:
        """Calculate time when bucket will be full."""
        tokens_needed = self.config.capacity - self._tokens
        if tokens_needed <= 0:
            return time.time()
        return time.time() + (tokens_needed / self.config.refill_rate)

    @property
    def metrics(self) -> RateLimitMetrics:
        """Get limiter metrics."""
        return self._metrics


class SlidingWindowRateLimiter:
    """
    Sliding window rate limiter.

    Example:
        >>> limiter = SlidingWindowRateLimiter(RateLimitConfig(rate=100, window_size=60))
        >>> result = await limiter.acquire()
    """

    def __init__(self, config: RateLimitConfig) -> None:
        self.config = config
        self._requests: list[float] = []
        self._lock = asyncio.Lock()
        self._metrics = RateLimitMetrics()

    async def acquire(self, tokens: float = 1.0) -> RateLimitResult:
        """Acquire request slot in window."""
        async with self._lock:
            now = time.time()
            window_start = now - self.config.window_size

            self._requests = [t for t in self._requests if t > window_start]

            if len(self._requests) + tokens <= self.config.rate:
                for _ in range(int(tokens)):
                    self._requests.append(now)

                self._metrics.total_requests += 1
                self._metrics.allowed_requests += 1
                remaining = self.config.rate - len(self._requests)

                return RateLimitResult(
                    allowed=True,
                    remaining=max(0, remaining),
                    reset_at=now + self.config.window_size,
                )
            else:
                self._metrics.total_requests += 1
                self._metrics.rejected_requests += 1

                oldest = min(self._requests) if self._requests else now
                retry_after = (oldest + self.config.window_size) - now

                return RateLimitResult(
                    allowed=False,
                    remaining=0,
                    reset_at=oldest + self.config.window_size,
                    retry_after=max(0, retry_after),
                )

    @property
    def metrics(self) -> RateLimitMetrics:
        """Get limiter metrics."""
        return self._metrics


class LeakyBucketRateLimiter:
    """
    Leaky bucket rate limiter.

    Example:
        >>> limiter = LeakyBucketRateLimiter(RateLimitConfig(rate=10, capacity=50))
        >>> result = await limiter.acquire()
    """

    def __init__(self, config: RateLimitConfig) -> None:
        self.config = config
        self._level = 0.0
        self._last_leak = time.time()
        self._lock = asyncio.Lock()
        self._metrics = RateLimitMetrics()

    async def acquire(self, tokens: float = 1.0) -> RateLimitResult:
        """Acquire space in bucket."""
        async with self._lock:
            await self._leak()

            if self._level + tokens <= self.config.capacity:
                self._level += tokens
                self._metrics.total_requests += 1
                self._metrics.allowed_requests += 1
                remaining = self.config.capacity - self._level

                return RateLimitResult(
                    allowed=True,
                    remaining=remaining,
                    reset_at=time.time() + (self._level / self.config.refill_rate),
                )
            else:
                self._metrics.total_requests += 1
                self._metrics.rejected_requests += 1
                leak_time = (self._level + tokens - self.config.capacity) / self.config.refill_rate

                return RateLimitResult(
                    allowed=False,
                    remaining=0,
                    reset_at=time.time() + leak_time,
                    retry_after=leak_time,
                )

    async def _leak(self) -> None:
        """Leak water from bucket."""
        now = time.time()
        elapsed = now - self._last_leak
        leaked = elapsed * self.config.refill_rate

        self._level = max(0, self._level - leaked)
        self._last_leak = now

    @property
    def metrics(self) -> RateLimitMetrics:
        """Get limiter metrics."""
        return self._metrics


class FixedWindowRateLimiter:
    """
    Fixed window rate limiter.

    Example:
        >>> limiter = FixedWindowRateLimiter(RateLimitConfig(rate=100, window_size=60))
        >>> result = await limiter.acquire()
    """

    def __init__(self, config: RateLimitConfig) -> None:
        self.config = config
        self._window_start = time.time()
        self._count = 0.0
        self._lock = asyncio.Lock()
        self._metrics = RateLimitMetrics()

    async def acquire(self, tokens: float = 1.0) -> RateLimitResult:
        """Acquire request slot in window."""
        async with self._lock:
            now = time.time()

            if now - self._window_start >= self.config.window_size:
                self._window_start = now
                self._count = 0.0

            if self._count + tokens <= self.config.rate:
                self._count += tokens
                self._metrics.total_requests += 1
                self._metrics.allowed_requests += 1
                remaining = self.config.rate - self._count

                return RateLimitResult(
                    allowed=True,
                    remaining=max(0, remaining),
                    reset_at=self._window_start + self.config.window_size,
                )
            else:
                self._metrics.total_requests += 1
                self._metrics.rejected_requests += 1
                retry_after = (self._window_start + self.config.window_size) - now

                return RateLimitResult(
                    allowed=False,
                    remaining=0,
                    reset_at=self._window_start + self.config.window_size,
                    retry_after=max(0, retry_after),
                )

    @property
    def metrics(self) -> RateLimitMetrics:
        """Get limiter metrics."""
        return self._metrics


class AdaptiveRateLimiter:
    """
    Adaptive rate limiter that adjusts based on success rate.

    Example:
        >>> limiter = AdaptiveRateLimiter(RateLimitConfig(rate=100))
        >>> result = await limiter.acquire()
        >>> await limiter.record_success()
    """

    def __init__(self, config: RateLimitConfig) -> None:
        self.config = config
        self._current_rate = config.rate
        self._successes = 0
        self._failures = 0
        self._lock = asyncio.Lock()
        self._metrics = RateLimitMetrics()
        self._window_start = time.time()

    async def acquire(self, tokens: float = 1.0) -> RateLimitResult:
        """Acquire request slot with adaptive rate."""
        async with self._lock:
            now = time.time()

            if now - self._window_start >= self.config.window_size:
                self._adjust_rate()
                self._window_start = now
                self._successes = 0
                self._failures = 0

            if self._current_rate >= tokens:
                self._current_rate -= tokens
                self._metrics.total_requests += 1
                self._metrics.allowed_requests += 1

                return RateLimitResult(
                    allowed=True,
                    remaining=self._current_rate,
                    reset_at=now + self.config.window_size,
                    metadata={"current_rate": self._current_rate},
                )
            else:
                self._metrics.total_requests += 1
                self._metrics.rejected_requests += 1

                return RateLimitResult(
                    allowed=False,
                    remaining=0,
                    reset_at=now + self.config.window_size,
                    retry_after=self.config.window_size,
                    metadata={"current_rate": self._current_rate},
                )

    async def record_success(self) -> None:
        """Record successful request."""
        async with self._lock:
            self._successes += 1

    async def record_failure(self) -> None:
        """Record failed request."""
        async with self._lock:
            self._failures += 1

    def _adjust_rate(self) -> None:
        """Adjust rate based on success/failure ratio."""
        total = self._successes + self._failures
        if total == 0:
            return

        success_rate = self._successes / total

        if success_rate >= self.config.adaptive_target:
            self._current_rate = min(
                self.config.capacity,
                self._current_rate * (1 + self.config.adaptive_increment),
            )
        else:
            self._current_rate = max(
                1.0,
                self._current_rate * (1 - self.config.adaptive_decrement),
            )

        self._metrics.current_rate = self._current_rate

    @property
    def metrics(self) -> RateLimitMetrics:
        """Get limiter metrics."""
        return self._metrics


class RateLimiter:
    """
    Unified rate limiter with algorithm selection.

    Example:
        >>> config = RateLimitConfig(algorithm=RateLimitAlgorithm.TOKEN_BUCKET)
        >>> limiter = RateLimiter(config)
        >>> result = await limiter.acquire(1)
    """

    def __init__(self, config: RateLimitConfig) -> None:
        self.config = config
        self._limiter = self._create_limiter()

    def _create_limiter(self) -> Any:
        """Create appropriate limiter instance."""
        if self.config.algorithm == RateLimitAlgorithm.TOKEN_BUCKET:
            return TokenBucketRateLimiter(self.config)
        elif self.config.algorithm == RateLimitAlgorithm.SLIDING_WINDOW:
            return SlidingWindowRateLimiter(self.config)
        elif self.config.algorithm == RateLimitAlgorithm.LEAKY_BUCKET:
            return LeakyBucketRateLimiter(self.config)
        elif self.config.algorithm == RateLimitAlgorithm.FIXED_WINDOW:
            return FixedWindowRateLimiter(self.config)
        elif self.config.algorithm == RateLimitAlgorithm.ADAPTIVE:
            return AdaptiveRateLimiter(self.config)
        else:
            return TokenBucketRateLimiter(self.config)

    async def acquire(self, tokens: float = 1.0) -> RateLimitResult:
        """Acquire tokens from rate limiter."""
        return await self._limiter.acquire(tokens)

    async def record_success(self) -> None:
        """Record successful request."""
        if isinstance(self._limiter, AdaptiveRateLimiter):
            await self._limiter.record_success()

    async def record_failure(self) -> None:
        """Record failed request."""
        if isinstance(self._limiter, AdaptiveRateLimiter):
            await self._limiter.record_failure()

    @property
    def metrics(self) -> RateLimitMetrics:
        """Get limiter metrics."""
        return self._limiter.metrics


class RateLimiterRegistry:
    """
    Registry for managing multiple rate limiters.

    Example:
        >>> registry = RateLimiterRegistry()
        >>> limiter = registry.get("api", RateLimitConfig(rate=100))
        >>> await limiter.acquire()
    """

    def __init__(self) -> None:
        self._limiters: dict[str, RateLimiter] = {}
        self._configs: dict[str, RateLimitConfig] = {}

    def get(
        self,
        name: str,
        config: Optional[RateLimitConfig] = None,
    ) -> RateLimiter:
        """Get or create rate limiter."""
        if name not in self._limiters:
            self._configs[name] = config or RateLimitConfig()
            self._limiters[name] = RateLimiter(self._configs[name])
            logger.info(f"Created rate limiter: {name} ({self._configs[name].algorithm.name})")
        return self._limiters[name]

    def remove(self, name: str) -> None:
        """Remove rate limiter."""
        if name in self._limiters:
            del self._limiters[name]
        if name in self._configs:
            del self._configs[name]

    def get_all_metrics(self) -> dict[str, RateLimitMetrics]:
        """Get metrics for all limiters."""
        return {name: limiter.metrics for name, limiter in self._limiters.items()}

    def list_limiters(self) -> list[str]:
        """List all registered limiters."""
        return list(self._limiters.keys())

    def __repr__(self) -> str:
        return f"RateLimiterRegistry(limiters={len(self._limiters)})"
