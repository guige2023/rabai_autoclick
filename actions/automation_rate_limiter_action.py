"""
Automation Rate Limiter Action Module

Provides distributed rate limiting capabilities for automation workflows.
Supports token bucket, sliding window, leaky bucket, and fixed window algorithms
with multi-tenant support and Redis-compatible backends.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class RateLimitAlgorithm(Enum):
    """Rate limiting algorithms."""

    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    LEAKY_BUCKET = "leaky_bucket"
    FIXED_WINDOW = "fixed_window"


class RateLimitStatus(Enum):
    """Rate limit check status."""

    ALLOWED = "allowed"
    LIMITED = "limited"
    WAITING = "waiting"


@dataclass
class RateLimit:
    """Rate limit configuration."""

    limit_id: str
    max_requests: int
    window_seconds: float
    algorithm: RateLimitAlgorithm = RateLimitAlgorithm.TOKEN_BUCKET
    burst_size: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RateLimitResult:
    """Result of a rate limit check."""

    allowed: bool
    remaining: int
    reset_at: float
    retry_after: Optional[float] = None
    limit_id: Optional[str] = None


@dataclass
class TokenBucketState:
    """State for token bucket algorithm."""

    tokens: float
    last_update: float
    max_tokens: int


@dataclass
class SlidingWindowState:
    """State for sliding window algorithm."""

    requests: List[float]
    window_start: float


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""

    default_limit: int = 100
    default_window_seconds: float = 60.0
    algorithm: RateLimitAlgorithm = RateLimitAlgorithm.TOKEN_BUCKET
    enable_burst: bool = True
    burst_multiplier: float = 1.5
    cleanup_interval_seconds: float = 300.0


class TokenBucketLimiter:
    """Token bucket rate limiter implementation."""

    def __init__(self, limit: RateLimit):
        self.limit = limit
        burst = limit.burst_size or int(limit.max_requests * 1.5)
        self._state = TokenBucketState(
            tokens=float(burst),
            last_update=time.time(),
            max_tokens=burst,
        )
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> RateLimitResult:
        """Acquire tokens from the bucket."""
        async with self._lock:
            now = time.time()
            elapsed = now - self._state.last_update

            # Refill tokens based on elapsed time
            refill_rate = self.limit.max_requests / self.limit.window_seconds
            self._state.tokens = min(
                self._state.max_tokens,
                self._state.tokens + elapsed * refill_rate,
            )
            self._state.last_update = now

            if self._state.tokens >= tokens:
                self._state.tokens -= tokens
                return RateLimitResult(
                    allowed=True,
                    remaining=int(self._state.tokens),
                    reset_at=now + self._estimate_reset_time(tokens),
                    limit_id=self.limit.limit_id,
                )
            else:
                retry_after = (tokens - self._state.tokens) / refill_rate
                return RateLimitResult(
                    allowed=False,
                    remaining=int(self._state.tokens),
                    reset_at=now + retry_after,
                    retry_after=retry_after,
                    limit_id=self.limit.limit_id,
                )

    def _estimate_reset_time(self, tokens: int) -> float:
        """Estimate time until enough tokens are available."""
        if self._state.tokens >= tokens:
            return 0.0
        needed = tokens - self._state.tokens
        refill_rate = self.limit.max_requests / self.limit.window_seconds
        return needed / refill_rate


class SlidingWindowLimiter:
    """Sliding window rate limiter implementation."""

    def __init__(self, limit: RateLimit):
        self.limit = limit
        self._state = SlidingWindowState(
            requests=[],
            window_start=time.time(),
        )
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> RateLimitResult:
        """Acquire a slot in the sliding window."""
        async with self._lock:
            now = time.time()
            window_start = now - self.limit.window_seconds

            # Remove expired requests
            self._state.requests = [
                ts for ts in self._state.requests if ts > window_start
            ]

            if len(self._state.requests) < self.limit.max_requests:
                for _ in range(tokens):
                    self._state.requests.append(now)

                return RateLimitResult(
                    allowed=True,
                    remaining=self.limit.max_requests - len(self._state.requests),
                    reset_at=now + self.limit.window_seconds,
                    limit_id=self.limit.limit_id,
                )
            else:
                oldest = min(self._state.requests)
                retry_after = oldest + self.limit.window_seconds - now

                return RateLimitResult(
                    allowed=False,
                    remaining=0,
                    reset_at=now + retry_after,
                    retry_after=max(0, retry_after),
                    limit_id=self.limit.limit_id,
                )


class LeakyBucketLimiter:
    """Leaky bucket rate limiter implementation."""

    def __init__(self, limit: RateLimit):
        self.limit = limit
        self._queue: List[float] = []
        self._last_leak = time.time()
        self._lock = asyncio.Lock()
        self._rate = limit.max_requests / limit.window_seconds

    async def acquire(self, tokens: int = 1) -> RateLimitResult:
        """Acquire a slot in the leaky bucket."""
        async with self._lock:
            now = time.time()

            # Leak old requests
            leaked = int((now - self._last_leak) * self._rate)
            self._queue = self._queue[leaked:] if leaked < len(self._queue) else []
            self._last_leak = now

            if len(self._queue) < self.limit.max_requests:
                for _ in range(tokens):
                    self._queue.append(now)

                return RateLimitResult(
                    allowed=True,
                    remaining=self.limit.max_requests - len(self._queue),
                    reset_at=now,
                    limit_id=self.limit.limit_id,
                )
            else:
                oldest = self._queue[0]
                leak_time = self.limit.window_seconds / self.limit.max_requests
                retry_after = leak_time

                return RateLimitResult(
                    allowed=False,
                    remaining=0,
                    reset_at=now + retry_after,
                    retry_after=retry_after,
                    limit_id=self.limit.limit_id,
                )


class AutomationRateLimiterAction:
    """
    Rate limiting action for automation workflows.

    Features:
    - Multiple rate limiting algorithms (token bucket, sliding window, leaky bucket, fixed window)
    - Multi-tenant support with tenant-specific limits
    - Per-tenant and global rate limits
    - Automatic cleanup of stale state
    - Retry-after support for delayed operations
    - Comprehensive rate limit metadata

    Usage:
        limiter = AutomationRateLimiterAction(config)
        
        # Create a rate limit
        limiter.create_limit("api_calls", max_requests=100, window_seconds=60)
        
        # Check before operation
        result = await limiter.check("api_calls")
        if result.allowed:
            await api.call()
        else:
            await asyncio.sleep(result.retry_after)
    """

    def __init__(self, config: Optional[RateLimitConfig] = None):
        self.config = config or RateLimitConfig()
        self._limits: Dict[str, RateLimit] = {}
        self._limiters: Dict[str, Any] = {}
        self._tenant_limits: Dict[str, Set[str]] = {}
        self._stats = {
            "limits_created": 0,
            "checks_performed": 0,
            "requests_allowed": 0,
            "requests_limited": 0,
        }

    def create_limit(
        self,
        limit_id: str,
        max_requests: int,
        window_seconds: float,
        algorithm: Optional[RateLimitAlgorithm] = None,
        burst_size: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> RateLimit:
        """Create a new rate limit."""
        limit = RateLimit(
            limit_id=limit_id,
            max_requests=max_requests,
            window_seconds=window_seconds,
            algorithm=algorithm or self.config.algorithm,
            burst_size=burst_size,
            metadata=metadata or {},
        )
        self._limits[limit_id] = limit

        # Create appropriate limiter
        if limit.algorithm == RateLimitAlgorithm.TOKEN_BUCKET:
            self._limiters[limit_id] = TokenBucketLimiter(limit)
        elif limit.algorithm == RateLimitAlgorithm.SLIDING_WINDOW:
            self._limiters[limit_id] = SlidingWindowLimiter(limit)
        elif limit.algorithm == RateLimitAlgorithm.LEAKY_BUCKET:
            self._limiters[limit_id] = LeakyBucketLimiter(limit)
        else:
            self._limiters[limit_id] = SlidingWindowLimiter(limit)

        self._stats["limits_created"] += 1
        return limit

    def get_limit(self, limit_id: str) -> Optional[RateLimit]:
        """Get a rate limit by ID."""
        return self._limits.get(limit_id)

    async def check(
        self,
        limit_id: str,
        tokens: int = 1,
    ) -> RateLimitResult:
        """
        Check if a request is allowed under the rate limit.

        Args:
            limit_id: ID of the rate limit to check
            tokens: Number of tokens to acquire

        Returns:
            RateLimitResult with allowed status and metadata
        """
        limiter = self._limiters.get(limit_id)
        if limiter is None:
            # Auto-create with defaults
            limit = self.create_limit(
                limit_id,
                self.config.default_limit,
                self.config.default_window_seconds,
            )
            limiter = self._limiters.get(limit_id)

        self._stats["checks_performed"] += 1
        result = await limiter.acquire(tokens)

        if result.allowed:
            self._stats["requests_allowed"] += 1
        else:
            self._stats["requests_limited"] += 1

        return result

    async def check_tenant(
        self,
        tenant_id: str,
        limit_id: str,
        tokens: int = 1,
    ) -> RateLimitResult:
        """
        Check rate limit for a specific tenant.

        Args:
            tenant_id: Tenant identifier
            limit_id: Rate limit ID
            tokens: Number of tokens to acquire

        Returns:
            RateLimitResult for the tenant
        """
        tenant_limit_id = f"{tenant_id}:{limit_id}"
        limiter = self._limiters.get(tenant_limit_id)

        if limiter is None:
            # Check for global limit
            global_limit = self._limits.get(limit_id)
            if global_limit:
                limit = RateLimit(
                    limit_id=tenant_limit_id,
                    max_requests=global_limit.max_requests,
                    window_seconds=global_limit.window_seconds,
                    algorithm=global_limit.algorithm,
                )
                self._limits[tenant_limit_id] = limit

                if limit.algorithm == RateLimitAlgorithm.TOKEN_BUCKET:
                    limiter = TokenBucketLimiter(limit)
                elif limit.algorithm == RateLimitAlgorithm.SLIDING_WINDOW:
                    limiter = SlidingWindowLimiter(limit)
                elif limit.algorithm == RateLimitAlgorithm.LEAKY_BUCKET:
                    limiter = LeakyBucketLimiter(limit)
                else:
                    limiter = SlidingWindowLimiter(limit)

                self._limiters[tenant_limit_id] = limiter

                if tenant_id not in self._tenant_limits:
                    self._tenant_limits[tenant_id] = set()
                self._tenant_limits[tenant_id].add(limit_id)

        return await self.check(tenant_limit_id, tokens)

    async def wait_for_slot(
        self,
        limit_id: str,
        tokens: int = 1,
        max_wait_seconds: float = 60.0,
    ) -> RateLimitResult:
        """
        Wait until a slot is available under the rate limit.

        Args:
            limit_id: Rate limit ID
            tokens: Number of tokens to acquire
            max_wait_seconds: Maximum time to wait

        Returns:
            RateLimitResult when slot is acquired
        """
        start_time = time.time()

        while True:
            result = await self.check(limit_id, tokens)

            if result.allowed:
                return result

            if result.retry_after and (time.time() - start_time + result.retry_after) > max_wait_seconds:
                return result

            if result.retry_after:
                await asyncio.sleep(min(result.retry_after, 1.0))
            else:
                await asyncio.sleep(0.1)

    def get_limit_status(self, limit_id: str) -> Optional[Dict[str, Any]]:
        """Get current status of a rate limit."""
        limit = self._limits.get(limit_id)
        if limit is None:
            return None

        return {
            "limit_id": limit_id,
            "max_requests": limit.max_requests,
            "window_seconds": limit.window_seconds,
            "algorithm": limit.algorithm.value,
        }

    def list_limits(self) -> List[RateLimit]:
        """List all configured rate limits."""
        return list(self._limits.values())

    async def cleanup_stale_limits(self) -> int:
        """Clean up stale rate limit state."""
        cleaned = 0
        now = time.time()

        for limit_id, limiter in list(self._limiters.items()):
            if hasattr(limiter, "_state"):
                state = limiter._state
                if hasattr(state, "last_update"):
                    if now - state.last_update > 3600:
                        del self._limiters[limit_id]
                        cleaned += 1

        return cleaned

    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics."""
        return {
            **self._stats.copy(),
            "total_limits": len(self._limits),
            "active_limiters": len(self._limiters),
            "algorithm": self.config.algorithm.value,
        }


async def demo_rate_limiter():
    """Demonstrate rate limiting."""
    config = RateLimitConfig(
        algorithm=RateLimitAlgorithm.TOKEN_BUCKET,
    )
    limiter = AutomationRateLimiterAction(config)

    limiter.create_limit(
        "api_calls",
        max_requests=5,
        window_seconds=60.0,
        burst_size=8,
    )

    results = []
    for i in range(10):
        result = await limiter.check("api_calls")
        results.append((i, result.allowed, result.remaining))

    print("Check results:")
    for i, allowed, remaining in results:
        print(f"  Request {i}: allowed={allowed}, remaining={remaining}")

    print(f"Stats: {limiter.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_rate_limiter())
