"""API Rate Limiter Action Module.

Provides rate limiting with token bucket, sliding window,
leaky bucket algorithms for API clients.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set
import logging

logger = logging.getLogger(__name__)


class RateLimitAlgorithm(Enum):
    """Rate limiting algorithm."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    LEAKY_BUCKET = "leaky_bucket"
    FIXED_WINDOW = "fixed_window"


@dataclass
class RateLimitConfig:
    """Rate limit configuration."""
    requests_per_second: float = 10.0
    burst_size: int = 20
    algorithm: RateLimitAlgorithm = RateLimitAlgorithm.TOKEN_BUCKET


@dataclass
class RateLimitResult:
    """Result of rate limit check."""
    allowed: bool
    remaining: int
    retry_after: Optional[float] = None
    reset_at: Optional[float] = None


class APIRateLimiterAction:
    """Rate limiter for API requests.

    Example:
        limiter = APIRateLimiterAction(
            RateLimitConfig(requests_per_second=10, burst_size=20)
        )

        result = await limiter.acquire("user_123")
        if result.allowed:
            await api_call()
        else:
            await asyncio.sleep(result.retry_after)
    """

    def __init__(self, config: Optional[RateLimitConfig] = None) -> None:
        self.config = config or RateLimitConfig()
        self._buckets: Dict[str, Dict] = {}
        self._window_counts: Dict[str, List[float]] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()

    async def acquire(
        self,
        key: str,
        cost: int = 1,
    ) -> RateLimitResult:
        """Acquire rate limit token.

        Args:
            key: Identifier (user, API key, IP, etc.)
            cost: Number of tokens to acquire

        Returns:
            RateLimitResult with acquisition status
        """
        async with self._global_lock:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
                self._init_bucket(key)

        async with self._locks[key]:
            if self.config.algorithm == RateLimitAlgorithm.TOKEN_BUCKET:
                return await self._acquire_token_bucket(key, cost)
            elif self.config.algorithm == RateLimitAlgorithm.SLIDING_WINDOW:
                return await self._acquire_sliding_window(key, cost)
            elif self.config.algorithm == RateLimitAlgorithm.LEAKY_BUCKET:
                return await self._acquire_leaky_bucket(key, cost)
            else:
                return await self._acquire_fixed_window(key, cost)

    def _init_bucket(self, key: str) -> None:
        """Initialize rate limit bucket for key."""
        self._buckets[key] = {
            "tokens": float(self.config.burst_size),
            "last_update": time.time(),
        }
        self._window_counts[key] = []

    async def _acquire_token_bucket(
        self,
        key: str,
        cost: int,
    ) -> RateLimitResult:
        """Token bucket rate limiting."""
        bucket = self._buckets[key]
        now = time.time()
        elapsed = now - bucket["last_update"]

        tokens_to_add = elapsed * self.config.requests_per_second
        bucket["tokens"] = min(
            self.config.burst_size,
            bucket["tokens"] + tokens_to_add
        )
        bucket["last_update"] = now

        if bucket["tokens"] >= cost:
            bucket["tokens"] -= cost
            return RateLimitResult(
                allowed=True,
                remaining=int(bucket["tokens"]),
                reset_at=now + (cost / self.config.requests_per_second),
            )

        retry_after = (cost - bucket["tokens"]) / self.config.requests_per_second
        return RateLimitResult(
            allowed=False,
            remaining=0,
            retry_after=retry_after,
            reset_at=now + retry_after,
        )

    async def _acquire_sliding_window(
        self,
        key: str,
        cost: int,
    ) -> RateLimitResult:
        """Sliding window rate limiting."""
        now = time.time()
        window = 1.0 / self.config.requests_per_second

        if key not in self._window_counts:
            self._window_counts[key] = []

        self._window_counts[key] = [
            t for t in self._window_counts[key]
            if now - t < window
        ]

        if len(self._window_counts[key]) + cost <= self.config.burst_size:
            for _ in range(cost):
                self._window_counts[key].append(now)

            return RateLimitResult(
                allowed=True,
                remaining=self.config.burst_size - len(self._window_counts[key]),
                reset_at=now + window,
            )

        oldest = self._window_counts[key][0] if self._window_counts[key] else now
        retry_after = (oldest + window) - now

        return RateLimitResult(
            allowed=False,
            remaining=0,
            retry_after=max(0, retry_after),
            reset_at=now + retry_after if retry_after > 0 else now + window,
        )

    async def _acquire_leaky_bucket(
        self,
        key: str,
        cost: int,
    ) -> RateLimitResult:
        """Leaky bucket rate limiting."""
        bucket = self._buckets[key]
        now = time.time()
        elapsed = now - bucket["last_update"]

        leak_rate = self.config.requests_per_second
        leaked = elapsed * leak_rate
        bucket["tokens"] = max(0, bucket["tokens"] - leaked)
        bucket["last_update"] = now

        if bucket["tokens"] + cost <= self.config.burst_size:
            bucket["tokens"] += cost
            return RateLimitResult(
                allowed=True,
                remaining=self.config.burst_size - int(bucket["tokens"]),
            )

        retry_after = cost / leak_rate
        return RateLimitResult(
            allowed=False,
            remaining=0,
            retry_after=retry_after,
            reset_at=now + retry_after,
        )

    async def _acquire_fixed_window(
        self,
        key: str,
        cost: int,
    ) -> RateLimitResult:
        """Fixed window rate limiting."""
        now = time.time()
        window_size = 1.0 / self.config.requests_per_second
        window_start = int(now / window_size) * window_size

        if key not in self._window_counts:
            self._window_counts[key] = {}

        self._window_counts[key] = {
            k: v for k, v in self._window_counts[key].items()
            if float(k) >= window_start
        }

        current_count = sum(self._window_counts[key].values())

        if current_count + cost <= self.config.burst_size:
            self._window_counts[key][str(now)] = cost

            return RateLimitResult(
                allowed=True,
                remaining=self.config.burst_size - (current_count + cost),
                reset_at=window_start + window_size,
            )

        retry_after = (window_start + window_size) - now
        return RateLimitResult(
            allowed=False,
            remaining=0,
            retry_after=max(0, retry_after),
            reset_at=window_start + window_size,
        )

    async def reset(self, key: str) -> None:
        """Reset rate limit for key."""
        async with self._global_lock:
            if key in self._buckets:
                self._init_bucket(key)
            if key in self._window_counts:
                self._window_counts[key] = []

    async def reset_all(self) -> None:
        """Reset all rate limits."""
        async with self._global_lock:
            keys = list(self._buckets.keys())
            for key in keys:
                self._init_bucket(key)
            self._window_counts.clear()

    def get_remaining(self, key: str) -> int:
        """Get remaining requests for key."""
        if key not in self._buckets:
            return self.config.burst_size

        if self.config.algorithm == RateLimitAlgorithm.TOKEN_BUCKET:
            return int(self._buckets[key]["tokens"])
        elif key in self._window_counts:
            return max(0, self.config.burst_size - len(self._window_counts[key]))

        return self.config.burst_size
