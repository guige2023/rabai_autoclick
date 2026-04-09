"""Rate Limiter Token Bucket Action Module.

Token bucket rate limiter implementation with async support.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from enum import Enum
from typing import Generic, TypeVar

T = TypeVar("T")


class TokenBucketState(Enum):
    """Token bucket states."""
    ALLOWED = "allowed"
    DENIED = "denied"
    THROTTLED = "throttled"


@dataclass
class TokenBucketConfig:
    """Token bucket configuration."""
    capacity: int = 100
    refill_rate: float = 10.0
    initial_tokens: float | None = None
    tokens_per_request: float = 1.0


@dataclass
class TokenBucketResult:
    """Result of token bucket operation."""
    allowed: bool
    state: TokenBucketState
    tokens_remaining: float
    wait_time: float = 0.0
    timestamp: float = 0.0


class TokenBucket(Generic[T]):
    """Token bucket rate limiter."""

    def __init__(self, config: TokenBucketConfig | None = None) -> None:
        self.config = config or TokenBucketConfig()
        self.capacity = self.config.capacity
        self.refill_rate = self.config.refill_rate
        self.tokens_per_request = self.config.tokens_per_request
        self._tokens = (
            self.config.initial_tokens
            if self.config.initial_tokens is not None
            else self.capacity
        )
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float | None = None) -> TokenBucketResult:
        """Acquire tokens from bucket."""
        tokens_needed = tokens or self.tokens_per_request
        async with self._lock:
            self._refill()
            if self._tokens >= tokens_needed:
                self._tokens -= tokens_needed
                return TokenBucketResult(
                    allowed=True,
                    state=TokenBucketState.ALLOWED,
                    tokens_remaining=self._tokens,
                    timestamp=time.time()
                )
            wait_time = (tokens_needed - self._tokens) / self.refill_rate
            return TokenBucketResult(
                allowed=False,
                state=TokenBucketState.DENIED,
                tokens_remaining=self._tokens,
                wait_time=wait_time,
                timestamp=time.time()
            )

    async def wait_for_tokens(
        self,
        tokens: float | None = None,
        timeout: float | None = None
    ) -> TokenBucketResult:
        """Wait until tokens are available."""
        tokens_needed = tokens or self.tokens_per_request
        start = time.monotonic()
        while True:
            result = await self.acquire(tokens_needed)
            if result.allowed:
                return result
            if timeout and (time.monotonic() - start) >= timeout:
                return TokenBucketResult(
                    allowed=False,
                    state=TokenBucketState.DENIED,
                    tokens_remaining=self._tokens,
                    wait_time=timeout,
                    timestamp=time.time()
                )
            await asyncio.sleep(min(result.wait_time, 0.1))

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(
            self.capacity,
            self._tokens + elapsed * self.refill_rate
        )
        self._last_refill = now

    def get_tokens(self) -> float:
        """Get current token count."""
        return self._tokens

    def reset(self) -> None:
        """Reset bucket to full capacity."""
        self._tokens = self.capacity
        self._last_refill = time.monotonic()


class MultiTokenBucket:
    """Multiple token buckets for different resources."""

    def __init__(self) -> None:
        self._buckets: dict[str, TokenBucket] = {}
        self._lock = asyncio.Lock()

    def create_bucket(
        self,
        name: str,
        config: TokenBucketConfig | None = None
    ) -> TokenBucket:
        """Create a named bucket."""
        bucket = TokenBucket(config)
        self._buckets[name] = bucket
        return bucket

    async def acquire(
        self,
        bucket_name: str,
        tokens: float | None = None
    ) -> TokenBucketResult | None:
        """Acquire from named bucket."""
        async with self._lock:
            bucket = self._buckets.get(bucket_name)
            if not bucket:
                return None
            return await bucket.acquire(tokens)

    async def acquire_all(
        self,
        requests: dict[str, float | None]
    ) -> dict[str, TokenBucketResult]:
        """Acquire from multiple buckets atomically."""
        results = {}
        async with self._lock:
            for name, tokens in requests.items():
                bucket = self._buckets.get(name)
                if bucket:
                    results[name] = await bucket.acquire(tokens)
                else:
                    results[name] = TokenBucketResult(
                        allowed=False,
                        state=TokenBucketState.DENIED,
                        tokens_remaining=0,
                        timestamp=time.time()
                    )
            all_allowed = all(r.allowed for r in results.values())
            if not all_allowed:
                for name, result in results.items():
                    if not result.allowed:
                        self._buckets[name]._tokens += requests.get(name, 1)
            return results
