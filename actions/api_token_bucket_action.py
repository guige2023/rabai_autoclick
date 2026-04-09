"""API Token Bucket Action module.

Implements token bucket rate limiting algorithm for API request
throttling. Supports burst handling, configurable refill rates,
and per-client bucket isolation.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class TokenBucket:
    """Token bucket for rate limiting.

    Tokens are added at a constant rate up to max_capacity.
    Each operation consumes tokens. If no tokens are available,
    the operation must wait for refill.
    """

    capacity: float
    tokens: float
    refill_rate: float
    last_refill: float = field(default_factory=time.monotonic)

    def __post_init__(self):
        if self.capacity <= 0:
            raise ValueError("capacity must be positive")
        if self.refill_rate <= 0:
            raise ValueError("refill_rate must be positive")

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.last_refill = now

        tokens_to_add = elapsed * self.refill_rate
        self.tokens = min(self.capacity, self.tokens + tokens_to_add)

    def consume(self, tokens: float = 1.0) -> bool:
        """Try to consume tokens without blocking.

        Args:
            tokens: Number of tokens to consume

        Returns:
            True if tokens were consumed, False if insufficient
        """
        self._refill()

        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False

    async def acquire(self, tokens: float = 1.0) -> None:
        """Acquire tokens, waiting if necessary.

        Args:
            tokens: Number of tokens to acquire

        Raises:
            ValueError: If tokens exceeds bucket capacity
        """
        if tokens > self.capacity:
            raise ValueError(
                f"Cannot acquire {tokens} tokens, "
                f"capacity is {self.capacity}"
            )

        while not self.consume(tokens):
            wait_time = (tokens - self.tokens) / self.refill_rate
            await asyncio.sleep(wait_time)

    @property
    def available_tokens(self) -> float:
        """Get current available tokens."""
        self._refill()
        return self.tokens

    @property
    def wait_time_for(self, tokens: float) -> float:
        """Get estimated wait time for tokens."""
        self._refill()
        if self.tokens >= tokens:
            return 0.0
        return (tokens - self.tokens) / self.refill_rate


@dataclass
class BucketConfig:
    """Configuration for a token bucket."""

    capacity: float = 100.0
    refill_rate: float = 10.0
    initial_tokens: Optional[float] = None


class TokenBucketRegistry:
    """Registry for managing multiple token buckets.

    Supports per-client bucket isolation and shared buckets
    for global rate limiting.
    """

    def __init__(self, default_config: Optional[BucketConfig] = None):
        self.default_config = default_config or BucketConfig()
        self._buckets: dict[str, TokenBucket] = {}
        self._lock = asyncio.Lock()

    async def get_bucket(
        self,
        client_id: str,
        config: Optional[BucketConfig] = None,
    ) -> TokenBucket:
        """Get or create a bucket for a client.

        Args:
            client_id: Unique client identifier
            config: Optional custom config for this bucket

        Returns:
            TokenBucket for the client
        """
        async with self._lock:
            if client_id not in self._buckets:
                cfg = config or self.default_config
                initial = (
                    cfg.initial_tokens
                    if cfg.initial_tokens is not None
                    else cfg.capacity
                )
                self._buckets[client_id] = TokenBucket(
                    capacity=cfg.capacity,
                    tokens=initial,
                    refill_rate=cfg.refill_rate,
                )
            return self._buckets[client_id]

    async def remove_bucket(self, client_id: str) -> bool:
        """Remove a client's bucket."""
        async with self._lock:
            if client_id in self._buckets:
                del self._buckets[client_id]
                return True
            return False

    def get_stats(self) -> dict[str, dict[str, Any]]:
        """Get statistics for all buckets."""
        return {
            client_id: {
                "available_tokens": bucket.available_tokens,
                "capacity": bucket.capacity,
                "refill_rate": bucket.refill_rate,
            }
            for client_id, bucket in self._buckets.items()
        }


class SlidingWindowRateLimiter:
    """Sliding window rate limiter.

    Tracks requests in a sliding time window for smoother
    rate limiting compared to fixed windows.
    """

    def __init__(self, max_requests: int, window_seconds: float):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        """Try to acquire a request slot.

        Returns:
            True if allowed, False if rate limited
        """
        async with self._lock:
            now = time.monotonic()
            cutoff = now - self.window_seconds

            self._requests = [t for t in self._requests if t > cutoff]

            if len(self._requests) < self.max_requests:
                self._requests.append(now)
                return True
            return False

    async def wait_for_slot(self) -> None:
        """Wait until a slot is available."""
        while True:
            if await self.acquire():
                return

            async with self._lock:
                if self._requests:
                    oldest = min(self._requests)
                    wait_time = oldest + self.window_seconds - time.monotonic()
                    if wait_time > 0:
                        await asyncio.sleep(wait_time)

    @property
    def current_count(self) -> int:
        """Get current request count in window."""
        now = time.monotonic()
        cutoff = now - self.window_seconds
        return sum(1 for t in self._requests if t > cutoff)


class TokenBucketWithPriority(TokenBucket):
    """Extended token bucket with priority support.

    Higher priority requests can "borrow" tokens from
    future refill, up to a burst limit.
    """

    def __init__(
        self,
        capacity: float,
        tokens: float,
        refill_rate: float,
        last_refill: float = 0.0,
        burst_multiplier: float = 2.0,
    ):
        super().__init__(capacity, tokens, refill_rate, last_refill)
        self.burst_multiplier = burst_multiplier

    def consume_with_priority(self, tokens: float, priority: int = 5) -> bool:
        """Consume tokens with priority-based bursting.

        Args:
            tokens: Tokens to consume
            priority: Priority level (1-10, higher = more burst allowed)

        Returns:
            True if consumed, False otherwise
        """
        self._refill()

        burst_capacity = self.capacity * (
            1 + (priority - 5) / 10 * (self.burst_multiplier - 1)
        )
        effective_tokens = min(self.tokens, burst_capacity)

        if effective_tokens >= tokens:
            self.tokens -= tokens
            return True
        return False
