"""Rate limit bucket action module.

Provides token bucket and leaky bucket rate limiting algorithms
for API throttling and request rate control.
"""

from __future__ import annotations

import time
import threading
from typing import Any, Optional
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class BucketType(Enum):
    """Bucket algorithm types."""
    TOKEN_BUCKET = "token_bucket"
    LEAKY_BUCKET = "leaky_bucket"


@dataclass
class BucketConfig:
    """Rate limit bucket configuration."""
    bucket_type: BucketType = BucketType.TOKEN_BUCKET
    capacity: int = 100
    refill_rate: float = 10.0
    tokens: Optional[float] = None


class TokenBucket:
    """Token bucket rate limiter."""

    def __init__(self, capacity: int, refill_rate: float, tokens: Optional[float] = None):
        """Initialize token bucket.

        Args:
            capacity: Maximum tokens
            refill_rate: Tokens per second
            tokens: Initial tokens (default: capacity)
        """
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = tokens if tokens is not None else float(capacity)
        self.last_refill = time.time()
        self._lock = threading.Lock()

    def consume(self, tokens: int = 1) -> bool:
        """Try to consume tokens.

        Args:
            tokens: Number of tokens to consume

        Returns:
            True if tokens consumed, False if insufficient
        """
        with self._lock:
            self._refill()

            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill
        new_tokens = elapsed * self.refill_rate
        self.tokens = min(self.capacity, self.tokens + new_tokens)
        self.last_refill = now

    def get_available_tokens(self) -> float:
        """Get current available tokens."""
        with self._lock:
            self._refill()
            return self.tokens


class LeakyBucket:
    """Leaky bucket rate limiter."""

    def __init__(self, capacity: int, leak_rate: float):
        """Initialize leaky bucket.

        Args:
            capacity: Maximum bucket size
            leak_rate: Units leaked per second
        """
        self.capacity = capacity
        self.leak_rate = leak_rate
        self.water_level = 0.0
        self.last_leak = time.time()
        self._lock = threading.Lock()

    def add(self, units: int = 1) -> bool:
        """Try to add units to bucket.

        Args:
            units: Number of units to add

        Returns:
            True if added, False if bucket would overflow
        """
        with self._lock:
            self._leak()

            if self.water_level + units <= self.capacity:
                self.water_level += units
                return True
            return False

    def _leak(self) -> None:
        """Leak water based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_leak
        leaked = elapsed * self.leak_rate
        self.water_level = max(0, self.water_level - leaked)
        self.last_leak = now

    def get_water_level(self) -> float:
        """Get current water level."""
        with self._lock:
            self._leak()
            return self.water_level


class RateLimiter:
    """Multi-bucket rate limiter."""

    def __init__(self, config: BucketConfig):
        """Initialize rate limiter.

        Args:
            config: Bucket configuration
        """
        self.config = config
        if config.bucket_type == BucketType.TOKEN_BUCKET:
            self._bucket = TokenBucket(
                capacity=config.capacity,
                refill_rate=config.refill_rate,
                tokens=config.tokens,
            )
        else:
            self._bucket = LeakyBucket(
                capacity=config.capacity,
                leak_rate=config.refill_rate,
            )

    def allow(self, tokens: int = 1) -> bool:
        """Check if request is allowed.

        Args:
            tokens: Number of tokens/units

        Returns:
            True if allowed
        """
        if self.config.bucket_type == BucketType.TOKEN_BUCKET:
            return self._bucket.consume(tokens)
        else:
            return self._bucket.add(tokens)

    def get_limit(self) -> dict[str, Any]:
        """Get rate limit information.

        Returns:
            Dictionary with limit info
        """
        if self.config.bucket_type == BucketType.TOKEN_BUCKET:
            return {
                "type": "token_bucket",
                "capacity": self.config.capacity,
                "refill_rate": self.config.refill_rate,
                "available": self._bucket.get_available_tokens(),
            }
        else:
            return {
                "type": "leaky_bucket",
                "capacity": self.config.capacity,
                "leak_rate": self.config.refill_rate,
                "water_level": self._bucket.get_water_level(),
            }


class SlidingWindowLimiter:
    """Sliding window rate limiter."""

    def __init__(self, max_requests: int, window_seconds: float):
        """Initialize sliding window limiter.

        Args:
            max_requests: Maximum requests in window
            window_seconds: Window size in seconds
        """
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: list[float] = []
        self._lock = threading.Lock()

    def allow(self) -> bool:
        """Check if request is allowed.

        Returns:
            True if allowed
        """
        with self._lock:
            now = time.time()
            cutoff = now - self.window_seconds

            self._requests = [t for t in self._requests if t > cutoff]

            if len(self._requests) < self.max_requests:
                self._requests.append(now)
                return True
            return False

    def get_remaining(self) -> int:
        """Get remaining requests in window."""
        with self._lock:
            now = time.time()
            cutoff = now - self.window_seconds
            self._requests = [t for t in self._requests if t > cutoff]
            return max(0, self.max_requests - len(self._requests))


class MultiLimiter:
    """Rate limiter with multiple buckets per client."""

    def __init__(self, limiters: dict[str, RateLimiter]):
        """Initialize multi-limiter.

        Args:
            limiters: Dict of client_id -> RateLimiter
        """
        self.limiters = limiters
        self._lock = threading.Lock()

    def allow(self, client_id: str, tokens: int = 1) -> bool:
        """Check if client request is allowed.

        Args:
            client_id: Client identifier
            tokens: Number of tokens

        Returns:
            True if allowed
        """
        limiter = self.limiters.get(client_id)
        if not limiter:
            return True
        return limiter.allow(tokens)

    def add_limiter(self, client_id: str, limiter: RateLimiter) -> None:
        """Add limiter for client."""
        with self._lock:
            self.limiters[client_id] = limiter

    def remove_limiter(self, client_id: str) -> None:
        """Remove limiter for client."""
        with self._lock:
            self.limiters.pop(client_id, None)


def create_token_bucket_limiter(
    capacity: int = 100,
    refill_rate: float = 10.0,
) -> RateLimiter:
    """Create token bucket rate limiter.

    Args:
        capacity: Maximum tokens
        refill_rate: Tokens per second

    Returns:
        RateLimiter instance
    """
    config = BucketConfig(
        bucket_type=BucketType.TOKEN_BUCKET,
        capacity=capacity,
        refill_rate=refill_rate,
    )
    return RateLimiter(config)


def create_sliding_window_limiter(
    max_requests: int,
    window_seconds: float,
) -> SlidingWindowLimiter:
    """Create sliding window rate limiter.

    Args:
        max_requests: Maximum requests in window
        window_seconds: Window size in seconds

    Returns:
        SlidingWindowLimiter instance
    """
    return SlidingWindowLimiter(max_requests, window_seconds)
