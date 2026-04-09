"""
Token Bucket Rate Limiter Utility.

A token bucket algorithm implementation for rate limiting requests
with support for burst handling and configurable refill rates.

Example:
    >>> limiter = TokenBucketRateLimiter(capacity=100, refill_rate=10)
    >>> if limiter.allow_request():
    ...     process_request()
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class BucketConfig:
    """Configuration for a token bucket."""
    capacity: int = 100
    refill_rate: float = 10.0
    initial_tokens: Optional[float] = None

    def __post_init__(self):
        if self.initial_tokens is None:
            self.initial_tokens = self.capacity


@dataclass
class BucketStats:
    """Statistics for a token bucket."""
    total_requests: int = 0
    allowed_requests: int = 0
    denied_requests: int = 0
    tokens_consumed: float = 0.0
    last_refill_time: float = field(default_factory=time.time)
    last_request_time: float = 0.0


class TokenBucketRateLimiter:
    """
    Token bucket rate limiter with thread-safe operations.

    The token bucket algorithm allows burst traffic up to the bucket
    capacity while maintaining an average rate limit over time.
    """

    def __init__(self, capacity: int = 100, refill_rate: float = 10.0, initial_tokens: Optional[float] = None):
        """
        Initialize the token bucket rate limiter.

        Args:
            capacity: Maximum number of tokens in the bucket
            refill_rate: Number of tokens added per second
            initial_tokens: Starting number of tokens (defaults to capacity)
        """
        self._capacity = float(capacity)
        self._refill_rate = float(refill_rate)
        self._tokens = float(initial_tokens if initial_tokens is not None else capacity)
        self._last_update = time.time()
        self._lock = threading.Lock()
        self._stats = BucketStats()

    def allow_request(self, cost: float = 1.0) -> bool:
        """
        Check if a request should be allowed.

        Args:
            cost: Number of tokens required for this request

        Returns:
            True if the request is allowed, False otherwise
        """
        with self._lock:
            self._refill()

            if self._tokens >= cost:
                self._tokens -= cost
                self._stats.allowed_requests += 1
                self._stats.tokens_consumed += cost
                self._stats.last_request_time = time.time()
                return True

            self._stats.denied_requests += 1
            return False

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_update

        if elapsed > 0:
            tokens_to_add = elapsed * self._refill_rate
            self._tokens = min(self._capacity, self._tokens + tokens_to_add)
            self._last_update = now
            self._stats.last_refill_time = now

    def get_wait_time(self, cost: float = 1.0) -> float:
        """
        Get the time to wait before a request can be allowed.

        Args:
            cost: Number of tokens required

        Returns:
            Seconds to wait (0 if request can be made immediately)
        """
        with self._lock:
            self._refill()

            if self._tokens >= cost:
                return 0.0

            tokens_needed = cost - self._tokens
            return tokens_needed / self._refill_rate

    def get_stats(self) -> BucketStats:
        """Get current bucket statistics."""
        with self._lock:
            return BucketStats(
                total_requests=self._stats.total_requests,
                allowed_requests=self._stats.allowed_requests,
                denied_requests=self._stats.denied_requests,
                tokens_consumed=self._stats.tokens_consumed,
                last_refill_time=self._stats.last_refill_time,
                last_request_time=self._stats.last_request_time
            )

    @property
    def current_tokens(self) -> float:
        """Get current number of tokens in the bucket."""
        with self._lock:
            self._refill()
            return self._tokens

    def reset(self) -> None:
        """Reset the bucket to full capacity."""
        with self._lock:
            self._tokens = self._capacity
            self._last_update = time.time()

    def set_rate(self, refill_rate: float) -> None:
        """
        Adjust the refill rate dynamically.

        Args:
            refill_rate: New refill rate in tokens per second
        """
        with self._lock:
            self._refill()
            self._refill_rate = float(refill_rate)


class MultiTenantTokenBucket:
    """
    Multi-tenant token bucket manager.

    Provides isolated rate limiting for multiple tenants using
    separate token buckets for each tenant.
    """

    def __init__(self, default_capacity: int = 100, default_refill_rate: float = 10.0):
        """
        Initialize multi-tenant bucket manager.

        Args:
            default_capacity: Default bucket capacity for new tenants
            default_refill_rate: Default refill rate for new tenants
        """
        self._default_capacity = default_capacity
        self._default_refill_rate = default_refill_rate
        self._buckets: dict[str, TokenBucketRateLimiter] = {}
        self._lock = threading.Lock()

    def get_bucket(self, tenant_id: str) -> TokenBucketRateLimiter:
        """Get or create a bucket for a tenant."""
        with self._lock:
            if tenant_id not in self._buckets:
                self._buckets[tenant_id] = TokenBucketRateLimiter(
                    capacity=self._default_capacity,
                    refill_rate=self._default_refill_rate
                )
            return self._buckets[tenant_id]

    def allow_request(self, tenant_id: str, cost: float = 1.0) -> bool:
        """Allow a request for a specific tenant."""
        bucket = self.get_bucket(tenant_id)
        return bucket.allow_request(cost)

    def remove_tenant(self, tenant_id: str) -> bool:
        """Remove a tenant's bucket."""
        with self._lock:
            if tenant_id in self._buckets:
                del self._buckets[tenant_id]
                return True
            return False

    def get_all_tenant_stats(self) -> dict[str, BucketStats]:
        """Get statistics for all tenants."""
        with self._lock:
            return {tid: bucket.get_stats() for tid, bucket in self._buckets.items()}
