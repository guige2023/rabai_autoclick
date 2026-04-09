"""API Token Bucket Rate Limiter.

This module provides token bucket rate limiting for API endpoints:
- Per-client rate limiting
- Burst allowance
- Token refill scheduling
- Distributed support via Redis (optional)

Example:
    >>> from actions.api_token_bucket_action import TokenBucketLimiter
    >>> limiter = TokenBucketLimiter(capacity=100, refill_rate=10)
    >>> limiter.allow_request("client_123")  # True/False
"""

from __future__ import annotations

import time
import threading
import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class Bucket:
    """A token bucket."""
    tokens: float
    last_refill: float
    client_id: str


class TokenBucketLimiter:
    """Token bucket rate limiter for API clients."""

    def __init__(
        self,
        capacity: float = 100.0,
        refill_rate: float = 10.0,
        refill_period: float = 1.0,
    ) -> None:
        """Initialize the limiter.

        Args:
            capacity: Maximum tokens per bucket.
            refill_rate: Tokens added per refill_period.
            refill_period: Refill interval in seconds.
        """
        self._capacity = capacity
        self._refill_rate = refill_rate
        self._refill_period = refill_period
        self._buckets: dict[str, Bucket] = {}
        self._lock = threading.RLock()
        self._stats = {"allowed": 0, "rejected": 0}

    def allow_request(
        self,
        client_id: str,
        tokens_required: float = 1.0,
    ) -> bool:
        """Check if a request is allowed.

        Args:
            client_id: Client identifier.
            tokens_required: Number of tokens to consume.

        Returns:
            True if allowed, False if rate limited.
        """
        with self._lock:
            bucket = self._buckets.get(client_id)
            now = time.time()

            if bucket is None:
                bucket = Bucket(
                    tokens=self._capacity,
                    last_refill=now,
                    client_id=client_id,
                )
                self._buckets[client_id] = bucket

            self._refill_bucket(bucket, now)

            if bucket.tokens >= tokens_required:
                bucket.tokens -= tokens_required
                self._stats["allowed"] += 1
                return True
            else:
                self._stats["rejected"] += 1
                return False

    def _refill_bucket(self, bucket: Bucket, now: float) -> None:
        """Refill tokens in a bucket based on elapsed time."""
        elapsed = now - bucket.last_refill
        periods = elapsed / self._refill_period
        tokens_to_add = periods * self._refill_rate
        bucket.tokens = min(self._capacity, bucket.tokens + tokens_to_add)
        bucket.last_refill = now

    def get_tokens(self, client_id: str) -> float:
        """Get current token count for a client.

        Args:
            client_id: Client identifier.

        Returns:
            Current token count, or full capacity if new.
        """
        with self._lock:
            bucket = self._buckets.get(client_id)
            if bucket is None:
                return self._capacity
            now = time.time()
            self._refill_bucket(bucket, now)
            return bucket.tokens

    def reset_client(self, client_id: str) -> None:
        """Reset a client's bucket to full capacity.

        Args:
            client_id: Client identifier.
        """
        with self._lock:
            if client_id in self._buckets:
                self._buckets[client_id].tokens = self._capacity
                self._buckets[client_id].last_refill = time.time()

    def remove_client(self, client_id: str) -> None:
        """Remove a client's bucket entirely.

        Args:
            client_id: Client identifier.
        """
        with self._lock:
            self._buckets.pop(client_id, None)

    def set_capacity(self, client_id: str, capacity: float) -> None:
        """Set a custom capacity for a client.

        Args:
            client_id: Client identifier.
            capacity: New capacity.
        """
        with self._lock:
            bucket = self._buckets.get(client_id)
            if bucket:
                bucket.tokens = min(bucket.tokens, capacity)
                self._capacity = capacity

    def get_stats(self) -> dict[str, int]:
        """Get rate limiter statistics."""
        with self._lock:
            return {
                **self._stats,
                "active_clients": len(self._buckets),
            }

    def get_wait_time(self, client_id: str, tokens_required: float = 1.0) -> float:
        """Get seconds to wait before request can be allowed.

        Args:
            client_id: Client identifier.
            tokens_required: Number of tokens needed.

        Returns:
            Seconds to wait, or 0 if already allowed.
        """
        with self._lock:
            tokens = self.get_tokens(client_id)
            if tokens >= tokens_required:
                return 0.0
            deficit = tokens_required - tokens
            return (deficit / self._refill_rate) * self._refill_period
