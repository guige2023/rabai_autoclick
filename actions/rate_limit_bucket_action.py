"""Rate limit bucket action for token bucket rate limiting.

Provides distributed rate limiting with token bucket algorithm,
supporting multiple buckets and configurable limits.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class BucketType(Enum):
    TOKEN_BUCKET = "token_bucket"
    LEAKY_BUCKET = "leaky_bucket"
    SLIDING_WINDOW = "sliding_window"


@dataclass
class RateLimitBucket:
    name: str
    bucket_type: BucketType
    capacity: float
    refill_rate: float
    tokens: float
    last_refill: float
    requests: list[float] = field(default_factory=list)


@dataclass
class RateLimitResult:
    allowed: bool
    remaining: float
    reset_at: float
    retry_after: Optional[float] = None


class RateLimitBucketAction:
    """Token bucket rate limiter with multiple bucket support.

    Args:
        default_capacity: Default bucket capacity.
        default_refill_rate: Default refill rate (tokens/second).
    """

    def __init__(
        self,
        default_capacity: float = 100.0,
        default_refill_rate: float = 10.0,
    ) -> None:
        self._buckets: dict[str, RateLimitBucket] = {}
        self._default_capacity = default_capacity
        self._default_refill_rate = default_refill_rate
        self._violation_handlers: list[Callable[[str, str], None]] = []

    def create_bucket(
        self,
        name: str,
        bucket_type: BucketType = BucketType.TOKEN_BUCKET,
        capacity: Optional[float] = None,
        refill_rate: Optional[float] = None,
    ) -> bool:
        """Create a new rate limit bucket.

        Args:
            name: Bucket name.
            bucket_type: Type of bucket algorithm.
            capacity: Maximum tokens.
            refill_rate: Refill rate per second.

        Returns:
            True if created successfully.
        """
        if name in self._buckets:
            logger.warning(f"Bucket already exists: {name}")
            return False

        bucket = RateLimitBucket(
            name=name,
            bucket_type=bucket_type,
            capacity=capacity or self._default_capacity,
            refill_rate=refill_rate or self._default_refill_rate,
            tokens=capacity or self._default_capacity,
            last_refill=time.time(),
        )

        self._buckets[name] = bucket
        logger.debug(f"Created rate limit bucket: {name}")
        return True

    def delete_bucket(self, name: str) -> bool:
        """Delete a rate limit bucket.

        Args:
            name: Bucket name.

        Returns:
            True if deleted.
        """
        if name in self._buckets:
            del self._buckets[name]
            return True
        return False

    def check_rate_limit(
        self,
        bucket_name: str,
        tokens: float = 1.0,
    ) -> RateLimitResult:
        """Check if request is allowed under rate limit.

        Args:
            bucket_name: Bucket name.
            tokens: Number of tokens to consume.

        Returns:
            Rate limit result.
        """
        bucket = self._buckets.get(bucket_name)
        if not bucket:
            self.create_bucket(bucket_name)
            bucket = self._buckets[bucket_name]

        now = time.time()

        self._refill_bucket(bucket, now)

        if bucket.tokens >= tokens:
            bucket.tokens -= tokens

            reset_at = now + (bucket.capacity - bucket.tokens) / bucket.refill_rate

            return RateLimitResult(
                allowed=True,
                remaining=bucket.tokens,
                reset_at=reset_at,
            )
        else:
            retry_after = (tokens - bucket.tokens) / bucket.refill_rate

            for handler in self._violation_handlers:
                try:
                    handler(bucket_name, "rate_limit_exceeded")
                except Exception as e:
                    logger.error(f"Violation handler error: {e}")

            return RateLimitResult(
                allowed=False,
                remaining=bucket.tokens,
                reset_at=now + retry_after,
                retry_after=retry_after,
            )

    def _refill_bucket(self, bucket: RateLimitBucket, now: float) -> None:
        """Refill bucket tokens based on elapsed time.

        Args:
            bucket: Bucket to refill.
            now: Current timestamp.
        """
        elapsed = now - bucket.last_refill
        tokens_to_add = elapsed * bucket.refill_rate

        bucket.tokens = min(bucket.capacity, bucket.tokens + tokens_to_add)
        bucket.last_refill = now

    def get_bucket_status(self, bucket_name: str) -> Optional[dict[str, Any]]:
        """Get current bucket status.

        Args:
            bucket_name: Bucket name.

        Returns:
            Bucket status dictionary.
        """
        bucket = self._buckets.get(bucket_name)
        if not bucket:
            return None

        now = time.time()
        self._refill_bucket(bucket, now)

        return {
            "name": bucket.name,
            "type": bucket.bucket_type.value,
            "capacity": bucket.capacity,
            "tokens": bucket.tokens,
            "remaining": bucket.tokens,
            "refill_rate": bucket.refill_rate,
            "last_refill": bucket.last_refill,
            "utilization": (bucket.capacity - bucket.tokens) / bucket.capacity,
        }

    def reset_bucket(self, bucket_name: str) -> bool:
        """Reset a bucket to full capacity.

        Args:
            bucket_name: Bucket name.

        Returns:
            True if reset.
        """
        bucket = self._buckets.get(bucket_name)
        if not bucket:
            return False

        bucket.tokens = bucket.capacity
        bucket.last_refill = time.time()
        return True

    def set_rate_limit(
        self,
        bucket_name: str,
        capacity: float,
        refill_rate: float,
    ) -> bool:
        """Update bucket rate limit parameters.

        Args:
            bucket_name: Bucket name.
            capacity: New capacity.
            refill_rate: New refill rate.

        Returns:
            True if updated.
        """
        bucket = self._buckets.get(bucket_name)
        if not bucket:
            return False

        bucket.capacity = capacity
        bucket.refill_rate = refill_rate
        bucket.tokens = min(bucket.tokens, capacity)
        return True

    def register_violation_handler(
        self,
        handler: Callable[[str, str], None],
    ) -> None:
        """Register a handler for rate limit violations.

        Args:
            handler: Callback function(bucket_name, violation_type).
        """
        self._violation_handlers.append(handler)

    def get_all_buckets(self) -> list[str]:
        """Get list of all bucket names.

        Returns:
            List of bucket names.
        """
        return list(self._buckets.keys())

    def get_stats(self) -> dict[str, Any]:
        """Get rate limit statistics.

        Returns:
            Dictionary with stats.
        """
        return {
            "total_buckets": len(self._buckets),
            "bucket_types": {
                bt.value: sum(1 for b in self._buckets.values() if b.bucket_type == bt)
                for bt in BucketType
            },
            "default_capacity": self._default_capacity,
            "default_refill_rate": self._default_refill_rate,
        }
