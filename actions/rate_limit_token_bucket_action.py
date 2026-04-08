"""Rate Limit Token Bucket Action Module.

Provides token bucket algorithm for
rate limiting.
"""

import time
import threading
from typing import Any, Dict, Optional
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class TokenBucket:
    """Token bucket implementation."""
    bucket_id: str
    capacity: float
    refill_rate: float
    tokens: float
    last_refill: float = field(default_factory=time.time)
    locked: bool = False


class RateLimitManager:
    """Manages rate limiting."""

    def __init__(self):
        self._buckets: Dict[str, TokenBucket] = {}
        self._lock = threading.RLock()

    def create_bucket(
        self,
        bucket_id: str,
        capacity: float,
        refill_rate: float
    ) -> bool:
        """Create a token bucket."""
        with self._lock:
            if bucket_id in self._buckets:
                return False

            self._buckets[bucket_id] = TokenBucket(
                bucket_id=bucket_id,
                capacity=capacity,
                refill_rate=refill_rate,
                tokens=capacity
            )
            return True

    def consume(self, bucket_id: str, tokens: float = 1.0) -> bool:
        """Try to consume tokens."""
        with self._lock:
            bucket = self._buckets.get(bucket_id)
            if not bucket:
                return False

            self._refill(bucket)

            if bucket.tokens >= tokens:
                bucket.tokens -= tokens
                return True

            return False

    def _refill(self, bucket: TokenBucket) -> None:
        """Refill bucket based on time elapsed."""
        now = time.time()
        elapsed = now - bucket.last_refill

        tokens_to_add = elapsed * bucket.refill_rate
        bucket.tokens = min(bucket.capacity, bucket.tokens + tokens_to_add)
        bucket.last_refill = now

    def get_bucket_info(self, bucket_id: str) -> Optional[Dict]:
        """Get bucket info."""
        with self._lock:
            bucket = self._buckets.get(bucket_id)
            if not bucket:
                return None

            self._refill(bucket)

            return {
                "bucket_id": bucket.bucket_id,
                "capacity": bucket.capacity,
                "refill_rate": bucket.refill_rate,
                "tokens": bucket.tokens
            }


class RateLimitTokenBucketAction(BaseAction):
    """Action for token bucket rate limiting."""

    def __init__(self):
        super().__init__("rate_limit_token_bucket")
        self._manager = RateLimitManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute rate limit action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "consume":
                return self._consume(params)
            elif operation == "info":
                return self._info(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create bucket."""
        success = self._manager.create_bucket(
            bucket_id=params.get("bucket_id", ""),
            capacity=params.get("capacity", 100),
            refill_rate=params.get("refill_rate", 10)
        )
        return ActionResult(success=success)

    def _consume(self, params: Dict) -> ActionResult:
        """Consume tokens."""
        allowed = self._manager.consume(
            params.get("bucket_id", ""),
            params.get("tokens", 1)
        )
        return ActionResult(success=True, data={"allowed": allowed})

    def _info(self, params: Dict) -> ActionResult:
        """Get bucket info."""
        info = self._manager.get_bucket_info(params.get("bucket_id", ""))
        if info is None:
            return ActionResult(success=False, message="Bucket not found")
        return ActionResult(success=True, data=info)
