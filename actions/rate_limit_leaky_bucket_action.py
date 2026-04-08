"""Rate Limit Leaky Bucket Action Module.

Provides leaky bucket algorithm for
rate limiting.
"""

import time
import threading
from typing import Any, Dict
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class LeakyBucket:
    """Leaky bucket state."""
    bucket_id: str
    capacity: float
    leak_rate: float
    level: float = 0.0
    last_update: float = field(default_factory=time.time)


class LeakyBucketManager:
    """Manages leaky bucket rate limiting."""

    def __init__(self):
        self._buckets: Dict[str, LeakyBucket] = {}
        self._lock = threading.RLock()

    def create_bucket(
        self,
        bucket_id: str,
        capacity: float,
        leak_rate: float
    ) -> bool:
        """Create leaky bucket."""
        with self._lock:
            if bucket_id in self._buckets:
                return False

            self._buckets[bucket_id] = LeakyBucket(
                bucket_id=bucket_id,
                capacity=capacity,
                leak_rate=leak_rate
            )
            return True

    def add_request(self, bucket_id: str, amount: float = 1.0) -> bool:
        """Try to add request to bucket."""
        with self._lock:
            bucket = self._buckets.get(bucket_id)
            if not bucket:
                return False

            now = time.time()
            elapsed = now - bucket.last_update
            bucket.level = max(0, bucket.level - elapsed * bucket.leak_rate)
            bucket.last_update = now

            if bucket.level + amount <= bucket.capacity:
                bucket.level += amount
                return True

            return False

    def get_bucket_info(self, bucket_id: str) -> Dict:
        """Get bucket info."""
        with self._lock:
            bucket = self._buckets.get(bucket_id)
            if not bucket:
                return {}

            now = time.time()
            current_level = max(0, bucket.level - (now - bucket.last_update) * bucket.leak_rate)

            return {
                "bucket_id": bucket.bucket_id,
                "capacity": bucket.capacity,
                "leak_rate": bucket.leak_rate,
                "current_level": current_level,
                "available": bucket.capacity - current_level
            }


class LeakyBucketRateLimitAction(BaseAction):
    """Action for leaky bucket rate limiting."""

    def __init__(self):
        super().__init__("rate_limit_leaky_bucket")
        self._manager = LeakyBucketManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute leaky bucket action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "add":
                return self._add(params)
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
            leak_rate=params.get("leak_rate", 10)
        )
        return ActionResult(success=success)

    def _add(self, params: Dict) -> ActionResult:
        """Add request."""
        allowed = self._manager.add_request(
            params.get("bucket_id", ""),
            params.get("amount", 1)
        )
        return ActionResult(success=True, data={"allowed": allowed})

    def _info(self, params: Dict) -> ActionResult:
        """Get bucket info."""
        info = self._manager.get_bucket_info(params.get("bucket_id", ""))
        if not info:
            return ActionResult(success=False, message="Bucket not found")
        return ActionResult(success=True, data=info)
