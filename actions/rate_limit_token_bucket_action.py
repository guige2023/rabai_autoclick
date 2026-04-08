"""Rate Limit Token Bucket Action Module.

Provides token bucket rate limiting implementation
with configurable bucket parameters.
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
    capacity: float
    tokens: float
    refill_rate: float
    last_refill: float = field(default_factory=time.time)
    lock: threading.Lock = field(default_factory=threading.Lock)


class TokenBucketManager:
    """Manages token bucket rate limiters."""

    def __init__(self):
        self._buckets: Dict[str, TokenBucket] = {}

    def create_bucket(
        self,
        name: str,
        capacity: float,
        refill_rate: float
    ) -> None:
        """Create a token bucket."""
        self._buckets[name] = TokenBucket(
            capacity=capacity,
            tokens=capacity,
            refill_rate=refill_rate
        )

    def consume(self, name: str, tokens: float = 1.0) -> tuple[bool, float]:
        """Consume tokens from bucket."""
        bucket = self._buckets.get(name)
        if not bucket:
            return True, float('inf')

        with bucket.lock:
            now = time.time()
            elapsed = now - bucket.last_refill
            bucket.tokens = min(
                bucket.capacity,
                bucket.tokens + elapsed * bucket.refill_rate
            )
            bucket.last_refill = now

            if bucket.tokens >= tokens:
                bucket.tokens -= tokens
                return True, bucket.tokens

            return False, bucket.tokens

    def get_available(self, name: str) -> Optional[float]:
        """Get available tokens."""
        bucket = self._buckets.get(name)
        if not bucket:
            return None

        with bucket.lock:
            now = time.time()
            elapsed = now - bucket.last_refill
            tokens = min(
                bucket.capacity,
                bucket.tokens + elapsed * bucket.refill_rate
            )
            return tokens


class RateLimitTokenBucketAction(BaseAction):
    """Action for token bucket rate limiting."""

    def __init__(self):
        super().__init__("rate_limit_token_bucket")
        self._manager = TokenBucketManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute token bucket action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "consume":
                return self._consume(params)
            elif operation == "available":
                return self._available(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create bucket."""
        self._manager.create_bucket(
            name=params.get("name", ""),
            capacity=params.get("capacity", 100),
            refill_rate=params.get("refill_rate", 10)
        )
        return ActionResult(success=True)

    def _consume(self, params: Dict) -> ActionResult:
        """Consume tokens."""
        allowed, remaining = self._manager.consume(
            params.get("name", ""),
            params.get("tokens", 1)
        )
        return ActionResult(success=True, data={
            "allowed": allowed,
            "remaining": remaining
        })

    def _available(self, params: Dict) -> ActionResult:
        """Get available tokens."""
        available = self._manager.get_available(params.get("name", ""))
        return ActionResult(success=True, data={"available": available})
