"""Throttling Action Module.

Provides throttling control for operations
with burst and sustain rates.
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
class ThrottleBucket:
    """Token bucket for throttling."""
    capacity: float
    refill_rate: float
    tokens: float
    last_refill: float = field(default_factory=time.time)
    lock: threading.Lock = field(default_factory=threading.Lock)


class ThrottleManager:
    """Manages throttling for operations."""

    def __init__(self):
        self._buckets: Dict[str, ThrottleBucket] = {}

    def create_throttle(
        self,
        name: str,
        capacity: float,
        refill_rate: float
    ) -> None:
        """Create a throttle."""
        self._buckets[name] = ThrottleBucket(
            capacity=capacity,
            refill_rate=refill_rate,
            tokens=capacity
        )

    def check(self, name: str, tokens: float = 1.0) -> tuple[bool, float]:
        """Check if operation is allowed."""
        bucket = self._buckets.get(name)
        if not bucket:
            return True, float('inf')

        with bucket.lock:
            self._refill(bucket)
            if bucket.tokens >= tokens:
                bucket.tokens -= tokens
                return True, bucket.tokens
            return False, bucket.tokens

    def _refill(self, bucket: ThrottleBucket) -> None:
        """Refill bucket tokens."""
        now = time.time()
        elapsed = now - bucket.last_refill
        tokens_to_add = elapsed * bucket.refill_rate
        bucket.tokens = min(bucket.capacity, bucket.tokens + tokens_to_add)
        bucket.last_refill = now


class ThrottlingAction(BaseAction):
    """Action for throttling operations."""

    def __init__(self):
        super().__init__("throttling")
        self._manager = ThrottleManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute throttling action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "check":
                return self._check(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create a throttle."""
        self._manager.create_throttle(
            name=params.get("name", ""),
            capacity=params.get("capacity", 10),
            refill_rate=params.get("refill_rate", 1.0)
        )
        return ActionResult(success=True, message="Throttle created")

    def _check(self, params: Dict) -> ActionResult:
        """Check throttling."""
        allowed, remaining = self._manager.check(
            params.get("name", ""),
            params.get("tokens", 1)
        )
        return ActionResult(success=allowed, data={"allowed": allowed, "remaining": remaining})
