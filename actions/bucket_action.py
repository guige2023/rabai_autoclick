"""Bucket action module for RabAI AutoClick.

Provides token bucket utilities:
- TokenBucket: Token bucket implementation
- BucketRegistry: Manage buckets
"""

from typing import Any, Callable, Dict, List, Optional
import threading
import time
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TokenBucket:
    """Token bucket with configurable parameters."""

    def __init__(
        self,
        capacity: int,
        refill_rate: float,
        initial_tokens: Optional[float] = None,
    ):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._tokens = initial_tokens if initial_tokens is not None else float(capacity)
        self._last_refill = time.time()
        self._lock = threading.Lock()
        self._total_added = 0
        self._total_consumed = 0

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_refill
        tokens_to_add = elapsed * self.refill_rate
        self._tokens = min(self.capacity, self._tokens + tokens_to_add)
        self._last_refill = now
        self._total_added += tokens_to_add

    def consume(self, tokens: int = 1) -> bool:
        """Try to consume tokens."""
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                self._total_consumed += tokens
                return True
            return False

    def get_tokens(self) -> float:
        """Get current token count."""
        with self._lock:
            self._refill()
            return self._tokens

    def wait_for_tokens(self, tokens: int = 1, timeout: Optional[float] = None) -> bool:
        """Wait until tokens are available."""
        start_time = time.time()
        while True:
            if self.consume(tokens):
                return True
            if timeout and (time.time() - start_time) >= timeout:
                return False
            time.sleep(0.01)

    def get_stats(self) -> Dict[str, Any]:
        """Get bucket statistics."""
        with self._lock:
            self._refill()
            return {
                "capacity": self.capacity,
                "tokens": self._tokens,
                "refill_rate": self.refill_rate,
                "total_added": self._total_added,
                "total_consumed": self._total_consumed,
            }


class BucketRegistry:
    """Registry for token buckets."""

    def __init__(self):
        self._buckets: Dict[str, TokenBucket] = {}
        self._lock = threading.Lock()

    def create(
        self,
        name: str,
        capacity: int,
        refill_rate: float,
        initial_tokens: Optional[float] = None,
    ) -> TokenBucket:
        """Create a new bucket."""
        with self._lock:
            bucket = TokenBucket(capacity, refill_rate, initial_tokens)
            self._buckets[name] = bucket
            return bucket

    def get(self, name: str) -> Optional[TokenBucket]:
        """Get a bucket by name."""
        with self._lock:
            return self._buckets.get(name)

    def delete(self, name: str) -> bool:
        """Delete a bucket."""
        with self._lock:
            if name in self._buckets:
                del self._buckets[name]
                return True
            return False

    def list_buckets(self) -> List[str]:
        """List all bucket names."""
        with self._lock:
            return list(self._buckets.keys())


class BucketAction(BaseAction):
    """Token bucket management action."""
    action_type = "bucket"
    display_name = "令牌桶"
    description = "令牌桶管理"

    def __init__(self):
        super().__init__()
        self._registry = BucketRegistry()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "consume":
                return self._consume(params)
            elif operation == "wait":
                return self._wait(params)
            elif operation == "stats":
                return self._stats(params)
            elif operation == "list":
                return self._list(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Bucket error: {str(e)}")

    def _create(self, params: Dict[str, Any]) -> ActionResult:
        """Create a token bucket."""
        name = params.get("name", str(uuid.uuid4()))
        capacity = params.get("capacity", 100)
        refill_rate = params.get("refill_rate", 10.0)
        initial_tokens = params.get("initial_tokens")

        bucket = self._registry.create(name, capacity, refill_rate, initial_tokens)

        return ActionResult(success=True, message=f"Bucket created: {name}", data={"name": name})

    def _consume(self, params: Dict[str, Any]) -> ActionResult:
        """Consume tokens from bucket."""
        name = params.get("name")
        tokens = params.get("tokens", 1)

        if not name:
            return ActionResult(success=False, message="name is required")

        bucket = self._registry.get(name)
        if not bucket:
            return ActionResult(success=False, message=f"Bucket not found: {name}")

        consumed = bucket.consume(tokens)

        return ActionResult(success=consumed, message="Consumed" if consumed else "Not enough tokens", data={"consumed": consumed})

    def _wait(self, params: Dict[str, Any]) -> ActionResult:
        """Wait for tokens and consume."""
        name = params.get("name")
        tokens = params.get("tokens", 1)
        timeout = params.get("timeout")

        if not name:
            return ActionResult(success=False, message="name is required")

        bucket = self._registry.get(name)
        if not bucket:
            return ActionResult(success=False, message=f"Bucket not found: {name}")

        consumed = bucket.wait_for_tokens(tokens, timeout)

        return ActionResult(success=consumed, message="Consumed" if consumed else "Timeout")

    def _stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get bucket statistics."""
        name = params.get("name")

        if not name:
            return ActionResult(success=False, message="name is required")

        bucket = self._registry.get(name)
        if not bucket:
            return ActionResult(success=False, message=f"Bucket not found: {name}")

        stats = bucket.get_stats()

        return ActionResult(success=True, message="Stats retrieved", data={"name": name, "stats": stats})

    def _list(self, params: Dict[str, Any]) -> ActionResult:
        """List all buckets."""
        buckets = self._registry.list_buckets()

        return ActionResult(success=True, message=f"{len(buckets)} buckets", data={"buckets": buckets})
