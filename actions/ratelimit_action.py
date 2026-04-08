"""Rate limit action module for RabAI AutoClick.

Provides rate limiting utilities:
- TokenBucket: Token bucket rate limiter
- SlidingWindow: Sliding window rate limiter
- FixedWindow: Fixed window rate limiter
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
    """Token bucket rate limiter."""

    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._tokens = float(capacity)
        self._last_refill = time.time()
        self._lock = threading.Lock()
        self._total_requests = 0
        self._allowed_requests = 0

    def _refill(self) -> None:
        """Refill tokens."""
        now = time.time()
        elapsed = now - self._last_refill
        tokens_to_add = elapsed * self.refill_rate
        self._tokens = min(self.capacity, self._tokens + tokens_to_add)
        self._last_refill = now

    def allow(self, tokens: int = 1) -> bool:
        """Check if request is allowed."""
        with self._lock:
            self._refill()
            self._total_requests += 1

            if self._tokens >= tokens:
                self._tokens -= tokens
                self._allowed_requests += 1
                return True

            return False

    def get_tokens(self) -> float:
        """Get current tokens."""
        with self._lock:
            self._refill()
            return self._tokens

    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter stats."""
        with self._lock:
            self._refill()
            total = self._total_requests
            allowed = self._allowed_requests
            return {
                "tokens": self._tokens,
                "capacity": self.capacity,
                "refill_rate": self.refill_rate,
                "total_requests": total,
                "allowed_requests": allowed,
                "rejected_requests": total - allowed,
                "allow_rate": allowed / total if total > 0 else 0.0,
            }


class SlidingWindow:
    """Sliding window rate limiter."""

    def __init__(self, max_requests: int, window_size: float):
        self.max_requests = max_requests
        self.window_size = window_size
        self._requests: List[float] = []
        self._lock = threading.Lock()
        self._total_requests = 0
        self._allowed_requests = 0

    def _cleanup(self) -> None:
        """Remove old requests."""
        now = time.time()
        cutoff = now - self.window_size
        self._requests = [r for r in self._requests if r > cutoff]

    def allow(self) -> bool:
        """Check if request is allowed."""
        with self._lock:
            self._cleanup()
            self._total_requests += 1

            if len(self._requests) < self.max_requests:
                self._requests.append(time.time())
                self._allowed_requests += 1
                return True

            return False

    def get_remaining(self) -> int:
        """Get remaining requests."""
        with self._lock:
            self._cleanup()
            return max(0, self.max_requests - len(self._requests))

    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter stats."""
        with self._lock:
            self._cleanup()
            total = self._total_requests
            allowed = self._allowed_requests
            return {
                "remaining": max(0, self.max_requests - len(self._requests)),
                "max_requests": self.max_requests,
                "window_size": self.window_size,
                "total_requests": total,
                "allowed_requests": allowed,
                "rejected_requests": total - allowed,
            }


class FixedWindow:
    """Fixed window rate limiter."""

    def __init__(self, max_requests: int, window_size: float):
        self.max_requests = max_requests
        self.window_size = window_size
        self._count = 0
        self._window_start = time.time()
        self._lock = threading.Lock()
        self._total_requests = 0
        self._allowed_requests = 0

    def _reset_if_needed(self) -> None:
        """Reset window if expired."""
        now = time.time()
        if now - self._window_start >= self.window_size:
            self._count = 0
            self._window_start = now

    def allow(self) -> bool:
        """Check if request is allowed."""
        with self._lock:
            self._reset_if_needed()
            self._total_requests += 1

            if self._count < self.max_requests:
                self._count += 1
                self._allowed_requests += 1
                return True

            return False

    def get_remaining(self) -> int:
        """Get remaining requests."""
        with self._lock:
            self._reset_if_needed()
            return max(0, self.max_requests - self._count)

    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter stats."""
        with self._lock:
            self._reset_if_needed()
            total = self._total_requests
            allowed = self._allowed_requests
            return {
                "remaining": max(0, self.max_requests - self._count),
                "max_requests": self.max_requests,
                "window_size": self.window_size,
                "total_requests": total,
                "allowed_requests": allowed,
                "rejected_requests": total - allowed,
            }


class RateLimitAction(BaseAction):
    """Rate limit action."""
    action_type = "ratelimit"
    display_name = "限流器"
    description = "流量限制"

    def __init__(self):
        super().__init__()
        self._limiters: Dict[str, Any] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")

            if operation == "create_token_bucket":
                return self._create_token_bucket(params)
            elif operation == "create_sliding_window":
                return self._create_sliding_window(params)
            elif operation == "create_fixed_window":
                return self._create_fixed_window(params)
            elif operation == "allow":
                return self._allow(params)
            elif operation == "stats":
                return self._stats(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"RateLimit error: {str(e)}")

    def _create_token_bucket(self, params: Dict[str, Any]) -> ActionResult:
        """Create token bucket limiter."""
        name = params.get("name", str(uuid.uuid4()))
        capacity = params.get("capacity", 100)
        refill_rate = params.get("refill_rate", 10.0)

        limiter = TokenBucket(capacity, refill_rate)
        self._limiters[name] = limiter

        return ActionResult(success=True, message=f"Token bucket created: {name}", data={"name": name})

    def _create_sliding_window(self, params: Dict[str, Any]) -> ActionResult:
        """Create sliding window limiter."""
        name = params.get("name", str(uuid.uuid4()))
        max_requests = params.get("max_requests", 100)
        window_size = params.get("window_size", 60.0)

        limiter = SlidingWindow(max_requests, window_size)
        self._limiters[name] = limiter

        return ActionResult(success=True, message=f"Sliding window created: {name}", data={"name": name})

    def _create_fixed_window(self, params: Dict[str, Any]) -> ActionResult:
        """Create fixed window limiter."""
        name = params.get("name", str(uuid.uuid4()))
        max_requests = params.get("max_requests", 100)
        window_size = params.get("window_size", 60.0)

        limiter = FixedWindow(max_requests, window_size)
        self._limiters[name] = limiter

        return ActionResult(success=True, message=f"Fixed window created: {name}", data={"name": name})

    def _allow(self, params: Dict[str, Any]) -> ActionResult:
        """Check if request is allowed."""
        name = params.get("name")

        if not name:
            return ActionResult(success=False, message="name is required")

        limiter = self._limiters.get(name)
        if not limiter:
            return ActionResult(success=False, message=f"Limiter not found: {name}")

        allowed = limiter.allow()

        return ActionResult(success=True, message="Allowed" if allowed else "Rejected", data={"allowed": allowed})

    def _stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get limiter stats."""
        name = params.get("name")

        if not name:
            return ActionResult(success=False, message="name is required")

        limiter = self._limiters.get(name)
        if not limiter:
            return ActionResult(success=False, message=f"Limiter not found: {name}")

        stats = limiter.get_stats()

        return ActionResult(success=True, message="Stats retrieved", data={"name": name, "stats": stats})
