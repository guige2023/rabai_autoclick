"""限流 action module for RabAI AutoClick.

Provides rate limiting utilities:
- TokenBucket: Token bucket limiter
- SlidingWindow: Sliding window limiter
- RateLimiter: Combined rate limiter
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


class TokenBucketLimiter:
    """Token bucket rate limiter."""

    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._tokens = float(capacity)
        self._last_refill = time.time()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        """Refill tokens based on time."""
        now = time.time()
        elapsed = now - self._last_refill
        tokens_to_add = elapsed * self.refill_rate
        self._tokens = min(self.capacity, self._tokens + tokens_to_add)
        self._last_refill = now

    def allow(self, tokens: int = 1) -> bool:
        """Check if request is allowed."""
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def get_wait_time(self, tokens: int = 1) -> float:
        """Get wait time until tokens available."""
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                return 0.0
            return (tokens - self._tokens) / self.refill_rate


class SlidingWindowLimiter:
    """Sliding window rate limiter."""

    def __init__(self, max_requests: int, window_size: float):
        self.max_requests = max_requests
        self.window_size = window_size
        self._requests: List[float] = []
        self._lock = threading.Lock()

    def _cleanup(self) -> None:
        """Remove old requests."""
        now = time.time()
        cutoff = now - self.window_size
        self._requests = [r for r in self._requests if r > cutoff]

    def allow(self) -> bool:
        """Check if request is allowed."""
        with self._lock:
            self._cleanup()
            if len(self._requests) < self.max_requests:
                self._requests.append(time.time())
                return True
            return False

    def get_remaining(self) -> int:
        """Get remaining requests in window."""
        with self._lock:
            self._cleanup()
            return max(0, self.max_requests - len(self._requests))


class RateLimiter:
    """Combined rate limiter."""

    def __init__(self, requests_per_second: float, burst_size: Optional[int] = None):
        self.requests_per_second = requests_per_second
        self.burst_size = burst_size or int(requests_per_second)
        self._limiter = TokenBucketLimiter(self.burst_size, requests_per_second)

    def allow(self) -> bool:
        """Check if request is allowed."""
        return self._limiter.allow()

    def wait_and_allow(self, timeout: Optional[float] = None) -> bool:
        """Wait for rate limit and allow."""
        wait_time = self._limiter.get_wait_time()
        if timeout and wait_time > timeout:
            return False
        if wait_time > 0:
            time.sleep(wait_time)
        return self._limiter.allow()


class RateLimitAction(BaseAction):
    """Rate limit management action."""
    action_type = "rate_limit"
    display_name = "限流器"
    description = "流量限制"

    def __init__(self):
        super().__init__()
        self._limiters: Dict[str, RateLimiter] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "allow":
                return self._allow(params)
            elif operation == "wait":
                return self._wait(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"RateLimit error: {str(e)}")

    def _create(self, params: Dict[str, Any]) -> ActionResult:
        """Create a rate limiter."""
        name = params.get("name", str(uuid.uuid4()))
        requests_per_second = params.get("requests_per_second", 10.0)
        burst_size = params.get("burst_size")

        limiter = RateLimiter(requests_per_second, burst_size)
        self._limiters[name] = limiter

        return ActionResult(success=True, message=f"Rate limiter created: {name}", data={"name": name})

    def _allow(self, params: Dict[str, Any]) -> ActionResult:
        """Check if request is allowed."""
        name = params.get("name")

        if not name:
            return ActionResult(success=False, message="name is required")

        limiter = self._limiters.get(name)
        if not limiter:
            return ActionResult(success=False, message=f"Limiter not found: {name}")

        allowed = limiter.allow()

        return ActionResult(success=allowed, message="Allowed" if allowed else "Rate limited")

    def _wait(self, params: Dict[str, Any]) -> ActionResult:
        """Wait for rate limit and allow."""
        name = params.get("name")
        timeout = params.get("timeout")

        if not name:
            return ActionResult(success=False, message="name is required")

        limiter = self._limiters.get(name)
        if not limiter:
            return ActionResult(success=False, message=f"Limiter not found: {name}")

        allowed = limiter.wait_and_allow(timeout)

        return ActionResult(success=allowed, message="Allowed" if allowed else "Timeout")
