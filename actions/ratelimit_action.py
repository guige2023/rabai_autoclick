"""Rate limit action module for RabAI AutoClick.

Provides rate limiting operations:
- RateLimitTokenBucketAction: Token bucket rate limiter
- RateLimitSlidingWindowAction: Sliding window rate limiter
- RateLimitFixedWindowAction: Fixed window rate limiter
- RateLimitAdaptiveAction: Adaptive rate limiter
"""

import time
import threading
from typing import Any, Dict, Optional
from dataclasses import dataclass


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TokenBucket:
    """Token bucket rate limiter."""
    def __init__(self, capacity: float, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._tokens = capacity
        self._last_refill = time.time()
        self._lock = threading.Lock()

    def consume(self, tokens: float = 1.0) -> bool:
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def _refill(self):
        now = time.time()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
        self._last_refill = now

    def get_wait_time(self, tokens: float = 1.0) -> float:
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                return 0.0
            return (tokens - self._tokens) / self.refill_rate


class SlidingWindow:
    """Sliding window rate limiter."""
    def __init__(self, max_requests: int, window_seconds: float):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: list = []
        self._lock = threading.Lock()

    def is_allowed(self) -> bool:
        with self._lock:
            now = time.time()
            cutoff = now - self.window_seconds
            self._requests = [t for t in self._requests if t > cutoff]
            if len(self._requests) < self.max_requests:
                self._requests.append(now)
                return True
            return False

    def get_wait_time(self) -> float:
        with self._lock:
            if len(self._requests) < self.max_requests:
                return 0.0
            oldest = min(self._requests)
            return max(0.0, self.window_seconds - (time.time() - oldest))


class FixedWindow:
    """Fixed window rate limiter."""
    def __init__(self, max_requests: int, window_seconds: float):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests = 0
        self._window_start = time.time()
        self._lock = threading.Lock()

    def is_allowed(self) -> bool:
        with self._lock:
            now = time.time()
            if now - self._window_start >= self.window_seconds:
                self._requests = 0
                self._window_start = now
            if self._requests < self.max_requests:
                self._requests += 1
                return True
            return False

    def get_wait_time(self) -> float:
        with self._lock:
            elapsed = time.time() - self._window_start
            if elapsed >= self.window_seconds:
                return 0.0
            return self.window_seconds - elapsed


class AdaptiveRateLimiter:
    """Adaptive rate limiter that adjusts based on success/failure."""
    def __init__(self, initial_rate: float, min_rate: float, max_rate: float):
        self.current_rate = initial_rate
        self.min_rate = min_rate
        self.max_rate = max_rate
        self._success_count = 0
        self._failure_count = 0
        self._lock = threading.Lock()
        self._last_adjust = time.time()
        self._adjust_interval = 10.0

    def record_success(self):
        with self._lock:
            self._success_count += 1
            self._maybe_adjust(increase=True)

    def record_failure(self):
        with self._lock:
            self._failure_count += 1
            self._maybe_adjust(increase=False)

    def _maybe_adjust(self, increase: bool):
        now = time.time()
        if now - self._last_adjust < self._adjust_interval:
            return
        self._last_adjust = now
        if increase:
            self.current_rate = min(self.max_rate, self.current_rate * 1.1)
        else:
            self.current_rate = max(self.min_rate, self.current_rate * 0.9)
        self._success_count = 0
        self._failure_count = 0

    def is_allowed(self) -> bool:
        return True

    def get_current_rate(self) -> float:
        return self.current_rate


_limiters: Dict[str, Any] = {}
_lock = threading.Lock()


class RateLimitTokenBucketAction(BaseAction):
    """Token bucket rate limiting."""
    action_type = "ratelimit_token_bucket"
    display_name = "令牌桶限流"
    description = "令牌桶算法限流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", "default")
            capacity = params.get("capacity", 10)
            refill_rate = params.get("refill_rate", 1.0)
            tokens = params.get("tokens", 1)
            wait = params.get("wait", False)

            with _lock:
                if key not in _limiters:
                    _limiters[key] = TokenBucket(capacity, refill_rate)
                limiter = _limiters[key]

            if wait:
                wait_time = limiter.get_wait_time(tokens)
                return ActionResult(
                    success=True,
                    message=f"Wait time: {wait_time:.2f}s",
                    data={"wait_time": wait_time, "key": key}
                )

            allowed = limiter.consume(tokens)
            wait_time = limiter.get_wait_time(tokens) if not allowed else 0

            return ActionResult(
                success=allowed,
                message="Request allowed" if allowed else f"Rate limited, wait {wait_time:.2f}s",
                data={"allowed": allowed, "wait_time": wait_time, "key": key}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Rate limit token bucket failed: {str(e)}")


class RateLimitSlidingWindowAction(BaseAction):
    """Sliding window rate limiting."""
    action_type = "ratelimit_sliding_window"
    display_name = "滑动窗口限流"
    description = "滑动窗口算法限流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", "default")
            max_requests = params.get("max_requests", 100)
            window_seconds = params.get("window_seconds", 60.0)

            with _lock:
                if key not in _limiters:
                    _limiters[key] = SlidingWindow(max_requests, window_seconds)
                limiter = _limiters[key]

            allowed = limiter.is_allowed()
            wait_time = limiter.get_wait_time()

            return ActionResult(
                success=allowed,
                message="Request allowed" if allowed else f"Rate limited, wait {wait_time:.2f}s",
                data={"allowed": allowed, "wait_time": wait_time, "key": key}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Rate limit sliding window failed: {str(e)}")


class RateLimitFixedWindowAction(BaseAction):
    """Fixed window rate limiting."""
    action_type = "ratelimit_fixed_window"
    display_name = "固定窗口限流"
    description = "固定窗口算法限流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", "default")
            max_requests = params.get("max_requests", 100)
            window_seconds = params.get("window_seconds", 60.0)

            with _lock:
                if key not in _limiters:
                    _limiters[key] = FixedWindow(max_requests, window_seconds)
                limiter = _limiters[key]

            allowed = limiter.is_allowed()
            wait_time = limiter.get_wait_time()

            return ActionResult(
                success=allowed,
                message="Request allowed" if allowed else f"Rate limited, wait {wait_time:.2f}s",
                data={"allowed": allowed, "wait_time": wait_time, "key": key}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Rate limit fixed window failed: {str(e)}")


class RateLimitAdaptiveAction(BaseAction):
    """Adaptive rate limiting."""
    action_type = "ratelimit_adaptive"
    display_name = "自适应限流"
    description = "自适应限流调节"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", "default")
            initial_rate = params.get("initial_rate", 10.0)
            min_rate = params.get("min_rate", 1.0)
            max_rate = params.get("max_rate", 100.0)
            record_success = params.get("record_success", False)
            record_failure = params.get("record_failure", False)

            with _lock:
                if key not in _limiters:
                    _limiters[key] = AdaptiveRateLimiter(initial_rate, min_rate, max_rate)
                limiter = _limiters[key]

            if record_success:
                limiter.record_success()
            if record_failure:
                limiter.record_failure()

            current_rate = limiter.get_current_rate()

            return ActionResult(
                success=True,
                message=f"Current rate: {current_rate:.2f} req/s",
                data={"current_rate": current_rate, "key": key}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Rate limit adaptive failed: {str(e)}")
