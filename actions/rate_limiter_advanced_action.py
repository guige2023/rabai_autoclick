"""Rate Limiter Advanced Action Module.

Provides advanced rate limiting with sliding window,
token bucket, and concurrent request limiting.
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
class SlidingWindowLimiter:
    """Sliding window rate limiter."""
    max_requests: int
    window_seconds: float
    requests: list = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)


class AdvancedRateLimiter:
    """Advanced rate limiting with multiple strategies."""

    def __init__(self):
        self._sliding_windows: Dict[str, SlidingWindowLimiter] = {}
        self._token_buckets: Dict[str, Dict] = {}
        self._locks: Dict[str, threading.Lock] = {}

    def sliding_window_check(
        self,
        key: str,
        max_requests: int,
        window_seconds: float
    ) -> tuple[bool, int]:
        """Check sliding window rate limit."""
        if key not in self._locks:
            self._locks[key] = threading.Lock()

        limiter = self._sliding_windows.get(key)
        if not limiter:
            limiter = SlidingWindowLimiter(
                max_requests=max_requests,
                window_seconds=window_seconds
            )
            self._sliding_windows[key] = limiter

        with limiter.lock:
            now = time.time()
            cutoff = now - window_seconds

            limiter.requests = [
                t for t in limiter.requests if t > cutoff
            ]

            if len(limiter.requests) < max_requests:
                limiter.requests.append(now)
                return True, max_requests - len(limiter.requests) - 1

            return False, 0

    def token_bucket_consume(
        self,
        key: str,
        tokens: float,
        capacity: float,
        refill_rate: float
    ) -> tuple[bool, float]:
        """Consume tokens from bucket."""
        if key not in self._locks:
            self._locks[key] = threading.Lock()

        if key not in self._token_buckets:
            self._token_buckets[key] = {
                "tokens": capacity,
                "last_refill": time.time()
            }

        bucket = self._token_buckets[key]

        with self._locks[key]:
            now = time.time()
            elapsed = now - bucket["last_refill"]
            bucket["tokens"] = min(
                capacity,
                bucket["tokens"] + elapsed * refill_rate
            )
            bucket["last_refill"] = now

            if bucket["tokens"] >= tokens:
                bucket["tokens"] -= tokens
                return True, bucket["tokens"]
            return False, bucket["tokens"]


class RateLimiterAdvancedAction(BaseAction):
    """Action for advanced rate limiting."""

    def __init__(self):
        super().__init__("rate_limiter_advanced")
        self._limiter = AdvancedRateLimiter()

    def execute(self, params: Dict) -> ActionResult:
        """Execute rate limiter action."""
        try:
            operation = params.get("operation", "sliding_window")

            if operation == "sliding_window":
                return self._sliding_window(params)
            elif operation == "token_bucket":
                return self._token_bucket(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _sliding_window(self, params: Dict) -> ActionResult:
        """Sliding window rate limit check."""
        allowed, remaining = self._limiter.sliding_window_check(
            params.get("key", ""),
            params.get("max_requests", 100),
            params.get("window_seconds", 60)
        )
        return ActionResult(success=True, data={
            "allowed": allowed,
            "remaining": remaining
        })

    def _token_bucket(self, params: Dict) -> ActionResult:
        """Token bucket consume."""
        allowed, remaining = self._limiter.token_bucket_consume(
            params.get("key", ""),
            params.get("tokens", 1),
            params.get("capacity", 100),
            params.get("refill_rate", 10)
        )
        return ActionResult(success=True, data={
            "allowed": allowed,
            "remaining": remaining
        })
