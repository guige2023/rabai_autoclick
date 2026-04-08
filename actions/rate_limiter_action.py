"""Rate Limiter Action Module.

Provides distributed rate limiting with token bucket, sliding window,
leaky bucket, and fixed window algorithms.
"""
from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class RateLimitAlgorithm(Enum):
    """Rate limiting algorithm."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    LEAKY_BUCKET = "leaky_bucket"
    FIXED_WINDOW = "fixed_window"


@dataclass
class RateLimitConfig:
    """Rate limit configuration."""
    rate: float
    capacity: int
    algorithm: RateLimitAlgorithm
    window_seconds: float = 60.0


@dataclass
class RateLimitResult:
    """Rate limit check result."""
    allowed: bool
    limit_type: str
    remaining: int
    reset_at: float
    retry_after: float = 0.0


@dataclass
class TokenBucket:
    """Token bucket state."""
    tokens: float
    last_update: float
    capacity: int
    refill_rate: float


class TokenBucketLimiter:
    """Token bucket rate limiter."""

    def __init__(self, capacity: int, refill_rate: float):
        self._bucket = TokenBucket(
            tokens=float(capacity),
            last_update=time.time(),
            capacity=capacity,
            refill_rate=refill_rate
        )

    def allow(self, tokens: int = 1) -> RateLimitResult:
        """Check if request is allowed."""
        now = time.time()
        elapsed = now - self._bucket.last_update

        self._bucket.tokens = min(
            self._bucket.capacity,
            self._bucket.tokens + elapsed * self._bucket.refill_rate
        )
        self._bucket.last_update = now

        if self._bucket.tokens >= tokens:
            self._bucket.tokens -= tokens
            return RateLimitResult(
                allowed=True,
                limit_type="token_bucket",
                remaining=int(self._bucket.tokens),
                reset_at=now + (self._bucket.capacity - self._bucket.tokens) / self._bucket.refill_rate
            )

        retry_after = (tokens - self._bucket.tokens) / self._bucket.refill_rate
        return RateLimitResult(
            allowed=False,
            limit_type="token_bucket",
            remaining=0,
            reset_at=now + retry_after,
            retry_after=retry_after
        )


class SlidingWindowLimiter:
    """Sliding window rate limiter."""

    def __init__(self, max_requests: int, window_seconds: float):
        self._max = max_requests
        self._window = window_seconds
        self._requests: List[float] = []

    def allow(self, tokens: int = 1) -> RateLimitResult:
        """Check if request is allowed."""
        now = time.time()
        cutoff = now - self._window

        self._requests = [t for t in self._requests if t > cutoff]

        if len(self._requests) + tokens <= self._max:
            self._requests.extend([now] * tokens)
            return RateLimitResult(
                allowed=True,
                limit_type="sliding_window",
                remaining=self._max - len(self._requests),
                reset_at=now + self._window
            )

        oldest = min(self._requests) if self._requests else now
        retry_after = (oldest + self._window) - now

        return RateLimitResult(
            allowed=False,
            limit_type="sliding_window",
            remaining=0,
            reset_at=now + retry_after,
            retry_after=max(0, retry_after)
        )


class LeakyBucketLimiter:
    """Leaky bucket rate limiter."""

    def __init__(self, capacity: int, leak_rate: float):
        self._capacity = capacity
        self._leak_rate = leak_rate
        self._level = 0.0
        self._last_leak = time.time()

    def _leak(self) -> None:
        """Leak water from bucket."""
        now = time.time()
        leaked = (now - self._last_leak) * self._leak_rate
        self._level = max(0, self._level - leaked)
        self._last_leak = now

    def allow(self, tokens: int = 1) -> RateLimitResult:
        """Check if request is allowed."""
        self._leak()

        if self._level + tokens <= self._capacity:
            self._level += tokens
            return RateLimitResult(
                allowed=True,
                limit_type="leaky_bucket",
                remaining=int(self._capacity - self._level),
                reset_at=time.time() + (self._level / self._leak_rate) if self._leak_rate > 0 else 0
            )

        retry_after = (self._capacity - self._level + tokens) / self._leak_rate

        return RateLimitResult(
            allowed=False,
            limit_type="leaky_bucket",
            remaining=0,
            reset_at=time.time() + retry_after,
            retry_after=retry_after
        )


class FixedWindowLimiter:
    """Fixed window rate limiter."""

    def __init__(self, max_requests: int, window_seconds: float):
        self._max = max_requests
        self._window = window_seconds
        self._count = 0
        self._window_start = time.time()

    def allow(self, tokens: int = 1) -> RateLimitResult:
        """Check if request is allowed."""
        now = time.time()

        if now - self._window_start >= self._window:
            self._count = 0
            self._window_start = now

        if self._count + tokens <= self._max:
            self._count += tokens
            return RateLimitResult(
                allowed=True,
                limit_type="fixed_window",
                remaining=self._max - self._count,
                reset_at=self._window_start + self._window
            )

        retry_after = (self._window_start + self._window) - now

        return RateLimitResult(
            allowed=False,
            limit_type="fixed_window",
            remaining=0,
            reset_at=self._window_start + self._window,
            retry_after=max(0, retry_after)
        )


class RateLimiterStore:
    """Store for rate limiters."""

    def __init__(self):
        self._limiters: Dict[str, Any] = {}

    def get_or_create(self, key: str, algorithm: RateLimitAlgorithm,
                      **kwargs) -> Any:
        """Get or create rate limiter."""
        if key not in self._limiters:
            if algorithm == RateLimitAlgorithm.TOKEN_BUCKET:
                capacity = kwargs.get("capacity", 100)
                refill_rate = kwargs.get("refill_rate", 10)
                self._limiters[key] = TokenBucketLimiter(capacity, refill_rate)
            elif algorithm == RateLimitAlgorithm.SLIDING_WINDOW:
                max_requests = kwargs.get("max_requests", 100)
                window = kwargs.get("window_seconds", 60)
                self._limiters[key] = SlidingWindowLimiter(max_requests, window)
            elif algorithm == RateLimitAlgorithm.LEAKY_BUCKET:
                capacity = kwargs.get("capacity", 100)
                leak_rate = kwargs.get("leak_rate", 10)
                self._limiters[key] = LeakyBucketLimiter(capacity, leak_rate)
            elif algorithm == RateLimitAlgorithm.FIXED_WINDOW:
                max_requests = kwargs.get("max_requests", 100)
                window = kwargs.get("window_seconds", 60)
                self._limiters[key] = FixedWindowLimiter(max_requests, window)

        return self._limiters[key]


_global_store = RateLimiterStore()


class RateLimiterAction:
    """Rate limiting action.

    Example:
        action = RateLimiterAction()

        action.configure("api", "token_bucket", capacity=100, refill_rate=10)
        result = action.check("api", "client-123")
    """

    def __init__(self, store: Optional[RateLimiterStore] = None):
        self._store = store or _global_store
        self._configs: Dict[str, RateLimitConfig] = {}

    def configure(self, name: str, algorithm: str,
                  rate: float = 10, capacity: int = 100,
                  window_seconds: float = 60) -> Dict[str, Any]:
        """Configure rate limiter."""
        try:
            algo = RateLimitAlgorithm(algorithm)
        except ValueError:
            return {"success": False, "message": f"Invalid algorithm: {algorithm}"}

        self._configs[name] = RateLimitConfig(
            rate=rate,
            capacity=capacity,
            algorithm=algo,
            window_seconds=window_seconds
        )

        return {
            "success": True,
            "name": name,
            "algorithm": algo.value,
            "capacity": capacity,
            "rate": rate,
            "message": f"Configured {name} with {algo.value}"
        }

    def check(self, name: str, key: str = "default",
             tokens: int = 1) -> Dict[str, Any]:
        """Check rate limit."""
        if name not in self._configs:
            return {"success": False, "message": f"Limiter {name} not configured"}

        config = self._configs[name]
        limiter_key = f"{name}:{key}"

        limiter = self._store.get_or_create(
            limiter_key,
            config.algorithm,
            capacity=config.capacity,
            refill_rate=config.rate,
            max_requests=int(config.rate * config.window_seconds),
            window_seconds=config.window_seconds
        )

        result = limiter.allow(tokens)

        return {
            "success": True,
            "allowed": result.allowed,
            "limit_type": result.limit_type,
            "remaining": result.remaining,
            "reset_at": result.reset_at,
            "retry_after": result.retry_after,
            "message": "Allowed" if result.allowed else "Rate limited"
        }


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute rate limiter action."""
    operation = params.get("operation", "check")
    action = RateLimiterAction()

    try:
        if operation == "configure":
            name = params.get("name", "")
            algorithm = params.get("algorithm", "token_bucket")
            if not name:
                return {"success": False, "message": "name required"}
            return action.configure(
                name=name,
                algorithm=algorithm,
                rate=params.get("rate", 10),
                capacity=params.get("capacity", 100),
                window_seconds=params.get("window_seconds", 60)
            )

        elif operation == "check":
            name = params.get("name", "")
            key = params.get("key", "default")
            tokens = params.get("tokens", 1)
            if not name:
                return {"success": False, "message": "name required"}
            return action.check(name, key, tokens)

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"Rate limiter error: {str(e)}"}
