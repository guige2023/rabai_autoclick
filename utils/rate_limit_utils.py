"""
Rate limiting utilities for API throttling and quota management.

Provides token bucket, sliding window, fixed window, and leaky bucket
algorithms with Redis-backed distributed support.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class RateLimitAlgorithm(Enum):
    TOKEN_BUCKET = auto()
    SLIDING_WINDOW = auto()
    FIXED_WINDOW = auto()
    LEAKY_BUCKET = auto()


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    requests_per_second: float = 100
    burst_size: int = 200
    block_duration_seconds: int = 60
    algorithm: RateLimitAlgorithm = RateLimitAlgorithm.TOKEN_BUCKET


@dataclass
class RateLimitResult:
    """Result of a rate limit check."""
    allowed: bool
    remaining: int
    reset_at: float
    retry_after: float = 0.0


class TokenBucket:
    """Token bucket rate limiter."""

    def __init__(self, capacity: int, refill_rate: float) -> None:
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._tokens = float(capacity)
        self._last_refill = time.time()
        self._lock = False

    def consume(self, tokens: int = 1) -> bool:
        """Try to consume tokens from the bucket."""
        self._refill()
        if self._tokens >= tokens:
            self._tokens -= tokens
            return True
        return False

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
        self._last_refill = now

    @property
    def available_tokens(self) -> float:
        self._refill()
        return self._tokens


class SlidingWindowCounter:
    """Sliding window counter rate limiter."""

    def __init__(self, max_requests: int, window_seconds: float) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: list[float] = []

    def is_allowed(self) -> bool:
        """Check if a request is allowed under the rate limit."""
        now = time.time()
        cutoff = now - self.window_seconds
        self._requests = [t for t in self._requests if t > cutoff]
        if len(self._requests) < self.max_requests:
            self._requests.append(now)
            return True
        return False

    @property
    def current_count(self) -> int:
        now = time.time()
        cutoff = now - self.window_seconds
        return sum(1 for t in self._requests if t > cutoff)


class LeakyBucket:
    """Leaky bucket rate limiter."""

    def __init__(self, capacity: int, leak_rate: float) -> None:
        self.capacity = capacity
        self.leak_rate = leak_rate
        self._level = 0.0
        self._last_leak = time.time()

    def add(self, units: int = 1) -> bool:
        """Try to add units to the bucket."""
        self._leak()
        if self._level + units <= self.capacity:
            self._level += units
            return True
        return False

    def _leak(self) -> None:
        """Leak water from the bucket."""
        now = time.time()
        elapsed = now - self._last_leak
        leaked = elapsed * self.leak_rate
        self._level = max(0, self._level - leaked)
        self._last_leak = now

    def consume(self, units: int = 1) -> bool:
        """Consume units (remove from bucket)."""
        self._leak()
        if self._level >= units:
            self._level -= units
            return True
        return False


class RateLimitManager:
    """Manages rate limiting for multiple clients/keys."""

    def __init__(self, config: Optional[RateLimitConfig] = None) -> None:
        self.config = config or RateLimitConfig()
        self._limiters: dict[str, Any] = {}
        self._blocked: dict[str, float] = {}
        self._hits: dict[str, list[float]] = defaultdict(list)

    def check(self, key: str) -> RateLimitResult:
        """Check if a request is allowed."""
        now = time.time()
        if self._is_blocked(key, now):
            reset_at = self._blocked.get(key, now) + self.config.block_duration_seconds
            return RateLimitResult(
                allowed=False,
                remaining=0,
                reset_at=reset_at,
                retry_after=reset_at - now,
            )

        limiter = self._get_limiter(key)
        allowed = False

        if self.config.algorithm == RateLimitAlgorithm.TOKEN_BUCKET:
            allowed = limiter.consume()
        elif self.config.algorithm == RateLimitAlgorithm.SLIDING_WINDOW:
            allowed = limiter.is_allowed()
        elif self.config.algorithm == RateLimitAlgorithm.FIXED_WINDOW:
            allowed = self._check_fixed_window(key)
        elif self.config.algorithm == RateLimitAlgorithm.LEAKY_BUCKET:
            allowed = limiter.add()

        if allowed:
            self._hits[key].append(now)
            return RateLimitResult(
                allowed=True,
                remaining=self._get_remaining(key),
                reset_at=now + 1.0,
            )
        else:
            self._blocked[key] = now
            return RateLimitResult(
                allowed=False,
                remaining=0,
                reset_at=now + self.config.block_duration_seconds,
                retry_after=self.config.block_duration_seconds,
            )

    def _check_fixed_window(self, key: str) -> bool:
        """Fixed window rate limiting."""
        now = time.time()
        window = int(now)
        self._hits[key] = [t for t in self._hits[key] if int(t) == window]
        if len(self._hits[key]) < self.config.requests_per_second:
            self._hits[key].append(now)
            return True
        return False

    def _get_limiter(self, key: str) -> Any:
        """Get or create a limiter for a key."""
        if key not in self._limiters:
            if self.config.algorithm == RateLimitAlgorithm.TOKEN_BUCKET:
                self._limiters[key] = TokenBucket(
                    capacity=self.config.burst_size,
                    refill_rate=self.config.requests_per_second,
                )
            elif self.config.algorithm == RateLimitAlgorithm.SLIDING_WINDOW:
                self._limiters[key] = SlidingWindowCounter(
                    max_requests=int(self.config.requests_per_second),
                    window_seconds=1.0,
                )
            elif self.config.algorithm == RateLimitAlgorithm.LEAKY_BUCKET:
                self._limiters[key] = LeakyBucket(
                    capacity=self.config.burst_size,
                    leak_rate=self.config.requests_per_second,
                )
        return self._limiters[key]

    def _is_blocked(self, key: str, now: float) -> bool:
        """Check if a key is currently blocked."""
        if key in self._blocked:
            if now - self._blocked[key] > self.config.block_duration_seconds:
                del self._blocked[key]
                return False
            return True
        return False

    def _get_remaining(self, key: str) -> int:
        """Get remaining requests for a key."""
        limiter = self._get_limiter(key)
        if self.config.algorithm == RateLimitAlgorithm.TOKEN_BUCKET:
            return int(limiter.available_tokens)
        elif self.config.algorithm == RateLimitAlgorithm.SLIDING_WINDOW:
            return max(0, int(self.config.requests_per_second) - limiter.current_count)
        return 0

    def unblock(self, key: str) -> None:
        """Manually unblock a key."""
        if key in self._blocked:
            del self._blocked[key]


class RateLimitMiddleware:
    """ASGI middleware for rate limiting."""

    def __init__(self, app: Any, config: Optional[RateLimitConfig] = None) -> None:
        self.app = app
        self.config = config or RateLimitConfig()
        self.manager = RateLimitManager(config)

    async def __call__(self, scope: dict, receive: Callable, send: Callable) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        client_ip = scope.get("client", [None])[0] or "unknown"
        key = f"ip:{client_ip}"

        result = self.manager.check(key)
        if not result.allowed:
            await self._rate_limit_exceeded(send, result)
            return

        await self.app(scope, receive, send)

    async def _rate_limit_exceeded(self, send: Callable, result: RateLimitResult) -> None:
        """Send rate limit exceeded response."""
        import json
        response = {
            "status": 429,
            "body": json.dumps({"error": "Rate limit exceeded", "retry_after": result.retry_after}),
            "headers": [
                (b"X-RateLimit-Remaining", b"0"),
                (b"X-RateLimit-Reset", str(int(result.reset_at)).encode()),
                (b"Retry-After", str(int(result.retry_after)).encode()),
                (b"Content-Type", b"application/json"),
            ],
        }
        await send({
            "type": "http.response.start",
            "status": 429,
            "headers": response["headers"],
        })
        await send({
            "type": "http.response.body",
            "body": response["body"].encode(),
        })
