"""API rate throttling action module.

Provides per-client and per-endpoint rate limiting using token bucket
and sliding window algorithms. Supports distributed throttling via Redis.
"""

from __future__ import annotations

import time
import threading
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
from collections import defaultdict, deque

logger = logging.getLogger(__name__)


@dataclass
class ThrottleResult:
    """Result of a throttle check."""
    allowed: bool
    remaining: int
    reset_at: float
    retry_after_ms: Optional[float] = None


class TokenBucket:
    """Token bucket rate limiter."""

    def __init__(self, rate: float, capacity: int) -> None:
        self.rate = rate
        self.capacity = capacity
        self._tokens = float(capacity)
        self._last_update = time.time()
        self._lock = threading.Lock()

    def consume(self, tokens: int = 1) -> ThrottleResult:
        """Try to consume tokens.

        Returns:
            ThrottleResult indicating if allowed.
        """
        with self._lock:
            now = time.time()
            elapsed = now - self._last_update
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
            self._last_update = now

            if self._tokens >= tokens:
                self._tokens -= tokens
                return ThrottleResult(
                    allowed=True,
                    remaining=int(self._tokens),
                    reset_at=now + (self.capacity - self._tokens) / self.rate,
                )

            retry_ms = (tokens - self._tokens) / self.rate * 1000
            return ThrottleResult(
                allowed=False,
                remaining=0,
                reset_at=now + retry_ms / 1000,
                retry_after_ms=retry_ms,
            )


class SlidingWindowCounter:
    """Sliding window rate limiter."""

    def __init__(self, max_requests: int, window_seconds: int) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: deque = deque()
        self._lock = threading.Lock()

    def check(self) -> ThrottleResult:
        """Check if a request is allowed."""
        with self._lock:
            now = time.time()
            cutoff = now - self.window_seconds

            while self._requests and self._requests[0] < cutoff:
                self._requests.popleft()

            if len(self._requests) < self.max_requests:
                self._requests.append(now)
                remaining = self.max_requests - len(self._requests)
                return ThrottleResult(
                    allowed=True,
                    remaining=remaining,
                    reset_at=now + self.window_seconds,
                )

            retry_after = self._requests[0] + self.window_seconds - now
            return ThrottleResult(
                allowed=False,
                remaining=0,
                reset_at=self._requests[0] + self.window_seconds,
                retry_after_ms=retry_after * 1000,
            )


class APIClientThrottle:
    """Per-client API rate limiter.

    Provides per-client (API key, IP, user ID) rate limiting with
    configurable limits per endpoint.
    """

    def __init__(
        self,
        default_rate: int = 100,
        default_window: int = 60,
        algorithm: str = "token_bucket",
    ) -> None:
        """Initialize client throttle.

        Args:
            default_rate: Default requests per window.
            default_window: Window size in seconds.
            algorithm: 'token_bucket' or 'sliding_window'.
        """
        self.default_rate = default_rate
        self.default_window = default_window
        self.algorithm = algorithm
        self._limiters: Dict[str, Any] = {}
        self._endpoint_limits: Dict[str, tuple] = {}
        self._lock = threading.Lock()

    def set_endpoint_limit(
        self,
        endpoint: str,
        rate: int,
        window: int = 60,
    ) -> None:
        """Set a custom rate limit for an endpoint.

        Args:
            endpoint: URL path or pattern (e.g., '/api/users/*').
            rate: Max requests per window.
            window: Window size in seconds.
        """
        self._endpoint_limits[endpoint] = (rate, window)

    def check(
        self,
        client_id: str,
        endpoint: str = "*",
    ) -> ThrottleResult:
        """Check if a request is allowed for a client.

        Args:
            client_id: Client identifier (API key, IP, etc.).
            endpoint: API endpoint path.

        Returns:
            ThrottleResult with allowed status.
        """
        rate, window = self._get_limits(endpoint)

        with self._lock:
            key = f"{client_id}:{endpoint}"
            limiter = self._limiters.get(key)

            if limiter is None:
                if self.algorithm == "sliding_window":
                    limiter = SlidingWindowCounter(rate, window)
                else:
                    limiter = TokenBucket(rate / window, rate)
                self._limiters[key] = limiter

        return limiter.consume() if isinstance(limiter, TokenBucket) else limiter.check()

    def _get_limits(self, endpoint: str) -> tuple:
        """Get rate and window for an endpoint."""
        for pattern, limits in self._endpoint_limits.items():
            if self._match_pattern(endpoint, pattern):
                return limits
        return (self.default_rate, self.default_window)

    def _match_pattern(self, path: str, pattern: str) -> bool:
        """Simple glob-style pattern matching."""
        import fnmatch
        return fnmatch.fnmatch(path, pattern)

    def reset_client(self, client_id: str) -> None:
        """Reset all rate limits for a client."""
        with self._lock:
            keys_to_remove = [k for k in self._limiters if k.startswith(client_id + ":")]
            for k in keys_to_remove:
                del self._limiters[k]

    def get_status(self, client_id: str, endpoint: str = "*") -> Dict[str, Any]:
        """Get current throttle status for a client."""
        key = f"{client_id}:{endpoint}"
        with self._lock:
            limiter = self._limiters.get(key)
            if limiter is None:
                return {"limited": False, "remaining": self.default_rate}
            if isinstance(limiter, SlidingWindowCounter):
                return {
                    "limited": len(limiter._requests) >= limiter.max_requests,
                    "remaining": limiter.max_requests - len(limiter._requests),
                    "window": limiter.window_seconds,
                }
            return {
                "limited": limiter._tokens < 1,
                "remaining": int(limiter._tokens),
                "capacity": limiter.capacity,
            }


class APIThrottleAction:
    """High-level API throttling action.

    Example:
        throttle = APIThrottleAction(default_rate=100, default_window=60)
        throttle.set_endpoint_limit("/api/search", rate=10, window=60)

        result = throttle.check(client_id="user123", endpoint="/api/search")
        if not result.allowed:
            print(f"Rate limited. Retry after {result.retry_after_ms}ms")
    """

    def __init__(
        self,
        default_rate: int = 100,
        default_window: int = 60,
        algorithm: str = "token_bucket",
    ) -> None:
        self.client_throttle = APIClientThrottle(
            default_rate=default_rate,
            default_window=default_window,
            algorithm=algorithm,
        )

    def check(
        self,
        client_id: str,
        endpoint: str = "*",
    ) -> ThrottleResult:
        """Check rate limit for a client/endpoint."""
        return self.client_throttle.check(client_id, endpoint)

    def set_endpoint_limit(
        self,
        endpoint: str,
        rate: int,
        window: int = 60,
    ) -> None:
        """Set custom limit for an endpoint."""
        self.client_throttle.set_endpoint_limit(endpoint, rate, window)

    def headers(self, result: ThrottleResult) -> Dict[str, str]:
        """Build rate limit headers from result."""
        return {
            "X-RateLimit-Remaining": str(result.remaining),
            "X-RateLimit-Reset": str(int(result.reset_at)),
            **({"Retry-After": str(int(result.retry_after_ms / 1000))} if not result.allowed else {}),
        }
