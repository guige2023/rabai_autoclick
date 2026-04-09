"""
Token bucket rate limiter action.

Provides distributed rate limiting with configurable bucket parameters.
"""

from typing import Any, Optional
import time
import threading


class TokenBucketRateLimiterAction:
    """Token bucket algorithm for rate limiting."""

    def __init__(
        self,
        capacity: int = 100,
        refill_rate: float = 10.0,
        tokens: Optional[float] = None,
    ) -> None:
        """
        Initialize token bucket rate limiter.

        Args:
            capacity: Maximum bucket capacity (tokens)
            refill_rate: Tokens added per second
            tokens: Initial token count (defaults to capacity)
        """
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._tokens = tokens if tokens is not None else float(capacity)
        self._last_refill = time.time()
        self._lock = threading.Lock()

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Check if request is allowed under rate limit.

        Args:
            params: Dictionary containing:
                - tokens_required: Number of tokens to consume (default 1)
                - client_id: Optional client identifier for tracking

        Returns:
            Dictionary with:
                - allowed: Boolean indicating if request is permitted
                - remaining_tokens: Tokens left in bucket
                - retry_after: Seconds to wait if not allowed
        """
        tokens_required = params.get("tokens_required", 1)
        client_id = params.get("client_id", "default")

        allowed, remaining, retry_after = self._consume(tokens_required)

        return {
            "allowed": allowed,
            "remaining_tokens": remaining,
            "retry_after": retry_after,
            "client_id": client_id,
            "timestamp": time.time(),
        }

    def _consume(self, tokens: float) -> tuple[bool, float, float]:
        """Consume tokens from bucket."""
        with self._lock:
            self._refill()

            if self._tokens >= tokens:
                self._tokens -= tokens
                return True, self._tokens, 0.0
            else:
                deficit = tokens - self._tokens
                retry_after = deficit / self.refill_rate
                return False, self._tokens, retry_after

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_refill
        new_tokens = elapsed * self.refill_rate
        self._tokens = min(self.capacity, self._tokens + new_tokens)
        self._last_refill = now

    def get_bucket_status(self) -> dict[str, Any]:
        """Get current bucket status."""
        with self._lock:
            self._refill()
            return {
                "capacity": self.capacity,
                "tokens": self._tokens,
                "refill_rate": self.refill_rate,
                "last_refill": self._last_refill,
            }

    def reset(self) -> None:
        """Reset bucket to full capacity."""
        with self._lock:
            self._tokens = float(self.capacity)
            self._last_refill = time.time()


class SlidingWindowRateLimiterAction:
    """Sliding window rate limiter for smoother rate limiting."""

    def __init__(self, max_requests: int = 100, window_size: float = 60.0) -> None:
        """
        Initialize sliding window rate limiter.

        Args:
            max_requests: Maximum requests allowed in window
            window_size: Window size in seconds
        """
        self.max_requests = max_requests
        self.window_size = window_size
        self._requests: list[float] = []
        self._lock = threading.Lock()

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Check if request is allowed.

        Args:
            params: Dictionary containing:
                - client_id: Client identifier

        Returns:
            Dictionary with allowed status and remaining requests
        """
        client_id = params.get("client_id", "default")
        now = time.time()

        allowed, remaining, reset_time = self._check_request(now)

        return {
            "allowed": allowed,
            "remaining": remaining,
            "reset_after": reset_time - now,
            "client_id": client_id,
        }

    def _check_request(self, timestamp: float) -> tuple[bool, int, float]:
        """Check if request is allowed."""
        with self._lock:
            cutoff = timestamp - self.window_size
            self._requests = [r for r in self._requests if r > cutoff]

            if len(self._requests) < self.max_requests:
                self._requests.append(timestamp)
                reset_time = timestamp + self.window_size
                return True, self.max_requests - len(self._requests), reset_time
            else:
                oldest = min(self._requests)
                reset_time = oldest + self.window_size
                return False, 0, reset_time

    def get_status(self) -> dict[str, Any]:
        """Get current rate limiter status."""
        with self._lock:
            now = time.time()
            cutoff = now - self.window_size
            active_requests = [r for r in self._requests if r > cutoff]
            return {
                "max_requests": self.max_requests,
                "window_size": self.window_size,
                "active_requests": len(active_requests),
                "remaining": self.max_requests - len(active_requests),
            }
