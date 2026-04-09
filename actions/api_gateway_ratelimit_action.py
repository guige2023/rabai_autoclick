"""
API Gateway Rate Limit Action Module.

Token bucket and sliding window rate limiting for API
gateways with distributed counter support.
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Optional
import logging

logger = logging.getLogger(__name__)


@dataclass
class RateLimitConfig:
    """Rate limit configuration."""
    requests_per_second: float
    burst_size: int
    block_duration: float = 60.0


@dataclass
class TokenBucket:
    """Token bucket state."""
    tokens: float
    last_update: float
    max_tokens: float
    refill_rate: float


@dataclass
class SlidingWindow:
    """Sliding window counter state."""
    timestamps: list[float]
    window_size: float
    max_requests: int


class APIGatewayRateLimitAction:
    """
    Rate limiting using token bucket and sliding window algorithms.

    Example:
        limiter = APIGatewayRateLimitAction()
        limiter.configure(requests_per_second=100, burst_size=200)

        allowed, remaining = limiter.check_rate_limit("client_123")
        if not allowed:
            raise TooManyRequestsError()
    """

    def __init__(self):
        """Initialize rate limiter."""
        self._token_buckets: dict[str, TokenBucket] = {}
        self._sliding_windows: dict[str, SlidingWindow] = {}
        self._blocked: dict[str, float] = {}
        self._lock = asyncio.Lock()

    def configure(
        self,
        requests_per_second: float,
        burst_size: int,
        block_duration: float = 60.0
    ) -> None:
        """Configure default rate limit."""
        self.default_config = RateLimitConfig(
            requests_per_second=requests_per_second,
            burst_size=burst_size,
            block_duration=block_duration
        )

    def check_rate_limit(
        self,
        client_id: str,
        requests: int = 1,
        algorithm: str = "token_bucket"
    ) -> tuple[bool, int]:
        """
        Check if request is within rate limit.

        Args:
            client_id: Client identifier.
            requests: Number of requests to check.
            algorithm: "token_bucket" or "sliding_window".

        Returns:
            Tuple of (allowed, remaining_requests).
        """
        now = time.time()

        if client_id in self._blocked:
            if now < self._blocked[client_id]:
                return False, 0
            del self._blocked[client_id]

        if algorithm == "sliding_window":
            return self._check_sliding_window(client_id, requests, now)
        return self._check_token_bucket(client_id, requests, now)

    def _check_token_bucket(
        self,
        client_id: str,
        requests: int,
        now: float
    ) -> tuple[bool, int]:
        """Check rate limit using token bucket algorithm."""
        config = getattr(self, 'default_config', RateLimitConfig(100, 200))

        if client_id not in self._token_buckets:
            self._token_buckets[client_id] = TokenBucket(
                tokens=float(config.burst_size),
                last_update=now,
                max_tokens=float(config.burst_size),
                refill_rate=config.requests_per_second
            )

        bucket = self._token_buckets[client_id]
        elapsed = now - bucket.last_update
        bucket.tokens = min(bucket.max_tokens, bucket.tokens + elapsed * bucket.refill_rate)
        bucket.last_update = now

        if bucket.tokens >= requests:
            bucket.tokens -= requests
            remaining = int(bucket.tokens)
            return True, remaining

        return False, 0

    def _check_sliding_window(
        self,
        client_id: str,
        requests: int,
        now: float
    ) -> tuple[bool, int]:
        """Check rate limit using sliding window algorithm."""
        window_size = 1.0
        max_requests = int(getattr(self, 'default_config', RateLimitConfig(100, 200)).requests_per_second)

        if client_id not in self._sliding_windows:
            self._sliding_windows[client_id] = SlidingWindow(
                timestamps=[],
                window_size=window_size,
                max_requests=max_requests
            )

        window = self._sliding_windows[client_id]
        cutoff = now - window.window_size
        window.timestamps = [t for t in window.timestamps if t > cutoff]

        if len(window.timestamps) + requests <= window.max_requests:
            window.timestamps.extend([now] * requests)
            remaining = window.max_requests - len(window.timestamps)
            return True, remaining

        return False, 0

    def block_client(self, client_id: str, duration: Optional[float] = None) -> None:
        """Block a client temporarily."""
        config = getattr(self, 'default_config', RateLimitConfig(100, 200))
        self._blocked[client_id] = time.time() + (duration or config.block_duration)
        logger.warning(f"Blocked client: {client_id}")

    def unblock_client(self, client_id: str) -> None:
        """Unblock a client."""
        if client_id in self._blocked:
            del self._blocked[client_id]

    def get_remaining(self, client_id: str) -> int:
        """Get remaining requests for client."""
        allowed, remaining = self.check_rate_limit(client_id, algorithm="sliding_window")
        return remaining

    def reset_client(self, client_id: str) -> None:
        """Reset rate limit state for client."""
        self._token_buckets.pop(client_id, None)
        self._sliding_windows.pop(client_id, None)
        self._blocked.pop(client_id, None)

    def get_stats(self) -> dict:
        """Get rate limiter statistics."""
        return {
            "token_buckets": len(self._token_buckets),
            "sliding_windows": len(self._sliding_windows),
            "blocked_clients": len(self._blocked)
        }
