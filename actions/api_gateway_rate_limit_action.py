"""
API Gateway Rate Limit Action Module.

Rate limiting for API gateway with token bucket, sliding window,
or fixed window algorithms. Supports per-client and global limits.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class RateLimitConfig:
    """Rate limit configuration."""
    algorithm: str = "token_bucket"  # token_bucket, sliding_window, fixed_window
    max_requests: int = 100
    window_seconds: int = 60
    burst_size: int = 10


@dataclass
class RateLimitResult:
    """Result of rate limit check."""
    allowed: bool
    remaining: int
    reset_at: float
    retry_after: Optional[float] = None


class APIGatewayRateLimitAction(BaseAction):
    """Rate limiting middleware for API gateway."""

    def __init__(self) -> None:
        super().__init__("api_gateway_rate_limit")
        self._buckets: dict[str, dict[str, Any]] = {}
        self._config = RateLimitConfig()

    def execute(self, context: dict, params: dict) -> dict:
        """
        Check and update rate limit.

        Args:
            context: Execution context
            params: Parameters:
                - client_id: Client identifier
                - request_count: Number of requests in this call
                - algorithm: token_bucket, sliding_window, fixed_window
                - max_requests: Max requests per window
                - window_seconds: Time window in seconds
                - burst_size: Burst capacity (for token bucket)

        Returns:
            RateLimitResult with allowed status and metadata
        """
        import time

        client_id = params.get("client_id", "default")
        request_count = params.get("request_count", 1)
        algorithm = params.get("algorithm", "token_bucket")
        max_requests = params.get("max_requests", self._config.max_requests)
        window_seconds = params.get("window_seconds", self._config.window_seconds)
        burst_size = params.get("burst_size", self._config.burst_size)

        now = time.time()

        if client_id not in self._buckets:
            self._buckets[client_id] = {}

        if algorithm == "token_bucket":
            return self._check_token_bucket(client_id, request_count, max_requests, window_seconds, burst_size, now)
        elif algorithm == "sliding_window":
            return self._check_sliding_window(client_id, request_count, max_requests, window_seconds, now)
        elif algorithm == "fixed_window":
            return self._check_fixed_window(client_id, request_count, max_requests, window_seconds, now)
        else:
            return RateLimitResult(allowed=True, remaining=max_requests, reset_at=now + window_seconds).__dict__

    def _check_token_bucket(self, client_id: str, count: int, max_req: int, window: int, burst: int, now: float) -> RateLimitResult:
        """Token bucket algorithm."""
        bucket = self._buckets.get(client_id, {})
        tokens = bucket.get("tokens", float(burst))
        last_update = bucket.get("last_update", now)

        tokens = min(burst, tokens + (now - last_update) * (max_req / window))
        if tokens >= count:
            tokens -= count
            allowed = True
        else:
            allowed = False

        self._buckets[client_id] = {"tokens": tokens, "last_update": now}
        remaining = int(tokens)
        retry_after = None if allowed else (count - tokens) * (window / max_req)

        return RateLimitResult(
            allowed=allowed,
            remaining=max(0, remaining),
            reset_at=now + window,
            retry_after=retry_after
        )

    def _check_sliding_window(self, client_id: str, count: int, max_req: int, window: int, now: float) -> RateLimitResult:
        """Sliding window algorithm."""
        bucket = self._buckets.get(client_id, {})
        requests = bucket.get("requests", [])
        cutoff = now - window
        requests = [r for r in requests if r > cutoff]

        total_count = len(requests) + count
        if total_count <= max_req:
            requests.extend([now] * count)
            allowed = True
        else:
            allowed = False
            total_count = len(requests)

        self._buckets[client_id] = {"requests": requests}
        remaining = max(0, max_req - total_count)
        reset_at = now + window
        retry_after = window if not allowed else None

        return RateLimitResult(allowed=allowed, remaining=remaining, reset_at=reset_at, retry_after=retry_after)

    def _check_fixed_window(self, client_id: str, count: int, max_req: int, window: int, now: float) -> RateLimitResult:
        """Fixed window algorithm."""
        import math
        bucket = self._buckets.get(client_id, {})
        current_window = math.floor(now / window)
        window_key = f"w_{current_window}"
        request_count = bucket.get(window_key, 0)

        if request_count + count <= max_req:
            bucket[window_key] = request_count + count
            allowed = True
        else:
            allowed = False

        self._buckets[client_id] = bucket
        remaining = max(0, max_req - request_count - count)
        reset_at = (current_window + 1) * window
        retry_after = reset_at - now if not allowed else None

        return RateLimitResult(allowed=allowed, remaining=remaining, reset_at=reset_at, retry_after=retry_after)

    def configure(self, algorithm: str = "token_bucket", max_requests: int = 100, window_seconds: int = 60, burst_size: int = 10) -> None:
        """Configure rate limit settings."""
        self._config = RateLimitConfig(
            algorithm=algorithm,
            max_requests=max_requests,
            window_seconds=window_seconds,
            burst_size=burst_size
        )

    def reset_client(self, client_id: str) -> None:
        """Reset rate limit for a client."""
        if client_id in self._buckets:
            del self._buckets[client_id]
