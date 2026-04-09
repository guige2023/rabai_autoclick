"""
API Rate Limit Quotas Action Module

Manages per-client rate limiting with configurable quotas.
"""

from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import time
import threading
import math


class QuotaScope(Enum):
    """Quota scope types."""
    GLOBAL = "global"
    PER_CLIENT = "per_client"
    PER_ENDPOINT = "per_endpoint"
    PER_CLIENT_AND_ENDPOINT = "per_client_and_endpoint"


class QuotaExceededAction(Enum):
    """Action when quota is exceeded."""
    REJECT = "reject"
    QUEUE = "queue"
    DELAY = "delay"


@dataclass
class QuotaLimit:
    """Single quota limit definition."""
    requests: int  # Max requests
    window_seconds: int  # Time window


@dataclass
class ClientQuota:
    """Client-specific quota configuration."""
    client_id: str
    limits: List[QuotaLimit]
    scope: QuotaScope = QuotaScope.PER_CLIENT
    burst_allowance: int = 0  # Extra requests allowed in burst


@dataclass
class QuotaResult:
    """Result of a quota check."""
    allowed: bool
    remaining: int
    reset_at: float
    retry_after_ms: Optional[int] = None


@dataclass
class RateLimitEntry:
    """Internal rate limit tracking entry."""
    client_id: str
    endpoint: Optional[str]
    request_count: int = 0
    window_start: float = field(default_factory=time.time)
    tokens: int = 0


class TokenBucket:
    """Token bucket algorithm for rate limiting."""

    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate  # Tokens per second
        self.tokens = float(capacity)
        self.last_refill = time.time()
        self.lock = threading.Lock()

    def consume(self, tokens: int = 1) -> bool:
        """Try to consume tokens. Returns True if allowed."""
        with self.lock:
            self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill
        refill = elapsed * self.refill_rate
        self.tokens = min(self.capacity, self.tokens + refill)
        self.last_refill = now

    def get_available(self) -> float:
        """Get available tokens."""
        with self.lock:
            self._refill()
            return self.tokens


class SlidingWindowCounter:
    """Sliding window counter algorithm."""

    def __init__(self, max_requests: int, window_seconds: int):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests: List[float] = []
        self.lock = threading.Lock()

    def is_allowed(self) -> bool:
        """Check if request is allowed."""
        with self.lock:
            now = time.time()
            # Remove expired entries
            self.requests = [t for t in self.requests if now - t < self.window_seconds]

            if len(self.requests) < self.max_requests:
                self.requests.append(now)
                return True
            return False

    def get_remaining(self) -> int:
        """Get remaining requests in current window."""
        with self.lock:
            now = time.time()
            self.requests = [t for t in self.requests if now - t < self.window_seconds]
            return max(0, self.max_requests - len(self.requests))

    def get_reset_time(self) -> float:
        """Get timestamp when window will reset."""
        with self.lock:
            if not self.requests:
                return time.time()
            oldest = min(self.requests)
            return oldest + self.window_seconds


class ApiRateLimitQuotasAction:
    """
    Manages per-client API rate limiting with configurable quotas.

    Supports multiple algorithms (token bucket, sliding window) and
    various quota scopes (global, per-client, per-endpoint).

    Example:
        manager = ApiRateLimitQuotasAction()
        manager.add_client_quota(ClientQuota(
            client_id="client_123",
            limits=[QuotaLimit(requests=100, window_seconds=60)]
        ))
        result = manager.check_quota("client_123", "/api/users")
    """

    def __init__(
        self,
        default_limit: Optional[QuotaLimit] = None,
        algorithm: str = "sliding_window"
    ):
        """
        Initialize rate limit quotas manager.

        Args:
            default_limit: Default quota limit if client not found
            algorithm: Rate limiting algorithm ("token_bucket" or "sliding_window")
        """
        self.default_limit = default_limit or QuotaLimit(requests=100, window_seconds=60)
        self.algorithm = algorithm
        self.client_quotas: Dict[str, ClientQuota] = {}
        self.global_limiters: Dict[Tuple[str, str], SlidingWindowCounter] = {}
        self.token_buckets: Dict[Tuple[str, str], TokenBucket] = {}
        self.lock = threading.RLock()

    def add_client_quota(self, quota: ClientQuota) -> None:
        """Add or update a client quota configuration."""
        with self.lock:
            self.client_quotas[quota.client_id] = quota

    def remove_client_quota(self, client_id: str) -> None:
        """Remove a client quota configuration."""
        with self.lock:
            self.client_quotas.pop(client_id, None)
            # Clean up associated limiters
            keys_to_remove = [k for k in self.global_limiters if k[0] == client_id]
            for key in keys_to_remove:
                self.global_limiters.pop(key, None)
                self.token_buckets.pop(key, None)

    def check_quota(
        self,
        client_id: str,
        endpoint: Optional[str] = None,
        scope: QuotaScope = QuotaScope.PER_CLIENT
    ) -> QuotaResult:
        """
        Check if a request is allowed under the quota.

        Args:
            client_id: Client identifier
            endpoint: API endpoint path
            scope: Quota scope to check

        Returns:
            QuotaResult indicating if request is allowed
        """
        with self.lock:
            quota = self.client_quotas.get(client_id)
            if not quota:
                quota = ClientQuota(
                    client_id=client_id,
                    limits=[self.default_limit]
                )

            # Find the most appropriate limit
            limit = quota.limits[0] if quota.limits else self.default_limit

            # Build limiter key
            if scope == QuotaScope.GLOBAL:
                key = ("global", endpoint or "")
            elif scope == QuotaScope.PER_ENDPOINT:
                key = (endpoint or "",)
            elif scope == QuotaScope.PER_CLIENT_AND_ENDPOINT:
                key = (client_id, endpoint or "")
            else:
                key = (client_id,)

            if self.algorithm == "token_bucket":
                return self._check_token_bucket(key, limit, quota)
            else:
                return self._check_sliding_window(key, limit)

    def _check_token_bucket(
        self,
        key: Tuple[str, ...],
        limit: QuotaLimit,
        quota: ClientQuota
    ) -> QuotaResult:
        """Check using token bucket algorithm."""
        bucket_key = key + (self.algorithm,)
        if bucket_key not in self.token_buckets:
            # capacity = limit + burst, refill_rate = limit / window
            capacity = limit.requests + quota.burst_allowance
            refill_rate = limit.requests / limit.window_seconds
            self.token_buckets[bucket_key] = TokenBucket(capacity, refill_rate)

        bucket = self.token_buckets[bucket_key]
        allowed = bucket.consume()

        if allowed:
            return QuotaResult(
                allowed=True,
                remaining=int(bucket.get_available()),
                reset_at=time.time() + limit.window_seconds
            )
        else:
            return QuotaResult(
                allowed=False,
                remaining=0,
                reset_at=time.time() + limit.window_seconds,
                retry_after_ms=int(limit.window_seconds * 1000)
            )

    def _check_sliding_window(
        self,
        key: Tuple[str, ...],
        limit: QuotaLimit
    ) -> QuotaResult:
        """Check using sliding window algorithm."""
        if key not in self.global_limiters:
            self.global_limiters[key] = SlidingWindowCounter(
                limit.requests, limit.window_seconds
            )

        limiter = self.global_limiters[key]
        allowed = limiter.is_allowed()

        if allowed:
            return QuotaResult(
                allowed=True,
                remaining=limiter.get_remaining(),
                reset_at=limiter.get_reset_time()
            )
        else:
            reset_at = limiter.get_reset_time()
            retry_ms = int((reset_at - time.time()) * 1000)
            return QuotaResult(
                allowed=False,
                remaining=0,
                reset_at=reset_at,
                retry_after_ms=max(0, retry_ms)
            )

    def get_stats(self, client_id: str) -> Dict[str, Any]:
        """Get rate limiting statistics for a client."""
        with self.lock:
            stats = {"client_id": client_id, "endpoints": {}}

            for key, limiter in self.global_limiters.items():
                if key[0] == client_id:
                    endpoint = key[1] if len(key) > 1 else "global"
                    stats["endpoints"][endpoint] = {
                        "remaining": limiter.get_remaining(),
                        "reset_at": limiter.get_reset_time()
                    }

            return stats

    def reset(self, client_id: Optional[str] = None) -> None:
        """Reset rate limit counters."""
        with self.lock:
            if client_id:
                keys = [k for k in self.global_limiters if k[0] == client_id]
                for key in keys:
                    self.global_limiters.pop(key, None)
                    self.token_buckets.pop(key + (self.algorithm,), None)
            else:
                self.global_limiters.clear()
                self.token_buckets.clear()
