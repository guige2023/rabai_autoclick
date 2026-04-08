"""API Rate Limiter Action Module.

Provides rate limiting, throttling, and quota management
for API endpoints and services.
"""

from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
import time
import asyncio
import hashlib


class RateLimitStrategy(Enum):
    """Rate limiting strategies."""
    FIXED_WINDOW = "fixed_window"
    SLIDING_WINDOW = "sliding_window"
    TOKEN_BUCKET = "token_bucket"
    LEAKY_BUCKET = "leaky_bucket"


@dataclass
class RateLimitConfig:
    """Configuration for a rate limit."""
    requests_per_window: int
    window_seconds: int
    strategy: RateLimitStrategy = RateLimitStrategy.FIXED_WINDOW
    burst_limit: Optional[int] = None
    block_duration: int = 0
    key_prefix: str = ""


@dataclass
class RateLimitResult:
    """Result of a rate limit check."""
    allowed: bool
    remaining: int
    reset_at: datetime
    retry_after: Optional[int] = None
    limit_type: str = "request"


@dataclass
class Quota:
    """Represents a usage quota."""
    name: str
    total: int
    used: int = 0
    window_start: Optional[datetime] = None
    window_duration: int = 0
    grace_period: int = 0

    def is_exhausted(self) -> bool:
        """Check if quota is exhausted."""
        if self.window_start:
            window_end = self.window_start + timedelta(seconds=self.window_duration)
            if datetime.now() > window_end:
                self.reset()
                return False
        return self.used >= self.total

    def reset(self):
        """Reset quota usage."""
        self.used = 0
        self.window_start = datetime.now()

    def consume(self, amount: int = 1) -> bool:
        """Attempt to consume quota."""
        if self.is_exhausted():
            return False
        self.used += amount
        return True

    def remaining(self) -> int:
        """Get remaining quota."""
        return max(0, self.total - self.used)


class FixedWindowLimiter:
    """Fixed window rate limiter."""

    def __init__(self, config: RateLimitConfig):
        self.config = config
        self._requests: Dict[str, List[datetime]] = {}

    def _get_key(self, identifier: str) -> str:
        """Generate storage key."""
        prefix = self.config.key_prefix or "fixed"
        return f"{prefix}:{identifier}"

    def check(self, identifier: str) -> RateLimitResult:
        """Check if request is allowed."""
        key = self._get_key(identifier)
        now = datetime.now()
        window_start = now.replace(
            microsecond=0, second=0, minute=0
        )
        if now.second >= 0:
            window_start = window_start

        if key not in self._requests:
            self._requests[key] = []

        self._requests[key] = [
            t for t in self._requests[key]
            if t > now - timedelta(seconds=self.config.window_seconds)
        ]

        if len(self._requests[key]) >= self.config.requests_per_window:
            reset_at = window_start + timedelta(seconds=self.config.window_seconds)
            retry_after = int((reset_at - now).total_seconds())
            return RateLimitResult(
                allowed=False,
                remaining=0,
                reset_at=reset_at,
                retry_after=max(0, retry_after),
            )

        self._requests[key].append(now)
        remaining = self.config.requests_per_window - len(self._requests[key])
        reset_at = window_start + timedelta(seconds=self.config.window_seconds)

        return RateLimitResult(
            allowed=True,
            remaining=remaining,
            reset_at=reset_at,
        )


class SlidingWindowLimiter:
    """Sliding window rate limiter."""

    def __init__(self, config: RateLimitConfig):
        self.config = config
        self._requests: Dict[str, List[datetime]] = {}

    def _get_key(self, identifier: str) -> str:
        """Generate storage key."""
        return f"sliding:{self.config.key_prefix}:{identifier}"

    def check(self, identifier: str) -> RateLimitResult:
        """Check if request is allowed."""
        key = self._get_key(identifier)
        now = datetime.now()
        cutoff = now - timedelta(seconds=self.config.window_seconds)

        if key not in self._requests:
            self._requests[key] = []

        self._requests[key] = [t for t in self._requests[key] if t > cutoff]

        if len(self._requests[key]) >= self.config.requests_per_window:
            oldest = min(self._requests[key]) if self._requests[key] else now
            reset_at = oldest + timedelta(seconds=self.config.window_seconds)
            return RateLimitResult(
                allowed=False,
                remaining=0,
                reset_at=reset_at,
                retry_after=int((oldest - now).total_seconds() * -1),
            )

        self._requests[key].append(now)
        remaining = self.config.requests_per_window - len(self._requests[key])

        return RateLimitResult(
            allowed=True,
            remaining=remaining,
            reset_at=now + timedelta(seconds=self.config.window_seconds),
        )


class TokenBucketLimiter:
    """Token bucket rate limiter with burst support."""

    def __init__(self, config: RateLimitConfig):
        self.config = config
        self._buckets: Dict[str, Dict[str, float]] = {}

    def _get_key(self, identifier: str) -> str:
        """Generate storage key."""
        return f"token:{self.config.key_prefix}:{identifier}"

    def check(self, identifier: str, tokens: int = 1) -> RateLimitResult:
        """Check if request is allowed."""
        key = self._get_key(identifier)
        now = time.time()

        if key not in self._buckets:
            rate = self.config.requests_per_window / self.config.window_seconds
            self._buckets[key] = {
                "tokens": float(self.config.burst_limit or self.config.requests_per_window),
                "last_refill": now,
                "rate": rate,
            }

        bucket = self._buckets[key]
        elapsed = now - bucket["last_refill"]
        bucket["tokens"] = min(
            bucket.get("tokens", 0) + elapsed * bucket["rate"],
            self.config.burst_limit or self.config.requests_per_window,
        )
        bucket["last_refill"] = now

        if bucket["tokens"] >= tokens:
            bucket["tokens"] -= tokens
            return RateLimitResult(
                allowed=True,
                remaining=int(bucket["tokens"]),
                reset_at=datetime.fromtimestamp(now + (tokens / bucket["rate"])),
            )

        tokens_needed = tokens - bucket["tokens"]
        wait_time = int(tokens_needed / bucket["rate"])
        return RateLimitResult(
            allowed=False,
            remaining=0,
            reset_at=datetime.fromtimestamp(now + wait_time),
            retry_after=wait_time,
        )


class RateLimitManager:
    """Central manager for all rate limiting."""

    def __init__(self):
        self._limiters: Dict[str, Any] = {}
        self._quotas: Dict[str, Quota] = {}
        self._blocked: Dict[str, datetime] = {}
        self._metrics: Dict[str, int] = {"allowed": 0, "rejected": 0}

    def create_limiter(
        self,
        name: str,
        config: RateLimitConfig,
    ) -> Any:
        """Create a rate limiter with given config."""
        if config.strategy == RateLimitStrategy.FIXED_WINDOW:
            limiter = FixedWindowLimiter(config)
        elif config.strategy == RateLimitStrategy.SLIDING_WINDOW:
            limiter = SlidingWindowLimiter(config)
        elif config.strategy == RateLimitStrategy.TOKEN_BUCKET:
            limiter = TokenBucketLimiter(config)
        else:
            limiter = FixedWindowLimiter(config)

        self._limiters[name] = limiter
        return limiter

    def check_rate_limit(
        self,
        limiter_name: str,
        identifier: str,
    ) -> RateLimitResult:
        """Check rate limit for identifier."""
        if self.is_blocked(identifier):
            return RateLimitResult(
                allowed=False,
                remaining=0,
                reset_at=self._blocked.get(identifier, datetime.now()),
                retry_after=config.block_duration if 'config' in dir() else 0,
            )

        limiter = self._limiters.get(limiter_name)
        if not limiter:
            return RateLimitResult(
                allowed=True,
                remaining=-1,
                reset_at=datetime.now(),
            )

        result = limiter.check(identifier)

        if result.allowed:
            self._metrics["allowed"] += 1
        else:
            self._metrics["rejected"] += 1

        return result

    def block(self, identifier: str, duration_seconds: int):
        """Block an identifier for specified duration."""
        self._blocked[identifier] = datetime.now() + timedelta(seconds=duration_seconds)

    def is_blocked(self, identifier: str) -> bool:
        """Check if identifier is blocked."""
        if identifier not in self._blocked:
            return False
        if datetime.now() > self._blocked[identifier]:
            del self._blocked[identifier]
            return False
        return True

    def create_quota(
        self,
        name: str,
        total: int,
        window_duration: int = 0,
        grace_period: int = 0,
    ) -> Quota:
        """Create a new quota."""
        quota = Quota(
            name=name,
            total=total,
            window_duration=window_duration,
            grace_period=grace_period,
        )
        self._quotas[name] = quota
        return quota

    def check_quota(self, quota_name: str, amount: int = 1) -> bool:
        """Check and consume quota."""
        quota = self._quotas.get(quota_name)
        if not quota:
            return True
        return quota.consume(amount)

    def get_quota_status(self, quota_name: str) -> Dict[str, Any]:
        """Get quota status."""
        quota = self._quotas.get(quota_name)
        if not quota:
            return {"error": f"Quota {quota_name} not found"}
        return {
            "name": quota.name,
            "total": quota.total,
            "used": quota.used,
            "remaining": quota.remaining(),
            "is_exhausted": quota.is_exhausted(),
        }

    def get_metrics(self) -> Dict[str, Any]:
        """Get rate limiting metrics."""
        total = self._metrics["allowed"] + self._metrics["rejected"]
        return {
            "allowed": self._metrics["allowed"],
            "rejected": self._metrics["rejected"],
            "total": total,
            "rejection_rate": (
                self._metrics["rejected"] / total if total > 0 else 0
            ),
            "blocked_identifiers": len(self._blocked),
            "active_quotas": len(self._quotas),
        }


class APIRateLimitAction:
    """High-level API rate limiting action."""

    def __init__(self, manager: Optional[RateLimitManager] = None):
        self.manager = manager or RateLimitManager()
        self._middleware: List[Callable] = []
        self._fallback_handlers: Dict[str, Callable] = {}

    def add_limiter(
        self,
        name: str,
        requests_per_window: int,
        window_seconds: int,
        strategy: RateLimitStrategy = RateLimitStrategy.FIXED_WINDOW,
    ) -> APIRateLimitAction:
        """Add a named limiter."""
        config = RateLimitConfig(
            requests_per_window=requests_per_window,
            window_seconds=window_seconds,
            strategy=strategy,
            key_prefix=name,
        )
        self.manager.create_limiter(name, config)
        return self

    async def check_request(
        self,
        limiter_name: str,
        identifier: str,
    ) -> RateLimitResult:
        """Check if request should be allowed."""
        return self.manager.check_rate_limit(limiter_name, identifier)

    def add_quota(
        self,
        name: str,
        total: int,
        window_duration: int = 0,
    ) -> APIRateLimitAction:
        """Add a quota."""
        self.manager.create_quota(name, total, window_duration)
        return self

    def get_metrics(self) -> Dict[str, Any]:
        """Get all metrics."""
        return self.manager.get_metrics()


# Module exports
__all__ = [
    "APIRateLimitAction",
    "FixedWindowLimiter",
    "LeakyBucketLimiter",
    "Quota",
    "RateLimitConfig",
    "RateLimitManager",
    "RateLimitResult",
    "RateLimitStrategy",
    "SlidingWindowLimiter",
    "TokenBucketLimiter",
]
