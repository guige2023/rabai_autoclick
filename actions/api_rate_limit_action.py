"""
API Rate Limit Action Module

Provides rate limiting, throttling, and quota management for API operations.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict
import asyncio
import time


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    max_requests: int = 100
    window_seconds: int = 60
    burst_size: int = 10
    strategy: str = "sliding_window"  # sliding_window, token_bucket, fixed_window


@dataclass
class RateLimitResult:
    """Result of a rate limit check."""
    allowed: bool
    remaining: int
    reset_at: datetime
    retry_after: Optional[float] = None
    limit_type: str = "request"


class TokenBucket:
    """Token bucket algorithm implementation."""
    
    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_refill = time.monotonic()
    
    def consume(self, tokens: int = 1) -> bool:
        """Try to consume tokens from the bucket."""
        self._refill()
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False
    
    def _refill(self):
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now


@dataclass
class SlidingWindowCounter:
    """Sliding window counter for rate limiting."""
    timestamps: list = field(default_factory=list)
    window_size: int = 60
    
    def is_allowed(self, max_requests: int) -> bool:
        """Check if request is allowed within window."""
        now = time.time()
        cutoff = now - self.window_size
        self.timestamps = [t for t in self.timestamps if t > cutoff]
        
        if len(self.timestamps) < max_requests:
            self.timestamps.append(now)
            return True
        return False
    
    def get_remaining(self, max_requests: int) -> int:
        """Get remaining requests in current window."""
        now = time.time()
        cutoff = now - self.window_size
        self.timestamps = [t for t in self.timestamps if t > cutoff]
        return max(0, max_requests - len(self.timestamps))


class RateLimitAction:
    """Main rate limiting action handler."""
    
    def __init__(self, config: Optional[RateLimitConfig] = None):
        self.config = config or RateLimitConfig()
        self._buckets: dict[str, TokenBucket] = {}
        self._sliding_windows: dict[str, SlidingWindowCounter] = {}
        self._quotas: dict[str, dict] = defaultdict(dict)
        self._locks: dict[str, asyncio.Lock] = {}
    
    async def check_rate_limit(
        self,
        key: str,
        operation: str = "default"
    ) -> RateLimitResult:
        """
        Check if operation is allowed under rate limits.
        
        Args:
            key: Identifier for rate limit scope (user, ip, api_key, etc.)
            operation: Operation type for differentiated limits
            
        Returns:
            RateLimitResult indicating if request is allowed
        """
        now = datetime.now()
        
        if self.config.strategy == "token_bucket":
            return await self._check_token_bucket(key, now)
        elif self.config.strategy == "fixed_window":
            return await self._check_fixed_window(key, now)
        else:
            return await self._check_sliding_window(key, now)
    
    async def _check_token_bucket(
        self,
        key: str,
        now: datetime
    ) -> RateLimitResult:
        """Token bucket rate limiting."""
        if key not in self._buckets:
            self._locks[key] = asyncio.Lock()
            self._buckets[key] = TokenBucket(
                self.config.burst_size,
                self.config.max_requests / self.config.window_seconds
            )
        
        async with self._locks[key]:
            bucket = self._buckets[key]
            allowed = bucket.consume()
            
            remaining = int(bucket.tokens)
            reset_at = now + timedelta(seconds=self.config.window_seconds)
            
            return RateLimitResult(
                allowed=allowed,
                remaining=max(0, remaining),
                reset_at=reset_at,
                limit_type="token_bucket"
            )
    
    async def _check_sliding_window(
        self,
        key: str,
        now: datetime
    ) -> RateLimitResult:
        """Sliding window rate limiting."""
        if key not in self._sliding_windows:
            self._sliding_windows[key] = SlidingWindowCounter(
                window_size=self.config.window_seconds
            )
        
        window = self._sliding_windows[key]
        allowed = window.is_allowed(self.config.max_requests)
        remaining = window.get_remaining(self.config.max_requests)
        
        reset_at = now + timedelta(seconds=self.config.window_seconds)
        
        return RateLimitResult(
            allowed=allowed,
            remaining=remaining,
            reset_at=reset_at,
            limit_type="sliding_window"
        )
    
    async def _check_fixed_window(
        self,
        key: str,
        now: datetime
    ) -> RateLimitResult:
        """Fixed window rate limiting."""
        window_key = f"{key}:{int(now.timestamp() // self.config.window_seconds)}"
        
        if window_key not in self._quotas:
            self._quotas[window_key] = {"count": 0, "window_start": now}
        
        quota = self._quotas[window_key]
        allowed = quota["count"] < self.config.max_requests
        
        if allowed:
            quota["count"] += 1
        
        remaining = max(0, self.config.max_requests - quota["count"])
        window_end = now + timedelta(seconds=self.config.window_seconds)
        
        return RateLimitResult(
            allowed=allowed,
            remaining=remaining,
            reset_at=window_end,
            limit_type="fixed_window"
        )
    
    async def get_quota_status(
        self,
        key: str,
        quota_type: str = "daily"
    ) -> dict[str, Any]:
        """Get current quota status for a key."""
        now = datetime.now()
        
        if quota_type == "daily":
            day_key = f"{key}:{now.date()}"
            if day_key not in self._quotas:
                return {
                    "used": 0,
                    "limit": self.config.max_requests * 60,  # Approximate daily limit
                    "remaining": self.config.max_requests * 60,
                    "resets_at": datetime.combine(now.date() + timedelta(days=1), datetime.min.time())
                }
            
            return {
                "used": self._quotas[day_key]["count"],
                "limit": self.config.max_requests * 60,
                "remaining": max(0, self.config.max_requests * 60 - self._quotas[day_key]["count"]),
                "resets_at": datetime.combine(now.date() + timedelta(days=1), datetime.min.time())
            }
        
        return {"error": f"Unknown quota type: {quota_type}"}
    
    async def reset_limit(self, key: str) -> bool:
        """Manually reset rate limit for a key."""
        if key in self._buckets:
            del self._buckets[key]
        if key in self._sliding_windows:
            del self._sliding_windows[key]
        if key in self._locks:
            del self._locks[key]
        return True
    
    async def adjust_limit(
        self,
        key: str,
        max_requests: Optional[int] = None,
        window_seconds: Optional[int] = None
    ) -> dict[str, Any]:
        """Dynamically adjust rate limits."""
        if max_requests is not None:
            self.config.max_requests = max_requests
        if window_seconds is not None:
            self.config.window_seconds = window_seconds
        
        return {
            "max_requests": self.config.max_requests,
            "window_seconds": self.config.window_seconds,
            "key": key
        }
