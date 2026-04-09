"""API Rate Limiter Action Module.

Provides rate limiting capabilities for API operations including
token bucket, sliding window, and leaky bucket algorithms.

Example:
    >>> from actions.api.api_rate_limiter_action import APIRateLimiterAction
    >>> action = APIRateLimiterAction()
    >>> allowed = await action.check_limit("client_123")
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, Optional
import threading


class RateLimitAlgorithm(Enum):
    """Rate limiting algorithm types."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    LEAKY_BUCKET = "leaky_bucket"
    FIXED_WINDOW = "fixed_window"


class RateLimitStatus(Enum):
    """Status of a rate limit check."""
    ALLOWED = "allowed"
    RATE_LIMITED = "rate_limited"
    QUOTA_EXCEEDED = "quota_exceeded"
    BLOCKED = "blocked"


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting.
    
    Attributes:
        algorithm: Rate limiting algorithm to use
        requests_per_second: Maximum requests per second
        requests_per_minute: Maximum requests per minute
        requests_per_hour: Maximum requests per hour
        burst_size: Maximum burst size
        block_duration: Duration to block when exceeded (seconds)
    """
    algorithm: RateLimitAlgorithm = RateLimitAlgorithm.TOKEN_BUCKET
    requests_per_second: float = 10.0
    requests_per_minute: int = 100
    requests_per_hour: int = 1000
    burst_size: int = 20
    block_duration: float = 60.0


@dataclass
class ClientBucket:
    """Token bucket state for a client.
    
    Attributes:
        client_id: Unique client identifier
        tokens: Current available tokens
        last_update: Last token refill timestamp
        request_count: Total requests made
        blocked_until: Block expiration time
    """
    client_id: str
    tokens: float
    last_update: float
    request_count: int = 0
    blocked_until: Optional[float] = None
    window_requests: Dict[str, int] = field(default_factory=dict)


class APIRateLimiterAction:
    """Rate limiter for API operations.
    
    Provides configurable rate limiting using various algorithms
    to prevent API abuse and ensure fair resource distribution.
    
    Attributes:
        config: Rate limiting configuration
        buckets: Client token buckets
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[RateLimitConfig] = None,
    ) -> None:
        """Initialize rate limiter with configuration.
        
        Args:
            config: Rate limit configuration. Uses defaults if not provided.
        """
        self.config = config or RateLimitConfig()
        self.buckets: Dict[str, ClientBucket] = {}
        self._lock = threading.RLock()
    
    async def check_limit(
        self,
        client_id: str,
        cost: float = 1.0,
    ) -> RateLimitStatus:
        """Check if request is within rate limits.
        
        Args:
            client_id: Unique client identifier
            cost: Request cost (tokens to consume)
        
        Returns:
            RateLimitStatus indicating if request is allowed
        """
        bucket = self._get_or_create_bucket(client_id)
        current_time = time.time()
        
        if bucket.blocked_until and current_time < bucket.blocked_until:
            return RateLimitStatus.BLOCKED
        
        if self._is_rate_limited(bucket, current_time):
            bucket.blocked_until = current_time + self.config.block_duration
            return RateLimitStatus.RATE_LIMITED
        
        if self._check_quota_exceeded(bucket):
            return RateLimitStatus.QUOTA_EXCEEDED
        
        self._consume_tokens(bucket, cost, current_time)
        bucket.request_count += 1
        
        return RateLimitStatus.ALLOWED
    
    def _get_or_create_bucket(self, client_id: str) -> ClientBucket:
        """Get or create bucket for client.
        
        Args:
            client_id: Unique client identifier
        
        Returns:
            ClientBucket instance
        """
        with self._lock:
            if client_id not in self.buckets:
                self.buckets[client_id] = ClientBucket(
                    client_id=client_id,
                    tokens=float(self.config.burst_size),
                    last_update=time.time(),
                )
            return self.buckets[client_id]
    
    def _is_rate_limited(
        self,
        bucket: ClientBucket,
        current_time: float,
    ) -> bool:
        """Check if client is rate limited.
        
        Args:
            bucket: Client bucket state
            current_time: Current timestamp
        
        Returns:
            True if rate limited
        """
        self._refill_tokens(bucket, current_time)
        return bucket.tokens < 1.0
    
    def _check_quota_exceeded(self, bucket: ClientBucket) -> bool:
        """Check if quota is exceeded.
        
        Args:
            bucket: Client bucket state
        
        Returns:
            True if quota exceeded
        """
        window_key = self._get_current_window_key()
        
        if window_key not in bucket.window_requests:
            bucket.window_requests.clear()
            bucket.window_requests[window_key] = 0
        
        if bucket.window_requests.get(window_key, 0) >= self.config.requests_per_minute:
            return True
        
        bucket.window_requests[window_key] = (
            bucket.window_requests.get(window_key, 0) + 1
        )
        return False
    
    def _consume_tokens(
        self,
        bucket: ClientBucket,
        cost: float,
        current_time: float,
    ) -> None:
        """Consume tokens from bucket.
        
        Args:
            bucket: Client bucket state
            cost: Number of tokens to consume
            current_time: Current timestamp
        """
        self._refill_tokens(bucket, current_time)
        bucket.tokens = max(0.0, bucket.tokens - cost)
        bucket.last_update = current_time
    
    def _refill_tokens(
        self,
        bucket: ClientBucket,
        current_time: float,
    ) -> None:
        """Refill tokens based on elapsed time.
        
        Args:
            bucket: Client bucket state
            current_time: Current timestamp
        """
        elapsed = current_time - bucket.last_update
        refill_rate = self.config.requests_per_second
        
        new_tokens = elapsed * refill_rate
        bucket.tokens = min(
            float(self.config.burst_size),
            bucket.tokens + new_tokens,
        )
        bucket.last_update = current_time
    
    def _get_current_window_key(self) -> str:
        """Get current time window key.
        
        Returns:
            Window key string
        """
        return datetime.now().strftime("%Y%m%d%H%M")
    
    async def get_limit_info(
        self,
        client_id: str,
    ) -> Dict[str, Any]:
        """Get rate limit information for client.
        
        Args:
            client_id: Unique client identifier
        
        Returns:
            Dictionary with limit information
        """
        bucket = self._get_or_create_bucket(client_id)
        current_time = time.time()
        
        return {
            "client_id": client_id,
            "available_tokens": bucket.tokens,
            "request_count": bucket.request_count,
            "blocked": bucket.blocked_until is not None
                and current_time < bucket.blocked_until,
            "block_remaining": max(
                0,
                (bucket.blocked_until or 0) - current_time
            ) if bucket.blocked_until else 0,
            "limit_per_second": self.config.requests_per_second,
            "limit_per_minute": self.config.requests_per_minute,
        }
    
    async def reset_client(self, client_id: str) -> None:
        """Reset rate limit for client.
        
        Args:
            client_id: Unique client identifier
        """
        with self._lock:
            if client_id in self.buckets:
                del self.buckets[client_id]
    
    async def reset_all(self) -> None:
        """Reset all rate limits."""
        with self._lock:
            self.buckets.clear()
