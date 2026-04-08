"""
API Throttle Action - Rate limiting for API requests.

This module provides rate limiting capabilities for
controlling API request throughput.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum


class ThrottleStrategy(Enum):
    """Throttling strategies."""
    TOKEN_BUCKET = "token_bucket"
    LEAKY_BUCKET = "leaky_bucket"
    FIXED_WINDOW = "fixed_window"


@dataclass
class ThrottleConfig:
    """Configuration for throttling."""
    rate: int = 100
    window: float = 60.0
    burst: int = 10


class TokenBucketThrottle:
    """Token bucket rate limiter."""
    
    def __init__(self, config: ThrottleConfig) -> None:
        self.config = config
        self.tokens = config.rate
        self.last_update = time.time()
    
    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens."""
        self._refill()
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False
    
    def _refill(self) -> None:
        """Refill tokens."""
        now = time.time()
        elapsed = now - self.last_update
        self.tokens = min(self.config.rate, self.tokens + elapsed * (self.config.rate / self.config.window))
        self.last_update = now


class APIThrottleAction:
    """API throttle action for automation workflows."""
    
    def __init__(self, rate: int = 100, window: float = 60.0) -> None:
        self.config = ThrottleConfig(rate=rate, window=window)
        self.throttle = TokenBucketThrottle(self.config)
    
    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire permission to proceed."""
        return await self.throttle.acquire(tokens)


__all__ = ["ThrottleStrategy", "ThrottleConfig", "TokenBucketThrottle", "APIThrottleAction"]
