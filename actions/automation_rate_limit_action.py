"""
Automation Rate Limit Module.

Provides rate limiting with token bucket and sliding window algorithms,
distributed rate limiting support, and adaptive rate control.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple
from collections import deque
import logging

logger = logging.getLogger(__name__)


class RateLimitAlgorithm(Enum):
    """Rate limiting algorithm types."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"
    ADAPTIVE = "adaptive"


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    algorithm: RateLimitAlgorithm = RateLimitAlgorithm.TOKEN_BUCKET
    rate: float = 100.0  # requests per second
    burst: float = 200.0  # max burst size
    window_size: float = 1.0  # window for sliding window
    enable_adaptive: bool = False
    min_rate: float = 10.0
    max_rate: float = 1000.0
    scale_up_threshold: float = 0.5  # 50% utilization
    scale_down_threshold: float = 0.2  # 20% utilization
    scale_factor: float = 1.5


@dataclass
class RateLimitResult:
    """Result of a rate limit check."""
    allowed: bool
    current_rate: float
    remaining: float
    retry_after: Optional[float] = None
    tokens_available: float = 0.0


class RateLimiter:
    """
    Rate limiter with multiple algorithm support.
    
    Example:
        limiter = RateLimiter(RateLimitConfig(
            algorithm=RateLimitAlgorithm.TOKEN_BUCKET,
            rate=100,
            burst=200
        ))
        
        result = await limiter.acquire()
        if result.allowed:
            await api_call()
        else:
            await asyncio.sleep(result.retry_after)
    """
    
    def __init__(self, config: Optional[RateLimitConfig] = None) -> None:
        """
        Initialize rate limiter.
        
        Args:
            config: Rate limit configuration.
        """
        self.config = config or RateLimitConfig()
        self._lock = asyncio.Lock()
        
        if self.config.algorithm == RateLimitAlgorithm.TOKEN_BUCKET:
            self._bucket_tokens = self.config.burst
            self._last_refill = time.time()
        elif self.config.algorithm == RateLimitAlgorithm.SLIDING_WINDOW:
            self._window: deque = deque()
        elif self.config.algorithm == RateLimitAlgorithm.FIXED_WINDOW:
            self._window_start = time.time()
            self._window_count = 0
        elif self.config.algorithm == RateLimitAlgorithm.ADAPTIVE:
            self._current_rate = self.config.rate
            self._bucket_tokens = self.config.burst
            self._last_refill = time.time()
            
    async def acquire(
        self,
        tokens: float = 1.0,
        client_id: Optional[str] = None,
    ) -> RateLimitResult:
        """
        Acquire tokens from the rate limiter.
        
        Args:
            tokens: Number of tokens to acquire.
            client_id: Optional client identifier for distributed limiting.
            
        Returns:
            RateLimitResult with acquisition status.
        """
        async with self._lock:
            if self.config.algorithm == RateLimitAlgorithm.TOKEN_BUCKET:
                return self._acquire_token_bucket(tokens)
            elif self.config.algorithm == RateLimitAlgorithm.SLIDING_WINDOW:
                return await self._acquire_sliding_window(tokens)
            elif self.config.algorithm == RateLimitAlgorithm.FIXED_WINDOW:
                return self._acquire_fixed_window(tokens)
            elif self.config.algorithm == RateLimitAlgorithm.ADAPTIVE:
                return self._acquire_adaptive(tokens)
            else:
                return RateLimitResult(allowed=True, current_rate=self.config.rate, remaining=self.config.rate)
                
    async def try_acquire(
        self,
        tokens: float = 1.0,
        timeout: float = 0.0,
    ) -> bool:
        """
        Try to acquire tokens with optional timeout.
        
        Args:
            tokens: Number of tokens to acquire.
            timeout: Maximum time to wait (0 = no wait).
            
        Returns:
            True if acquired within timeout.
        """
        start = time.time()
        
        while True:
            result = await self.acquire(tokens)
            
            if result.allowed:
                return True
                
            if timeout > 0 and time.time() - start >= timeout:
                return False
                
            if result.retry_after:
                await asyncio.sleep(min(result.retry_after, 0.1))
            else:
                await asyncio.sleep(0.01)
                
    def _acquire_token_bucket(self, tokens: float) -> RateLimitResult:
        """Acquire from token bucket."""
        now = time.time()
        elapsed = now - self._last_refill
        
        # Refill tokens
        refill_amount = elapsed * self.config.rate
        self._bucket_tokens = min(
            self.config.burst,
            self._bucket_tokens + refill_amount
        )
        self._last_refill = now
        
        if self._bucket_tokens >= tokens:
            self._bucket_tokens -= tokens
            return RateLimitResult(
                allowed=True,
                current_rate=self.config.rate,
                remaining=self._bucket_tokens,
                tokens_available=self._bucket_tokens,
            )
        else:
            # Calculate time until enough tokens
            tokens_needed = tokens - self._bucket_tokens
            retry_after = tokens_needed / self.config.rate
            
            return RateLimitResult(
                allowed=False,
                current_rate=self.config.rate,
                remaining=self._bucket_tokens,
                retry_after=retry_after,
                tokens_available=self._bucket_tokens,
            )
            
    async def _acquire_sliding_window(
        self,
        tokens: float,
    ) -> RateLimitResult:
        """Acquire from sliding window counter."""
        now = time.time()
        window_size = self.config.window_size
        
        # Remove expired entries
        cutoff = now - window_size
        while self._window and self._window[0] < cutoff:
            self._window.popleft()
            
        # Check current count
        current_count = len(self._window)
        
        if current_count < self.config.rate * window_size:
            self._window.append(now)
            remaining = (self.config.rate * window_size) - current_count - 1
            
            return RateLimitResult(
                allowed=True,
                current_rate=self.config.rate,
                remaining=max(0, remaining),
            )
        else:
            # Calculate retry time
            oldest = self._window[0]
            retry_after = oldest + window_size - now
            
            return RateLimitResult(
                allowed=False,
                current_rate=self.config.rate,
                remaining=0,
                retry_after=max(0, retry_after),
            )
            
    def _acquire_fixed_window(self, tokens: float) -> RateLimitResult:
        """Acquire from fixed window counter."""
        now = time.time()
        
        # Check if we're in a new window
        if now - self._window_start >= self.config.window_size:
            self._window_start = now
            self._window_count = 0
            
        if self._window_count < self.config.rate * self.config.window_size:
            self._window_count += 1
            remaining = (self.config.rate * self.config.window_size) - self._window_count
            
            return RateLimitResult(
                allowed=True,
                current_rate=self.config.rate,
                remaining=max(0, remaining),
            )
        else:
            retry_after = self._window_start + self.config.window_size - now
            
            return RateLimitResult(
                allowed=False,
                current_rate=self.config.rate,
                remaining=0,
                retry_after=max(0, retry_after),
            )
            
    def _acquire_adaptive(self, tokens: float) -> RateLimitResult:
        """Acquire with adaptive rate adjustment."""
        # First, do token bucket acquisition
        result = self._acquire_token_bucket(tokens)
        
        # Calculate current utilization
        utilization = 1 - (result.remaining / self.config.burst)
        
        # Adjust rate based on utilization
        if utilization > self.config.scale_up_threshold:
            # Scale up
            self._current_rate = min(
                self._current_rate * self.config.scale_factor,
                self.config.max_rate
            )
            logger.debug(f"Adaptive rate scaled up to {self._current_rate}")
        elif utilization < self.config.scale_down_threshold:
            # Scale down
            self._current_rate = max(
                self._current_rate / self.config.scale_factor,
                self.config.min_rate
            )
            logger.debug(f"Adaptive rate scaled down to {self._current_rate}")
            
        result.current_rate = self._current_rate
        return result
        
    def get_stats(self) -> Dict[str, any]:
        """Get rate limiter statistics."""
        return {
            "algorithm": self.config.algorithm.value,
            "rate": self.config.rate,
            "burst": self.config.burst,
            "current_rate": getattr(self, "_current_rate", self.config.rate),
            "tokens": getattr(self, "_bucket_tokens", self.config.burst),
        }
        
    def reset(self) -> None:
        """Reset the rate limiter."""
        if self.config.algorithm == RateLimitAlgorithm.TOKEN_BUCKET:
            self._bucket_tokens = self.config.burst
            self._last_refill = time.time()
        elif self.config.algorithm == RateLimitAlgorithm.SLIDING_WINDOW:
            self._window.clear()
        elif self.config.algorithm == RateLimitAlgorithm.FIXED_WINDOW:
            self._window_start = time.time()
            self._window_count = 0


class DistributedRateLimiter:
    """
    Distributed rate limiter using Redis or similar.
    
    Example:
        limiter = DistributedRateLimiter(
            redis_client=redis,
            key_prefix="rate_limit",
            rate=100,
            burst=200
        )
        
        result = await limiter.acquire(client_id="user_123")
    """
    
    def __init__(
        self,
        redis_client: Any,
        key_prefix: str = "rate_limit",
        rate: float = 100.0,
        burst: float = 200.0,
    ) -> None:
        """
        Initialize distributed rate limiter.
        
        Args:
            redis_client: Redis client instance.
            key_prefix: Key prefix for rate limit entries.
            rate: Requests per second.
            burst: Burst size.
        """
        self.redis = redis_client
        self.key_prefix = key_prefix
        self.rate = rate
        self.burst = burst
        
    async def acquire(self, client_id: str, tokens: float = 1.0) -> RateLimitResult:
        """
        Acquire tokens with distributed tracking.
        
        Args:
            client_id: Unique client identifier.
            tokens: Number of tokens to acquire.
            
        Returns:
            RateLimitResult.
        """
        key = f"{self.key_prefix}:{client_id}"
        
        # Lua script for atomic token bucket
        lua_script = """
        local key = KEYS[1]
        local rate = tonumber(ARGV[1])
        local burst = tonumber(ARGV[2])
        local tokens = tonumber(ARGV[3])
        local now = tonumber(ARGV[4])
        
        local bucket = redis.call('HGETALL', key)
        local last_refill = tonumber(bucket[2]) or now
        local bucket_tokens = tonumber(bucket[4]) or burst
        
        local elapsed = now - last_refill
        bucket_tokens = math.min(burst, bucket_tokens + (elapsed * rate))
        
        if bucket_tokens >= tokens then
            bucket_tokens = bucket_tokens - tokens
            redis.call('HSET', key, 'last_refill', now, 'tokens', bucket_tokens)
            redis.call('EXPIRE', key, 3600)
            return {1, bucket_tokens}
        else
            local retry_after = (tokens - bucket_tokens) / rate
            return {0, bucket_tokens, retry_after}
        end
        """
        
        try:
            result = await self.redis.eval(
                lua_script,
                1,
                key,
                self.rate,
                self.burst,
                tokens,
                time.time(),
            )
            
            allowed = bool(result[0])
            remaining = float(result[1])
            retry_after = float(result[2]) if len(result) > 2 and result[2] else None
            
            return RateLimitResult(
                allowed=allowed,
                current_rate=self.rate,
                remaining=remaining,
                retry_after=retry_after,
                tokens_available=remaining,
            )
            
        except Exception as e:
            logger.error(f"Distributed rate limit error: {e}")
            # Fail open
            return RateLimitResult(
                allowed=True,
                current_rate=self.rate,
                remaining=self.rate,
            )
