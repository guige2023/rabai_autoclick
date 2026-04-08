"""
API Rate Limiter Action Module.

Provides rate limiting for API requests using token bucket,
leaky bucket, sliding window, and fixed window algorithms.

Author: RabAi Team
"""

from __future__ import annotations

import json
import sys
import os
import time
import threading
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RateLimitAlgorithm(Enum):
    """Rate limiting algorithms."""
    TOKEN_BUCKET = "token_bucket"
    LEAKY_BUCKET = "leaky_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    requests_per_second: float = 10.0
    burst_size: int = 20
    requests_per_minute: Optional[int] = None
    requests_per_hour: Optional[int] = None
    requests_per_day: Optional[int] = None
    algorithm: RateLimitAlgorithm = RateLimitAlgorithm.TOKEN_BUCKET


@dataclass
class RateLimitResult:
    """Result of a rate limit check."""
    allowed: bool
    remaining: int
    reset_at: float
    retry_after: Optional[float] = None
    limit_type: str = "second"


class TokenBucketRateLimiter:
    """Token bucket rate limiter implementation."""
    
    def __init__(self, rate: float, burst: int):
        self.rate = rate
        self.burst = burst
        self._tokens = float(burst)
        self._last_update = time.time()
        self._lock = threading.Lock()
    
    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_update
        self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
        self._last_update = now
    
    def acquire(self, tokens: int = 1) -> RateLimitResult:
        """Try to acquire tokens."""
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return RateLimitResult(
                    allowed=True,
                    remaining=int(self._tokens),
                    reset_at=self._last_update,
                    limit_type="second"
                )
            else:
                wait_time = (tokens - self._tokens) / self.rate
                return RateLimitResult(
                    allowed=False,
                    remaining=0,
                    reset_at=self._last_update + wait_time,
                    retry_after=wait_time,
                    limit_type="second"
                )


class LeakyBucketRateLimiter:
    """Leaky bucket rate limiter implementation."""
    
    def __init__(self, rate: float, capacity: int):
        self.rate = rate
        self.capacity = capacity
        self._level = 0.0
        self._last_update = time.time()
        self._lock = threading.Lock()
    
    def acquire(self, tokens: int = 1) -> RateLimitResult:
        """Try to acquire tokens."""
        with self._lock:
            now = time.time()
            elapsed = now - self._last_update
            self._level = max(0, self._level - elapsed * self.rate)
            self._last_update = now
            
            if self._level + tokens <= self.capacity:
                self._level += tokens
                return RateLimitResult(
                    allowed=True,
                    remaining=int(self.capacity - self._level),
                    reset_at=now + (self.capacity - self._level) / self.rate,
                    limit_type="leaky_bucket"
                )
            else:
                wait_time = (self._level + tokens - self.capacity) / self.rate
                return RateLimitResult(
                    allowed=False,
                    remaining=0,
                    reset_at=now + wait_time,
                    retry_after=wait_time,
                    limit_type="leaky_bucket"
                )


class SlidingWindowRateLimiter:
    """Sliding window rate limiter implementation."""
    
    def __init__(self, max_requests: int, window_seconds: float):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: deque = deque()
        self._lock = threading.Lock()
    
    def acquire(self, tokens: int = 1) -> RateLimitResult:
        """Try to acquire tokens."""
        with self._lock:
            now = time.time()
            cutoff = now - self.window_seconds
            
            while self._requests and self._requests[0] < cutoff:
                self._requests.popleft()
            
            if len(self._requests) + tokens <= self.max_requests:
                for _ in range(tokens):
                    self._requests.append(now)
                return RateLimitResult(
                    allowed=True,
                    remaining=self.max_requests - len(self._requests),
                    reset_at=self._requests[0] + self.window_seconds if self._requests else now,
                    limit_type="sliding_window"
                )
            else:
                if self._requests:
                    oldest = self._requests[0]
                    wait_time = (oldest + self.window_seconds) - now
                else:
                    wait_time = self.window_seconds
                return RateLimitResult(
                    allowed=False,
                    remaining=0,
                    reset_at=now + wait_time,
                    retry_after=max(0, wait_time),
                    limit_type="sliding_window"
                )


class FixedWindowRateLimiter:
    """Fixed window rate limiter implementation."""
    
    def __init__(self, max_requests: int, window_seconds: float):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: Dict[int, int] = {}
        self._lock = threading.Lock()
    
    def _get_window_key(self, timestamp: float) -> int:
        """Get the window key for a timestamp."""
        return int(timestamp / self.window_seconds)
    
    def acquire(self, tokens: int = 1) -> RateLimitResult:
        """Try to acquire tokens."""
        with self._lock:
            now = time.time()
            current_key = self._get_window_key(now)
            
            current_count = self._requests.get(current_key, 0)
            current_window_start = current_key * self.window_seconds
            next_window_start = (current_key + 1) * self.window_seconds
            
            if current_count + tokens <= self.max_requests:
                self._requests[current_key] = current_count + tokens
                
                old_keys = [k for k in self._requests if k < current_key]
                for k in old_keys:
                    del self._requests[k]
                
                return RateLimitResult(
                    allowed=True,
                    remaining=self.max_requests - current_count - tokens,
                    reset_at=next_window_start,
                    limit_type="fixed_window"
                )
            else:
                wait_time = next_window_start - now
                return RateLimitResult(
                    allowed=False,
                    remaining=0,
                    reset_at=next_window_start,
                    retry_after=max(0, wait_time),
                    limit_type="fixed_window"
                )


class ApiRateLimiterAction(BaseAction):
    """API rate limiter action.
    
    Provides rate limiting for API requests using multiple
    configurable algorithms with per-endpoint tracking.
    """
    action_type = "api_rate_limiter"
    display_name = "API限流器"
    description = "API请求速率限制"
    
    def __init__(self):
        super().__init__()
        self._limiters: Dict[str, Any] = {}
        self._default_config = RateLimitConfig()
        self._lock = threading.Lock()
        self._stats: Dict[str, Dict[str, Any]] = {}
    
    def _create_limiter(self, config: RateLimitConfig, key: str) -> Any:
        """Create a rate limiter based on config."""
        if config.algorithm == RateLimitAlgorithm.TOKEN_BUCKET:
            return TokenBucketRateLimiter(config.requests_per_second, config.burst_size)
        elif config.algorithm == RateLimitAlgorithm.LEAKY_BUCKET:
            return LeakyBucketRateLimiter(config.requests_per_second, config.burst_size)
        elif config.algorithm == RateLimitAlgorithm.SLIDING_WINDOW:
            max_req = config.requests_per_minute or int(config.requests_per_second * 60)
            return SlidingWindowRateLimiter(max_req, 60.0)
        elif config.algorithm == RateLimitAlgorithm.FIXED_WINDOW:
            max_req = config.requests_per_minute or int(config.requests_per_second * 60)
            return FixedWindowRateLimiter(max_req, 60.0)
        else:
            return TokenBucketRateLimiter(config.requests_per_second, config.burst_size)
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute rate limit check for an API request.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - endpoint: The API endpoint identifier
                - config: Optional rate limit config
                - tokens: Number of tokens to acquire (default 1)
                - wait: Whether to wait if rate limited (default False)
                
        Returns:
            ActionResult with rate limit check results.
        """
        start_time = time.time()
        
        endpoint = params.get("endpoint", "default")
        tokens = params.get("tokens", 1)
        wait = params.get("wait", False)
        
        config_dict = params.get("config", {})
        algorithm = RateLimitAlgorithm(config_dict.get("algorithm", "token_bucket"))
        
        config = RateLimitConfig(
            requests_per_second=config_dict.get("requests_per_second", 10.0),
            burst_size=config_dict.get("burst_size", 20),
            requests_per_minute=config_dict.get("requests_per_minute"),
            requests_per_hour=config_dict.get("requests_per_hour"),
            requests_per_day=config_dict.get("requests_per_day"),
            algorithm=algorithm
        )
        
        with self._lock:
            limiter_key = f"{endpoint}:{algorithm.value}"
            
            if limiter_key not in self._limiters:
                self._limiters[limiter_key] = self._create_limiter(config, limiter_key)
            
            limiter = self._limiters[limiter_key]
        
        if wait:
            result = self._wait_for_token(limiter, tokens, start_time)
        else:
            result = limiter.acquire(tokens)
        
        self._record_stats(endpoint, result)
        
        return ActionResult(
            success=result.allowed,
            message="Rate limit check passed" if result.allowed else f"Rate limited, retry after {result.retry_after:.2f}s",
            data={
                "endpoint": endpoint,
                "allowed": result.allowed,
                "remaining": result.remaining,
                "reset_at": result.reset_at,
                "retry_after": result.retry_after,
                "limit_type": result.limit_type
            },
            duration=time.time() - start_time
        )
    
    def _wait_for_token(self, limiter: Any, tokens: int, start_time: float, max_wait: float = 60.0) -> RateLimitResult:
        """Wait for a token to become available."""
        deadline = start_time + max_wait
        while time.time() < deadline:
            result = limiter.acquire(tokens)
            if result.allowed:
                return result
            sleep_time = min(result.retry_after or 0.1, 0.5)
            time.sleep(sleep_time)
        
        result = limiter.acquire(tokens)
        return result
    
    def _record_stats(self, endpoint: str, result: RateLimitResult) -> None:
        """Record statistics for an endpoint."""
        if endpoint not in self._stats:
            self._stats[endpoint] = {
                "total_requests": 0,
                "allowed_requests": 0,
                "rejected_requests": 0,
                "total_wait_time": 0.0
            }
        
        stats = self._stats[endpoint]
        stats["total_requests"] += 1
        if result.allowed:
            stats["allowed_requests"] += 1
        else:
            stats["rejected_requests"] += 1
            if result.retry_after:
                stats["total_wait_time"] += result.retry_after
    
    def get_stats(self, endpoint: Optional[str] = None) -> Dict[str, Any]:
        """Get rate limiting statistics."""
        if endpoint:
            return self._stats.get(endpoint, {})
        return dict(self._stats)
    
    def reset_limiter(self, endpoint: str) -> bool:
        """Reset rate limiter for an endpoint."""
        with self._lock:
            keys_to_remove = [k for k in self._limiters if k.startswith(f"{endpoint}:")]
            for key in keys_to_remove:
                del self._limiters[key]
            if endpoint in self._stats:
                del self._stats[endpoint]
            return len(keys_to_remove) > 0
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate rate limiter parameters."""
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return []
