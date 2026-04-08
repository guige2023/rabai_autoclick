"""API rate limiter action module for RabAI AutoClick.

Provides rate limiting for API requests with support for
token bucket, sliding window, and fixed window algorithms.
"""

import time
import sys
import os
import threading
from typing import Any, Dict, Optional
from collections import deque
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RateLimitAlgorithm(Enum):
    """Rate limiting algorithms."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"


@dataclass
class RateLimitConfig:
    """Rate limit configuration."""
    requests_per_second: float = 10
    burst_size: int = 20
    window_size: float = 1.0


class ApiRateLimiterAction(BaseAction):
    """API rate limiter action for throttling requests.
    
    Supports token bucket, sliding window, and fixed window
    rate limiting algorithms.
    """
    action_type = "api_rate_limiter"
    display_name = "API限流器"
    description = "API请求限流"
    
    def __init__(self):
        super().__init__()
        self._limiters: Dict[str, 'RateLimiter'] = {}
        self._lock = threading.RLock()
    
    def get_limiter(self, name: str, algorithm: str = 'token_bucket') -> 'RateLimiter':
        """Get or create rate limiter."""
        key = f"{name}:{algorithm}"
        
        if key not in self._limiters:
            config = RateLimitConfig()
            if algorithm == 'token_bucket':
                self._limiters[key] = TokenBucketLimiter(config)
            elif algorithm == 'sliding_window':
                self._limiters[key] = SlidingWindowLimiter(config)
            elif algorithm == 'fixed_window':
                self._limiters[key] = FixedWindowLimiter(config)
            else:
                self._limiters[key] = TokenBucketLimiter(config)
        
        return self._limiters[key]
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute rate limiting operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                operation: check|acquire|reset
                name: Limiter name
                algorithm: token_bucket|sliding_window|fixed_window
                tokens: Tokens to acquire (default 1).
        
        Returns:
            ActionResult with rate limit result.
        """
        operation = params.get('operation', 'check')
        name = params.get('name', 'default')
        algorithm = params.get('algorithm', 'token_bucket')
        tokens = params.get('tokens', 1)
        
        limiter = self.get_limiter(name, algorithm)
        
        if params.get('requests_per_second'):
            limiter.config.requests_per_second = params['requests_per_second']
        if params.get('burst_size'):
            limiter.config.burst_size = params['burst_size']
        if params.get('window_size'):
            limiter.config.window_size = params['window_size']
        
        if operation == 'check':
            allowed = limiter.allow(tokens)
            return ActionResult(
                success=True,
                message=f"{name}: {'allowed' if allowed else 'blocked'}",
                data={
                    'allowed': allowed,
                    'remaining': limiter.remaining(),
                    'algorithm': algorithm
                }
            )
        elif operation == 'acquire':
            return self._acquire(name, algorithm, tokens)
        elif operation == 'reset':
            return self._reset(name, algorithm)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")
    
    def _acquire(self, name: str, algorithm: str, tokens: int) -> ActionResult:
        """Acquire tokens from limiter."""
        limiter = self.get_limiter(name, algorithm)
        allowed = limiter.allow(tokens)
        
        return ActionResult(
            success=allowed,
            message=f"{name}: {'acquired' if allowed else 'rate limited'}",
            data={
                'acquired': allowed,
                'remaining': limiter.remaining(),
                'algorithm': algorithm
            }
        )
    
    def _reset(self, name: str, algorithm: str) -> ActionResult:
        """Reset limiter."""
        key = f"{name}:{algorithm}"
        
        with self._lock:
            if key in self._limiters:
                del self._limiters[key]
        
        return ActionResult(
            success=True,
            message=f"Limiter {name} reset",
            data={'name': name, 'algorithm': algorithm}
        )


class RateLimiter:
    """Base rate limiter."""
    
    def __init__(self, config: RateLimitConfig):
        self.config = config
    
    def allow(self, tokens: int = 1) -> bool:
        raise NotImplementedError
    
    def remaining(self) -> int:
        raise NotImplementedError


class TokenBucketLimiter(RateLimiter):
    """Token bucket rate limiter."""
    
    def __init__(self, config: RateLimitConfig):
        super().__init__(config)
        self._lock = threading.Lock()
        self._tokens = float(config.burst_size)
        self._last_update = time.time()
    
    def allow(self, tokens: int = 1) -> bool:
        with self._lock:
            self._refill()
            
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False
    
    def _refill(self) -> None:
        now = time.time()
        elapsed = now - self._last_update
        self._tokens = min(
            self.config.burst_size,
            self._tokens + elapsed * self.config.requests_per_second
        )
        self._last_update = now
    
    def remaining(self) -> int:
        with self._lock:
            self._refill()
            return int(self._tokens)


class SlidingWindowLimiter(RateLimiter):
    """Sliding window rate limiter."""
    
    def __init__(self, config: RateLimitConfig):
        super().__init__(config)
        self._lock = threading.Lock()
        self._requests = deque()
        self._window_size = config.window_size
        self._max_requests = int(config.requests_per_second * config.window_size)
    
    def allow(self, tokens: int = 1) -> bool:
        with self._lock:
            now = time.time()
            cutoff = now - self._window_size
            
            while self._requests and self._requests[0] < cutoff:
                self._requests.popleft()
            
            if len(self._requests) + tokens <= self._max_requests:
                for _ in range(tokens):
                    self._requests.append(now)
                return True
            return False
    
    def remaining(self) -> int:
        with self._lock:
            now = time.time()
            cutoff = now - self._window_size
            
            while self._requests and self._requests[0] < cutoff:
                self._requests.popleft()
            
            return max(0, self._max_requests - len(self._requests))


class FixedWindowLimiter(RateLimiter):
    """Fixed window rate limiter."""
    
    def __init__(self, config: RateLimitConfig):
        super().__init__(config)
        self._lock = threading.Lock()
        self._window_size = config.window_size
        self._max_requests = int(config.requests_per_second * config.window_size)
        self._window_start = time.time()
        self._count = 0
    
    def allow(self, tokens: int = 1) -> bool:
        with self._lock:
            now = time.time()
            
            if now - self._window_start >= self._window_size:
                self._window_start = now
                self._count = 0
            
            if self._count + tokens <= self._max_requests:
                self._count += tokens
                return True
            return False
    
    def remaining(self) -> int:
        with self._lock:
            now = time.time()
            
            if now - self._window_start >= self._window_size:
                return self._max_requests
            
            return max(0, self._max_requests - self._count)
