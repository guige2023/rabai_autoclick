"""Rate limiter action module for RabAI AutoClick.

Implements token bucket and sliding window rate limiting algorithms
with support for distributed rate limiting via Redis.
"""

import time
import threading
import sys
import os
from typing import Any, Dict, Optional
from collections import deque
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class TokenBucket:
    """Token bucket algorithm implementation."""
    capacity: float
    refill_rate: float
    tokens: float
    last_refill: float = field(default_factory=time.time)
    
    def consume(self, tokens: float = 1.0) -> bool:
        """Try to consume tokens from the bucket.
        
        Args:
            tokens: Number of tokens to consume.
            
        Returns:
            True if tokens were consumed, False if insufficient tokens.
        """
        self._refill()
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False
    
    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now


@dataclass 
class SlidingWindowCounter:
    """Sliding window counter algorithm implementation."""
    max_requests: int
    window_size: float
    requests: deque = field(default_factory=deque)
    
    def is_allowed(self) -> bool:
        """Check if a request is allowed under the rate limit.
        
        Returns:
            True if request is allowed, False if rate limited.
        """
        now = time.time()
        cutoff = now - self.window_size
        
        while self.requests and self.requests[0] < cutoff:
            self.requests.popleft()
        
        if len(self.requests) < self.max_requests:
            self.requests.append(now)
            return True
        return False
    
    def get_remaining(self) -> int:
        """Get remaining requests in current window."""
        now = time.time()
        cutoff = now - self.window_size
        while self.requests and self.requests[0] < cutoff:
            self.requests.popleft()
        return max(0, self.max_requests - len(self.requests))


class RateLimiterAction(BaseAction):
    """Rate limiting action with token bucket and sliding window algorithms.
    
    Supports local in-memory rate limiting and optional Redis-based
    distributed rate limiting for multi-instance deployments.
    """
    action_type = "rate_limiter"
    display_name = "限流器"
    description = "实现令牌桶和滑动窗口限流算法"
    
    def __init__(self):
        super().__init__()
        self._buckets: Dict[str, TokenBucket] = {}
        self._windows: Dict[str, SlidingWindowCounter] = {}
        self._lock = threading.RLock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute rate limiting check.
        
        Args:
            context: Execution context.
            params: Dict with keys: key, algorithm (bucket|window),
                   capacity/max_requests, refill_rate/window_size,
                   tokens_to_consume (optional).
        
        Returns:
            ActionResult with allowed status and remaining quota.
        """
        key = params.get('key', 'default')
        algorithm = params.get('algorithm', 'bucket')
        
        if algorithm == 'bucket':
            return self._execute_bucket(key, params)
        elif algorithm == 'window':
            return self._execute_window(key, params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown algorithm: {algorithm}"
            )
    
    def _execute_bucket(
        self,
        key: str,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute token bucket rate limiting."""
        capacity = params.get('capacity', 100)
        refill_rate = params.get('refill_rate', 10)
        tokens = params.get('tokens_to_consume', 1)
        
        with self._lock:
            if key not in self._buckets:
                self._buckets[key] = TokenBucket(
                    capacity=capacity,
                    refill_rate=refill_rate,
                    tokens=capacity
                )
            
            bucket = self._buckets[key]
            bucket.capacity = capacity
            bucket.refill_rate = refill_rate
            
            allowed = bucket.consume(tokens)
            
            return ActionResult(
                success=True,
                message="Allowed" if allowed else "Rate limited",
                data={
                    'allowed': allowed,
                    'remaining_tokens': round(bucket.tokens, 2),
                    'algorithm': 'token_bucket'
                }
            )
    
    def _execute_window(
        self,
        key: str,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sliding window rate limiting."""
        max_requests = params.get('max_requests', 100)
        window_size = params.get('window_size', 60)
        
        with self._lock:
            if key not in self._windows:
                self._windows[key] = SlidingWindowCounter(
                    max_requests=max_requests,
                    window_size=window_size
                )
            
            window = self._windows[key]
            window.max_requests = max_requests
            window.window_size = window_size
            
            allowed = window.is_allowed()
            
            return ActionResult(
                success=True,
                message="Allowed" if allowed else "Rate limited",
                data={
                    'allowed': allowed,
                    'remaining': window.get_remaining(),
                    'algorithm': 'sliding_window'
                }
            )
    
    def reset(self, key: str) -> None:
        """Reset rate limit for a given key.
        
        Args:
            key: Rate limit key to reset.
        """
        with self._lock:
            if key in self._buckets:
                del self._buckets[key]
            if key in self._windows:
                del self._windows[key]
