"""Bucket action module for RabAI AutoClick.

Provides rate limiting and throttling actions using token bucket,
leaky bucket, and sliding window algorithms.
"""

import time
import threading
import sys
import os
from typing import Any, Dict, Optional
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TokenBucket:
    """Token bucket rate limiter implementation.
    
    Tokens are added at a constant rate up to max_capacity.
    Each acquire() call consumes one token.
    """
    
    def __init__(self, rate: float, capacity: int):
        """Initialize token bucket.
        
        Args:
            rate: Tokens added per second.
            capacity: Maximum token capacity.
        """
        self.rate = rate
        self.capacity = capacity
        self._tokens = float(capacity)
        self._last_update = time.time()
        self._lock = threading.Lock()
    
    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_update
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last_update = now
    
    def acquire(self, tokens: int = 1, blocking: bool = True, timeout: float = -1) -> bool:
        """Acquire tokens from the bucket.
        
        Args:
            tokens: Number of tokens to acquire.
            blocking: Whether to wait if insufficient tokens.
            timeout: Max seconds to wait (-1 = infinite).
        
        Returns:
            True if tokens acquired, False otherwise.
        """
        start = time.time()
        
        with self._lock:
            while True:
                self._refill()
                
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return True
                
                if not blocking:
                    return False
                
                if timeout > 0 and (time.time() - start) >= timeout:
                    return False
                
                wait_time = (tokens - self._tokens) / self.rate
                if timeout > 0:
                    wait_time = min(wait_time, timeout - (time.time() - start))
                
                if wait_time > 0:
                    time.sleep(min(wait_time, 0.1))
    
    def get_tokens(self) -> float:
        """Get current number of available tokens."""
        with self._lock:
            self._refill()
            return self._tokens


class SlidingWindow:
    """Sliding window rate limiter implementation.
    
    Tracks requests in a time window and enforces a maximum count.
    """
    
    def __init__(self, max_requests: int, window_seconds: float):
        """Initialize sliding window.
        
        Args:
            max_requests: Maximum requests allowed in window.
            window_seconds: Size of the time window in seconds.
        """
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: list = []
        self._lock = threading.Lock()
    
    def _clean_old(self) -> None:
        """Remove requests outside the current window."""
        cutoff = time.time() - self.window_seconds
        self._requests = [t for t in self._requests if t > cutoff]
    
    def acquire(self, count: int = 1, blocking: bool = True, timeout: float = -1) -> bool:
        """Attempt to record requests in the window.
        
        Args:
            count: Number of requests to record.
            blocking: Whether to wait if limit exceeded.
            timeout: Max seconds to wait.
        
        Returns:
            True if recorded, False otherwise.
        """
        start = time.time()
        
        with self._lock:
            while True:
                self._clean_old()
                
                if len(self._requests) + count <= self.max_requests:
                    self._requests.extend([time.time()] * count)
                    return True
                
                if not blocking:
                    return False
                
                if timeout > 0 and (time.time() - start) >= timeout:
                    return False
                
                oldest = self._requests[0] if self._requests else time.time()
                wait_time = (oldest + self.window_seconds) - time.time()
                
                if timeout > 0:
                    wait_time = min(wait_time, timeout - (time.time() - start))
                
                if wait_time > 0:
                    time.sleep(min(wait_time, 0.1))
    
    def get_count(self) -> int:
        """Get current request count in window."""
        with self._lock:
            self._clean_old()
            return len(self._requests)


# Global limiter storage
_limiters: Dict[str, Any] = {}
_limiter_lock = threading.Lock()


class RateLimitAcquireAction(BaseAction):
    """Acquire a rate limit token (token bucket algorithm).
    
    Blocks until token is available or timeout.
    """
    action_type = "rate_limit_acquire"
    display_name = "令牌桶获取"
    description = "从令牌桶获取速率限制令牌"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Acquire a rate limit token.
        
        Args:
            context: Execution context.
            params: Dict with keys: limiter_name, rate, capacity, 
                   tokens, blocking, timeout.
        
        Returns:
            ActionResult with acquisition status.
        """
        limiter_name = params.get('limiter_name', 'default')
        rate = params.get('rate', 10.0)
        capacity = params.get('capacity', 10)
        tokens = params.get('tokens', 1)
        blocking = params.get('blocking', True)
        timeout = params.get('timeout', -1.0)
        
        with _limiter_lock:
            if limiter_name not in _limiters:
                _limiters[limiter_name] = TokenBucket(rate, capacity)
            limiter = _limiters[limiter_name]
        
        acquired = limiter.acquire(tokens=tokens, blocking=blocking, timeout=timeout)
        
        if acquired:
            return ActionResult(
                success=True,
                message=f"Acquired {tokens} token(s) from {limiter_name}",
                data={"limiter": limiter_name, "tokens_acquired": tokens, "available": limiter.get_tokens()}
            )
        else:
            return ActionResult(
                success=False,
                message=f"Failed to acquire token from {limiter_name} (timeout)"
            )


class RateLimitTryAction(BaseAction):
    """Try to acquire rate limit token without blocking."""
    action_type = "rate_limit_try"
    display_name = "令牌桶尝试"
    description = "非阻塞尝试获取令牌"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Try to acquire a token without blocking.
        
        Args:
            context: Execution context.
            params: Dict with keys: limiter_name, tokens.
        
        Returns:
            ActionResult with acquisition result.
        """
        limiter_name = params.get('limiter_name', 'default')
        tokens = params.get('tokens', 1)
        
        with _limiter_lock:
            if limiter_name not in _limiters:
                return ActionResult(
                    success=True,
                    message=f"Limiter {limiter_name} not initialized",
                    data={"acquired": False, "reason": "not_initialized"}
                )
            limiter = _limiters[limiter_name]
        
        acquired = limiter.acquire(tokens=tokens, blocking=False)
        
        return ActionResult(
            success=True,
            message="Token acquired" if acquired else "No tokens available",
            data={"acquired": acquired, "available": limiter.get_tokens()}
        )


class SlidingWindowLimitAction(BaseAction):
    """Rate limit using sliding window algorithm."""
    action_type = "sliding_window_limit"
    display_name = "滑动窗口限流"
    description = "滑动窗口算法速率限制"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Acquire slots in sliding window.
        
        Args:
            context: Execution context.
            params: Dict with keys: limiter_name, max_requests, 
                   window_seconds, count, blocking, timeout.
        
        Returns:
            ActionResult with acquisition status.
        """
        limiter_name = params.get('limiter_name', 'default')
        max_requests = params.get('max_requests', 100)
        window_seconds = params.get('window_seconds', 60.0)
        count = params.get('count', 1)
        blocking = params.get('blocking', True)
        timeout = params.get('timeout', -1.0)
        
        with _limiter_lock:
            key = f"sw_{limiter_name}"
            if key not in _limiters:
                _limiters[key] = SlidingWindow(max_requests, window_seconds)
            limiter = _limiters[key]
        
        acquired = limiter.acquire(count=count, blocking=blocking, timeout=timeout)
        
        if acquired:
            return ActionResult(
                success=True,
                message=f"Acquired {count} slot(s) in {limiter_name}",
                data={"count": limiter.get_count(), "max": max_requests}
            )
        else:
            return ActionResult(
                success=False,
                message=f"Sliding window {limiter_name} limit exceeded (timeout)"
            )


class RateLimitStatusAction(BaseAction):
    """Get the current status of a rate limiter."""
    action_type = "rate_limit_status"
    display_name = "限流器状态"
    description = "查看限流器当前状态"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get rate limiter status.
        
        Args:
            context: Execution context.
            params: Dict with keys: limiter_name.
        
        Returns:
            ActionResult with limiter statistics.
        """
        limiter_name = params.get('limiter_name', 'default')
        
        with _limiter_lock:
            if limiter_name not in _limiters:
                return ActionResult(
                    success=True,
                    message=f"Limiter {limiter_name} not found",
                    data={"exists": False}
                )
            limiter = _limiters[limiter_name]
        
        if isinstance(limiter, TokenBucket):
            return ActionResult(
                success=True,
                message=f"Token bucket {limiter_name} status",
                data={
                    "type": "token_bucket",
                    "available_tokens": limiter.get_tokens(),
                    "capacity": limiter.capacity,
                    "rate": limiter.rate
                }
            )
        elif isinstance(limiter, SlidingWindow):
            return ActionResult(
                success=True,
                message=f"Sliding window {limiter_name} status",
                data={
                    "type": "sliding_window",
                    "current_count": limiter.get_count(),
                    "max_requests": limiter.max_requests,
                    "window_seconds": limiter.window_seconds
                }
            )
        else:
            return ActionResult(
                success=True,
                message=f"Unknown limiter type for {limiter_name}",
                data={"type": str(type(limiter))}
            )
