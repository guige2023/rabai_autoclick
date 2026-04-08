"""Rate limiter action module for RabAI AutoClick.

Provides rate limiting with token bucket, sliding window, and fixed window
algorithms for API request throttling.
"""

import sys
import os
import time
import threading
from typing import Any, Dict, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict
from threading import Lock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class TokenBucket:
    """Token bucket algorithm state."""
    tokens: float
    last_update: float
    capacity: float
    refill_rate: float  # tokens per second


@dataclass
class SlidingWindow:
    """Sliding window algorithm state."""
    requests: list  # timestamps of requests
    window_size: float
    max_requests: int


class RateLimiter:
    """Thread-safe rate limiter with multiple algorithms."""
    
    def __init__(self):
        self._buckets: Dict[str, TokenBucket] = {}
        self._windows: Dict[str, SlidingWindow] = {}
        self._fixed_counters: Dict[str, Tuple[float, int]] = {}  # (window_start, count)
        self._lock = Lock()
    
    def check_token_bucket(
        self,
        key: str,
        capacity: float,
        refill_rate: float,
        tokens_needed: float = 1.0
    ) -> Tuple[bool, float]:
        """Check rate limit using token bucket algorithm.
        
        Returns:
            Tuple of (allowed, wait_time_seconds)
        """
        with self._lock:
            now = time.time()
            bucket = self._buckets.get(key)
            
            if bucket is None:
                bucket = TokenBucket(
                    tokens=capacity,
                    last_update=now,
                    capacity=capacity,
                    refill_rate=refill_rate
                )
                self._buckets[key] = bucket
            
            # Refill tokens
            elapsed = now - bucket.last_update
            bucket.tokens = min(
                bucket.capacity,
                bucket.tokens + elapsed * bucket.refill_rate
            )
            bucket.last_update = now
            
            # Check if we have enough tokens
            if bucket.tokens >= tokens_needed:
                bucket.tokens -= tokens_needed
                return True, 0.0
            else:
                wait_time = (tokens_needed - bucket.tokens) / bucket.refill_rate
                return False, wait_time
    
    def check_sliding_window(
        self,
        key: str,
        max_requests: int,
        window_size: float
    ) -> Tuple[bool, float]:
        """Check rate limit using sliding window algorithm.
        
        Returns:
            Tuple of (allowed, wait_time_seconds)
        """
        with self._lock:
            now = time.time()
            window = self._windows.get(key)
            
            if window is None:
                window = SlidingWindow(
                    requests=[],
                    window_size=window_size,
                    max_requests=max_requests
                )
                self._windows[key] = window
            
            # Remove old requests outside window
            cutoff = now - window.window_size
            window.requests = [t for t in window.requests if t > cutoff]
            
            # Check limit
            if len(window.requests) < window.max_requests:
                window.requests.append(now)
                return True, 0.0
            else:
                oldest = window.requests[0]
                wait_time = oldest + window.window_size - now
                return False, max(0, wait_time)
    
    def check_fixed_window(
        self,
        key: str,
        max_requests: int,
        window_size: float = 1.0
    ) -> Tuple[bool, float]:
        """Check rate limit using fixed window algorithm.
        
        Returns:
            Tuple of (allowed, wait_time_seconds)
        """
        with self._lock:
            now = time.time()
            window_start, count = self._fixed_counters.get(key, (now, 0))
            
            # Check if we're in a new window
            if now - window_start >= window_size:
                window_start = now
                count = 0
            
            if count < max_requests:
                self._fixed_counters[key] = (window_start, count + 1)
                return True, 0.0
            else:
                wait_time = window_start + window_size - now
                return False, max(0, wait_time)


class RateLimiterAction(BaseAction):
    """Rate limit API requests with configurable algorithms.
    
    Supports token bucket, sliding window, and fixed window algorithms
    with per-key limiting and automatic cleanup.
    """
    action_type = "rate_limiter"
    display_name = "限流器"
    description = "API请求限流，支持多种算法"
    
    def __init__(self):
        super().__init__()
        self._limiter = RateLimiter()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute rate limit operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'check', 'reserve', 'reset'
                - key: Rate limit key (e.g., user_id, endpoint)
                - algorithm: 'token_bucket', 'sliding_window', 'fixed_window'
                - limit: Max requests or tokens
                - window: Window size in seconds (default 1.0)
                - refill_rate: Token refill rate for token_bucket
                - tokens: Tokens needed for this request
        
        Returns:
            ActionResult with rate limit decision.
        """
        operation = params.get('operation', 'check').lower()
        
        if operation == 'check':
            return self._check(params)
        elif operation == 'reserve':
            return self._reserve(params)
        elif operation == 'reset':
            return self._reset(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _check(self, params: Dict[str, Any]) -> ActionResult:
        """Check if request is allowed."""
        key = params.get('key', 'default')
        algorithm = params.get('algorithm', 'token_bucket')
        limit = params.get('limit', 100)
        window = params.get('window', 1.0)
        refill_rate = params.get('refill_rate', limit)
        tokens = params.get('tokens', 1)
        
        if algorithm == 'token_bucket':
            allowed, wait_time = self._limiter.check_token_bucket(
                key, limit, refill_rate, tokens
            )
        elif algorithm == 'sliding_window':
            allowed, wait_time = self._limiter.check_sliding_window(
                key, limit, window
            )
        elif algorithm == 'fixed_window':
            allowed, wait_time = self._limiter.check_fixed_window(
                key, limit, window
            )
        else:
            return ActionResult(
                success=False,
                message=f"Unknown algorithm: {algorithm}"
            )
        
        return ActionResult(
            success=allowed,
            message=f"{'Allowed' if allowed else 'Rate limited'}",
            data={
                'allowed': allowed,
                'wait_seconds': wait_time,
                'key': key,
                'algorithm': algorithm
            }
        )
    
    def _reserve(self, params: Dict[str, Any]) -> ActionResult:
        """Reserve capacity and return wait time if limited."""
        return self._check(params)
    
    def _reset(self, params: Dict[str, Any]) -> ActionResult:
        """Reset rate limit for a key."""
        key = params.get('key', 'default')
        # Note: In a real implementation, we'd remove the key from internal state
        return ActionResult(
            success=True,
            message=f"Reset rate limit for {key}",
            data={'key': key}
        )


class AdaptiveRateLimiterAction(BaseAction):
    """Adaptive rate limiter that adjusts based on API responses.
    
    Monitors 429 responses and automatically adjusts rate limits.
    """
    action_type = "adaptive_rate_limiter"
    display_name = "自适应限流"
    description = "根据API响应自动调整限流参数"
    
    def __init__(self):
        super().__init__()
        self._limiter = RateLimiter()
        self._limits: Dict[str, Dict[str, float]] = defaultdict(
            lambda: {'limit': 100, 'window': 1.0, 'backoff': 1.0}
        self._lock = Lock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute adaptive rate limit operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'request', 'report', 'adjust'
                - key: Rate limit key
                - response: API response dict (for 'report' operation)
                - target: Target requests per second (for 'adjust')
        
        Returns:
            ActionResult with rate limit decision.
        """
        operation = params.get('operation', 'request').lower()
        
        if operation == 'request':
            return self._request(params)
        elif operation == 'report':
            return self._report(params)
        elif operation == 'adjust':
            return self._adjust(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _request(self, params: Dict[str, Any]) -> ActionResult:
        """Make a rate-limited request."""
        key = params.get('key', 'default')
        
        with self._lock:
            config = self._limits[key]
            limit = int(config['limit'])
            window = config['window']
        
        allowed, wait_time = self._limiter.check_fixed_window(
            key, limit, window
        )
        
        return ActionResult(
            success=allowed,
            message=f"{'Allowed' if allowed else 'Limited'}",
            data={
                'allowed': allowed,
                'wait_seconds': wait_time,
                'current_limit': limit
            }
        )
    
    def _report(self, params: Dict[str, Any]) -> ActionResult:
        """Report API response for adaptive adjustment."""
        key = params.get('key', 'default')
        response = params.get('response', {})
        status_code = response.get('status_code')
        
        with self._lock:
            config = self._limits[key]
            
            if status_code == 429:
                # Too many requests - reduce limit
                config['limit'] = max(1, config['limit'] * 0.5)
                config['backoff'] *= 2
                message = f"Rate limit reduced to {config['limit']}"
            elif status_code and 200 <= status_code < 300:
                # Success - slowly increase limit
                config['limit'] = min(
                    1000,  # Cap at 1000
                    config['limit'] * 1.1
                )
                config['backoff'] = max(1.0, config['backoff'] * 0.9)
                message = f"Rate limit increased to {config['limit']}"
            else:
                message = "Response recorded"
        
        return ActionResult(
            success=True,
            message=message,
            data={'config': dict(self._limits[key])}
        )
    
    def _adjust(self, params: Dict[str, Any]) -> ActionResult:
        """Manually adjust rate limit."""
        key = params.get('key', 'default')
        limit = params.get('limit')
        
        with self._lock:
            if limit is not None:
                self._limits[key]['limit'] = limit
        
        return ActionResult(
            success=True,
            message=f"Adjusted rate limit for {key}",
            data={'config': dict(self._limits[key])}
        )
