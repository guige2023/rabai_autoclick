"""Rate limiter action module for RabAI AutoClick.

Provides rate limiting with token bucket, sliding window,
fixed window, and leaky bucket algorithms.
"""

import time
import sys
import os
import threading
from typing import Any, Dict, List, Optional, Union
from collections import deque
from abc import ABC, abstractmethod

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RateLimiter(ABC):
    """Abstract base class for rate limiters."""
    
    @abstractmethod
    def allow_request(self) -> bool:
        """Check if a request should be allowed."""
        pass
    
    @abstractmethod
    def get_wait_time(self) -> float:
        """Get time to wait before next allowed request."""
        pass


class TokenBucketLimiter(RateLimiter):
    """Token bucket rate limiter implementation."""
    
    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_refill = time.time()
        self.lock = threading.Lock()
    
    def allow_request(self) -> bool:
        with self.lock:
            self._refill()
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False
    
    def get_wait_time(self) -> float:
        with self.lock:
            self._refill()
            if self.tokens >= 1:
                return 0
            return (1 - self.tokens) / self.refill_rate
    
    def _refill(self):
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + (elapsed * self.refill_rate))
        self.last_refill = now


class SlidingWindowLimiter(RateLimiter):
    """Sliding window rate limiter implementation."""
    
    def __init__(self, max_requests: int, window_seconds: float):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests = deque()
        self.lock = threading.Lock()
    
    def allow_request(self) -> bool:
        with self.lock:
            now = time.time()
            cutoff = now - self.window_seconds
            
            while self.requests and self.requests[0] < cutoff:
                self.requests.popleft()
            
            if len(self.requests) < self.max_requests:
                self.requests.append(now)
                return True
            return False
    
    def get_wait_time(self) -> float:
        with self.lock:
            now = time.time()
            cutoff = now - self.window_seconds
            
            while self.requests and self.requests[0] < cutoff:
                self.requests.popleft()
            
            if len(self.requests) < self.max_requests:
                return 0
            
            oldest = self.requests[0]
            return max(0, (oldest + self.window_seconds) - now)


class FixedWindowLimiter(RateLimiter):
    """Fixed window rate limiter implementation."""
    
    def __init__(self, max_requests: int, window_seconds: float):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.window_start = time.time()
        self.count = 0
        self.lock = threading.Lock()
    
    def allow_request(self) -> bool:
        with self.lock:
            now = time.time()
            if now - self.window_start >= self.window_seconds:
                self.window_start = now
                self.count = 0
            
            if self.count < self.max_requests:
                self.count += 1
                return True
            return False
    
    def get_wait_time(self) -> float:
        with self.lock:
            now = time.time()
            if now - self.window_start >= self.window_seconds:
                return 0
            return max(0, self.window_start + self.window_seconds - now)


class LeakyBucketLimiter(RateLimiter):
    """Leaky bucket rate limiter implementation."""
    
    def __init__(self, capacity: int, leak_rate: float):
        self.capacity = capacity
        self.leak_rate = leak_rate
        self.level = 0
        self.last_leak = time.time()
        self.lock = threading.Lock()
    
    def allow_request(self) -> bool:
        with self.lock:
            self._leak()
            if self.level < self.capacity:
                self.level += 1
                return True
            return False
    
    def get_wait_time(self) -> float:
        with self.lock:
            self._leak()
            if self.level < self.capacity:
                return 0
            return (self.level - self.capacity + 1) / self.leak_rate
    
    def _leak(self):
        now = time.time()
        elapsed = now - self.last_leak
        leaked = elapsed * self.leak_rate
        self.level = max(0, self.level - leaked)
        self.last_leak = now


class RateLimiterAction(BaseAction):
    """Rate limiting with multiple algorithm support.
    
    Supports token bucket, sliding window, fixed window,
    and leaky bucket algorithms.
    """
    action_type = "rate_limiter"
    display_name = "限流器"
    description = "流量限制，支持多种算法"
    
    _limiters: Dict[str, RateLimiter] = {}
    _lock = threading.Lock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute rate limiter operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: action (create, check, wait,
                   reset, info), limiter_config.
        
        Returns:
            ActionResult with operation result.
        """
        action = params.get('action', 'check')
        
        if action == 'create':
            return self._create_limiter(params)
        elif action == 'check':
            return self._check_request(params)
        elif action == 'wait':
            return self._wait_for_slot(params)
        elif action == 'reset':
            return self._reset_limiter(params)
        elif action == 'info':
            return self._get_limiter_info(params)
        elif action == 'list':
            return self._list_limiters()
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )
    
    def _create_limiter(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Create a new rate limiter."""
        name = params.get('name', 'default')
        algorithm = params.get('algorithm', 'token_bucket')
        capacity = params.get('capacity', 10)
        rate = params.get('rate', 1.0)
        window = params.get('window', 60.0)
        
        with self._lock:
            if name in self._limiters:
                return ActionResult(
                    success=False,
                    message=f"Limiter '{name}' already exists"
                )
            
            if algorithm == 'token_bucket':
                limiter = TokenBucketLimiter(capacity, rate)
            elif algorithm == 'sliding_window':
                limiter = SlidingWindowLimiter(capacity, window)
            elif algorithm == 'fixed_window':
                limiter = FixedWindowLimiter(capacity, window)
            elif algorithm == 'leaky_bucket':
                limiter = LeakyBucketLimiter(capacity, rate)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown algorithm: {algorithm}"
                )
            
            self._limiters[name] = limiter
        
        return ActionResult(
            success=True,
            message=f"Rate limiter '{name}' created ({algorithm})",
            data={
                'name': name,
                'algorithm': algorithm,
                'capacity': capacity,
                'rate': rate,
                'window': window
            }
        )
    
    def _check_request(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Check if a request is allowed."""
        name = params.get('name', 'default')
        
        with self._lock:
            if name not in self._limiters:
                return ActionResult(
                    success=False,
                    message=f"Limiter '{name}' not found"
                )
            limiter = self._limiters[name]
        
        allowed = limiter.allow_request()
        wait_time = limiter.get_wait_time() if not allowed else 0
        
        return ActionResult(
            success=allowed,
            message=f"Request {'allowed' if allowed else 'rejected'}",
            data={
                'allowed': allowed,
                'wait_time': wait_time,
                'limiter': name
            }
        )
    
    def _wait_for_slot(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Wait until a request is allowed."""
        name = params.get('name', 'default')
        max_wait = params.get('max_wait', 60.0)
        
        with self._lock:
            if name not in self._limiters:
                return ActionResult(
                    success=False,
                    message=f"Limiter '{name}' not found"
                )
            limiter = self._limiters[name]
        
        start_time = time.time()
        while True:
            if limiter.allow_request():
                elapsed = time.time() - start_time
                return ActionResult(
                    success=True,
                    message=f"Request allowed after {elapsed:.2f}s wait",
                    data={'wait_time': elapsed, 'limiter': name}
                )
            
            wait_time = limiter.get_wait_time()
            if wait_time > max_wait:
                return ActionResult(
                    success=False,
                    message=f"Timeout waiting for rate limit slot ({max_wait}s)",
                    data={'wait_time': wait_time, 'limiter': name}
                )
            
            time.sleep(min(wait_time, 0.1))
    
    def _reset_limiter(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Reset a rate limiter."""
        name = params.get('name')
        
        with self._lock:
            if name:
                if name not in self._limiters:
                    return ActionResult(
                        success=False,
                        message=f"Limiter '{name}' not found"
                    )
                del self._limiters[name]
                return ActionResult(
                    success=True,
                    message=f"Limiter '{name}' deleted"
                )
            else:
                self._limiters.clear()
                return ActionResult(
                    success=True,
                    message="All limiters deleted"
                )
    
    def _get_limiter_info(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get information about a rate limiter."""
        name = params.get('name', 'default')
        
        with self._lock:
            if name not in self._limiters:
                return ActionResult(
                    success=False,
                    message=f"Limiter '{name}' not found"
                )
            limiter = self._limiters[name]
        
        info = {
            'name': name,
            'wait_time': limiter.get_wait_time()
        }
        
        if isinstance(limiter, TokenBucketLimiter):
            info['algorithm'] = 'token_bucket'
            info['tokens'] = limiter.tokens
            info['capacity'] = limiter.capacity
            info['refill_rate'] = limiter.refill_rate
        elif isinstance(limiter, SlidingWindowLimiter):
            info['algorithm'] = 'sliding_window'
            info['requests_in_window'] = len(limiter.requests)
            info['max_requests'] = limiter.max_requests
            info['window_seconds'] = limiter.window_seconds
        elif isinstance(limiter, FixedWindowLimiter):
            info['algorithm'] = 'fixed_window'
            info['current_count'] = limiter.count
            info['max_requests'] = limiter.max_requests
        elif isinstance(limiter, LeakyBucketLimiter):
            info['algorithm'] = 'leaky_bucket'
            info['level'] = limiter.level
            info['capacity'] = limiter.capacity
            info['leak_rate'] = limiter.leak_rate
        
        return ActionResult(
            success=True,
            message=f"Limiter info for '{name}'",
            data=info
        )
    
    def _list_limiters(self) -> ActionResult:
        """List all rate limiters."""
        with self._lock:
            names = list(self._limiters.keys())
        
        return ActionResult(
            success=True,
            message=f"Found {len(names)} limiters",
            data={'limiters': names, 'count': len(names)}
        )
