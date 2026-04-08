"""Rate limiter v2 action module for RabAI AutoClick.

Provides advanced rate limiting with adaptive algorithms,
distributed rate limiting support, and quota management.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional, Union, Callable
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AdaptiveRateLimiter:
    """Adaptive rate limiter that adjusts based on load."""
    
    def __init__(
        self,
        initial_rate: float,
        min_rate: float,
        max_rate: float,
        increase_factor: float = 1.1,
        decrease_factor: float = 0.9
    ):
        self.current_rate = initial_rate
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.increase_factor = increase_factor
        self.decrease_factor = decrease_factor
        self._lock = threading.Lock()
    
    def record_success(self):
        """Record successful request."""
        with self._lock:
            self.current_rate = min(
                self.current_rate * self.increase_factor,
                self.max_rate
            )
    
    def record_failure(self):
        """Record failed request."""
        with self._lock:
            self.current_rate = max(
                self.current_rate * self.decrease_factor,
                self.min_rate
            )
    
    def get_rate(self) -> float:
        """Get current rate."""
        with self._lock:
            return self.current_rate


class SlidingWindowCounter:
    """Sliding window rate limiter using counters."""
    
    def __init__(self, max_requests: int, window_seconds: float):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests = deque()
        self.total_requests = 0
        self._lock = threading.Lock()
    
    def allow_request(self) -> bool:
        """Check if request is allowed."""
        with self._lock:
            now = time.time()
            cutoff = now - self.window_seconds
            
            while self.requests and self.requests[0] < cutoff:
                self.requests.popleft()
            
            if len(self.requests) < self.max_requests:
                self.requests.append(now)
                self.total_requests += 1
                return True
            
            return False
    
    def get_remaining(self) -> int:
        """Get remaining requests in window."""
        with self._lock:
            now = time.time()
            cutoff = now - self.window_seconds
            
            while self.requests and self.requests[0] < cutoff:
                self.requests.popleft()
            
            return max(0, self.max_requests - len(self.requests))


class QuotaManager:
    """Manages quotas for different clients/endpoints."""
    
    def __init__(self):
        self._quotas: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
    
    def set_quota(
        self,
        key: str,
        max_requests: int,
        window_seconds: float,
        burst: int = 0
    ):
        """Set quota for a key."""
        with self._lock:
            self._quotas[key] = {
                'max_requests': max_requests,
                'window_seconds': window_seconds,
                'burst': burst,
                'used': 0,
                'window_start': time.time()
            }
    
    def check_quota(self, key: str) -> tuple:
        """Check if request is allowed under quota."""
        with self._lock:
            if key not in self._quotas:
                return True, 0
            
            quota = self._quotas[key]
            now = time.time()
            
            if now - quota['window_start'] >= quota['window_seconds']:
                quota['used'] = 0
                quota['window_start'] = now
            
            remaining = quota['max_requests'] - quota['used']
            
            if remaining > 0:
                quota['used'] += 1
                return True, remaining - 1
            
            return False, 0
    
    def get_quota_info(self, key: str) -> Optional[Dict[str, Any]]:
        """Get quota information for a key."""
        with self._lock:
            return self._quotas.get(key)


class RateLimiterV2Action(BaseAction):
    """Advanced rate limiting with adaptive algorithms.
    
    Supports sliding window, token bucket, quota management,
    and adaptive rate limiting.
    """
    action_type = "rate_limiter_v2"
    display_name = "高级限流器"
    description = "高级流量限制，自适应算法和配额管理"
    
    def __init__(self):
        super().__init__()
        self._limiters: Dict[str, SlidingWindowCounter] = {}
        self._adaptive_limiters: Dict[str, AdaptiveRateLimiter] = {}
        self._quota_manager = QuotaManager()
        self._lock = threading.Lock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute rate limiter operations."""
        action = params.get('action', 'check')
        
        if action == 'create':
            return self._create_limiter(params)
        elif action == 'check':
            return self._check_request(params)
        elif action == 'set_quota':
            return self._set_quota(params)
        elif action == 'check_quota':
            return self._check_quota(params)
        elif action == 'quota_info':
            return self._get_quota_info(params)
        elif action == 'adaptive':
            return self._adaptive_rate(params)
        elif action == 'stats':
            return self._get_stats(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )
    
    def _create_limiter(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Create a sliding window limiter."""
        name = params.get('name', 'default')
        max_requests = params.get('max_requests', 100)
        window_seconds = params.get('window_seconds', 60.0)
        
        with self._lock:
            self._limiters[name] = SlidingWindowCounter(
                max_requests=max_requests,
                window_seconds=window_seconds
            )
        
        return ActionResult(
            success=True,
            message=f"Created limiter '{name}'",
            data={
                'name': name,
                'max_requests': max_requests,
                'window_seconds': window_seconds
            }
        )
    
    def _check_request(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Check if request is allowed."""
        name = params.get('name', 'default')
        
        with self._lock:
            if name not in self._limiters:
                return ActionResult(
                    success=False,
                    message=f"Limiter '{name}' not found"
                )
            limiter = self._limiters[name]
        
        allowed = limiter.allow_request()
        remaining = limiter.get_remaining()
        
        return ActionResult(
            success=allowed,
            message=f"Request {'allowed' if allowed else 'rejected'}",
            data={
                'allowed': allowed,
                'remaining': remaining,
                'limiter': name
            }
        )
    
    def _set_quota(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Set quota for a key."""
        key = params.get('key')
        max_requests = params.get('max_requests', 100)
        window_seconds = params.get('window_seconds', 3600)
        burst = params.get('burst', 0)
        
        self._quota_manager.set_quota(
            key=key,
            max_requests=max_requests,
            window_seconds=window_seconds,
            burst=burst
        )
        
        return ActionResult(
            success=True,
            message=f"Set quota for '{key}'",
            data={
                'key': key,
                'max_requests': max_requests,
                'window_seconds': window_seconds,
                'burst': burst
            }
        )
    
    def _check_quota(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Check quota for a key."""
        key = params.get('key')
        
        if not key:
            return ActionResult(success=False, message="key is required")
        
        allowed, remaining = self._quota_manager.check_quota(key)
        
        return ActionResult(
            success=allowed,
            message=f"Quota check: {'allowed' if allowed else 'rejected'}",
            data={
                'allowed': allowed,
                'remaining': remaining,
                'key': key
            }
        )
    
    def _get_quota_info(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get quota information."""
        key = params.get('key')
        
        if not key:
            return ActionResult(success=False, message="key is required")
        
        info = self._quota_manager.get_quota_info(key)
        
        if info:
            return ActionResult(
                success=True,
                message=f"Quota info for '{key}'",
                data={'quota': info}
            )
        else:
            return ActionResult(
                success=False,
                message=f"No quota found for '{key}'"
            )
    
    def _adaptive_rate(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Adaptive rate limiting."""
        name = params.get('name', 'default')
        operation = params.get('operation', 'get')
        
        with self._lock:
            if operation == 'create':
                initial = params.get('initial_rate', 100)
                min_rate = params.get('min_rate', 10)
                max_rate = params.get('max_rate', 1000)
                
                self._adaptive_limiters[name] = AdaptiveRateLimiter(
                    initial_rate=initial,
                    min_rate=min_rate,
                    max_rate=max_rate
                )
                
                return ActionResult(
                    success=True,
                    message=f"Created adaptive limiter '{name}'",
                    data={'rate': initial}
                )
            
            if name not in self._adaptive_limiters:
                return ActionResult(
                    success=False,
                    message=f"Adaptive limiter '{name}' not found"
                )
            
            limiter = self._adaptive_limiters[name]
            
            if operation == 'success':
                limiter.record_success()
                return ActionResult(
                    success=True,
                    message="Recorded success",
                    data={'rate': limiter.get_rate()}
                )
            elif operation == 'failure':
                limiter.record_failure()
                return ActionResult(
                    success=True,
                    message="Recorded failure",
                    data={'rate': limiter.get_rate()}
                )
            elif operation == 'get':
                return ActionResult(
                    success=True,
                    message=f"Current rate: {limiter.get_rate()}",
                    data={'rate': limiter.get_rate()}
                )
        
        return ActionResult(
            success=False,
            message=f"Unknown operation: {operation}"
        )
    
    def _get_stats(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get rate limiter statistics."""
        with self._lock:
            limiters_info = {}
            for name, limiter in self._limiters.items():
                limiters_info[name] = {
                    'remaining': limiter.get_remaining(),
                    'max_requests': limiter.max_requests,
                    'window_seconds': limiter.window_seconds
                }
            
            adaptive_info = {}
            for name, limiter in self._adaptive_limiters.items():
                adaptive_info[name] = {
                    'current_rate': limiter.get_rate(),
                    'min_rate': limiter.min_rate,
                    'max_rate': limiter.max_rate
                }
        
        return ActionResult(
            success=True,
            message="Rate limiter stats",
            data={
                'sliding_window': limiters_info,
                'adaptive': adaptive_info,
                'limiter_count': len(self._limiters),
                'adaptive_count': len(self._adaptive_limiters)
            }
        )
