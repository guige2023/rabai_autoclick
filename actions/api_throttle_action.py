"""API Throttle Action.

Rate limiting for API requests using token bucket, sliding window, and leaky bucket
algorithms with multi-key support and distributed throttle coordination.
"""

import sys
import os
import time
import threading
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict, deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiThrottleAction(BaseAction):
    """Rate limit API requests using configurable algorithms.
    
    Supports token bucket, sliding window, and leaky bucket algorithms
    with per-key limiting and thread-safe operations.
    """
    action_type = "api_throttle"
    display_name = "API限流"
    description = "API请求速率限制，支持令牌桶/滑动窗口/漏桶算法"

    def __init__(self):
        super().__init__()
        self._buckets: Dict[str, 'TokenBucket'] = {}
        self._windows: Dict[str, 'SlidingWindow'] = {}
        self._leaky: Dict[str, 'LeakyBucket'] = {}
        self._lock = threading.RLock()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute throttled API call or check throttle status.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - action: 'check', 'acquire', 'wait', or 'reset'.
                - key: Rate limit key (e.g., 'user:123' or 'api:endpoint').
                - algorithm: 'token_bucket', 'sliding_window', 'leaky_bucket'.
                - rate: Requests per interval.
                - interval: Interval in seconds.
                - burst: Burst capacity (for token bucket).
                - capacity: Max capacity (for leaky bucket).
                - save_to_var: Variable name for status result.
        
        Returns:
            ActionResult with throttle status.
        """
        try:
            action = params.get('action', 'check')
            key = params.get('key', 'default')
            algorithm = params.get('algorithm', 'token_bucket').lower()
            rate = params.get('rate', 10)
            interval = params.get('interval', 1.0)
            burst = params.get('burst', rate)
            capacity = params.get('capacity', rate)
            save_to_var = params.get('save_to_var', 'throttle_status')

            with self._lock:
                if action == 'reset':
                    self._reset_key(key, algorithm)
                    return ActionResult(success=True, message=f"Reset {key}/{algorithm}")

                if algorithm == 'token_bucket':
                    if key not in self._buckets:
                        self._buckets[key] = TokenBucket(rate, interval, burst)
                    bucket = self._buckets[key]
                    
                    if action == 'check':
                        allowed = bucket.try_acquire()
                        status = {'allowed': allowed, 'key': key, 'algorithm': algorithm}
                        context.set_variable(save_to_var, status)
                        return ActionResult(success=True, data=status)
                    
                    elif action == 'acquire':
                        acquired = bucket.acquire()
                        status = {'acquired': acquired, 'key': key, 'algorithm': algorithm}
                        context.set_variable(save_to_var, status)
                        return ActionResult(success=acquired, data=status)
                    
                    elif action == 'wait':
                        waited = bucket.wait_for_token()
                        status = {'waited': waited, 'key': key, 'algorithm': algorithm}
                        context.set_variable(save_to_var, status)
                        return ActionResult(success=True, data=status)

                elif algorithm == 'sliding_window':
                    if key not in self._windows:
                        self._windows[key] = SlidingWindow(rate, interval)
                    window = self._windows[key]
                    
                    if action == 'check':
                        allowed = window.try_add()
                        status = {'allowed': allowed, 'key': key, 'algorithm': algorithm}
                        context.set_variable(save_to_var, status)
                        return ActionResult(success=True, data=status)
                    
                    elif action == 'acquire':
                        acquired = window.add()
                        status = {'acquired': acquired, 'key': key, 'algorithm': algorithm}
                        context.set_variable(save_to_var, status)
                        return ActionResult(success=acquired, data=status)
                    
                    elif action == 'wait':
                        window.wait_for_slot()
                        status = {'waited': True, 'key': key, 'algorithm': algorithm}
                        context.set_variable(save_to_var, status)
                        return ActionResult(success=True, data=status)

                elif algorithm == 'leaky_bucket':
                    if key not in self._leaky:
                        self._leaky[key] = LeakyBucket(rate, interval, capacity)
                    leaky = self._leaky[key]
                    
                    if action == 'check':
                        allowed = leaky.try_add()
                        status = {'allowed': allowed, 'key': key, 'algorithm': algorithm}
                        context.set_variable(save_to_var, status)
                        return ActionResult(success=True, data=status)
                    
                    elif action == 'acquire':
                        acquired = leaky.add()
                        status = {'acquired': acquired, 'key': key, 'algorithm': algorithm}
                        context.set_variable(save_to_var, status)
                        return ActionResult(success=acquired, data=status)
                    
                    elif action == 'wait':
                        leaky.wait_for_drain()
                        status = {'waited': True, 'key': key, 'algorithm': algorithm}
                        context.set_variable(save_to_var, status)
                        return ActionResult(success=True, data=status)

                return ActionResult(success=False, message=f"Unknown algorithm: {algorithm}")

        except Exception as e:
            return ActionResult(success=False, message=f"Throttle error: {e}")

    def _reset_key(self, key: str, algorithm: str):
        """Reset throttle state for a key."""
        if algorithm == 'token_bucket' and key in self._buckets:
            del self._buckets[key]
        elif algorithm == 'sliding_window' and key in self._windows:
            del self._windows[key]
        elif algorithm == 'leaky_bucket' and key in self._leaky:
            del self._leaky[key]


class TokenBucket:
    """Token bucket rate limiter."""

    def __init__(self, rate: float, interval: float, burst: float):
        self.rate = rate
        self.interval = interval
        self.burst = burst
        self.tokens = burst
        self.last_update = time.time()
        self.lock = threading.Lock()

    def try_acquire(self) -> bool:
        """Check if request can be acquired without blocking."""
        with self.lock:
            self._refill()
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    def acquire(self, timeout: float = None) -> bool:
        """Try to acquire within timeout."""
        start = time.time()
        while True:
            with self.lock:
                self._refill()
                if self.tokens >= 1:
                    self.tokens -= 1
                    return True
            if timeout and (time.time() - start) >= timeout:
                return False
            time.sleep(0.01)

    def wait_for_token(self) -> bool:
        """Block until token is available."""
        while True:
            with self.lock:
                self._refill()
                if self.tokens >= 1:
                    self.tokens -= 1
                    return True
            time.sleep(0.05)

    def _refill(self):
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_update
        tokens_to_add = (elapsed / self.interval) * self.rate
        self.tokens = min(self.burst, self.tokens + tokens_to_add)
        self.last_update = now


class SlidingWindow:
    """Sliding window rate limiter."""

    def __init__(self, rate: float, window_size: float):
        self.rate = rate
        self.window_size = window_size
        self.requests = deque()
        self.lock = threading.Lock()

    def try_add(self) -> bool:
        """Check if request can be added without blocking."""
        with self.lock:
            self._cleanup()
            if len(self.requests) < self.rate:
                self.requests.append(time.time())
                return True
            return False

    def add(self, timeout: float = None) -> bool:
        """Try to add within timeout."""
        start = time.time()
        while True:
            with self.lock:
                self._cleanup()
                if len(self.requests) < self.rate:
                    self.requests.append(time.time())
                    return True
            if timeout and (time.time() - start) >= timeout:
                return False
            time.sleep(0.01)

    def wait_for_slot(self):
        """Block until slot is available."""
        while True:
            with self.lock:
                self._cleanup()
                if len(self.requests) < self.rate:
                    self.requests.append(time.time())
                    return
            time.sleep(0.05)

    def _cleanup(self):
        """Remove expired entries."""
        cutoff = time.time() - self.window_size
        while self.requests and self.requests[0] < cutoff:
            self.requests.popleft()


class LeakyBucket:
    """Leaky bucket rate limiter."""

    def __init__(self, rate: float, interval: float, capacity: float):
        self.rate = rate
        self.interval = interval
        self.capacity = capacity
        self.level = 0.0
        self.last_leak = time.time()
        self.lock = threading.Lock()

    def try_add(self) -> bool:
        """Check if request can be added without blocking."""
        with self.lock:
            self._leak()
            if self.level < self.capacity:
                self.level += 1
                return True
            return False

    def add(self, timeout: float = None) -> bool:
        """Try to add within timeout."""
        start = time.time()
        while True:
            with self.lock:
                self._leak()
                if self.level < self.capacity:
                    self.level += 1
                    return True
            if timeout and (time.time() - start) >= timeout:
                return False
            time.sleep(0.01)

    def wait_for_drain(self):
        """Block until bucket is drained enough."""
        while True:
            with self.lock:
                self._leak()
                if self.level < self.capacity:
                    self.level += 1
                    return
            time.sleep(0.05)

    def _leak(self):
        """Leak based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_leak
        leaked = (elapsed / self.interval) * self.rate
        self.level = max(0, self.level - leaked)
        self.last_leak = now
