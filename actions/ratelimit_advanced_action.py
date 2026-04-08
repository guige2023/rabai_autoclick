"""Rate limit advanced action module for RabAI AutoClick.

Provides adaptive and sophisticated rate limiting strategies
including sliding window log, token bucket with burst,
and priority rate limiting.
"""

import time
import threading
import sys
import os
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SlidingWindowLog:
    """Sliding window log rate limiter.
    
    Tracks exact request timestamps for precise rate limiting.
    """
    
    def __init__(self, max_requests: int, window_seconds: float):
        """Initialize sliding window log.
        
        Args:
            max_requests: Max requests allowed in window.
            window_seconds: Time window in seconds.
        """
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._log: deque = deque()
        self._lock = threading.Lock()
    
    def _clean_log(self) -> None:
        """Remove timestamps outside the window."""
        cutoff = time.time() - self.window_seconds
        while self._log and self._log[0] < cutoff:
            self._log.popleft()
    
    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        """Attempt to acquire a slot.
        
        Args:
            blocking: Whether to wait.
            timeout: Max seconds to wait.
        
        Returns:
            True if acquired.
        """
        start = time.time()
        
        while True:
            with self._lock:
                self._clean_log()
                
                if len(self._log) < self.max_requests:
                    self._log.append(time.time())
                    return True
                
                if not blocking:
                    return False
                
                if timeout > 0 and (time.time() - start) >= timeout:
                    return False
                
                sleep_time = min(0.05, self._log[0] + self.window_seconds - time.time())
                if timeout > 0:
                    sleep_time = min(sleep_time, timeout - (time.time() - start))
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
        
        return False
    
    def get_count(self) -> int:
        """Get current count in window."""
        with self._lock:
            self._clean_log()
            return len(self._log)
    
    def get_wait_time(self) -> float:
        """Get seconds until oldest request expires."""
        with self._lock:
            self._clean_log()
            if len(self._log) < self.max_requests:
                return 0.0
            return max(0.0, self._log[0] + self.window_seconds - time.time())


class TokenBucketBurst:
    """Token bucket with burst capability.
    
    Supports burst capacity up to max_tokens.
    """
    
    def __init__(self, rate: float, capacity: int, burst_multiplier: float = 2.0):
        """Initialize token bucket with burst.
        
        Args:
            rate: Tokens per second.
            capacity: Max token capacity.
            burst_multiplier: Maximum burst = capacity * multiplier.
        """
        self.rate = rate
        self.capacity = capacity
        self.burst_multiplier = burst_multiplier
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
        """Acquire tokens.
        
        Args:
            tokens: Number of tokens to acquire.
            blocking: Whether to wait.
            timeout: Max seconds to wait.
        
        Returns:
            True if acquired.
        """
        start = time.time()
        
        while True:
            with self._lock:
                self._refill()
                
                burst_capacity = int(self.capacity * self.burst_multiplier)
                available = min(self._tokens, burst_capacity)
                
                if available >= tokens:
                    self._tokens -= tokens
                    return True
                
                if not blocking:
                    return False
                
                if timeout > 0 and (time.time() - start) >= timeout:
                    return False
                
                wait_time = (tokens - available) / self.rate
                if timeout > 0:
                    wait_time = min(wait_time, timeout - (time.time() - start))
                
                if wait_time > 0:
                    time.sleep(min(wait_time, 0.1))
        
        return False
    
    def get_available(self) -> float:
        """Get available tokens."""
        with self._lock:
            self._refill()
            return self._tokens


class AdaptiveRateLimiter:
    """Adaptive rate limiter that adjusts based on success/failure.
    
    Decreases rate on failures, increases on successes.
    """
    
    def __init__(
        self,
        initial_rate: float,
        min_rate: float = 1.0,
        max_rate: float = 100.0,
        increase_factor: float = 1.1,
        decrease_factor: float = 0.5
    ):
        """Initialize adaptive rate limiter.
        
        Args:
            initial_rate: Starting rate.
            min_rate: Minimum allowed rate.
            max_rate: Maximum allowed rate.
            increase_factor: Multiplier on success.
            decrease_factor: Multiplier on failure.
        """
        self.rate = initial_rate
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.increase_factor = increase_factor
        self.decrease_factor = decrease_factor
        self._lock = threading.Lock()
        self._consecutive_successes = 0
        self._consecutive_failures = 0
    
    def on_success(self) -> None:
        """Called when a request succeeds."""
        with self._lock:
            self._consecutive_failures = 0
            self._consecutive_successes += 1
            
            if self._consecutive_successes >= 5:
                self.rate = min(self.max_rate, self.rate * self.increase_factor)
                self._consecutive_successes = 0
    
    def on_failure(self) -> None:
        """Called when a request fails."""
        with self._lock:
            self._consecutive_successes = 0
            self._consecutive_failures += 1
            
            if self._consecutive_failures >= 2:
                self.rate = max(self.min_rate, self.rate * self.decrease_factor)
                self._consecutive_failures = 0
    
    def get_rate(self) -> float:
        """Get current rate."""
        with self._lock:
            return self.rate


# Global limiters
_limiters: Dict[str, Any] = {}
_limiter_lock = threading.Lock()


class SlidingWindowLogAction(BaseAction):
    """Sliding window log rate limiter."""
    action_type = "sliding_window_log"
    display_name = "滑动窗口日志限流"
    description = "精确滑动窗口速率限制"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Acquire sliding window slot.
        
        Args:
            context: Execution context.
            params: Dict with keys: limiter_name, max_requests, window_seconds,
                   blocking, timeout.
        
        Returns:
            ActionResult with acquisition status.
        """
        limiter_name = params.get('limiter_name', 'default')
        max_requests = params.get('max_requests', 100)
        window_seconds = params.get('window_seconds', 60.0)
        blocking = params.get('blocking', True)
        timeout = params.get('timeout', -1.0)
        
        with _limiter_lock:
            if limiter_name not in _limiters:
                _limiters[limiter_name] = SlidingWindowLog(max_requests, window_seconds)
            limiter = _limiters[limiter_name]
        
        acquired = limiter.acquire(blocking=blocking, timeout=timeout)
        
        if acquired:
            return ActionResult(
                success=True,
                message=f"Acquired slot in {limiter_name}",
                data={"limiter": limiter_name, "current_count": limiter.get_count(), "max": max_requests}
            )
        else:
            return ActionResult(
                success=False,
                message=f"Rate limit exceeded for {limiter_name} (wait {limiter.get_wait_time():.2f}s)",
                data={"wait_time": round(limiter.get_wait_time(), 2)}
            )


class TokenBucketBurstAction(BaseAction):
    """Token bucket with burst capability."""
    action_type = "token_bucket_burst"
    display_name = "令牌桶突发"
    description = "支持突发流量的令牌桶"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Acquire burst tokens.
        
        Args:
            context: Execution context.
            params: Dict with keys: limiter_name, rate, capacity,
                   burst_multiplier, tokens, blocking, timeout.
        
        Returns:
            ActionResult with acquisition status.
        """
        limiter_name = params.get('limiter_name', 'default')
        rate = params.get('rate', 10.0)
        capacity = params.get('capacity', 10)
        burst_multiplier = params.get('burst_multiplier', 2.0)
        tokens = params.get('tokens', 1)
        blocking = params.get('blocking', True)
        timeout = params.get('timeout', -1.0)
        
        with _limiter_lock:
            if limiter_name not in _limiters:
                _limiters[limiter_name] = TokenBucketBurst(rate, capacity, burst_multiplier)
            limiter = _limiters[limiter_name]
        
        acquired = limiter.acquire(tokens=tokens, blocking=blocking, timeout=timeout)
        
        if acquired:
            return ActionResult(
                success=True,
                message=f"Acquired {tokens} tokens from {limiter_name}",
                data={"limiter": limiter_name, "available": limiter.get_available()}
            )
        else:
            return ActionResult(success=False, message=f"Failed to acquire tokens from {limiter_name}")


class AdaptiveRateLimitAction(BaseAction):
    """Adaptive rate limiter that adjusts dynamically."""
    action_type = "adaptive_rate_limit"
    display_name = "自适应限流"
    description = "根据成功率自动调整速率"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Adjust rate based on success/failure.
        
        Args:
            context: Execution context.
            params: Dict with keys: limiter_name, action (success/failure/get),
                   initial_rate.
        
        Returns:
            ActionResult with updated rate.
        """
        limiter_name = params.get('limiter_name', 'default')
        action = params.get('action', 'get')
        initial_rate = params.get('initial_rate', 10.0)
        
        with _limiter_lock:
            if limiter_name not in _limiters:
                _limiters[limiter_name] = AdaptiveRateLimiter(initial_rate)
            limiter = _limiters[limiter_name]
        
        if action == 'success':
            limiter.on_success()
            return ActionResult(success=True, message="Success recorded, rate adjusted", data={"rate": limiter.get_rate()})
        elif action == 'failure':
            limiter.on_failure()
            return ActionResult(success=True, message="Failure recorded, rate adjusted", data={"rate": limiter.get_rate()})
        else:
            return ActionResult(success=True, message="Current rate", data={"rate": limiter.get_rate()})


class RateLimitWaitAction(BaseAction):
    """Wait for rate limit availability."""
    action_type = "rate_limit_wait"
    display_name = "限流等待"
    description = "等待限流器可用"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Wait for rate limit.
        
        Args:
            context: Execution context.
            params: Dict with keys: limiter_name, max_wait.
        
        Returns:
            ActionResult with wait result.
        """
        limiter_name = params.get('limiter_name', 'default')
        max_wait = params.get('max_wait', 30.0)
        
        with _limiter_lock:
            if limiter_name not in _limiters:
                return ActionResult(success=True, message=f"Limiter {limiter_name} not found", data={"acquired": True, "waited": 0})
            limiter = _limiters[limiter_name]
        
        start_time = time.time()
        
        if hasattr(limiter, 'acquire'):
            acquired = limiter.acquire(blocking=True, timeout=max_wait)
            waited = time.time() - start_time
            
            if acquired:
                return ActionResult(success=True, message=f"Acquired after {waited:.2f}s", data={"acquired": True, "waited_seconds": round(waited, 2)})
            else:
                return ActionResult(success=False, message=f"Timeout after {waited:.2f}s", data={"acquired": False, "waited_seconds": round(waited, 2)})
        else:
            return ActionResult(success=True, message=f"Limiter type not supported for wait", data={"acquired": False})


class RateLimitResetAction(BaseAction):
    """Reset a rate limiter."""
    action_type = "rate_limit_reset"
    display_name = "限流重置"
    description = "重置限流器状态"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Reset limiter.
        
        Args:
            context: Execution context.
            params: Dict with keys: limiter_name.
        
        Returns:
            ActionResult with reset status.
        """
        limiter_name = params.get('limiter_name', 'default')
        
        with _limiter_lock:
            if limiter_name in _limiters:
                del _limiters[limiter_name]
                return ActionResult(success=True, message=f"Reset {limiter_name}")
            else:
                return ActionResult(success=True, message=f"Limiter {limiter_name} not found")
