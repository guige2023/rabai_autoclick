"""Rate limit action module for RabAI AutoClick.

Provides rate limiting and throttling for API calls,
task execution, and resource access control.
"""

import sys
import os
import time
import threading
from typing import Any, Dict, List, Optional
from collections import deque
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class TokenBucket:
    """Token bucket for rate limiting."""
    capacity: int
    refill_rate: float  # tokens per second
    tokens: float
    last_refill: float
    lock: threading.Lock

    def consume(self, tokens: int = 1) -> bool:
        """Try to consume tokens. Returns True if allowed."""
        with self.lock:
            self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    def _refill(self):
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now

    def wait_time(self, tokens: int = 1) -> float:
        """Get wait time in seconds until tokens available."""
        with self.lock:
            self._refill()
            if self.tokens >= tokens:
                return 0
            needed = tokens - self.tokens
            return needed / self.refill_rate


class RateLimitAction(BaseAction):
    """Rate limit task execution or API calls.
    
    Token bucket implementation with configurable
    capacity and refill rate.
    """
    action_type = "rate_limit"
    display_name = "限流"
    description = "使用令牌桶算法限制执行速率"

    _buckets: Dict[str, TokenBucket] = {}
    _lock = threading.Lock()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Apply rate limit.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - bucket_id: str (identifier for this bucket)
                - capacity: int (max tokens, default 10)
                - refill_rate: float (tokens per second)
                - tokens_requested: int (tokens to consume)
                - wait: bool (block until available)
                - max_wait: float (max seconds to wait)
                - save_to_var: str
        
        Returns:
            ActionResult with rate limit result.
        """
        bucket_id = params.get('bucket_id', 'default')
        capacity = params.get('capacity', 10)
        refill_rate = params.get('refill_rate', 1.0)
        tokens_requested = params.get('tokens_requested', 1)
        wait = params.get('wait', False)
        max_wait = params.get('max_wait', 30.0)
        save_to_var = params.get('save_to_var', 'rate_limit')

        with self._lock:
            if bucket_id not in self._buckets:
                self._buckets[bucket_id] = TokenBucket(
                    capacity=capacity,
                    refill_rate=refill_rate,
                    tokens=float(capacity),
                    last_refill=time.time(),
                    lock=threading.Lock(),
                )
            bucket = self._buckets[bucket_id]

        if wait:
            # Block until tokens available
            wait_time = bucket.wait_time(tokens_requested)
            if wait_time > max_wait:
                return ActionResult(
                    success=False,
                    data={'wait_time': wait_time, 'allowed': False},
                    message=f"Rate limit: would need to wait {wait_time:.1f}s (max {max_wait}s)"
                )
            if wait_time > 0:
                time.sleep(wait_time)

        allowed = bucket.consume(tokens_requested)

        result = {
            'bucket_id': bucket_id,
            'allowed': allowed,
            'tokens_remaining': bucket.tokens,
            'capacity': bucket.capacity,
            'refill_rate': bucket.refill_rate,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=allowed,
            data=result,
            message=f"Rate limit {'allowed' if allowed else 'denied'}: {bucket.tokens:.1f}/{bucket.capacity} tokens"
        )


class SlidingWindowRateAction(BaseAction):
    """Sliding window rate limiter.
    
    Count actions in a sliding time window and
    enforce maximum per window.
    """
    action_type = "sliding_window_rate"
    display_name = "滑动窗口限流"
    description = "滑动窗口速率限制器"

    _windows: Dict[str, deque] = {}
    _lock = threading.Lock()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Apply sliding window rate limit.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - window_id: str
                - window_size: float (seconds)
                - max_calls: int (max calls per window)
                - save_to_var: str
        
        Returns:
            ActionResult with rate limit result.
        """
        window_id = params.get('window_id', 'default')
        window_size = params.get('window_size', 60.0)
        max_calls = params.get('max_calls', 60)
        save_to_var = params.get('save_to_var', 'sliding_rate')

        with self._lock:
            if window_id not in self._windows:
                self._windows[window_id] = deque()
            window = self._windows[window_id]

        now = time.time()
        cutoff = now - window_size

        # Remove expired entries
        while window and window[0] < cutoff:
            window.popleft()

        current_count = len(window)
        allowed = current_count < max_calls

        if allowed:
            window.append(now)

        result = {
            'window_id': window_id,
            'allowed': allowed,
            'current_count': current_count,
            'max_calls': max_calls,
            'window_size': window_size,
            'remaining': max(0, max_calls - current_count - (1 if allowed else 0)),
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=allowed,
            data=result,
            message=f"Sliding rate: {current_count}/{max_calls} in last {window_size}s {'allowed' if allowed else 'denied'}"
        )


class ThrottleAction(BaseAction):
    """Throttle repeated actions - only execute if enough
    time has passed since last execution.
    
    Debounce-style throttling for repeated events.
    """
    action_type = "throttle"
    display_name = "节流"
    description = "节流：限制重复执行频率"

    _last_exec: Dict[str, float] = {}
    _lock = threading.Lock()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Apply throttle.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - throttle_id: str
                - min_interval: float (seconds between executions)
                - save_to_var: str
        
        Returns:
            ActionResult with throttle result.
        """
        throttle_id = params.get('throttle_id', 'default')
        min_interval = params.get('min_interval', 1.0)
        save_to_var = params.get('save_to_var', 'throttle')

        with self._lock:
            last = self._last_exec.get(throttle_id, 0)

        now = time.time()
        elapsed = now - last
        can_execute = elapsed >= min_interval

        if can_execute:
            self._last_exec[throttle_id] = now

        result = {
            'throttle_id': throttle_id,
            'can_execute': can_execute,
            'elapsed_since_last': elapsed,
            'min_interval': min_interval,
            'next_allowed_in': max(0, min_interval - elapsed) if not can_execute else 0,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Throttle {'EXECUTED' if can_execute else f'wait {min_interval - elapsed:.2f}s'}"
        )


class BurstLimitAction(BaseAction):
    """Burst rate limiter - allow sudden spikes up to
    a maximum, then limit sustained rate.
    
    Implements a simple burst-aware limiter.
    """
    action_type = "burst_limit"
    display_name = "突发限流"
    description = "突发限流：允许短时突发但限制持续速率"

    _bursts: Dict[str, deque] = {}
    _lock = threading.Lock()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Apply burst rate limit.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - burst_id: str
                - burst_size: int (max burst size)
                - sustain_rate: float (max per second sustained)
                - time_window: float (sustain window in seconds)
                - save_to_var: str
        
        Returns:
            ActionResult with burst limit result.
        """
        burst_id = params.get('burst_id', 'default')
        burst_size = params.get('burst_size', 10)
        sustain_rate = params.get('sustain_rate', 1.0)
        time_window = params.get('time_window', 60.0)
        save_to_var = params.get('save_to_var', 'burst_limit')

        with self._lock:
            if burst_id not in self._bursts:
                self._bursts[burst_id] = deque()
            timestamps = self._bursts[burst_id]

        now = time.time()
        cutoff = now - time_window

        # Clean old entries
        while timestamps and timestamps[0] < cutoff:
            timestamps.popleft()

        current_count = len(timestamps)
        allowed = current_count < burst_size

        if allowed:
            timestamps.append(now)

        result = {
            'burst_id': burst_id,
            'allowed': allowed,
            'current_burst': current_count,
            'burst_size': burst_size,
            'sustain_rate': sustain_rate,
            'remaining_burst': max(0, burst_size - current_count - (1 if allowed else 0)),
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=allowed,
            data=result,
            message=f"Burst limit: {current_count}/{burst_size} burst {'allowed' if allowed else 'denied'}"
        )
