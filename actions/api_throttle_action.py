"""
API Throttle Action Module

Rate limiting with token bucket, sliding window, and leaky bucket algorithms.
Per-client throttling, burst handling, and quota management.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class ThrottleAlgorithm(Enum):
    """Rate limiting algorithms."""
    
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    LEAKY_BUCKET = "leaky_bucket"
    FIXED_WINDOW = "fixed_window"


@dataclass
class ThrottleConfig:
    """Configuration for rate limiting."""
    
    algorithm: ThrottleAlgorithm = ThrottleAlgorithm.TOKEN_BUCKET
    rate: float = 100
    burst: float = 200
    window_seconds: float = 60
    per_client: bool = True
    block_duration_seconds: float = 60
    quota: Optional[float] = None
    quota_window_hours: float = 24


@dataclass
class ThrottleResult:
    """Result of throttle check."""
    
    allowed: bool
    remaining: float
    reset_at: float
    retry_after: Optional[float] = None


@dataclass
class ClientState:
    """State for a single client."""
    
    client_id: str
    tokens: float = 0
    last_update: float = field(default_factory=time.time)
    requests_made: int = 0
    blocked_until: float = 0
    quota_used: float = 0
    quota_reset: float = 0


class TokenBucketThrottler:
    """Token bucket rate limiter."""
    
    def __init__(self, rate: float, burst: float):
        self.rate = rate
        self.burst = burst
    
    def allow(self, state: ClientState) -> ThrottleResult:
        """Check if request is allowed."""
        now = time.time()
        elapsed = now - state.last_update
        
        state.tokens = min(self.burst, state.tokens + elapsed * self.rate)
        state.last_update = now
        
        if state.tokens >= 1:
            state.tokens -= 1
            state.requests_made += 1
            return ThrottleResult(
                allowed=True,
                remaining=state.tokens,
                reset_at=now + self.burst / self.rate
            )
        
        retry_after = (1 - state.tokens) / self.rate
        return ThrottleResult(
            allowed=False,
            remaining=state.tokens,
            reset_at=now + self.burst / self.rate,
            retry_after=retry_after
        )


class SlidingWindowThrottler:
    """Sliding window rate limiter."""
    
    def __init__(self, rate: float, window_seconds: float):
        self.rate = rate
        self.window_seconds = window_seconds
        self._requests: Dict[str, List[float]] = defaultdict(list)
    
    def allow(self, state: ClientState) -> ThrottleResult:
        """Check if request is allowed."""
        now = time.time()
        window_start = now - self.window_seconds
        
        client_requests = self._requests[state.client_id]
        client_requests[:] = [t for t in client_requests if t > window_start]
        
        if len(client_requests) < self.rate:
            client_requests.append(now)
            state.requests_made += 1
            return ThrottleResult(
                allowed=True,
                remaining=self.rate - len(client_requests),
                reset_at=now + self.window_seconds
            )
        
        oldest = client_requests[0]
        retry_after = oldest + self.window_seconds - now
        
        return ThrottleResult(
            allowed=False,
            remaining=0,
            reset_at=now + self.window_seconds,
            retry_after=retry_after
        )


class LeakyBucketThrottler:
    """Leaky bucket rate limiter."""
    
    def __init__(self, rate: float, capacity: float):
        self.rate = rate
        self.capacity = capacity
        self._buckets: Dict[str, float] = defaultdict(float)
    
    def allow(self, state: ClientState) -> ThrottleResult:
        """Check if request is allowed."""
        now = time.time()
        elapsed = now - state.last_update
        
        self._buckets[state.client_id] = max(
            0,
            self._buckets[state.client_id] - elapsed * self.rate
        )
        
        if self._buckets[state.client_id] < self.capacity:
            self._buckets[state.client_id] += 1
            state.requests_made += 1
            return ThrottleResult(
                allowed=True,
                remaining=self.capacity - self._buckets[state.client_id],
                reset_at=now + self.capacity / self.rate
            )
        
        leak_time = 1 / self.rate
        return ThrottleResult(
            allowed=False,
            remaining=0,
            reset_at=now + self.capacity * leak_time,
            retry_after=leak_time
        )


class FixedWindowThrottler:
    """Fixed window rate limiter."""
    
    def __init__(self, rate: float, window_seconds: float):
        self.rate = rate
        self.window_seconds = window_seconds
        self._windows: Dict[str, tuple[float, int]] = {}
    
    def allow(self, state: ClientState) -> ThrottleResult:
        """Check if request is allowed."""
        now = time.time()
        window_key = int(now / self.window_seconds)
        window_start = window_key * self.window_seconds
        
        client_window = self._windows.get(state.client_id)
        
        if not client_window or client_window[0] != window_key:
            self._windows[state.client_id] = (window_key, 1)
            state.requests_made += 1
            return ThrottleResult(
                allowed=True,
                remaining=self.rate - 1,
                reset_at=window_start + self.window_seconds
            )
        
        count = client_window[1]
        
        if count < self.rate:
            self._windows[state.client_id] = (window_key, count + 1)
            state.requests_made += 1
            return ThrottleResult(
                allowed=True,
                remaining=self.rate - count - 1,
                reset_at=window_start + self.window_seconds
            )
        
        return ThrottleResult(
            allowed=False,
            remaining=0,
            reset_at=window_start + self.window_seconds,
            retry_after=self.window_seconds
        )


class APIThrottleAction:
    """
    Main API throttle action handler.
    
    Provides rate limiting with multiple algorithms,
    per-client tracking, and quota management.
    """
    
    def __init__(self, config: Optional[ThrottleConfig] = None):
        self.config = config or ThrottleConfig()
        self._throttler = self._create_throttler()
        self._client_states: Dict[str, ClientState] = {}
        self._lock = asyncio.Lock()
        self._middleware: List[Callable] = []
    
    def _create_throttler(self):
        """Create throttler based on algorithm."""
        if self.config.algorithm == ThrottleAlgorithm.TOKEN_BUCKET:
            return TokenBucketThrottler(self.config.rate, self.config.burst)
        elif self.config.algorithm == ThrottleAlgorithm.SLIDING_WINDOW:
            return SlidingWindowThrottler(self.config.rate, self.config.window_seconds)
        elif self.config.algorithm == ThrottleAlgorithm.LEAKY_BUCKET:
            return LeakyBucketThrottler(self.config.rate, self.config.burst)
        elif self.config.algorithm == ThrottleAlgorithm.FIXED_WINDOW:
            return FixedWindowThrottler(self.config.rate, self.config.window_seconds)
        return TokenBucketThrottler(self.config.rate, self.config.burst)
    
    def _get_client_id(self, request: Dict) -> str:
        """Extract client ID from request."""
        if not self.config.per_client:
            return "_global_"
        
        headers = request.get("headers", {})
        api_key = headers.get("X-API-Key") or headers.get("api_key")
        if api_key:
            return f"key:{api_key[:16]}"
        
        client_ip = headers.get("X-Forwarded-For", "").split(",")[0].strip()
        if not client_ip:
            client_ip = request.get("client_ip", "unknown")
        
        return f"ip:{client_ip}"
    
    async def _get_client_state(self, client_id: str) -> ClientState:
        """Get or create client state."""
        async with self._lock:
            if client_id not in self._client_states:
                self._client_states[client_id] = ClientState(client_id=client_id)
            return self._client_states[client_id]
    
    async def check(self, request: Dict) -> ThrottleResult:
        """Check if request is allowed."""
        client_id = self._get_client_id(request)
        state = await self._get_client_state(client_id)
        
        now = time.time()
        
        if state.blocked_until > now:
            return ThrottleResult(
                allowed=False,
                remaining=0,
                reset_at=state.blocked_until,
                retry_after=state.blocked_until - now
            )
        
        if self.config.quota:
            if state.quota_reset < now:
                state.quota_used = 0
                state.quota_reset = now + self.config.quota_window_hours * 3600
            
            if state.quota_used >= self.config.quota:
                return ThrottleResult(
                    allowed=False,
                    remaining=0,
                    reset_at=state.quota_reset,
                    retry_after=state.quota_reset - now
                )
        
        result = self._throttler.allow(state)
        
        if not result.allowed and self.config.block_duration_seconds > 0:
            state.blocked_until = now + self.config.block_duration_seconds
        
        if result.allowed and self.config.quota:
            state.quota_used += 1
        
        for mw in self._middleware:
            await mw(request, result)
        
        return result
    
    async def check_and_update(
        self,
        request: Dict,
        handler: Callable
    ) -> tuple[bool, Any, ThrottleResult]:
        """Check throttle and execute handler if allowed."""
        result = await self.check(request)
        
        if not result.allowed:
            return False, {"error": "Rate limit exceeded", "retry_after": result.retry_after}, result
        
        try:
            if asyncio.iscoroutinefunction(handler):
                response = await handler(request)
            else:
                response = handler(request)
            return True, response, result
        except Exception as e:
            return False, {"error": str(e)}, result
    
    async def reset_client(self, client_id: str) -> bool:
        """Reset throttle state for a client."""
        async with self._lock:
            if client_id in self._client_states:
                del self._client_states[client_id]
                return True
            return False
    
    async def get_client_status(self, client_id: str) -> Optional[Dict]:
        """Get throttle status for a client."""
        state = await self._get_client_state(client_id)
        return {
            "client_id": state.client_id,
            "requests_made": state.requests_made,
            "blocked": state.blocked_until > time.time(),
            "blocked_until": state.blocked_until if state.blocked_until > time.time() else None,
            "quota_used": state.quota_used,
            "quota_reset": state.quota_reset if state.quota_reset > time.time() else None
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get throttle statistics."""
        return {
            "algorithm": self.config.algorithm.value,
            "rate": self.config.rate,
            "burst": self.config.burst,
            "total_clients": len(self._client_states),
            "blocked_clients": sum(
                1 for s in self._client_states.values()
                if s.blocked_until > time.time()
            )
        }
    
    def add_middleware(self, func: Callable) -> None:
        """Add throttle middleware."""
        self._middleware.append(func)
