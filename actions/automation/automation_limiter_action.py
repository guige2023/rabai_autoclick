"""Automation Limiter Action Module.

Provides rate limiting capabilities for automation workflows including
token bucket, leaky bucket, and sliding window algorithms.

Example:
    >>> from actions.automation.automation_limiter_action import AutomationLimiterAction
    >>> action = AutomationLimiterAction()
    >>> allowed = await action.try_acquire("task_1")
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
import threading


class LimiterType(Enum):
    """Rate limiter types."""
    TOKEN_BUCKET = "token_bucket"
    LEAKY_BUCKET = "leaky_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"


@dataclass
class LimiterConfig:
    """Configuration for rate limiting.
    
    Attributes:
        limiter_type: Type of rate limiter
        rate: Requests per interval
        interval: Interval in seconds
        burst_size: Maximum burst capacity
        block_duration: Duration to block when exceeded
    """
    limiter_type: LimiterType = LimiterType.TOKEN_BUCKET
    rate: float = 10.0
    interval: float = 1.0
    burst_size: float = 20.0
    block_duration: float = 60.0


@dataclass
class LimiterStats:
    """Rate limiter statistics.
    
    Attributes:
        total_requests: Total requests
        allowed: Allowed requests
        denied: Denied requests
        current_rate: Current request rate
    """
    total_requests: int
    allowed: int
    denied: int
    current_rate: float


class AutomationLimiterAction:
    """Rate limiter for automation tasks.
    
    Provides configurable rate limiting to prevent
    resource exhaustion and ensure fair task scheduling.
    
    Attributes:
        config: Limiter configuration
        _buckets: Client buckets
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[LimiterConfig] = None,
    ) -> None:
        """Initialize limiter action.
        
        Args:
            config: Limiter configuration
        """
        self.config = config or LimiterConfig()
        self._buckets: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._total_requests = 0
        self._allowed = 0
        self._denied = 0
    
    async def try_acquire(
        self,
        client_id: str,
        tokens: float = 1.0,
    ) -> bool:
        """Try to acquire tokens.
        
        Args:
            client_id: Client identifier
            tokens: Number of tokens to acquire
        
        Returns:
            True if acquired
        """
        self._total_requests += 1
        
        if self.config.limiter_type == LimiterType.TOKEN_BUCKET:
            result = self._acquire_token_bucket(client_id, tokens)
        elif self.config.limiter_type == LimiterType.LEAKY_BUCKET:
            result = self._acquire_leaky_bucket(client_id, tokens)
        elif self.config.limiter_type == LimiterType.SLIDING_WINDOW:
            result = self._acquire_sliding_window(client_id, tokens)
        else:
            result = self._acquire_fixed_window(client_id, tokens)
        
        if result:
            self._allowed += 1
        else:
            self._denied += 1
        
        return result
    
    def _acquire_token_bucket(
        self,
        client_id: str,
        tokens: float,
    ) -> bool:
        """Acquire from token bucket.
        
        Args:
            client_id: Client identifier
            tokens: Tokens to acquire
        
        Returns:
            True if acquired
        """
        current_time = time.time()
        
        with self._lock:
            if client_id not in self._buckets:
                self._buckets[client_id] = {
                    "tokens": self.config.burst_size,
                    "last_update": current_time,
                }
            
            bucket = self._buckets[client_id]
            
            elapsed = current_time - bucket["last_update"]
            refill = elapsed * (self.config.rate / self.config.interval)
            
            bucket["tokens"] = min(
                self.config.burst_size,
                bucket["tokens"] + refill,
            )
            bucket["last_update"] = current_time
            
            if bucket["tokens"] >= tokens:
                bucket["tokens"] -= tokens
                return True
            
            return False
    
    def _acquire_leaky_bucket(
        self,
        client_id: str,
        tokens: float,
    ) -> bool:
        """Acquire from leaky bucket.
        
        Args:
            client_id: Client identifier
            tokens: Tokens to add
        
        Returns:
            True if acquired
        """
        current_time = time.time()
        
        with self._lock:
            if client_id not in self._buckets:
                self._buckets[client_id] = {
                    "level": 0.0,
                    "last_update": current_time,
                }
            
            bucket = self._buckets[client_id]
            
            elapsed = current_time - bucket["last_update"]
            leak = elapsed * (self.config.rate / self.config.interval)
            
            bucket["level"] = max(0.0, bucket["level"] - leak)
            bucket["last_update"] = current_time
            
            if bucket["level"] + tokens <= self.config.burst_size:
                bucket["level"] += tokens
                return True
            
            return False
    
    def _acquire_sliding_window(
        self,
        client_id: str,
        tokens: float,
    ) -> bool:
        """Acquire from sliding window.
        
        Args:
            client_id: Client identifier
            tokens: Tokens to acquire
        
        Returns:
            True if acquired
        """
        current_time = time.time()
        window_start = current_time - self.config.interval
        
        with self._lock:
            if client_id not in self._buckets:
                self._buckets[client_id] = {"requests": []}
            
            bucket = self._buckets[client_id]
            
            bucket["requests"] = [
                t for t in bucket["requests"]
                if t > window_start
            ]
            
            current_count = len(bucket["requests"])
            
            if current_count + tokens <= self.config.rate:
                for _ in range(int(tokens)):
                    bucket["requests"].append(current_time)
                return True
            
            return False
    
    def _acquire_fixed_window(
        self,
        client_id: str,
        tokens: float,
    ) -> bool:
        """Acquire from fixed window.
        
        Args:
            client_id: Client identifier
            tokens: Tokens to acquire
        
        Returns:
            True if acquired
        """
        current_time = time.time()
        window_start = int(current_time / self.config.interval) * self.config.interval
        
        with self._lock:
            if client_id not in self._buckets:
                self._buckets[client_id] = {
                    "window_start": window_start,
                    "count": 0,
                }
            
            bucket = self._buckets[client_id]
            
            if bucket["window_start"] != window_start:
                bucket["window_start"] = window_start
                bucket["count"] = 0
            
            if bucket["count"] + tokens <= self.config.rate:
                bucket["count"] += tokens
                return True
            
            return False
    
    async def reset(self, client_id: str) -> None:
        """Reset limiter for client.
        
        Args:
            client_id: Client identifier
        """
        with self._lock:
            if client_id in self._buckets:
                del self._buckets[client_id]
    
    async def reset_all(self) -> None:
        """Reset all limiters."""
        with self._lock:
            self._buckets.clear()
    
    def get_stats(self) -> LimiterStats:
        """Get limiter statistics.
        
        Returns:
            LimiterStats
        """
        return LimiterStats(
            total_requests=self._total_requests,
            allowed=self._allowed,
            denied=self._denied,
            current_rate=self._allowed / max(1, self._total_requests) * 100,
        )
