"""Automation rate limiter action module for RabAI AutoClick.

Provides rate limiting for automation workflows:
- AutomationRateLimiter: Rate limit workflow executions
- TokenBucketLimiter: Token bucket rate limiting
- QuotaEnforcer: Enforce execution quotas
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
import time
import threading
import logging
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RateLimitStrategy(Enum):
    """Rate limiting strategies."""
    TOKEN_BUCKET = "token_bucket"
    LEAKY_BUCKET = "leaky_bucket"
    FIXED_WINDOW = "fixed_window"
    SLIDING_WINDOW = "sliding_window"


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    strategy: RateLimitStrategy = RateLimitStrategy.TOKEN_BUCKET
    rate: float = 60.0
    burst: float = 10.0
    window: float = 60.0
    block_on_limit: bool = True


class TokenBucketLimiter:
    """Token bucket rate limiter."""
    
    def __init__(self, rate: float, burst: float):
        self.rate = rate
        self.burst = burst
        self._tokens = burst
        self._last_update = time.time()
        self._lock = threading.Lock()
    
    def try_acquire(self, tokens: float = 1.0) -> Tuple[bool, float]:
        """Try to acquire tokens."""
        with self._lock:
            now = time.time()
            elapsed = now - self._last_update
            self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
            self._last_update = now
            
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True, 0.0
            
            wait_time = (tokens - self._tokens) / self.rate
            return False, wait_time


class AutomationRateLimiter:
    """Rate limiter for automation workflows."""
    
    def __init__(self, name: str, config: RateLimitConfig):
        self.name = name
        self.config = config
        self._limiters: Dict[str, TokenBucketLimiter] = {}
        self._lock = threading.Lock()
        self._stats = {"total_attempts": 0, "allowed": 0, "blocked": 0}
    
    def _get_limiter(self, key: str) -> TokenBucketLimiter:
        """Get or create limiter for key."""
        with self._lock:
            if key not in self._limiters:
                self._limiters[key] = TokenBucketLimiter(self.config.rate, self.config.burst)
            return self._limiters[key]
    
    def check(self, key: str = "default", tokens: float = 1.0) -> Tuple[bool, Optional[float]]:
        """Check if operation is allowed."""
        with self._lock:
            self._stats["total_attempts"] += 1
        
        limiter = self._get_limiter(key)
        allowed, wait_time = limiter.try_acquire(tokens)
        
        with self._lock:
            if allowed:
                self._stats["allowed"] += 1
            else:
                self._stats["blocked"] += 1
        
        if allowed:
            return True, None
        return False, wait_time
    
    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics."""
        with self._lock:
            return {
                "name": self.name,
                "tracked_keys": len(self._limiters),
                **{k: v for k, v in self._stats.items()},
            }


class AutomationRateLimiterAction(BaseAction):
    """Automation rate limiter action."""
    action_type = "automation_rate_limiter"
    display_name = "自动化限流器"
    description = "自动化工作流速率限制"
    
    def __init__(self):
        super().__init__()
        self._limiters: Dict[str, AutomationRateLimiter] = {}
        self._lock = threading.Lock()
    
    def _get_limiter(self, name: str, config: RateLimitConfig) -> AutomationRateLimiter:
        """Get or create rate limiter."""
        with self._lock:
            if name not in self._limiters:
                self._limiters[name] = AutomationRateLimiter(name, config)
            return self._limiters[name]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute rate limit operation."""
        try:
            name = params.get("name", "default")
            command = params.get("command", "check")
            
            config = RateLimitConfig(
                strategy=RateLimitStrategy[params.get("strategy", "token_bucket").upper()],
                rate=params.get("rate", 60.0),
                burst=params.get("burst", 10.0),
            )
            
            limiter = self._get_limiter(name, config)
            
            if command == "check":
                key = params.get("key", "default")
                tokens = params.get("tokens", 1.0)
                allowed, wait_time = limiter.check(key, tokens)
                
                if allowed:
                    return ActionResult(success=True, message="Allowed")
                return ActionResult(success=False, message=f"Rate limited, wait {wait_time:.2f}s", data={"wait_time": wait_time})
            
            elif command == "stats":
                stats = limiter.get_stats()
                return ActionResult(success=True, data={"stats": stats})
            
            elif command == "reset":
                with self._lock:
                    if name in self._limiters:
                        self._limiters[name]._limiters.clear()
                return ActionResult(success=True)
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"AutomationRateLimiterAction error: {str(e)}")
