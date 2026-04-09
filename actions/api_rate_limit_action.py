"""
API Rate Limiting Action Module.

Implements rate limiting for API requests using various strategies
including token bucket, sliding window, and fixed window algorithms.

Author: RabAI Team
"""

from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import time
import threading
from collections import defaultdict
from datetime import datetime, timedelta


class RateLimitStrategy(Enum):
    """Rate limiting strategies."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"
    LEAKY_BUCKET = "leaky_bucket"


class RateLimitExceeded(Exception):
    """Raised when rate limit is exceeded."""
    
    def __init__(self, retry_after: float, limit_type: str):
        self.retry_after = retry_after
        self.limit_type = limit_type
        super().__init__(f"Rate limit exceeded. Retry after {retry_after:.2f}s")


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    requests_per_second: float
    burst_size: int
    strategy: RateLimitStrategy = RateLimitStrategy.TOKEN_BUCKET


class TokenBucket:
    """
    Token bucket rate limiter implementation.
    
    Example:
        bucket = TokenBucket(rate=10.0, capacity=20)
        if bucket.allow():
            make_request()
        else:
            wait_time = bucket.get_wait_time()
    """
    
    def __init__(self, rate: float, capacity: int):
        self.rate = rate
        self.capacity = capacity
        self.tokens = float(capacity)
        self.last_update = time.monotonic()
        self._lock = threading.Lock()
    
    def allow(self, tokens: int = 1) -> bool:
        """Check if request is allowed."""
        with self._lock:
            self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False
    
    def get_wait_time(self, tokens: int = 1) -> float:
        """Get time to wait before request can be made."""
        with self._lock:
            self._refill()
            if self.tokens >= tokens:
                return 0.0
            return (tokens - self.tokens) / self.rate
    
    def _refill(self):
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self.last_update
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.last_update = now


class SlidingWindow:
    """
    Sliding window rate limiter.
    
    More accurate than fixed window but uses more memory.
    """
    
    def __init__(self, rate: float, window_size: float):
        self.rate = rate
        self.window_size = window_size
        self.requests: List[float] = []
        self._lock = threading.Lock()
    
    def allow(self) -> bool:
        """Check if request is allowed."""
        with self._lock:
            now = time.monotonic()
            cutoff = now - self.window_size
            
            # Remove old requests
            self.requests = [t for t in self.requests if t > cutoff]
            
            if len(self.requests) < self.rate:
                self.requests.append(now)
                return True
            return False
    
    def get_wait_time(self) -> float:
        """Get time to wait before request can be made."""
        with self._lock:
            now = time.monotonic()
            cutoff = now - self.window_size
            self.requests = [t for t in self.requests if t > cutoff]
            
            if len(self.requests) < self.rate:
                return 0.0
            
            oldest = min(self.requests)
            return (oldest + self.window_size) - now


class LeakyBucket:
    """
    Leaky bucket rate limiter.
    
    Smooths out burst traffic by processing at constant rate.
    """
    
    def __init__(self, rate: float, capacity: int):
        self.rate = rate
        self.capacity = capacity
        self.level = 0.0
        self.last_update = time.monotonic()
        self._lock = threading.Lock()
    
    def allow(self) -> bool:
        """Check if request is allowed."""
        with self._lock:
            self._process_leak()
            
            if self.level < self.capacity:
                self.level += 1
                return True
            return False
    
    def get_wait_time(self) -> float:
        """Get time to wait before request can be made."""
        with self._lock:
            self._process_leak()
            
            if self.level < self.capacity:
                return 0.0
            
            return (self.level - self.capacity + 1) / self.rate
    
    def _process_leak(self):
        """Process leaky bucket outflow."""
        now = time.monotonic()
        elapsed = now - self.last_update
        self.level = max(0, self.level - elapsed * self.rate)
        self.last_update = now


class FixedWindow:
    """Fixed window rate limiter."""
    
    def __init__(self, rate: float, window_size: float):
        self.rate = rate
        self.window_size = window_size
        self.counters: Dict[str, Tuple[float, int]] = {}
        self._lock = threading.Lock()
    
    def allow(self, key: str = "default") -> bool:
        """Check if request is allowed."""
        with self._lock:
            now = time.monotonic()
            window_start = int(now / self.window_size)
            
            if key not in self.counters or self.counters[key][0] != window_start:
                self.counters[key] = (window_start, 1)
                return True
            
            _, count = self.counters[key]
            if count < self.rate:
                self.counters[key] = (window_start, count + 1)
                return True
            
            return False
    
    def get_wait_time(self, key: str = "default") -> float:
        """Get time to wait before request can be made."""
        with self._lock:
            now = time.monotonic()
            window_start = int(now / self.window_size)
            window_end = (window_start + 1) * self.window_size
            
            if key not in self.counters or self.counters[key][0] != window_start:
                return 0.0
            
            _, count = self.counters[key]
            if count < self.rate:
                return 0.0
            
            return window_end - now


class RateLimiter:
    """
    Multi-client rate limiter with configurable strategies.
    
    Example:
        limiter = RateLimiter(strategy=RateLimitStrategy.TOKEN_BUCKET)
        limiter.add_client("api_client_1", rate=10, capacity=20)
        limiter.add_client("api_client_2", rate=5, capacity=10)
        
        if limiter.allow("api_client_1"):
            make_request()
    """
    
    def __init__(self, strategy: RateLimitStrategy = RateLimitStrategy.TOKEN_BUCKET):
        self.strategy = strategy
        self.limiters: Dict[str, Any] = {}
        self.global_limit: Optional[Any] = None
    
    def add_client(
        self,
        client_id: str,
        rate: float,
        capacity: int
    ) -> "RateLimiter":
        """Add a client with specific rate limit."""
        limiter = self._create_limiter(rate, capacity)
        self.limiters[client_id] = limiter
        return self
    
    def set_global_limit(self, rate: float, capacity: int) -> "RateLimiter":
        """Set global rate limit across all clients."""
        self.global_limit = self._create_limiter(rate, capacity)
        return self
    
    def allow(self, client_id: str) -> bool:
        """Check if request is allowed for client."""
        if client_id not in self.limiters:
            return True
        
        client_limiter = self.limiters[client_id]
        
        if not client_limiter.allow():
            return False
        
        if self.global_limit and not self.global_limit.allow():
            return False
        
        return True
    
    def get_wait_time(self, client_id: str) -> float:
        """Get wait time for client."""
        if client_id not in self.limiters:
            return 0.0
        
        client_wait = self.limiters[client_id].get_wait_time()
        
        if self.global_limit:
            global_wait = self.global_limit.get_wait_time()
            return max(client_wait, global_wait)
        
        return client_wait
    
    def _create_limiter(self, rate: float, capacity: int) -> Any:
        """Create limiter based on strategy."""
        if self.strategy == RateLimitStrategy.TOKEN_BUCKET:
            return TokenBucket(rate, capacity)
        elif self.strategy == RateLimitStrategy.SLIDING_WINDOW:
            return SlidingWindow(rate, 1.0)  # 1 second window
        elif self.strategy == RateLimitStrategy.LEAKY_BUCKET:
            return LeakyBucket(rate, capacity)
        elif self.strategy == RateLimitStrategy.FIXED_WINDOW:
            return FixedWindow(rate, 1.0)
        else:
            return TokenBucket(rate, capacity)
    
    def get_status(self) -> Dict[str, Any]:
        """Get rate limiter status."""
        return {
            "strategy": self.strategy.value,
            "clients": len(self.limiters),
            "has_global": self.global_limit is not None
        }


class BaseAction:
    """Base class for all actions."""
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Any:
        raise NotImplementedError


class APIRateLimitAction(BaseAction):
    """
    Rate limiting action for API requests.
    
    Parameters:
        strategy: Rate limiting strategy
        rate: Requests per second
        capacity: Bucket/window capacity
    
    Example:
        action = APIRateLimitAction()
        result = action.execute({"client_id": "user1"}, {
            "strategy": "token_bucket",
            "rate": 10.0,
            "capacity": 20
        })
    """
    
    _limiters: Dict[str, RateLimiter] = {}
    _lock = threading.Lock()
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute rate limiting check."""
        client_id = context.get("client_id", context.get("user_id", "default"))
        strategy_str = params.get("strategy", "token_bucket")
        rate = params.get("rate", 10.0)
        capacity = params.get("capacity", 20)
        
        strategy = RateLimitStrategy(strategy_str)
        
        with self._lock:
            if client_id not in self._limiters:
                self._limiters[client_id] = RateLimiter(strategy)
                self._limiters[client_id].add_client(client_id, rate, capacity)
            
            limiter = self._limiters[client_id]
        
        allowed = limiter.allow(client_id)
        wait_time = limiter.get_wait_time(client_id) if not allowed else 0
        
        return {
            "allowed": allowed,
            "wait_time": wait_time,
            "client_id": client_id,
            "strategy": strategy_str,
            "rate": rate,
            "capacity": capacity,
            "checked_at": datetime.now().isoformat()
        }
