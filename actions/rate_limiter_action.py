"""Rate Limiter action module for RabAI AutoClick.

Provides rate limiting with token bucket, sliding window, and fixed
window algorithms. Supports distributed rate limits with storage backends.
"""

import sys
import os
import json
import time
import threading
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RateLimitAlgorithm(Enum):
    """Rate limiting algorithm types."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"
    LEAKY_BUCKET = "leaky_bucket"


@dataclass
class RateLimitConfig:
    """Configuration for a rate limit."""
    name: str
    algorithm: RateLimitAlgorithm = RateLimitAlgorithm.TOKEN_BUCKET
    requests_per_second: float = 10.0
    burst_size: int = 20
    block_duration_seconds: float = 60.0
    description: str = ""


@dataclass
class RateLimitResult:
    """Result of a rate limit check."""
    allowed: bool
    remaining: int
    reset_at: float
    retry_after: Optional[float] = None
    limit: int = 0


class TokenBucket:
    """Token bucket rate limiter implementation."""
    
    def __init__(self, rate: float, burst: int):
        self.rate = rate  # tokens per second
        self.burst = burst
        self.tokens = float(burst)
        self.last_update = time.time()
        self.lock = threading.Lock()
    
    def consume(self, tokens: int = 1) -> bool:
        """Attempt to consume tokens.
        
        Returns True if tokens were consumed, False if rate limited.
        """
        with self.lock:
            now = time.time()
            # Refill tokens based on elapsed time
            elapsed = now - self.last_update
            self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False
    
    def get_available(self) -> tuple[int, float]:
        """Get available tokens and time until next token."""
        with self.lock:
            now = time.time()
            elapsed = now - self.last_update
            current_tokens = min(self.burst, self.tokens + elapsed * self.rate)
            wait_time = 0.0
            if current_tokens < 1:
                wait_time = (1 - current_tokens) / self.rate
            return int(current_tokens), wait_time


class SlidingWindowCounter:
    """Sliding window counter rate limiter implementation."""
    
    def __init__(self, max_requests: int, window_seconds: float):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests: List[float] = []
        self.lock = threading.Lock()
    
    def is_allowed(self) -> bool:
        """Check if request is allowed under rate limit."""
        with self.lock:
            now = time.time()
            cutoff = now - self.window_seconds
            
            # Remove expired entries
            self.requests = [t for t in self.requests if t > cutoff]
            
            if len(self.requests) < self.max_requests:
                self.requests.append(now)
                return True
            return False
    
    def get_remaining(self) -> int:
        """Get remaining requests in current window."""
        with self.lock:
            now = time.time()
            cutoff = now - self.window_seconds
            active = [t for t in self.requests if t > cutoff]
            return max(0, self.max_requests - len(active))


class FixedWindowCounter:
    """Fixed window counter rate limiter implementation."""
    
    def __init__(self, max_requests: int, window_seconds: float):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.window_start = time.time()
        self.count = 0
        self.lock = threading.Lock()
    
    def is_allowed(self) -> bool:
        """Check if request is allowed under rate limit."""
        with self.lock:
            now = time.time()
            
            # Reset if window has expired
            if now - self.window_start >= self.window_seconds:
                self.window_start = now
                self.count = 0
            
            if self.count < self.max_requests:
                self.count += 1
                return True
            return False
    
    def get_remaining(self) -> int:
        """Get remaining requests in current window."""
        with self.lock:
            now = time.time()
            if now - self.window_start >= self.window_seconds:
                return self.max_requests
            return max(0, self.max_requests - self.count)


class LeakyBucket:
    """Leaky bucket rate limiter implementation."""
    
    def __init__(self, rate: float, capacity: int):
        self.rate = rate  # requests per second (leak rate)
        self.capacity = capacity
        self.level = 0.0
        self.last_update = time.time()
        self.lock = threading.Lock()
    
    def is_allowed(self) -> bool:
        """Check if request is allowed under rate limit."""
        with self.lock:
            now = time.time()
            # Leak based on elapsed time
            elapsed = now - self.last_update
            self.level = max(0, self.level - elapsed * self.rate)
            self.last_update = now
            
            if self.level < self.capacity:
                self.level += 1
                return True
            return False
    
    def get_wait_time(self) -> float:
        """Get time until bucket is empty enough for next request."""
        with self.lock:
            if self.level < self.capacity:
                return 0.0
            return (self.level - self.capacity + 1) / self.rate


class Blocklist:
    """Simple blocklist for rate limit violations."""
    
    def __init__(self):
        self._blocked: Dict[str, float] = {}  # key -> unblock_time
        self.lock = threading.Lock()
    
    def block(self, key: str, duration: float) -> None:
        """Block a key for specified duration."""
        with self.lock:
            self._blocked[key] = time.time() + duration
    
    def is_blocked(self, key: str) -> bool:
        """Check if a key is currently blocked."""
        with self.lock:
            if key in self._blocked:
                if time.time() >= self._blocked[key]:
                    del self._blocked[key]
                    return False
                return True
            return False
    
    def unblock(self, key: str) -> bool:
        """Unblock a key. Returns True if key was blocked."""
        with self.lock:
            if key in self._blocked:
                del self._blocked[key]
                return True
            return False
    
    def get_remaining_block_time(self, key: str) -> float:
        """Get remaining block time for a key."""
        with self.lock:
            if key in self._blocked:
                remaining = self._blocked[key] - time.time()
                return max(0, remaining)
            return 0.0


class RateLimiter:
    """Main rate limiter with multiple algorithm support."""
    
    def __init__(self, persistence_path: Optional[str] = None):
        self._configs: Dict[str, RateLimitConfig] = {}
        self._limiters: Dict[str, Any] = {}  # Algorithm-specific limiter
        self._blocklist = Blocklist()
        self._persistence_path = persistence_path
        self._client_limits: Dict[str, Dict[str, int]] = defaultdict(dict)  # client -> limit_name -> count
        self._load()
    
    def _load(self) -> None:
        """Load rate limit configs from persistence."""
        if self._persistence_path and os.path.exists(self._persistence_path):
            try:
                with open(self._persistence_path, 'r') as f:
                    data = json.load(f)
                    for name, config_data in data.get("configs", {}).items():
                        config = RateLimitConfig(**config_data)
                        self._configs[name] = config
                        self._init_limiter(config)
            except (json.JSONDecodeError, TypeError):
                pass
    
    def _persist(self) -> None:
        """Persist rate limit configs."""
        if self._persistence_path:
            try:
                with open(self._persistence_path, 'w') as f:
                    json.dump({
                        "configs": {name: vars(config) for name, config in self._configs.items()}
                    }, f, indent=2)
            except OSError:
                pass
    
    def _init_limiter(self, config: RateLimitConfig) -> None:
        """Initialize a rate limiter based on algorithm."""
        if config.algorithm == RateLimitAlgorithm.TOKEN_BUCKET:
            self._limiters[config.name] = TokenBucket(
                config.requests_per_second, config.burst_size
            )
        elif config.algorithm == RateLimitAlgorithm.SLIDING_WINDOW:
            self._limiters[config.name] = SlidingWindowCounter(
                int(config.requests_per_second * config.burst_size), 
                float(config.burst_size)
            )
        elif config.algorithm == RateLimitAlgorithm.FIXED_WINDOW:
            self._limiters[config.name] = FixedWindowCounter(
                int(config.requests_per_second * config.burst_size),
                float(config.burst_size)
            )
        elif config.algorithm == RateLimitAlgorithm.LEAKY_BUCKET:
            self._limiters[config.name] = LeakyBucket(
                config.requests_per_second, config.burst_size
            )
    
    def create_limit(self, config: RateLimitConfig) -> None:
        """Create a new rate limit configuration."""
        self._configs[config.name] = config
        self._init_limiter(config)
        self._persist()
    
    def remove_limit(self, name: str) -> bool:
        """Remove a rate limit configuration."""
        if name in self._configs:
            del self._configs[name]
            self._limiters.pop(name, None)
            self._persist()
            return True
        return False
    
    def check_rate_limit(self, limit_name: str,
                         client_id: Optional[str] = None,
                         tokens: int = 1) -> RateLimitResult:
        """Check if a request is allowed under rate limit.
        
        Args:
            limit_name: Name of the rate limit configuration.
            client_id: Optional client identifier for per-client limits.
            tokens: Number of tokens to consume.
        
        Returns:
            RateLimitResult with allowed status and metadata.
        """
        if limit_name not in self._configs:
            return RateLimitResult(
                allowed=False, remaining=0, reset_at=0,
                limit=0, retry_after=None
            )
        
        config = self._configs[limit_name]
        key = f"{client_id}:{limit_name}" if client_id else limit_name
        
        # Check blocklist
        if self._blocklist.is_blocked(key):
            retry_after = self._blocklist.get_remaining_block_time(key)
            return RateLimitResult(
                allowed=False, remaining=0, reset_at=time.time() + retry_after,
                limit=config.burst_size, retry_after=retry_after
            )
        
        limiter = self._limiters.get(limit_name)
        if not limiter:
            return RateLimitResult(allowed=True, remaining=config.burst_size, reset_at=0, limit=config.burst_size)
        
        # Check with appropriate algorithm
        if config.algorithm == RateLimitAlgorithm.TOKEN_BUCKET:
            allowed = limiter.consume(tokens)
            remaining, wait_time = limiter.get_available()
        elif config.algorithm in (RateLimitAlgorithm.SLIDING_WINDOW, 
                                   RateLimitAlgorithm.FIXED_WINDOW):
            allowed = limiter.is_allowed()
            remaining = limiter.get_remaining()
            wait_time = 1.0 / config.requests_per_second if not allowed else 0.0
        elif config.algorithm == RateLimitAlgorithm.LEAKY_BUCKET:
            allowed = limiter.is_allowed()
            remaining = config.burst_size - int(limiter.level)
            wait_time = limiter.get_wait_time() if not allowed else 0.0
        else:
            allowed = True
            remaining = config.burst_size
            wait_time = 0.0
        
        if not allowed:
            # Block the client
            self._blocklist.block(key, config.block_duration_seconds)
            return RateLimitResult(
                allowed=False,
                remaining=0,
                reset_at=time.time() + wait_time,
                limit=config.burst_size,
                retry_after=wait_time
            )
        
        return RateLimitResult(
            allowed=True,
            remaining=remaining,
            reset_at=time.time() + 1.0 / config.requests_per_second,
            limit=config.burst_size
        )
    
    def unblock(self, limit_name: str, client_id: Optional[str] = None) -> bool:
        """Unblock a client from a rate limit."""
        key = f"{client_id}:{limit_name}" if client_id else limit_name
        return self._blocklist.unblock(key)
    
    def get_limit_stats(self, limit_name: str) -> Dict[str, Any]:
        """Get statistics for a rate limit."""
        if limit_name not in self._configs:
            return {}
        config = self._configs[limit_name]
        return {
            "name": limit_name,
            "algorithm": config.algorithm.value,
            "requests_per_second": config.requests_per_second,
            "burst_size": config.burst_size,
            "block_duration_seconds": config.block_duration_seconds
        }


class RateLimiterAction(BaseAction):
    """Enforce rate limits on operations.
    
    Supports token bucket, sliding window, fixed window, and leaky
    bucket algorithms with automatic blocking on violations.
    """
    action_type = "rate_limiter"
    display_name = "限流器"
    description = "执行速率限制，支持多种限流算法"
    
    def __init__(self):
        super().__init__()
        self._limiter: Optional[RateLimiter] = None
    
    def _get_limiter(self, params: Dict[str, Any]) -> RateLimiter:
        """Get or create the rate limiter."""
        if self._limiter is None:
            persistence_path = params.get("persistence_path")
            self._limiter = RateLimiter(persistence_path)
        return self._limiter
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute rate limiter operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: "create_limit", "remove_limit", "check",
                  "unblock", "get_stats", "list_limits"
                - For create: name, algorithm, requests_per_second, burst_size
                - For check: limit_name, client_id, tokens
                - For unblock: limit_name, client_id
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get("operation", "")
        
        try:
            if operation == "create_limit":
                return self._create_limit(params)
            elif operation == "remove_limit":
                return self._remove_limit(params)
            elif operation == "check":
                return self._check_limit(params)
            elif operation == "unblock":
                return self._unblock(params)
            elif operation == "get_stats":
                return self._get_stats(params)
            elif operation == "list_limits":
                return self._list_limits(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Rate limiter error: {str(e)}")
    
    def _create_limit(self, params: Dict[str, Any]) -> ActionResult:
        """Create a new rate limit."""
        limiter = self._get_limiter(params)
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="limit name is required")
        
        config = RateLimitConfig(
            name=name,
            algorithm=RateLimitAlgorithm(params.get("algorithm", "token_bucket")),
            requests_per_second=params.get("requests_per_second", 10.0),
            burst_size=params.get("burst_size", 20),
            block_duration_seconds=params.get("block_duration_seconds", 60.0),
            description=params.get("description", "")
        )
        limiter.create_limit(config)
        return ActionResult(
            success=True,
            message=f"Rate limit '{name}' created",
            data={"name": name, "algorithm": config.algorithm.value}
        )
    
    def _remove_limit(self, params: Dict[str, Any]) -> ActionResult:
        """Remove a rate limit."""
        limiter = self._get_limiter(params)
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="limit name is required")
        
        removed = limiter.remove_limit(name)
        return ActionResult(
            success=removed,
            message=f"Rate limit '{name}' removed" if removed else f"Rate limit '{name}' not found"
        )
    
    def _check_limit(self, params: Dict[str, Any]) -> ActionResult:
        """Check rate limit for a request."""
        limiter = self._get_limiter(params)
        limit_name = params.get("limit_name", "")
        client_id = params.get("client_id")
        tokens = params.get("tokens", 1)
        
        if not limit_name:
            return ActionResult(success=False, message="limit_name is required")
        
        result = limiter.check_rate_limit(limit_name, client_id, tokens)
        return ActionResult(
            success=result.allowed,
            message="Request allowed" if result.allowed else f"Rate limited, retry after {result.retry_after:.2f}s",
            data={
                "allowed": result.allowed,
                "remaining": result.remaining,
                "limit": result.limit,
                "retry_after": result.retry_after,
                "reset_at": result.reset_at
            }
        )
    
    def _unblock(self, params: Dict[str, Any]) -> ActionResult:
        """Unblock a client from a rate limit."""
        limiter = self._get_limiter(params)
        limit_name = params.get("limit_name", "")
        client_id = params.get("client_id")
        
        if not limit_name:
            return ActionResult(success=False, message="limit_name is required")
        
        unblocked = limiter.unblock(limit_name, client_id)
        return ActionResult(
            success=True,
            message=f"Client unblocked from '{limit_name}'" if unblocked else f"Client was not blocked"
        )
    
    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get statistics for a rate limit."""
        limiter = self._get_limiter(params)
        limit_name = params.get("limit_name", "")
        
        if not limit_name:
            return ActionResult(success=False, message="limit_name is required")
        
        stats = limiter.get_limit_stats(limit_name)
        if not stats:
            return ActionResult(success=False, message=f"Rate limit '{limit_name}' not found")
        return ActionResult(success=True, message="Stats retrieved", data=stats)
    
    def _list_limits(self, params: Dict[str, Any]) -> ActionResult:
        """List all rate limits."""
        limiter = self._get_limiter(params)
        limits = list(limiter._configs.keys())
        return ActionResult(
            success=True,
            message=f"Found {len(limits)} rate limits",
            data={"limits": limits}
        )
