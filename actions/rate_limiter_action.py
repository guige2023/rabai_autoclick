"""Rate limiter action module for RabAI AutoClick.

Provides rate limiting mechanisms:
- TokenBucket: Token bucket rate limiter
- SlidingWindow: Sliding window rate limiter
- LeakyBucket: Leaky bucket rate limiter
- FixedWindow: Fixed window counter
- AdaptiveRateLimiter: Adaptive rate limiting
- DistributedRateLimiter: Distributed rate limiting
"""

import time
import threading
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass
from collections import deque
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RateLimitStrategy(Enum):
    """Rate limiting strategies."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    LEAKY_BUCKET = "leaky_bucket"
    FIXED_WINDOW = "fixed_window"


@dataclass
class RateLimitResult:
    """Result of rate limit check."""
    allowed: bool
    current_rate: float
    remaining: int
    reset_at: float
    retry_after: Optional[float] = None


@dataclass
class RateLimitConfig:
    """Configuration for rate limiter."""
    rate: float  # requests per second
    burst: int = 1  # burst capacity
    strategy: RateLimitStrategy = RateLimitStrategy.TOKEN_BUCKET


class TokenBucket:
    """Token bucket rate limiter."""

    def __init__(self, rate: float, burst: int = 1):
        self.rate = rate
        self.burst = burst
        self.tokens = float(burst)
        self.last_update = time.time()
        self._lock = threading.Lock()

    def allow(self, tokens: int = 1) -> RateLimitResult:
        """Check if request is allowed."""
        with self._lock:
            now = time.time()
            elapsed = now - self.last_update
            self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
            self.last_update = now

            if self.tokens >= tokens:
                self.tokens -= tokens
                return RateLimitResult(
                    allowed=True,
                    current_rate=self.rate,
                    remaining=int(self.tokens),
                    reset_at=now,
                )
            else:
                retry_after = (tokens - self.tokens) / self.rate
                return RateLimitResult(
                    allowed=False,
                    current_rate=self.rate,
                    remaining=int(self.tokens),
                    reset_at=now + retry_after,
                    retry_after=retry_after,
                )

    def reset(self):
        """Reset the bucket."""
        with self._lock:
            self.tokens = float(self.burst)
            self.last_update = time.time()


class SlidingWindow:
    """Sliding window rate limiter."""

    def __init__(self, rate: float, window_size: float = 60.0):
        self.rate = rate
        self.window_size = window_size
        self.requests: deque = deque()
        self._lock = threading.Lock()

    def allow(self, tokens: int = 1) -> RateLimitResult:
        """Check if request is allowed."""
        with self._lock:
            now = time.time()
            cutoff = now - self.window_size

            while self.requests and self.requests[0] < cutoff:
                self.requests.popleft()

            current_count = len(self.requests)

            if current_count < self.rate * self.window_size:
                for _ in range(tokens):
                    self.requests.append(now)
                return RateLimitResult(
                    allowed=True,
                    current_rate=self.rate,
                    remaining=int(self.rate * self.window_size - current_count - tokens),
                    reset_at=now,
                )
            else:
                oldest = self.requests[0] if self.requests else now
                retry_after = oldest + self.window_size - now
                return RateLimitResult(
                    allowed=False,
                    current_rate=self.rate,
                    remaining=0,
                    reset_at=now + retry_after,
                    retry_after=retry_after,
                )

    def reset(self):
        """Reset the window."""
        with self._lock:
            self.requests.clear()


class LeakyBucket:
    """Leaky bucket rate limiter."""

    def __init__(self, rate: float, capacity: int):
        self.rate = rate
        self.capacity = capacity
        self.level = 0.0
        self.last_update = time.time()
        self._lock = threading.Lock()

    def allow(self, tokens: int = 1) -> RateLimitResult:
        """Check if request is allowed."""
        with self._lock:
            now = time.time()
            elapsed = now - self.last_update
            self.level = max(0, self.level - elapsed * self.rate)
            self.last_update = now

            if self.level + tokens <= self.capacity:
                self.level += tokens
                return RateLimitResult(
                    allowed=True,
                    current_rate=self.rate,
                    remaining=int(self.capacity - self.level),
                    reset_at=now,
                )
            else:
                retry_after = (self.level + tokens - self.capacity) / self.rate
                return RateLimitResult(
                    allowed=False,
                    current_rate=self.rate,
                    remaining=0,
                    reset_at=now + retry_after,
                    retry_after=retry_after,
                )

    def reset(self):
        """Reset the bucket."""
        with self._lock:
            self.level = 0.0
            self.last_update = time.time()


class FixedWindow:
    """Fixed window counter rate limiter."""

    def __init__(self, rate: float, window_size: float = 60.0):
        self.rate = rate
        self.window_size = window_size
        self.window_start = time.time()
        self.count = 0
        self._lock = threading.Lock()

    def allow(self, tokens: int = 1) -> RateLimitResult:
        """Check if request is allowed."""
        with self._lock:
            now = time.time()

            if now - self.window_start >= self.window_size:
                self.window_start = now
                self.count = 0

            if self.count < self.rate * self.window_size:
                self.count += tokens
                return RateLimitResult(
                    allowed=True,
                    current_rate=self.rate,
                    remaining=int(self.rate * self.window_size - self.count),
                    reset_at=self.window_start + self.window_size,
                )
            else:
                retry_after = self.window_start + self.window_size - now
                return RateLimitResult(
                    allowed=False,
                    current_rate=self.rate,
                    remaining=0,
                    reset_at=self.window_start + self.window_size,
                    retry_after=retry_after,
                )

    def reset(self):
        """Reset the window."""
        with self._lock:
            self.window_start = time.time()
            self.count = 0


class AdaptiveRateLimiter:
    """Adaptive rate limiter that adjusts based on success/failure."""

    def __init__(
        self,
        initial_rate: float,
        min_rate: float = 0.1,
        max_rate: float = 100.0,
        increase_factor: float = 1.1,
        decrease_factor: float = 0.5,
    ):
        self.current_rate = initial_rate
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.increase_factor = increase_factor
        self.decrease_factor = decrease_factor
        self._lock = threading.Lock()
        self._limiter = TokenBucket(initial_rate, burst=int(initial_rate))

    def allow(self, tokens: int = 1) -> RateLimitResult:
        """Check if request is allowed."""
        with self._lock:
            return self._limiter.allow(tokens)

    def report_success(self):
        """Report successful request to increase rate."""
        with self._lock:
            new_rate = min(self.current_rate * self.increase_factor, self.max_rate)
            if new_rate != self.current_rate:
                self.current_rate = new_rate
                self._limiter = TokenBucket(new_rate, burst=int(new_rate))

    def report_failure(self):
        """Report failed request to decrease rate."""
        with self._lock:
            new_rate = max(self.current_rate * self.decrease_factor, self.min_rate)
            if new_rate != self.current_rate:
                self.current_rate = new_rate
                self._limiter = TokenBucket(new_rate, burst=int(new_rate))


class RateLimiterManager:
    """Manage multiple rate limiters."""

    def __init__(self):
        self._limiters: Dict[str, Any] = {}
        self._lock = threading.RLock()

    def create_limiter(
        self,
        name: str,
        strategy: RateLimitStrategy,
        rate: float,
        burst: int = 1,
        window_size: float = 60.0,
    ) -> bool:
        """Create a new rate limiter."""
        with self._lock:
            if strategy == RateLimitStrategy.TOKEN_BUCKET:
                self._limiters[name] = TokenBucket(rate, burst)
            elif strategy == RateLimitStrategy.SLIDING_WINDOW:
                self._limiters[name] = SlidingWindow(rate, window_size)
            elif strategy == RateLimitStrategy.LEAKY_BUCKET:
                self._limiters[name] = LeakyBucket(rate, burst)
            elif strategy == RateLimitStrategy.FIXED_WINDOW:
                self._limiters[name] = FixedWindow(rate, window_size)
            else:
                return False
            return True

    def get_limiter(self, name: str) -> Optional[Any]:
        """Get a rate limiter by name."""
        with self._lock:
            return self._limiters.get(name)

    def allow(self, name: str, tokens: int = 1) -> Optional[RateLimitResult]:
        """Check if request is allowed."""
        limiter = self.get_limiter(name)
        if not limiter:
            return None
        return limiter.allow(tokens)

    def remove_limiter(self, name: str) -> bool:
        """Remove a rate limiter."""
        with self._lock:
            if name in self._limiters:
                del self._limiters[name]
                return True
            return False

    def list_limiters(self) -> List[str]:
        """List all rate limiter names."""
        with self._lock:
            return list(self._limiters.keys())


class RateLimiterAction(BaseAction):
    """Rate limiter action for automation."""
    action_type = "rate_limiter"
    display_name = "限流器"
    description = "多种限流策略管理"

    def __init__(self):
        super().__init__()
        self._manager = RateLimiterManager()
        self._adaptive_limiters: Dict[str, AdaptiveRateLimiter] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "allow")

            if operation == "create":
                return self._create_limiter(params)
            elif operation == "allow":
                return self._check_allow(params)
            elif operation == "remove":
                return self._remove_limiter(params)
            elif operation == "list":
                return self._list_limiters()
            elif operation == "adaptive":
                return self._adaptive_limiter(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Rate limiter error: {str(e)}")

    def _create_limiter(self, params: Dict) -> ActionResult:
        """Create a rate limiter."""
        name = params.get("name")
        strategy = params.get("strategy", "token_bucket").upper()
        rate = params.get("rate", 1.0)
        burst = params.get("burst", 1)
        window_size = params.get("window_size", 60.0)

        if not name:
            return ActionResult(success=False, message="name is required")

        try:
            strategy_enum = RateLimitStrategy[strategy]
        except KeyError:
            return ActionResult(success=False, message=f"Unknown strategy: {strategy}")

        success = self._manager.create_limiter(name, strategy_enum, rate, burst, window_size)

        return ActionResult(
            success=success,
            message=f"Limiter '{name}' created with {strategy} strategy" if success else "Failed to create limiter",
        )

    def _check_allow(self, params: Dict) -> ActionResult:
        """Check if request is allowed."""
        name = params.get("name", "default")
        tokens = params.get("tokens", 1)

        result = self._manager.allow(name, tokens)

        if result is None:
            self._manager.create_limiter(name, RateLimitStrategy.TOKEN_BUCKET, 1.0)
            result = self._manager.allow(name, tokens)

        return ActionResult(
            success=result.allowed,
            message="Allowed" if result.allowed else f"Rate limited, retry after {result.retry_after:.2f}s",
            data={
                "allowed": result.allowed,
                "remaining": result.remaining,
                "reset_at": result.reset_at,
                "retry_after": result.retry_after,
            },
        )

    def _remove_limiter(self, params: Dict) -> ActionResult:
        """Remove a rate limiter."""
        name = params.get("name")
        if not name:
            return ActionResult(success=False, message="name is required")

        success = self._manager.remove_limiter(name)
        return ActionResult(success=success, message="Limiter removed" if success else "Limiter not found")

    def _list_limiters(self) -> ActionResult:
        """List all limiters."""
        names = self._manager.list_limiters()
        return ActionResult(success=True, message=f"{len(names)} limiters", data={"limiters": names})

    def _adaptive_limiter(self, params: Dict) -> ActionResult:
        """Use adaptive rate limiter."""
        name = params.get("name", "adaptive_default")
        operation = params.get("sub_operation", "allow")
        initial_rate = params.get("initial_rate", 1.0)
        min_rate = params.get("min_rate", 0.1)
        max_rate = params.get("max_rate", 100.0)

        if name not in self._adaptive_limiters:
            self._adaptive_limiters[name] = AdaptiveRateLimiter(
                initial_rate=initial_rate,
                min_rate=min_rate,
                max_rate=max_rate,
            )

        limiter = self._adaptive_limiters[name]

        if operation == "allow":
            tokens = params.get("tokens", 1)
            result = limiter.allow(tokens)
            return ActionResult(
                success=result.allowed,
                message="Allowed" if result.allowed else f"Rate limited, retry after {result.retry_after:.2f}s",
                data={"allowed": result.allowed, "current_rate": limiter.current_rate},
            )
        elif operation == "success":
            limiter.report_success()
            return ActionResult(success=True, message=f"Rate increased to {limiter.current_rate:.2f}")
        elif operation == "failure":
            limiter.report_failure()
            return ActionResult(success=True, message=f"Rate decreased to {limiter.current_rate:.2f}")
        else:
            return ActionResult(success=False, message=f"Unknown sub_operation: {operation}")
