"""API Rate Optimizer Action Module.

Provides intelligent rate limit optimization using predictive
algorithms and adaptive throttling strategies.

Author: RabAi Team
"""

from __future__ import annotations

import time
import math
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional, Tuple

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class OptimizationStrategy(Enum):
    """Rate optimization strategies."""
    CONSERVATIVE = "conservative"
    BALANCED = "balanced"
    AGGRESSIVE = "aggressive"
    ADAPTIVE = "adaptive"


class RateWindow(Enum):
    """Rate calculation windows."""
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"


@dataclass
class RateLimitConfig:
    """Rate limit configuration."""
    requests_per_second: float = 10.0
    requests_per_minute: float = 100.0
    requests_per_hour: float = 1000.0
    burst_size: int = 20
    strategy: OptimizationStrategy = OptimizationStrategy.BALANCED


@dataclass
class RequestMetrics:
    """Metrics for a single request."""
    timestamp: float
    latency_ms: float
    success: bool
    status_code: Optional[int] = None
    response_size: int = 0


@dataclass
class RateState:
    """Current rate limiter state."""
    tokens: float
    last_update: float
    request_count: int = 0
    success_count: int = 0
    error_count: int = 0


class TokenBucket:
    """Token bucket rate limiter with refill support."""

    def __init__(
        self,
        capacity: float,
        refill_rate: float,
        initial_tokens: Optional[float] = None
    ):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = initial_tokens if initial_tokens is not None else capacity
        self.last_refill = time.time()

    def consume(self, tokens: float = 1.0) -> bool:
        """Attempt to consume tokens."""
        self._refill()

        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill

        new_tokens = elapsed * self.refill_rate
        self.tokens = min(self.capacity, self.tokens + new_tokens)
        self.last_refill = now

    def get_wait_time(self, tokens: float = 1.0) -> float:
        """Get wait time until tokens are available."""
        if self.tokens >= tokens:
            return 0.0

        deficit = tokens - self.tokens
        return deficit / self.refill_rate


class SlidingWindowCounter:
    """Sliding window rate counter."""

    def __init__(self, window_seconds: float, max_count: int):
        self.window_seconds = window_seconds
        self.max_count = max_count
        self.requests: Deque[float] = deque()

    def is_allowed(self) -> bool:
        """Check if request is allowed under rate limit."""
        self._evict_old()

        return len(self.requests) < self.max_count

    def record_request(self) -> None:
        """Record a new request."""
        self._evict_old()
        self.requests.append(time.time())

    def _evict_old(self) -> None:
        """Remove expired entries."""
        cutoff = time.time() - self.window_seconds

        while self.requests and self.requests[0] < cutoff:
            self.requests.popleft()

    def get_current_count(self) -> int:
        """Get current request count in window."""
        self._evict_old()
        return len(self.requests)


class AdaptiveRateOptimizer:
    """Adaptive rate optimizer with predictive capabilities."""

    def __init__(self, config: RateLimitConfig):
        self.config = config
        self.token_bucket = TokenBucket(
            capacity=config.burst_size,
            refill_rate=config.requests_per_second
        )
        self.second_counter = SlidingWindowCounter(1.0, int(config.requests_per_second))
        self.minute_counter = SlidingWindowCounter(60.0, int(config.requests_per_minute))
        self.hour_counter = SlidingWindowCounter(3600.0, int(config.requests_per_hour))

        self.metrics_history: Deque[RequestMetrics] = deque(maxlen=1000)
        self.state = RateState(
            tokens=config.burst_size,
            last_update=time.time()
        )

    def should_allow_request(self) -> Tuple[bool, Optional[float]]:
        """Check if request should be allowed."""
        if not self.second_counter.is_allowed():
            wait_time = self.token_bucket.get_wait_time()
            return False, max(wait_time, 0.1)

        if not self.minute_counter.is_allowed():
            return False, 1.0

        if not self.hour_counter.is_allowed():
            return False, 10.0

        if not self.token_bucket.consume():
            wait_time = self.token_bucket.get_wait_time()
            return False, wait_time

        self.second_counter.record_request()
        self.minute_counter.record_request()
        self.hour_counter.record_request()

        return True, None

    def record_result(
        self,
        latency_ms: float,
        success: bool,
        status_code: Optional[int] = None,
        response_size: int = 0
    ) -> None:
        """Record request result for analysis."""
        metrics = RequestMetrics(
            timestamp=time.time(),
            latency_ms=latency_ms,
            success=success,
            status_code=status_code,
            response_size=response_size
        )

        self.metrics_history.append(metrics)

        self.state.request_count += 1
        if success:
            self.state.success_count += 1
        else:
            self.state.error_count += 1

        self._adjust_rate()

    def _adjust_rate(self) -> None:
        """Dynamically adjust rate based on success/failure patterns."""
        if len(self.metrics_history) < 10:
            return

        recent = list(self.metrics_history)[-50:]
        error_rate = sum(1 for m in recent if not m.success) / len(recent)

        avg_latency = sum(m.latency_ms for m in recent) / len(recent)

        if error_rate > 0.1:
            self._decrease_rate(0.8)
        elif error_rate > 0.05:
            self._decrease_rate(0.9)
        elif avg_latency > 1000 and self.config.strategy == OptimizationStrategy.ADAPTIVE:
            self._decrease_rate(0.95)
        elif error_rate < 0.01 and avg_latency < 100:
            self._increase_rate(1.1)

    def _decrease_rate(self, factor: float) -> None:
        """Decrease rate limit by factor."""
        self.token_bucket.refill_rate *= factor
        self.token_bucket.capacity = max(1, int(self.token_bucket.capacity * factor))

    def _increase_rate(self, factor: float) -> None:
        """Increase rate limit by factor."""
        max_rate = self.config.requests_per_second * 2
        self.token_bucket.refill_rate = min(
            max_rate,
            self.token_bucket.refill_rate * factor
        )
        max_capacity = self.config.burst_size * 2
        self.token_bucket.capacity = min(
            max_capacity,
            int(self.token_bucket.capacity * factor)
        )

    def get_optimal_batch_size(self) -> int:
        """Calculate optimal batch size based on current state."""
        if self.token_bucket.tokens < 1:
            return 0

        recent = list(self.metrics_history)[-20:]
        if not recent:
            return int(self.token_bucket.capacity)

        avg_latency = sum(m.latency_ms for m in recent) / len(recent)

        if avg_latency < 50:
            return min(int(self.token_bucket.tokens), 50)
        elif avg_latency < 200:
            return min(int(self.token_bucket.tokens), 20)
        else:
            return min(int(self.token_bucket.tokens), 5)

    def get_statistics(self) -> Dict[str, Any]:
        """Get current rate limiting statistics."""
        recent = list(self.metrics_history)[-100:]

        total = len(recent)
        successes = sum(1 for m in recent if m.success)
        errors = total - successes

        avg_latency = sum(m.latency_ms for m in recent) / total if total > 0 else 0

        return {
            "tokens_available": self.token_bucket.tokens,
            "refill_rate": self.token_bucket.refill_rate,
            "capacity": self.token_bucket.capacity,
            "second_counter": self.second_counter.get_current_count(),
            "minute_counter": self.minute_counter.get_current_count(),
            "hour_counter": self.hour_counter.get_current_count(),
            "total_requests": self.state.request_count,
            "success_count": self.state.success_count,
            "error_count": self.state.error_count,
            "success_rate": successes / total if total > 0 else 0,
            "error_rate": errors / total if total > 0 else 0,
            "avg_latency_ms": avg_latency,
            "optimal_batch_size": self.get_optimal_batch_size()
        }


class APIRateOptimizerAction(BaseAction):
    """Action for API rate optimization operations."""

    def __init__(self):
        super().__init__("api_rate_optimizer")
        self._optimizers: Dict[str, AdaptiveRateOptimizer] = {}
        self._default_config = RateLimitConfig()

    def _get_or_create_optimizer(self, key: str) -> AdaptiveRateOptimizer:
        """Get or create optimizer for key."""
        if key not in self._optimizers:
            self._optimizers[key] = AdaptiveRateOptimizer(self._default_config)
        return self._optimizers[key]

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute rate optimizer action."""
        try:
            operation = params.get("operation", "should_allow")

            if operation == "should_allow":
                return self._should_allow(params)
            elif operation == "configure":
                return self._configure(params)
            elif operation == "record_result":
                return self._record_result(params)
            elif operation == "get_stats":
                return self._get_stats(params)
            elif operation == "get_batch_size":
                return self._get_batch_size(params)
            elif operation == "reset":
                return self._reset(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _should_allow(self, params: Dict[str, Any]) -> ActionResult:
        """Check if request should be allowed."""
        key = params.get("key", "default")
        optimizer = self._get_or_create_optimizer(key)

        allowed, wait_time = optimizer.should_allow_request()

        return ActionResult(
            success=True,
            data={
                "allowed": allowed,
                "wait_time_seconds": wait_time
            }
        )

    def _configure(self, params: Dict[str, Any]) -> ActionResult:
        """Configure rate limiter."""
        key = params.get("key", "default")

        config = RateLimitConfig(
            requests_per_second=params.get("requests_per_second", 10.0),
            requests_per_minute=params.get("requests_per_minute", 100.0),
            requests_per_hour=params.get("requests_per_hour", 1000.0),
            burst_size=params.get("burst_size", 20),
            strategy=OptimizationStrategy(params.get("strategy", "balanced"))
        )

        self._optimizers[key] = AdaptiveRateOptimizer(config)

        return ActionResult(
            success=True,
            message=f"Rate optimizer configured for: {key}"
        )

    def _record_result(self, params: Dict[str, Any]) -> ActionResult:
        """Record request result."""
        key = params.get("key", "default")
        optimizer = self._get_or_create_optimizer(key)

        optimizer.record_result(
            latency_ms=params.get("latency_ms", 0.0),
            success=params.get("success", True),
            status_code=params.get("status_code"),
            response_size=params.get("response_size", 0)
        )

        return ActionResult(success=True)

    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get rate limiting statistics."""
        key = params.get("key", "default")

        if key not in self._optimizers:
            return ActionResult(success=False, message=f"No optimizer for key: {key}")

        stats = self._optimizers[key].get_statistics()
        return ActionResult(success=True, data=stats)

    def _get_batch_size(self, params: Dict[str, Any]) -> ActionResult:
        """Get optimal batch size."""
        key = params.get("key", "default")
        optimizer = self._get_or_create_optimizer(key)

        batch_size = optimizer.get_optimal_batch_size()
        return ActionResult(success=True, data={"batch_size": batch_size})

    def _reset(self, params: Dict[str, Any]) -> ActionResult:
        """Reset optimizer state."""
        key = params.get("key", "default")

        if key in self._optimizers:
            del self._optimizers[key]

        return ActionResult(
            success=True,
            message=f"Optimizer reset for: {key}"
        )


from enum import Enum
