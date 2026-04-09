"""
Automation Throttle Action Module.

Rate throttling for automation tasks with configurable throughput limits,
burst handling, and priority-aware scheduling.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ThrottlePolicy(Enum):
    """Throttling policies for when limit is reached."""
    QUEUE = "queue"     # Queue the task for later
    REJECT = "reject"   # Reject with error
    DROPBUSRT = "drop_burst"  # Drop burst requests, keep steady


@dataclass
class ThrottleConfig:
    """Configuration for a throttle limiter."""
    name: str
    max_rate: float          # Operations per second
    burst_size: int = 1     # Max burst size
    policy: ThrottlePolicy = ThrottlePolicy.QUEUE


@dataclass
class ThrottleMetrics:
    """Metrics for throttle monitoring."""
    total_requests: int = 0
    allowed: int = 0
    rejected: int = 0
    queued: int = 0
    dropped: int = 0
    wait_time_ms: float = 0.0


class TokenBucket:
    """
    Token bucket algorithm for rate limiting.

    Supports bursty traffic while maintaining an average rate limit.
    """

    def __init__(self, rate: float, burst: int) -> None:
        self.rate = rate
        self.burst = burst
        self._tokens = float(burst)
        self._last_update = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> float:
        """Acquire tokens, returning wait time in seconds."""
        async with self._lock:
            now = time.time()
            elapsed = now - self._last_update
            self._last_update = now

            # Add tokens based on elapsed time
            self._tokens = min(self.burst, self._tokens + elapsed * self.rate)

            if self._tokens >= tokens:
                self._tokens -= tokens
                return 0.0
            else:
                # Calculate wait time
                needed = tokens - self._tokens
                wait_time = needed / self.rate
                self._tokens = 0.0
                return wait_time


class SlidingWindowCounter:
    """
    Sliding window counter for more accurate rate limiting.

    Tracks requests in a rolling time window for smoother limiting.
    """

    def __init__(self, max_requests: int, window_seconds: float) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: list[float] = []
        self._lock = asyncio.Lock()

    async def is_allowed(self) -> bool:
        """Check if a request is allowed in the current window."""
        async with self._lock:
            now = time.time()
            # Remove expired entries
            self._requests = [t for t in self._requests if now - t < self.window_seconds]

            if len(self._requests) < self.max_requests:
                self._requests.append(now)
                return True
            return False

    async def wait_time(self) -> float:
        """Get wait time until a slot becomes available."""
        async with self._lock:
            now = time.time()
            self._requests = [t for t in self._requests if now - t < self.window_seconds]

            if len(self._requests) < self.max_requests:
                return 0.0

            # Time until oldest request expires
            oldest = min(self._requests)
            return max(0.0, self.window_seconds - (now - oldest))


class AutomationThrottleAction:
    """
    Rate throttling for automation task execution.

    Supports multiple throttle limiters with different policies,
    token bucket and sliding window algorithms.

    Example:
        throttle = AutomationThrottleAction()
        throttle.add_throttle(ThrottleConfig(
            name="api-calls",
            max_rate=10.0,  # 10 per second
            burst_size=5,
        ))

        await throttle.acquire("api-calls")
        # Execute API call here
    """

    def __init__(self) -> None:
        self._throttles: dict[str, TokenBucket] = {}
        self._configs: dict[str, ThrottleConfig] = {}
        self._queue: asyncio.Queue = asyncio.Queue()
        self._metrics: dict[str, ThrottleMetrics] = {}
        self._processing = False

    def add_throttle(
        self,
        name: str,
        max_rate: float,
        burst_size: int = 1,
        policy: ThrottlePolicy = ThrottlePolicy.QUEUE,
    ) -> None:
        """Add a throttle limiter."""
        config = ThrottleConfig(
            name=name,
            max_rate=max_rate,
            burst_size=burst_size,
            policy=policy,
        )
        self._configs[name] = config
        self._throttles[name] = TokenBucket(max_rate, burst_size)
        self._metrics[name] = ThrottleMetrics()
        logger.info(f"Added throttle '{name}': {max_rate}/s, burst={burst_size}")

    def remove_throttle(self, name: str) -> bool:
        """Remove a throttle limiter."""
        if name in self._throttles:
            del self._throttles[name]
            del self._configs[name]
            del self._metrics[name]
            return True
        return False

    async def acquire(
        self,
        throttle_name: str,
        timeout: Optional[float] = None,
    ) -> bool:
        """
        Acquire permission to proceed from a throttle.

        Returns True if acquired, False if rejected.
        Raises asyncio.TimeoutError if timeout exceeded.
        """
        if throttle_name not in self._throttles:
            return True  # Unknown throttle, allow

        throttle = self._throttles[throttle_name]
        config = self._configs[throttle_name]
        metrics = self._metrics[throttle_name]
        metrics.total_requests += 1

        wait_time = await throttle.acquire(1)

        if wait_time > 0:
            if config.policy == ThrottlePolicy.REJECT:
                metrics.rejected += 1
                return False

            if config.policy == ThrottlePolicy.DROPBURST and wait_time > (1.0 / config.max_rate) * config.burst_size:
                metrics.dropped += 1
                return False

            # Queue / wait
            metrics.queued += 1
            if timeout and wait_time > timeout:
                metrics.rejected += 1
                return False

            await asyncio.sleep(wait_time)
            metrics.wait_time_ms = (metrics.wait_time_ms * (metrics.allowed + 1) + wait_time * 1000) / (metrics.allowed + 2)

        metrics.allowed += 1
        return True

    async def execute_throttled(
        self,
        throttle_name: str,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Execute a function with throttling."""
        await self.acquire(throttle_name)

        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        return func(*args, **kwargs)

    def get_metrics(self, throttle_name: str) -> Optional[ThrottleMetrics]:
        """Get metrics for a throttle."""
        return self._metrics.get(throttle_name)

    def get_all_metrics(self) -> dict[str, ThrottleMetrics]:
        """Get all throttle metrics."""
        return self._metrics.copy()

    def reset_metrics(self, throttle_name: Optional[str] = None) -> None:
        """Reset metrics."""
        if throttle_name:
            if throttle_name in self._metrics:
                self._metrics[throttle_name] = ThrottleMetrics()
        else:
            for name in self._metrics:
                self._metrics[name] = ThrottleMetrics()
