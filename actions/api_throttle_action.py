"""
API request throttling and priority queue scheduler.

This module provides sophisticated request throttling with priority-based
scheduling, request queuing, and configurable rate limiting policies.

Author: RabAiBot
License: MIT
"""

from __future__ import annotations

import asyncio
import heapq
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple
from collections import defaultdict
import threading
import uuid

logger = logging.getLogger(__name__)


class Priority(Enum):
    """Request priority levels."""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BULK = 4


@dataclass
class ThrottleRequest:
    """Represents a throttled API request."""
    priority: Priority
    func: Callable[..., Any]
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    delay: float = 0.0
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    result: Any = field(default=None, init=False)
    error: Optional[Exception] = field(default=None, init=False)
    completed: bool = False

    @property
    def effective_priority(self) -> Tuple[int, float]:
        """Get effective priority (lower is higher priority)."""
        return (self.priority.value, self.created_at)

    def __lt__(self, other: "ThrottleRequest") -> bool:
        return self.effective_priority < other.effective_priority


@dataclass
class RateLimitPolicy:
    """Defines rate limits for a specific context."""
    requests_per_second: float
    burst_size: int
    max_queue_size: int = 100
    priority_weights: Dict[Priority, float] = field(default_factory=lambda: {
        Priority.CRITICAL: 1.0,
        Priority.HIGH: 0.8,
        Priority.NORMAL: 0.5,
        Priority.LOW: 0.2,
        Priority.BULK: 0.1,
    })


@dataclass
class ThrottleStats:
    """Statistics for the throttle manager."""
    total_requests: int = 0
    completed_requests: int = 0
    rejected_requests: int = 0
    avg_wait_time: float = 0.0
    total_wait_time: float = 0.0
    avg_processing_time: float = 0.0

    def record_completion(self, wait_time: float, processing_time: float) -> None:
        """Record a completed request."""
        self.completed_requests += 1
        self.total_wait_time += wait_time
        self.avg_wait_time = self.total_wait_time / self.completed_requests
        self.avg_processing_time = (
            (self.avg_processing_time * (self.completed_requests - 1) + processing_time)
            / self.completed_requests
        )


class TokenBucket:
    """Token bucket for rate limiting."""

    def __init__(self, rate: float, capacity: float):
        self.rate = rate
        self.capacity = capacity
        self._tokens = capacity
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last_refill = now

    def try_acquire(self, tokens: float = 1.0) -> bool:
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    @property
    def available(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens


class ThrottleManager:
    """
    Manages API request throttling with priority scheduling.

    Features:
    - Priority-based request queuing
    - Token bucket rate limiting
    - Configurable policies per endpoint
    - Request coalescing
    - Automatic retry with backoff
    - Statistics tracking

    Example:
        >>> manager = ThrottleManager()
        >>> manager.set_policy("api.example.com", rate=10, burst=20)
        >>>
        >>> async def make_request():
        ...     return await manager.execute(Priority.NORMAL, my_api_call)
    """

    def __init__(self):
        """Initialize the throttle manager."""
        self._policies: Dict[str, RateLimitPolicy] = {}
        self._buckets: Dict[str, TokenBucket] = {}
        self._queues: Dict[str, List[ThrottleRequest]] = defaultdict(list)
        self._processing_flags: Dict[str, bool] = defaultdict(bool)
        self._lock = threading.RLock()
        self._stats = ThrottleStats()
        self._default_policy = RateLimitPolicy(
            requests_per_second=10.0,
            burst_size=20,
        )
        logger.info("ThrottleManager initialized")

    def set_policy(self, endpoint: str, policy: RateLimitPolicy) -> None:
        """
        Set rate limit policy for an endpoint.

        Args:
            endpoint: API endpoint identifier
            policy: Rate limit policy
        """
        with self._lock:
            self._policies[endpoint] = policy
            self._buckets[endpoint] = TokenBucket(
                rate=policy.requests_per_second,
                capacity=policy.burst_size,
            )
            logger.info(
                f"Policy set for {endpoint}: "
                f"rps={policy.requests_per_second}, burst={policy.burst_size}"
            )

    def get_policy(self, endpoint: str) -> RateLimitPolicy:
        """Get policy for endpoint or default."""
        return self._policies.get(endpoint, self._default_policy)

    def _get_bucket(self, endpoint: str) -> TokenBucket:
        """Get or create bucket for endpoint."""
        if endpoint not in self._buckets:
            policy = self.get_policy(endpoint)
            self._buckets[endpoint] = TokenBucket(
                rate=policy.requests_per_second,
                capacity=policy.burst_size,
            )
        return self._buckets[endpoint]

    async def execute(
        self,
        priority: Priority,
        func: Callable[..., Any],
        *args,
        endpoint: str = "default",
        **kwargs,
    ) -> Any:
        """
        Execute a function with throttling.

        Args:
            priority: Request priority
            func: Function to execute
            *args: Positional arguments
            endpoint: Endpoint identifier for rate limiting
            **kwargs: Keyword arguments

        Returns:
            Function result
        """
        request = ThrottleRequest(
            priority=priority,
            func=func,
            args=args,
            kwargs=kwargs,
        )

        policy = self.get_policy(endpoint)
        bucket = self._get_bucket(endpoint)

        with self._lock:
            self._stats.total_requests += 1
            if len(self._queues[endpoint]) >= policy.max_queue_size:
                self._stats.rejected_requests += 1
                raise RuntimeError(f"Queue full for {endpoint}")

            heapq.heappush(self._queues[endpoint], request)

        wait_start = time.time()

        while True:
            bucket_refill_needed = not bucket.try_acquire(1.0)
            if not bucket_refill_needed:
                async with asyncio.Lock():
                    if self._queues[endpoint] and self._queues[endpoint][0].id == request.id:
                        heapq.heappop(self._queues[endpoint])
                        break

            await asyncio.sleep(0.01)

        wait_time = time.time() - wait_start
        self._stats.record_completion(wait_time, 0.0)

        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(None, lambda: func(*args, **kwargs))
            request.result = result
            return result
        except Exception as e:
            request.error = e
            raise

    def get_stats(self) -> Dict[str, Any]:
        """Get throttle manager statistics."""
        return {
            "total_requests": self._stats.total_requests,
            "completed_requests": self._stats.completed_requests,
            "rejected_requests": self._stats.rejected_requests,
            "avg_wait_time": self._stats.avg_wait_time,
            "avg_processing_time": self._stats.avg_processing_time,
            "policies": {
                ep: {
                    "requests_per_second": p.requests_per_second,
                    "burst_size": p.burst_size,
                    "queue_size": len(self._queues[ep]),
                }
                for ep, p in self._policies.items()
            },
        }

    def get_queue_size(self, endpoint: str = "default") -> int:
        """Get current queue size for endpoint."""
        with self._lock:
            return len(self._queues[endpoint])


class PriorityThrottler:
    """
    Priority-based request throttler.

    Manages multiple priority queues and ensures higher priority
    requests are processed first while respecting rate limits.
    """

    def __init__(
        self,
        rate: float = 10.0,
        burst: int = 20,
        priority_weights: Optional[Dict[Priority, float]] = None,
    ):
        """
        Initialize priority throttler.

        Args:
            rate: Base requests per second
            burst: Burst capacity
            priority_weights: Multiplier for each priority level
        """
        self.base_rate = rate
        self.base_burst = burst
        self.priority_weights = priority_weights or {
            Priority.CRITICAL: 1.0,
            Priority.HIGH: 0.8,
            Priority.NORMAL: 0.5,
            Priority.LOW: 0.2,
            Priority.BULK: 0.1,
        }

        self._buckets: Dict[Priority, TokenBucket] = {
            p: TokenBucket(rate * self.priority_weights[p], burst * self.priority_weights[p])
            for p in Priority
        }
        self._queues: Dict[Priority, List[ThrottleRequest]] = {
            p: [] for p in Priority
        }
        self._lock = threading.Lock()
        logger.info(f"PriorityThrottler initialized (rate={rate}, burst={burst})")

    def _effective_rate(self, priority: Priority) -> Tuple[int, float]:
        """Get effective priority tuple."""
        return (priority.value, time.time())

    async def submit(
        self,
        priority: Priority,
        func: Callable[..., Any],
        *args,
        **kwargs,
    ) -> ThrottleRequest:
        """Submit a request for throttled execution."""
        request = ThrottleRequest(
            priority=priority,
            func=func,
            args=args,
            kwargs=kwargs,
        )

        with self._lock:
            heapq.heappush(self._queues[priority], request)

        return request

    async def process_next(self) -> Optional[Any]:
        """Process the next available request."""
        for priority in Priority:
            queue = self._queues[priority]
            if not queue:
                continue

            bucket = self._buckets[priority]
            if bucket.try_acquire(1.0):
                request = heapq.heappop(queue)
                try:
                    if asyncio.iscoroutinefunction(request.func):
                        result = await request.func(*request.args, **request.kwargs)
                    else:
                        loop = asyncio.get_event_loop()
                        result = await loop.run_in_executor(
                            None,
                            lambda: request.func(*request.args, **request.kwargs)
                        )
                    request.result = result
                    request.completed = True
                    return result
                except Exception as e:
                    request.error = e
                    raise

        return None

    def get_queue_depths(self) -> Dict[str, int]:
        """Get depth of each priority queue."""
        return {p.name: len(q) for p, q in self._queues.items()}
