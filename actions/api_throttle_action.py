"""
API Rate Throttling Action Module.

Provides token bucket and leaky bucket rate limiting algorithms
for API request throttling with burst handling.
"""

import time
import threading
import asyncio
from typing import Optional, Dict, Any, Callable
from dataclasses import dataclass, field
from collections import deque
from enum import Enum


class ThrottleAlgorithm(Enum):
    """Throttling algorithm types."""
    TOKEN_BUCKET = "token_bucket"
    LEAKY_BUCKET = "leaky_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"


@dataclass
class TokenBucket:
    """Token bucket state for rate limiting."""
    tokens: float
    last_update: float
    capacity: float
    refill_rate: float  # tokens per second


@dataclass
class LeakyBucket:
    """Leaky bucket state for rate limiting."""
    queue: deque
    leak_rate: float  # requests per second
    capacity: int
    last_leak: float


@dataclass
class SlidingWindowCounter:
    """Sliding window counter state."""
    timestamps: deque
    window_size: float  # seconds
    max_requests: int


class ThrottleConfig:
    """Configuration for rate throttling."""

    def __init__(
        self,
        algorithm: ThrottleAlgorithm = ThrottleAlgorithm.TOKEN_BUCKET,
        rate: float = 10.0,
        burst: float = 20.0,
        block_duration: float = 60.0,
    ):
        self.algorithm = algorithm
        self.rate = rate  # requests per second
        self.burst = burst  # max burst capacity
        self.block_duration = block_duration  # seconds to block when exceeded


class APIThrottleAction:
    """
    Rate limiting action using various throttling algorithms.

    Supports token bucket, leaky bucket, sliding window, and fixed window
    algorithms for different rate limiting needs.
    """

    def __init__(self, config: Optional[ThrottleConfig] = None):
        self.config = config or ThrottleConfig()
        self._buckets: Dict[str, Any] = {}
        self._lock = threading.RLock()
        self._blocked: Dict[str, float] = {}
        self._stats: Dict[str, int] = {"allowed": 0, "throttled": 0, "blocked": 0}

    def _get_token_bucket(self, key: str) -> TokenBucket:
        """Get or create token bucket for key."""
        if key not in self._buckets:
            self._buckets[key] = TokenBucket(
                tokens=self.config.burst,
                last_update=time.time(),
                capacity=self.config.burst,
                refill_rate=self.config.rate,
            )
        return self._buckets[key]

    def _get_leaky_bucket(self, key: str) -> LeakyBucket:
        """Get or create leaky bucket for key."""
        if key not in self._buckets:
            self._buckets[key] = LeakyBucket(
                queue=deque(),
                leak_rate=self.config.rate,
                capacity=int(self.config.burst),
                last_leak=time.time(),
            )
        return self._buckets[key]

    def _get_sliding_window(self, key: str) -> SlidingWindowCounter:
        """Get or create sliding window counter for key."""
        if key not in self._buckets:
            self._buckets[key] = SlidingWindowCounter(
                timestamps=deque(),
                window_size=1.0,
                max_requests=int(self.config.rate),
            )
        return self._buckets[key]

    def _refill_token_bucket(self, bucket: TokenBucket) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - bucket.last_update
        bucket.tokens = min(
            bucket.capacity,
            bucket.tokens + elapsed * bucket.refill_rate,
        )
        bucket.last_update = now

    def _leak_leaky_bucket(self, bucket: LeakyBucket) -> None:
        """Leak requests from the bucket."""
        now = time.time()
        elapsed = now - bucket.last_leak
        leaked = int(elapsed * bucket.leak_rate)
        for _ in range(leaked):
            if bucket.queue:
                bucket.queue.popleft()
        bucket.last_leak = now

    def _cleanup_sliding_window(self, counter: SlidingWindowCounter) -> None:
        """Remove expired timestamps from sliding window."""
        now = time.time()
        while counter.timestamps and counter.timestamps[0] < now - counter.window_size:
            counter.timestamps.popleft()

    def _is_blocked(self, key: str) -> bool:
        """Check if key is currently blocked."""
        if key in self._blocked:
            if time.time() >= self._blocked[key]:
                del self._blocked[key]
                return False
            return True
        return False

    def _block(self, key: str) -> None:
        """Block key for configured duration."""
        self._blocked[key] = time.time() + self.config.block_duration
        self._stats["blocked"] += 1

    async def acquire_async(self, key: str = "default") -> bool:
        """
        Acquire permission to make a request (async version).

        Args:
            key: Identifier for rate limit bucket (e.g., user ID, API key)

        Returns:
            True if request is allowed, False if throttled/blocked
        """
        if self._is_blocked(key):
            return False

        with self._lock:
            if self.config.algorithm == ThrottleAlgorithm.TOKEN_BUCKET:
                bucket = self._get_token_bucket(key)
                self._refill_token_bucket(bucket)
                if bucket.tokens >= 1.0:
                    bucket.tokens -= 1.0
                    self._stats["allowed"] += 1
                    return True
                self._stats["throttled"] += 1
                return False

            elif self.config.algorithm == ThrottleAlgorithm.LEAKY_BUCKET:
                bucket = self._get_leaky_bucket(key)
                self._leak_leaky_bucket(bucket)
                if len(bucket.queue) < bucket.capacity:
                    bucket.queue.append(time.time())
                    self._stats["allowed"] += 1
                    return True
                self._stats["throttled"] += 1
                self._block(key)
                return False

            elif self.config.algorithm == ThrottleAlgorithm.SLIDING_WINDOW:
                counter = self._get_sliding_window(key)
                self._cleanup_sliding_window(counter)
                if len(counter.timestamps) < counter.max_requests:
                    counter.timestamps.append(time.time())
                    self._stats["allowed"] += 1
                    return True
                self._stats["throttled"] += 1
                return False

            else:  # FIXED_WINDOW
                counter = self._get_sliding_window(key)
                self._cleanup_sliding_window(counter)
                if len(counter.timestamps) < counter.max_requests:
                    counter.timestamps.append(time.time())
                    self._stats["allowed"] += 1
                    return True
                self._stats["throttled"] += 1
                return False

    def acquire(self, key: str = "default") -> bool:
        """
        Acquire permission to make a request (sync version).

        Args:
            key: Identifier for rate limit bucket

        Returns:
            True if request is allowed, False if throttled/blocked
        """
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                future = asyncio.run_coroutine_threadsafe(
                    self.acquire_async(key), loop
                )
                return future.result(timeout=5.0)
            return asyncio.run(self.acquire_async(key))
        except Exception:
            return False

    async def execute_async(
        self,
        func: Callable,
        key: str = "default",
        *args,
        **kwargs
    ) -> Any:
        """
        Execute function with rate limiting.

        Args:
            func: Async function to execute
            key: Rate limit bucket key
            *args: Positional arguments for func
            **kwargs: Keyword arguments for func

        Returns:
            Function result if allowed, None if throttled

        Raises:
            RuntimeError: If rate limited and raise_on_throttle is True
        """
        if not await self.acquire_async(key):
            return None
        return await func(*args, **kwargs)

    def execute(
        self,
        func: Callable,
        key: str = "default",
        *args,
        **kwargs
    ) -> Any:
        """Execute function with rate limiting (sync version)."""
        if not self.acquire(key):
            return None
        if asyncio.iscoroutinefunction(func):
            return asyncio.run(func(*args, **kwargs))
        return func(*args, **kwargs)

    def get_stats(self) -> Dict[str, Any]:
        """Get throttling statistics."""
        return {
            "allowed": self._stats["allowed"],
            "throttled": self._stats["throttled"],
            "blocked": self._stats["blocked"],
            "active_buckets": len(self._buckets),
            "blocked_keys": list(self._blocked.keys()),
        }

    def reset(self, key: Optional[str] = None) -> None:
        """Reset throttling state for key or all keys."""
        with self._lock:
            if key:
                self._buckets.pop(key, None)
                self._blocked.pop(key, None)
            else:
                self._buckets.clear()
                self._blocked.clear()
                self._stats = {"allowed": 0, "throttled": 0, "blocked": 0}

    def wait_and_execute(
        self,
        func: Callable,
        key: str = "default",
        max_wait: float = 30.0,
    ) -> Any:
        """
        Wait for rate limit window and execute function.

        Args:
            func: Function to execute
            key: Rate limit bucket key
            max_wait: Maximum seconds to wait

        Returns:
            Function result or None if timeout
        """
        start = time.time()
        while time.time() - start < max_wait:
            if self.acquire(key):
                if asyncio.iscoroutinefunction(func):
                    return asyncio.run(func())
                return func()
            time.sleep(0.1)
        return None
