"""API throttle action for request rate limiting.

Implements token bucket and sliding window rate limiting
with configurable limits and burst handling.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


class ThrottleAlgorithm(Enum):
    """Rate limiting algorithms."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    LEAKY_BUCKET = "leaky_bucket"


@dataclass
class ThrottleConfig:
    """Configuration for rate limiting."""
    requests_per_second: float
    burst_size: int = 1
    algorithm: ThrottleAlgorithm = ThrottleAlgorithm.TOKEN_BUCKET
    window_size_seconds: float = 60.0


@dataclass
class ThrottleStats:
    """Statistics for throttle operations."""
    total_requests: int = 0
    throttled_requests: int = 0
    allowed_requests: int = 0
    wait_time_ms: float = 0.0


@dataclass
class ThrottleResult:
    """Result of a throttle check."""
    allowed: bool
    wait_time_ms: float
    remaining_tokens: float
    reset_in_ms: float


class APIThrottleAction:
    """Rate limit API requests.

    Args:
        config: Throttle configuration.
        client_id: Optional client identifier for per-client limiting.

    Example:
        >>> throttle = APIThrottleAction(requests_per_second=10)
        >>> result = await throttle.check()
        >>> if result.allowed:
        ...     await make_api_call()
    """

    def __init__(
        self,
        config: Optional[ThrottleConfig] = None,
        client_id: Optional[str] = None,
    ) -> None:
        self.config = config or ThrottleConfig(requests_per_second=10)
        self.client_id = client_id
        self._stats = ThrottleStats()

        if self.config.algorithm == ThrottleAlgorithm.TOKEN_BUCKET:
            self._tokens = float(self.config.burst_size)
            self._last_update = time.time()
        elif self.config.algorithm == ThrottleAlgorithm.SLIDING_WINDOW:
            self._requests: list[float] = []
        elif self.config.algorithm == ThrottleAlgorithm.LEAKY_BUCKET:
            self._bucket_level = 0.0
            self._last_leak = time.time()

    async def check(self) -> ThrottleResult:
        """Check if request is allowed under rate limit.

        Returns:
            Throttle result with allowed status and wait time.
        """
        self._stats.total_requests += 1

        if self.config.algorithm == ThrottleAlgorithm.TOKEN_BUCKET:
            return await self._check_token_bucket()
        elif self.config.algorithm == ThrottleAlgorithm.SLIDING_WINDOW:
            return await self._check_sliding_window()
        else:
            return await self._check_leaky_bucket()

    async def _check_token_bucket(self) -> ThrottleResult:
        """Check using token bucket algorithm.

        Returns:
            Throttle result.
        """
        now = time.time()
        elapsed = now - self._last_update

        tokens_to_add = elapsed * self.config.requests_per_second
        self._tokens = min(
            self.config.burst_size,
            self._tokens + tokens_to_add
        )
        self._last_update = now

        if self._tokens >= 1.0:
            self._tokens -= 1.0
            self._stats.allowed_requests += 1
            return ThrottleResult(
                allowed=True,
                wait_time_ms=0.0,
                remaining_tokens=self._tokens,
                reset_in_ms=0.0,
            )
        else:
            wait_time = (1.0 - self._tokens) / self.config.requests_per_second
            self._stats.throttled_requests += 1
            self._stats.wait_time_ms += wait_time * 1000
            return ThrottleResult(
                allowed=False,
                wait_time_ms=wait_time * 1000,
                remaining_tokens=self._tokens,
                reset_in_ms=wait_time * 1000,
            )

    async def _check_sliding_window(self) -> ThrottleResult:
        """Check using sliding window algorithm.

        Returns:
            Throttle result.
        """
        now = time.time()
        window_start = now - self.config.window_size_seconds

        self._requests = [
            ts for ts in self._requests if ts > window_start
        ]

        rps = self.config.requests_per_second
        if len(self._requests) < rps:
            self._requests.append(now)
            self._stats.allowed_requests += 1
            return ThrottleResult(
                allowed=True,
                wait_time_ms=0.0,
                remaining_tokens=rps - len(self._requests),
                reset_in_ms=0.0,
            )
        else:
            oldest = self._requests[0]
            wait_time = (oldest + self.config.window_size_seconds) - now
            self._stats.throttled_requests += 1
            self._stats.wait_time_ms += wait_time * 1000
            return ThrottleResult(
                allowed=False,
                wait_time_ms=wait_time * 1000,
                remaining_tokens=0.0,
                reset_in_ms=wait_time * 1000,
            )

    async def _check_leaky_bucket(self) -> ThrottleResult:
        """Check using leaky bucket algorithm.

        Returns:
            Throttle result.
        """
        now = time.time()
        elapsed = now - self._last_leak
        leaked = elapsed * self.config.requests_per_second

        self._bucket_level = max(0.0, self._bucket_level - leaked)
        self._last_leak = now

        if self._bucket_level < self.config.burst_size:
            self._bucket_level += 1.0
            self._stats.allowed_requests += 1
            return ThrottleResult(
                allowed=True,
                wait_time_ms=0.0,
                remaining_tokens=float(self.config.burst_size - self._bucket_level),
                reset_in_ms=0.0,
            )
        else:
            wait_time = 1.0 / self.config.requests_per_second
            self._stats.throttled_requests += 1
            self._stats.wait_time_ms += wait_time * 1000
            return ThrottleResult(
                allowed=False,
                wait_time_ms=wait_time * 1000,
                remaining_tokens=0.0,
                reset_in_ms=wait_time * 1000,
            )

    async def wait_if_needed(self) -> float:
        """Wait if rate limited and return wait time.

        Returns:
            Time waited in seconds.
        """
        result = await self.check()
        if not result.allowed:
            await asyncio.sleep(result.wait_time_ms / 1000.0)
            return result.wait_time_ms / 1000.0
        return 0.0

    def reset(self) -> None:
        """Reset throttle state."""
        if self.config.algorithm == ThrottleAlgorithm.TOKEN_BUCKET:
            self._tokens = float(self.config.burst_size)
            self._last_update = time.time()
        elif self.config.algorithm == ThrottleAlgorithm.SLIDING_WINDOW:
            self._requests.clear()
        elif self.config.algorithm == ThrottleAlgorithm.LEAKY_BUCKET:
            self._bucket_level = 0.0
            self._last_leak = time.time()

        self._stats = ThrottleStats()

    def get_stats(self) -> ThrottleStats:
        """Get throttle statistics.

        Returns:
            Current statistics.
        """
        return self._stats

    def get_remaining(self) -> float:
        """Get remaining requests in current window.

        Returns:
            Remaining request capacity.
        """
        if self.config.algorithm == ThrottleAlgorithm.TOKEN_BUCKET:
            return self._tokens
        elif self.config.algorithm == ThrottleAlgorithm.SLIDING_WINDOW:
            window_start = time.time() - self.config.window_size_seconds
            active = len([ts for ts in self._requests if ts > window_start])
            return max(0.0, self.config.requests_per_second - active)
        else:
            return float(self.config.burst_size - self._bucket_level)
