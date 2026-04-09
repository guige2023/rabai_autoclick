"""API throttling and rate limiting action."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class ThrottleType(str, Enum):
    """Type of throttling."""

    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"
    LEAKY_BUCKET = "leaky_bucket"


@dataclass
class ThrottleConfig:
    """Configuration for throttling."""

    name: str
    throttle_type: ThrottleType
    rate: float  # requests per window
    window_seconds: float
    burst_size: Optional[float] = None
    scope: str = "global"  # global, client, endpoint


@dataclass
class ThrottleResult:
    """Result of throttle check."""

    allowed: bool
    wait_time: float = 0
    current_rate: float = 0
    remaining: int = 0


class APIThrottleAction:
    """Implements rate limiting and throttling."""

    def __init__(
        self,
        on_throttled: Optional[Callable[[str, ThrottleResult], None]] = None,
    ):
        """Initialize throttler.

        Args:
            on_throttled: Callback when request is throttled.
        """
        self._configs: dict[str, ThrottleConfig] = {}
        self._buckets: dict[str, dict] = {}
        self._windows: dict[str, list[float]] = {}
        self._on_throttled = on_throttled

    def add_throttle(self, config: ThrottleConfig) -> None:
        """Add a throttle configuration."""
        self._configs[config.name] = config
        self._initialize_throttle(config)

    def _initialize_throttle(self, config: ThrottleConfig) -> None:
        """Initialize throttle state."""
        if config.throttle_type == ThrottleType.TOKEN_BUCKET:
            self._buckets[config.name] = {
                "tokens": config.burst_size or config.rate,
                "last_update": time.time(),
            }
        elif config.throttle_type == ThrottleType.SLIDING_WINDOW:
            self._windows[config.name] = []
        elif config.throttle_type == ThrottleType.FIXED_WINDOW:
            self._windows[config.name] = []
            self._buckets[config.name] = {
                "window_start": time.time(),
                "count": 0,
            }
        elif config.throttle_type == ThrottleType.LEAKY_BUCKET:
            self._buckets[config.name] = {
                "level": 0.0,
                "last_update": time.time(),
            }

    def _refill_token_bucket(self, name: str) -> None:
        """Refill token bucket."""
        config = self._configs[name]
        bucket = self._buckets[name]
        now = time.time()
        elapsed = now - bucket["last_update"]

        refill_rate = config.rate / config.window_seconds
        tokens_to_add = elapsed * refill_rate

        bucket["tokens"] = min(
            config.burst_size or config.rate,
            bucket["tokens"] + tokens_to_add,
        )
        bucket["last_update"] = now

    def _check_token_bucket(self, name: str, tokens_needed: float = 1.0) -> ThrottleResult:
        """Check token bucket throttle."""
        config = self._configs[name]
        bucket = self._buckets[name]

        self._refill_token_bucket(name)

        if bucket["tokens"] >= tokens_needed:
            bucket["tokens"] -= tokens_needed
            return ThrottleResult(
                allowed=True,
                wait_time=0,
                current_rate=config.rate,
                remaining=int(bucket["tokens"]),
            )
        else:
            tokens_deficit = tokens_needed - bucket["tokens"]
            refill_time = tokens_deficit / (config.rate / config.window_seconds)
            return ThrottleResult(
                allowed=False,
                wait_time=refill_time,
                current_rate=config.rate,
                remaining=0,
            )

    def _check_sliding_window(self, name: str) -> ThrottleResult:
        """Check sliding window throttle."""
        config = self._configs[name]
        now = time.time()
        cutoff = now - config.window_seconds

        if name not in self._windows:
            self._windows[name] = []

        self._windows[name] = [t for t in self._windows[name] if t > cutoff]

        if len(self._windows[name]) < config.rate:
            self._windows[name].append(now)
            return ThrottleResult(
                allowed=True,
                wait_time=0,
                current_rate=len(self._windows[name]) / config.window_seconds,
                remaining=int(config.rate - len(self._windows[name])),
            )
        else:
            oldest = self._windows[name][0]
            wait_time = oldest - cutoff
            return ThrottleResult(
                allowed=False,
                wait_time=wait_time,
                current_rate=config.rate / config.window_seconds,
                remaining=0,
            )

    def _check_fixed_window(self, name: str) -> ThrottleResult:
        """Check fixed window throttle."""
        config = self._configs[name]
        now = time.time()
        bucket = self._buckets[name]
        window_start = bucket["window_start"]
        window_end = window_start + config.window_seconds

        if now > window_end:
            bucket["window_start"] = now
            bucket["count"] = 0
            self._windows[name] = []

        if bucket["count"] < config.rate:
            bucket["count"] += 1
            return ThrottleResult(
                allowed=True,
                wait_time=0,
                current_rate=bucket["count"] / config.window_seconds,
                remaining=int(config.rate - bucket["count"]),
            )
        else:
            wait_time = window_end - now
            return ThrottleResult(
                allowed=False,
                wait_time=wait_time,
                current_rate=config.rate / config.window_seconds,
                remaining=0,
            )

    def _check_leaky_bucket(self, name: str) -> ThrottleResult:
        """Check leaky bucket throttle."""
        config = self._configs[name]
        bucket = self._buckets[name]
        now = time.time()
        elapsed = now - bucket["last_update"]

        leak_rate = config.rate / config.window_seconds
        bucket["level"] = max(0, bucket["level"] - elapsed * leak_rate)
        bucket["last_update"] = now

        max_level = config.burst_size or config.rate

        if bucket["level"] < max_level:
            bucket["level"] += 1
            return ThrottleResult(
                allowed=True,
                wait_time=0,
                current_rate=config.rate / config.window_seconds,
                remaining=int(max_level - bucket["level"]),
            )
        else:
            wait_time = 1.0 / leak_rate
            return ThrottleResult(
                allowed=False,
                wait_time=wait_time,
                current_rate=config.rate / config.window_seconds,
                remaining=0,
            )

    async def check(self, name: str) -> ThrottleResult:
        """Check if request is allowed.

        Args:
            name: Throttle configuration name.

        Returns:
            ThrottleResult with decision.
        """
        config = self._configs[name]
        result: ThrottleResult

        if config.throttle_type == ThrottleType.TOKEN_BUCKET:
            result = self._check_token_bucket(name)
        elif config.throttle_type == ThrottleType.SLIDING_WINDOW:
            result = self._check_sliding_window(name)
        elif config.throttle_type == ThrottleType.FIXED_WINDOW:
            result = self._check_fixed_window(name)
        elif config.throttle_type == ThrottleType.LEAKY_BUCKET:
            result = self._check_leaky_bucket(name)
        else:
            result = ThrottleResult(allowed=True)

        if not result.allowed and self._on_throttled:
            self._on_throttled(name, result)

        return result

    async def wait_if_needed(self, name: str) -> float:
        """Wait if throttle would block, then return.

        Args:
            name: Throttle configuration name.

        Returns:
            Actual wait time.
        """
        result = await self.check(name)
        if not result.allowed:
            await asyncio.sleep(result.wait_time)
            return result.wait_time
        return 0

    def get_remaining(self, name: str) -> int:
        """Get remaining requests in current window."""
        config = self._configs[name]

        if config.throttle_type == ThrottleType.TOKEN_BUCKET:
            bucket = self._buckets.get(name, {})
            return int(bucket.get("tokens", 0))
        elif config.throttle_type == ThrottleType.SLIDING_WINDOW:
            return int(config.rate - len(self._windows.get(name, [])))
        elif config.throttle_type == ThrottleType.FIXED_WINDOW:
            bucket = self._buckets.get(name, {})
            return int(config.rate - bucket.get("count", 0))
        elif config.throttle_type == ThrottleType.LEAKY_BUCKET:
            bucket = self._buckets.get(name, {})
            return int((config.burst_size or config.rate) - bucket.get("level", 0))

        return 0
