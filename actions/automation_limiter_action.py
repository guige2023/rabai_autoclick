"""Automation Limiter Action Module.

Provides rate limiting, throttling, and quota management for automation
workflows with multiple backoff strategies.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class LimitType(Enum):
    """Rate limit types."""
    TOKEN_BUCKET = "token_bucket"
    LEAKY_BUCKET = "leaky_bucket"
    FIXED_WINDOW = "fixed_window"
    SLIDING_WINDOW = "sliding_window"
    SLIDING_LOG = "sliding_log"


@dataclass
class LimitResult:
    """Result of a limit check."""
    allowed: bool
    remaining: int
    limit: int
    reset_at: float
    retry_after: float = 0.0


@dataclass
class QuotaStatus:
    """Status of a quota."""
    used: int
    limit: int
    remaining: int
    reset_at: float
    window_seconds: int


class TokenBucketLimiter:
    """Token bucket rate limiter."""

    def __init__(
        self,
        rate: float,
        capacity: int,
        tokens: Optional[float] = None
    ):
        self._rate = rate
        self._capacity = capacity
        self._tokens = tokens if tokens is not None else capacity
        self._last_update = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> LimitResult:
        """Attempt to acquire tokens."""
        async with self._lock:
            now = time.time()
            elapsed = now - self._last_update

            # Refill tokens based on elapsed time
            self._tokens = min(
                self._capacity,
                self._tokens + elapsed * self._rate
            )
            self._last_update = now

            if self._tokens >= tokens:
                self._tokens -= tokens
                return LimitResult(
                    allowed=True,
                    remaining=int(self._tokens),
                    limit=self._capacity,
                    reset_at=now + (self._capacity - self._tokens) / self._rate
                )
            else:
                retry_after = (tokens - self._tokens) / self._rate
                return LimitResult(
                    allowed=False,
                    remaining=int(self._tokens),
                    limit=self._capacity,
                    reset_at=now + retry_after,
                    retry_after=retry_after
                )

    async def reset(self) -> None:
        """Reset the bucket to full capacity."""
        async with self._lock:
            self._tokens = self._capacity


class LeakyBucketLimiter:
    """Leaky bucket rate limiter."""

    def __init__(self, rate: float, capacity: int):
        self._rate = rate
        self._capacity = capacity
        self._level = 0.0
        self._last_leak = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> LimitResult:
        """Attempt to add to the bucket."""
        async with self._lock:
            now = time.time()
            elapsed = now - self._last_leak

            # Leak tokens
            leaked = elapsed * self._rate
            self._level = max(0, self._level - leaked)
            self._last_leak = now

            if self._level + tokens <= self._capacity:
                self._level += tokens
                return LimitResult(
                    allowed=True,
                    remaining=int(self._capacity - self._level),
                    limit=self._capacity,
                    reset_at=now + (self._level / self._rate)
                )
            else:
                wait_time = (self._level + tokens - self._capacity) / self._rate
                return LimitResult(
                    allowed=False,
                    remaining=int(self._capacity - self._level),
                    limit=self._capacity,
                    reset_at=now + wait_time,
                    retry_after=wait_time
                )

    async def reset(self) -> None:
        """Reset the bucket."""
        async with self._lock:
            self._level = 0.0


class FixedWindowLimiter:
    """Fixed window rate limiter."""

    def __init__(self, limit: int, window_seconds: int):
        self._limit = limit
        self._window = window_seconds
        self._counts: Dict[str, Tuple[int, float]] = {}  # key -> (count, window_start)
        self._lock = asyncio.Lock()

    async def acquire(self, key: str = "default") -> LimitResult:
        """Attempt to acquire within the window."""
        async with self._lock:
            now = time.time()
            window_start = int(now / self._window) * self._window

            if key in self._counts:
                count, stored_start = self._counts[key]
                if stored_start == window_start:
                    if count < self._limit:
                        self._counts[key] = (count + 1, window_start)
                        return LimitResult(
                            allowed=True,
                            remaining=self._limit - count - 1,
                            limit=self._limit,
                            reset_at=window_start + self._window
                        )
                    else:
                        return LimitResult(
                            allowed=False,
                            remaining=0,
                            limit=self._limit,
                            reset_at=window_start + self._window,
                            retry_after=window_start + self._window - now
                        )
                else:
                    self._counts[key] = (1, window_start)
                    return LimitResult(
                        allowed=True,
                        remaining=self._limit - 1,
                        limit=self._limit,
                        reset_at=window_start + self._window
                    )
            else:
                self._counts[key] = (1, window_start)
                return LimitResult(
                    allowed=True,
                    remaining=self._limit - 1,
                    limit=self._limit,
                    reset_at=window_start + self._window
                )

    async def reset(self, key: str = "default") -> None:
        """Reset the counter for a key."""
        async with self._lock:
            self._counts.pop(key, None)


class SlidingWindowLimiter:
    """Sliding window rate limiter."""

    def __init__(self, limit: int, window_seconds: int):
        self._limit = limit
        self._window = window_seconds
        self._timestamps: Dict[str, List[float]] = defaultdict(list)
        self._lock = asyncio.Lock()

    async def acquire(self, key: str = "default") -> LimitResult:
        """Attempt to acquire within the sliding window."""
        async with self._lock:
            now = time.time()
            window_start = now - self._window

            # Remove old timestamps
            self._timestamps[key] = [
                ts for ts in self._timestamps[key]
                if ts > window_start
            ]

            if len(self._timestamps[key]) < self._limit:
                self._timestamps[key].append(now)
                return LimitResult(
                    allowed=True,
                    remaining=self._limit - len(self._timestamps[key]),
                    limit=self._limit,
                    reset_at=now + self._window
                )
            else:
                oldest = min(self._timestamps[key])
                retry_after = oldest + self._window - now
                return LimitResult(
                    allowed=False,
                    remaining=0,
                    limit=self._limit,
                    reset_at=oldest + self._window,
                    retry_after=max(0, retry_after)
                )

    async def reset(self, key: str = "default") -> None:
        """Reset the timestamps for a key."""
        async with self._lock:
            self._timestamps.pop(key, None)


class QuotaManager:
    """Manages quotas for different resources."""

    def __init__(self):
        self._quotas: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def set_quota(
        self,
        name: str,
        limit: int,
        window_seconds: int
    ) -> None:
        """Set a quota."""
        async with self._lock:
            self._quotas[name] = {
                "limit": limit,
                "window_seconds": window_seconds,
                "used": 0,
                "reset_at": time.time() + window_seconds
            }

    async def check_quota(self, name: str) -> Optional[QuotaStatus]:
        """Check quota status."""
        async with self._lock:
            quota = self._quotas.get(name)
            if not quota:
                return None

            now = time.time()
            if now >= quota["reset_at"]:
                quota["used"] = 0
                quota["reset_at"] = now + quota["window_seconds"]

            return QuotaStatus(
                used=quota["used"],
                limit=quota["limit"],
                remaining=quota["limit"] - quota["used"],
                reset_at=quota["reset_at"],
                window_seconds=quota["window_seconds"]
            )

    async def consume(self, name: str, amount: int = 1) -> Tuple[bool, QuotaStatus]:
        """Consume from a quota."""
        async with self._lock:
            quota = self._quotas.get(name)
            if not quota:
                return False, QuotaStatus(0, 0, 0, 0, 0)

            now = time.time()
            if now >= quota["reset_at"]:
                quota["used"] = 0
                quota["reset_at"] = now + quota["window_seconds"]

            if quota["used"] + amount <= quota["limit"]:
                quota["used"] += amount
                return True, QuotaStatus(
                    used=quota["used"],
                    limit=quota["limit"],
                    remaining=quota["limit"] - quota["used"],
                    reset_at=quota["reset_at"],
                    window_seconds=quota["window_seconds"]
                )
            else:
                return False, QuotaStatus(
                    used=quota["used"],
                    limit=quota["limit"],
                    remaining=0,
                    reset_at=quota["reset_at"],
                    window_seconds=quota["window_seconds"]
                )

    async def reset_quota(self, name: str) -> bool:
        """Reset a quota."""
        async with self._lock:
            if name in self._quotas:
                self._quotas[name]["used"] = 0
                self._quotas[name]["reset_at"] = time.time() + self._quotas[name]["window_seconds"]
                return True
            return False


class AutomationLimiterAction:
    """Main action class for rate limiting and quotas."""

    def __init__(self):
        self._limiters: Dict[str, Any] = {}
        self._quota_manager = QuotaManager()
        self._default_type = LimitType.TOKEN_BUCKET

    def create_limiter(
        self,
        name: str,
        limit_type: LimitType,
        rate: float,
        capacity: int
    ) -> None:
        """Create a named limiter."""
        if limit_type == LimitType.TOKEN_BUCKET:
            self._limiters[name] = TokenBucketLimiter(rate, capacity)
        elif limit_type == LimitType.LEAKY_BUCKET:
            self._limiters[name] = LeakyBucketLimiter(rate, capacity)
        elif limit_type == LimitType.FIXED_WINDOW:
            self._limiters[name] = FixedWindowLimiter(int(rate), int(capacity))
        elif limit_type == LimitType.SLIDING_WINDOW:
            self._limiters[name] = SlidingWindowLimiter(int(rate), int(capacity))
        else:
            raise ValueError(f"Unknown limit type: {limit_type}")

    async def acquire(
        self,
        name: str,
        tokens: int = 1
    ) -> LimitResult:
        """Acquire from a limiter."""
        limiter = self._limiters.get(name)
        if not limiter:
            # Create default token bucket
            self.create_limiter(name, self._default_type, 10, 10)
            limiter = self._limiters[name]
        return await limiter.acquire(tokens)

    async def reset_limiter(self, name: str) -> bool:
        """Reset a limiter."""
        limiter = self._limiters.get(name)
        if limiter and hasattr(limiter, "reset"):
            await limiter.reset()
            return True
        return False

    async def execute(
        self,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the automation limiter action.

        Args:
            context: Dictionary containing:
                - operation: Operation to perform
                - name: Limiter/quota name
                - Other operation-specific fields

        Returns:
            Dictionary with limit results.
        """
        operation = context.get("operation", "acquire")

        if operation == "acquire":
            name = context.get("name", "default")
            tokens = context.get("tokens", 1)

            result = await self.acquire(name, tokens)
            return {
                "success": True,
                "allowed": result.allowed,
                "remaining": result.remaining,
                "limit": result.limit,
                "reset_at": result.reset_at,
                "retry_after": round(result.retry_after, 3)
            }

        elif operation == "create_limiter":
            name = context.get("name", "default")
            limit_type_str = context.get("type", "token_bucket")
            rate = context.get("rate", 10)
            capacity = context.get("capacity", 10)

            try:
                limit_type = LimitType(limit_type_str)
            except ValueError:
                limit_type = self._default_type

            self.create_limiter(name, limit_type, rate, capacity)
            return {"success": True, "name": name}

        elif operation == "reset_limiter":
            name = context.get("name", "default")
            success = await self.reset_limiter(name)
            return {"success": success}

        elif operation == "set_quota":
            name = context.get("name", "")
            limit = context.get("limit", 100)
            window = context.get("window_seconds", 60)
            await self._quota_manager.set_quota(name, limit, window)
            return {"success": True}

        elif operation == "check_quota":
            name = context.get("name", "")
            status = await self._quota_manager.check_quota(name)
            if status:
                return {
                    "success": True,
                    "quota": {
                        "used": status.used,
                        "limit": status.limit,
                        "remaining": status.remaining,
                        "reset_at": status.reset_at,
                        "window_seconds": status.window_seconds
                    }
                }
            return {"success": False, "error": "Quota not found"}

        elif operation == "consume_quota":
            name = context.get("name", "")
            amount = context.get("amount", 1)
            success, status = await self._quota_manager.consume(name, amount)
            return {
                "success": success,
                "quota": {
                    "used": status.used,
                    "limit": status.limit,
                    "remaining": status.remaining
                }
            }

        elif operation == "reset_quota":
            name = context.get("name", "")
            success = await self._quota_manager.reset_quota(name)
            return {"success": success}

        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
