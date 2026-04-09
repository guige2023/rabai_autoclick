"""
API Debounce Action Module.

Provides debouncing and throttling for API calls
with timing control and queue management.
"""

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional
import uuid


class DebounceStrategy(Enum):
    """Debounce strategies."""
    LEADING = "leading"
    TRAILING = "trailing"
    BOTH = "both"


@dataclass
class DebounceConfig:
    """Debounce configuration."""
    wait: float = 300.0
    strategy: DebounceStrategy = DebounceStrategy.BOTH
    max_wait: float = 5000.0
    leading_timeout: float = 0.0


@dataclass
class ThrottleConfig:
    """Throttle configuration."""
    rate: float = 10.0
    burst: int = 20
    window: float = 1.0


class TokenBucket:
    """Token bucket for rate limiting."""

    def __init__(self, rate: float, burst: int):
        self.rate = rate
        self.burst = burst
        self._tokens = float(burst)
        self._last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_update
            self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
            self._last_update = now

            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False


class APIDebounceAction:
    """
    API debounce and throttle.

    Example:
        debouncer = APIDebounceAction(wait=500, strategy=DebounceStrategy.TRAILING)

        async def my_api_call():
            return await debouncer.debounce(some_function, *args)
    """

    def __init__(
        self,
        wait: float = 300.0,
        strategy: DebounceStrategy = DebounceStrategy.BOTH
    ):
        self.config = DebounceConfig(wait=wait, strategy=strategy)
        self._timers: dict[str, asyncio.Task] = {}
        self._pending_calls: dict[str, tuple] = {}
        self._last_call_time: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def debounce(
        self,
        func: Callable,
        key: str = "default",
        *args: Any,
        **kwargs: Any
    ) -> Any:
        """Debounce function call."""
        call_id = key or str(uuid.uuid4())

        async with self._lock:
            if call_id in self._timers:
                self._timers[call_id].cancel()

            is_leading = self._should_execute_leading(call_id)

            if is_leading:
                result = await self._execute(func, *args, **kwargs)
                self._last_call_time[call_id] = time.monotonic()
                return result

            self._pending_calls[call_id] = (func, args, kwargs)

            timer = asyncio.create_task(self._delayed_execute(call_id))
            self._timers[call_id] = timer

            return None

    def _should_execute_leading(self, key: str) -> bool:
        """Check if leading edge should execute."""
        if self.config.strategy in (DebounceStrategy.LEADING, DebounceStrategy.BOTH):
            last = self._last_call_time.get(key, 0)
            if time.monotonic() - last >= self.config.leading_timeout:
                return True
        return False

    async def _delayed_execute(self, call_id: str) -> None:
        """Execute after delay."""
        try:
            await asyncio.sleep(self.config.wait / 1000.0)
        except asyncio.CancelledError:
            return

        async with self._lock:
            if call_id not in self._pending_calls:
                return

            func, args, kwargs = self._pending_calls.pop(call_id)

        if self.config.strategy in (DebounceStrategy.TRAILING, DebounceStrategy.BOTH):
            await self._execute(func, *args, **kwargs)

        async with self._lock:
            if call_id in self._timers:
                del self._timers[call_id]

    async def _execute(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """Execute function."""
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        return func(*args, **kwargs)

    def cancel(self, key: str = "default") -> None:
        """Cancel pending debounced call."""
        if key in self._timers:
            self._timers[key].cancel()
            del self._timers[key]
        if key in self._pending_calls:
            del self._pending_calls[key]

    def cancel_all(self) -> None:
        """Cancel all pending calls."""
        for timer in self._timers.values():
            timer.cancel()
        self._timers.clear()
        self._pending_calls.clear()


class APIThrottleAction:
    """
    API throttling with token bucket.

    Example:
        throttle = APIThrottleAction(rate=10, burst=20)

        await throttle.acquire()
        result = api_call()
    """

    def __init__(self, rate: float = 10.0, burst: int = 20):
        self.config = ThrottleConfig(rate=rate, burst=burst)
        self._bucket = TokenBucket(rate, burst)

    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire throttle token."""
        return await self._bucket.acquire(tokens)

    async def execute(
        self,
        func: Callable,
        *args: Any,
        **kwargs: Any
    ) -> Any:
        """Execute with throttling."""
        await self.acquire()
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        return func(*args, **kwargs)
