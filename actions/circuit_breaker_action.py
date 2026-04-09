"""
Circuit Breaker Action Module.

Provides fault tolerance with circuit breaker pattern,
failure tracking, and automatic recovery.
"""

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 60.0
    half_open_max_calls: int = 3
    excluded_exceptions: tuple = ()


@dataclass
class CircuitBreakerStats:
    """Circuit breaker statistics."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    state_changes: int = 0
    last_state_change: float = field(default_factory=time.time)
    consecutive_failures: int = 0
    consecutive_successes: int = 0

    @property
    def failure_rate(self) -> float:
        if self.total_calls == 0:
            return 0.0
        return self.failed_calls / self.total_calls

    @property
    def current_state(self) -> CircuitState:
        return CircuitState.CLOSED


class CircuitBreaker:
    """Circuit breaker implementation."""

    def __init__(self, config: Optional[CircuitBreakerConfig] = None):
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._stats = CircuitBreakerStats()
        self._last_failure_time: Optional[float] = None
        self._lock = asyncio.Lock()
        self._half_open_calls = 0

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        return self._state

    @property
    def stats(self) -> CircuitBreakerStats:
        """Get circuit breaker stats."""
        return self._stats

    async def _can_execute(self) -> bool:
        """Check if execution is allowed."""
        if self._state == CircuitState.CLOSED:
            return True

        if self._state == CircuitState.HALF_OPEN:
            return self._half_open_calls < self.config.half_open_max_calls

        if self._state == CircuitState.OPEN:
            if self._should_attempt_reset():
                await self._transition_to_half_open()
                return True
            return False

        return False

    def _should_attempt_reset(self) -> bool:
        """Check if should attempt reset after timeout."""
        if self._last_failure_time is None:
            return True
        return (time.time() - self._last_failure_time) >= self.config.timeout

    async def _transition_to_half_open(self) -> None:
        """Transition to half-open state."""
        self._state = CircuitState.HALF_OPEN
        self._half_open_calls = 0
        self._stats.state_changes += 1
        self._stats.last_state_change = time.time()

    async def _transition_to_open(self) -> None:
        """Transition to open state."""
        self._state = CircuitState.OPEN
        self._last_failure_time = time.time()
        self._stats.state_changes += 1
        self._stats.last_state_change = time.time()

    async def _transition_to_closed(self) -> None:
        """Transition to closed state."""
        self._state = CircuitState.CLOSED
        self._stats.consecutive_failures = 0
        self._stats.state_changes += 1
        self._stats.last_state_change = time.time()

    async def record_success(self) -> None:
        """Record successful call."""
        async with self._lock:
            self._stats.total_calls += 1
            self._stats.successful_calls += 1
            self._stats.consecutive_failures = 0
            self._stats.consecutive_successes += 1

            if self._state == CircuitState.HALF_OPEN:
                if self._stats.consecutive_successes >= self.config.success_threshold:
                    await self._transition_to_closed()

    async def record_failure(self) -> None:
        """Record failed call."""
        async with self._lock:
            self._stats.total_calls += 1
            self._stats.failed_calls += 1
            self._stats.consecutive_failures += 1
            self._stats.consecutive_successes = 0

            if self._state == CircuitState.HALF_OPEN:
                await self._transition_to_open()
            elif self._state == CircuitState.CLOSED:
                if self._stats.consecutive_failures >= self.config.failure_threshold:
                    await self._transition_to_open()

    async def record_rejection(self) -> None:
        """Record rejected call."""
        async with self._lock:
            self._stats.rejected_calls += 1

    async def execute(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any
    ) -> T:
        """Execute function with circuit breaker."""
        if not await self._can_execute():
            await self.record_rejection()
            raise CircuitOpenError(
                f"Circuit breaker is {self._state.value}"
            )

        if self._state == CircuitState.HALF_OPEN:
            self._half_open_calls += 1

        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            await self.record_success()
            return result
        except self.config.excluded_exceptions:
            await self.record_success()
            raise
        except Exception:
            await self.record_failure()
            raise


class CircuitOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass


class CircuitBreakerAction:
    """
    Circuit breaker for fault tolerance.

    Example:
        cb = CircuitBreakerAction(
            failure_threshold=5,
            timeout=60.0
        )

        try:
            result = await cb.execute(api_call)
        except CircuitOpenError:
            print("Service unavailable")
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        timeout: float = 60.0,
        excluded_exceptions: tuple = ()
    ):
        config = CircuitBreakerConfig(
            failure_threshold=failure_threshold,
            success_threshold=success_threshold,
            timeout=timeout,
            excluded_exceptions=excluded_exceptions
        )
        self._cb = CircuitBreaker(config)

    async def execute(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any
    ) -> T:
        """Execute with circuit breaker."""
        return await self._cb.execute(func, *args, **kwargs)

    @property
    def state(self) -> CircuitState:
        """Current state."""
        return self._cb.state

    @property
    def stats(self) -> CircuitBreakerStats:
        """Statistics."""
        return self._cb.stats

    async def reset(self) -> None:
        """Manually reset circuit breaker."""
        self._cb._state = CircuitState.CLOSED
        self._cb._stats.consecutive_failures = 0
        self._cb._stats.consecutive_successes = 0
