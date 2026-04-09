"""
Circuit Breaker Action Module.

Implements circuit breaker pattern for fault tolerance.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Coroutine, Dict, Optional, TypeVar, Generic


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


@dataclass
class CircuitConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: float = 30.0
    half_open_max_calls: int = 3


@dataclass
class CircuitStats:
    """Circuit breaker statistics."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    state_changes: int = 0
    last_failure_time: Optional[float] = None
    last_success_time: Optional[float] = None


T = TypeVar("T")


class CircuitBreakerAction:
    """
    Circuit breaker for preventing cascade failures.

    States:
    - CLOSED: Normal operation, calls pass through
    - OPEN: Failures exceeded threshold, calls rejected
    - HALF_OPEN: Testing if service recovered
    """

    def __init__(
        self,
        name: str,
        config: Optional[CircuitConfig] = None,
    ) -> None:
        self.name = name
        self.config = config or CircuitConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0
        self._stats = CircuitStats()
        self._lock = asyncio.Lock()

    async def call(
        self,
        func: Callable[..., Coroutine[Any, Any, T]],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """
        Execute a call through the circuit breaker.

        Args:
            func: Async function to call
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Result from func

        Raises:
            CircuitBreakerOpen: If circuit is open
        """
        await self._lock

        if self._state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self._state = CircuitState.HALF_OPEN
                self._half_open_calls = 0
                self._stats.state_changes += 1
            else:
                self._stats.rejected_calls += 1
                raise CircuitBreakerOpen(f"Circuit {self.name} is OPEN")

        if self._state == CircuitState.HALF_OPEN:
            if self._half_open_calls >= self.config.half_open_max_calls:
                self._stats.rejected_calls += 1
                raise CircuitBreakerOpen(
                    f"Circuit {self.name} is HALF_OPEN, max calls reached"
                )
            self._half_open_calls += 1

        self._lock.release()

        self._stats.total_calls += 1

        try:
            result = await func(*args, **kwargs)
            await self._on_success()
            return result
        except Exception as e:
            await self._on_failure()
            raise e

    async def _on_success(self) -> None:
        """Handle successful call."""
        async with self._lock:
            self._stats.successful_calls += 1
            self._stats.last_success_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
                    self._stats.state_changes += 1
            elif self._state == CircuitState.CLOSED:
                self._failure_count = max(0, self._failure_count - 1)

    async def _on_failure(self) -> None:
        """Handle failed call."""
        async with self._lock:
            self._stats.failed_calls += 1
            self._stats.last_failure_time = time.time()
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                self._stats.state_changes += 1
            elif self._failure_count >= self.config.failure_threshold:
                self._state = CircuitState.OPEN
                self._stats.state_changes += 1

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self._last_failure_time is None:
            return True
        return time.time() - self._last_failure_time >= self.config.timeout_seconds

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        return self._state

    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        return {
            "name": self.name,
            "state": self._state.name,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "total_calls": self._stats.total_calls,
            "successful_calls": self._stats.successful_calls,
            "failed_calls": self._stats.failed_calls,
            "rejected_calls": self._stats.rejected_calls,
            "state_changes": self._stats.state_changes,
            "last_failure_time": self._stats.last_failure_time,
            "last_success_time": self._stats.last_success_time,
        }

    async def reset(self) -> None:
        """Manually reset the circuit breaker."""
        async with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._half_open_calls = 0
            self._stats.state_changes += 1


class CircuitBreakerOpen(Exception):
    """Exception raised when circuit is open."""
    pass
