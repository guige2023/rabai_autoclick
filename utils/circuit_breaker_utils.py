"""
Circuit breaker utilities for fault tolerance and graceful degradation.

Provides circuit breaker state machine, half-open probing,
failure tracking, and fallback execution.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Coroutine, Optional

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = auto()   # Normal operation
    OPEN = auto()     # Failing fast
    HALF_OPEN = auto()  # Testing recovery


@dataclass
class CircuitConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 30.0
    half_open_max_calls: int = 3
    excluded_exceptions: list[type] = field(default_factory=list)


@dataclass
class CircuitStats:
    """Statistics for a circuit breaker."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    state: CircuitState = CircuitState.CLOSED
    last_failure_time: float = 0.0
    last_success_time: float = 0.0
    consecutive_failures: int = 0
    consecutive_successes: int = 0


class CircuitBreaker:
    """Circuit breaker pattern implementation."""

    def __init__(self, name: str, config: Optional[CircuitConfig] = None) -> None:
        self.name = name
        self.config = config or CircuitConfig()
        self._state = CircuitState.CLOSED
        self._stats = CircuitStats()
        self._half_open_calls = 0
        self._lock = asyncio.Lock() if asyncio.get_event_loop().is_running() else None

    async def call(
        self,
        func: Callable[..., Coroutine[Any, Any, Any]],
        *args: Any,
        fallback: Optional[Callable[..., Any]] = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a function with circuit breaker protection."""
        if not await self._can_execute():
            self._stats.rejected_calls += 1
            if fallback:
                logger.info("Circuit %s open, using fallback", self.name)
                return await self._execute_fallback(fallback, *args, **kwargs)
            raise CircuitOpenError(f"Circuit {self.name} is OPEN")

        self._stats.total_calls += 1

        try:
            result = await func(*args, **kwargs)
            await self._on_success()
            return result
        except Exception as e:
            if self._is_excluded(e):
                return None
            await self._on_failure()
            if fallback:
                return await self._execute_fallback(fallback, *args, **kwargs)
            raise

    def _is_excluded(self, error: Exception) -> bool:
        """Check if exception type is excluded from circuit counting."""
        return any(isinstance(error, exc_type) for exc_type in self.config.excluded_exceptions)

    async def _can_execute(self) -> bool:
        """Check if a call can be executed."""
        if self._state == CircuitState.CLOSED:
            return True

        if self._state == CircuitState.OPEN:
            if time.time() - self._stats.last_failure_time >= self.config.timeout:
                await self._transition_to(CircuitState.HALF_OPEN)
                return True
            return False

        if self._state == CircuitState.HALF_OPEN:
            return self._half_open_calls < self.config.half_open_max_calls

        return False

    async def _on_success(self) -> None:
        """Handle successful call."""
        self._stats.consecutive_failures = 0
        self._stats.consecutive_successes += 1
        self._stats.successful_calls += 1
        self._stats.last_success_time = time.time()

        if self._state == CircuitState.HALF_OPEN:
            if self._stats.consecutive_successes >= self.config.success_threshold:
                await self._transition_to(CircuitState.CLOSED)

    async def _on_failure(self) -> None:
        """Handle failed call."""
        self._stats.consecutive_successes = 0
        self._stats.consecutive_failures += 1
        self._stats.failed_calls += 1
        self._stats.last_failure_time = time.time()

        if self._state == CircuitState.HALF_OPEN:
            await self._transition_to(CircuitState.OPEN)
        elif self._stats.consecutive_failures >= self.config.failure_threshold:
            await self._transition_to(CircuitState.OPEN)

    async def _transition_to(self, new_state: CircuitState) -> None:
        """Transition to a new circuit state."""
        logger.info("Circuit %s: %s -> %s", self.name, self._state.name, new_state.name)
        self._state = new_state
        self._stats.state = new_state

        if new_state == CircuitState.HALF_OPEN:
            self._half_open_calls = 0
            self._stats.consecutive_successes = 0
        elif new_state == CircuitState.CLOSED:
            self._stats.consecutive_failures = 0
            self._stats.consecutive_successes = 0

    async def _execute_fallback(self, fallback: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Execute a fallback function."""
        if asyncio.iscoroutinefunction(fallback):
            return await fallback(*args, **kwargs)
        return fallback(*args, **kwargs)

    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def stats(self) -> CircuitStats:
        return self._stats

    def reset(self) -> None:
        """Reset the circuit breaker to closed state."""
        self._state = CircuitState.CLOSED
        self._stats = CircuitStats()
        self._half_open_calls = 0
        logger.info("Circuit %s reset", self.name)

    def get_health(self) -> dict[str, Any]:
        """Get circuit breaker health metrics."""
        return {
            "name": self.name,
            "state": self._state.name,
            "stats": {
                "total_calls": self._stats.total_calls,
                "successful_calls": self._stats.successful_calls,
                "failed_calls": self._stats.failed_calls,
                "rejected_calls": self._stats.rejected_calls,
                "consecutive_failures": self._stats.consecutive_failures,
                "success_rate": (
                    self._stats.successful_calls / self._stats.total_calls * 100
                    if self._stats.total_calls > 0 else 100.0
                ),
            },
        }


class CircuitOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass


class CircuitBreakerRegistry:
    """Registry for managing multiple circuit breakers."""

    def __init__(self) -> None:
        self._breakers: dict[str, CircuitBreaker] = {}

    def get_or_create(self, name: str, config: Optional[CircuitConfig] = None) -> CircuitBreaker:
        """Get an existing circuit breaker or create a new one."""
        if name not in self._breakers:
            self._breakers[name] = CircuitBreaker(name, config)
        return self._breakers[name]

    def get(self, name: str) -> Optional[CircuitBreaker]:
        """Get a circuit breaker by name."""
        return self._breakers.get(name)

    def all_stats(self) -> dict[str, dict[str, Any]]:
        """Get stats for all circuit breakers."""
        return {name: cb.get_health() for name, cb in self._breakers.items()}

    def reset_all(self) -> None:
        """Reset all circuit breakers."""
        for cb in self._breakers.values():
            cb.reset()
