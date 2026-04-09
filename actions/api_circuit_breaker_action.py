"""
Circuit breaker pattern implementation for API fault tolerance.

This module implements the circuit breaker pattern to prevent cascading
failures and provide graceful degradation when downstream services fail.

Author: RabAiBot
License: MIT
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, Optional, TypeVar, Generic

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = auto()  # Normal operation
    OPEN = auto()    # Failing, reject requests
    HALF_OPEN = auto()  # Testing if service recovered


@dataclass
class CircuitConfig:
    """Configuration for circuit breaker behavior."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 60.0
    half_open_max_calls: int = 3
    excluded_exceptions: tuple = ()


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
    consecutive_failures: int = 0
    consecutive_successes: int = 0

    @property
    def failure_rate(self) -> float:
        """Calculate failure rate."""
        if self.total_calls == 0:
            return 0.0
        return self.failed_calls / self.total_calls


class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open."""
    def __init__(self, name: str, timeout: float):
        self.name = name
        self.timeout = timeout
        super().__init__(f"Circuit breaker '{name}' is OPEN. Retry after {timeout}s")


class CircuitBreaker:
    """
    Circuit breaker for protecting against cascading failures.

    State Machine:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Failures exceeded threshold, requests rejected
    - HALF_OPEN: Testing recovery, limited requests allowed

    Features:
    - Configurable failure/success thresholds
    - Automatic state transitions
    - Exception filtering
    - Statistics tracking
    - Async support

    Example:
        >>> cb = CircuitBreaker("payment_api", failure_threshold=5, timeout=30)
        >>> with cb:
        ...     response = call_payment_api()
    """

    def __init__(
        self,
        name: str,
        config: Optional[CircuitConfig] = None,
        fallback: Optional[Callable[..., T]] = None,
    ):
        """
        Initialize circuit breaker.

        Args:
            name: Identifier for this circuit breaker
            config: Circuit breaker configuration
            fallback: Optional fallback function
        """
        self.name = name
        self.config = config or CircuitConfig()
        self.fallback = fallback
        self._state = CircuitState.CLOSED
        self._stats = CircuitStats()
        self._last_state_change = time.time()
        self._half_open_calls = 0
        logger.info(f"Circuit breaker '{name}' initialized")

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        if self._state == CircuitState.OPEN:
            elapsed = time.time() - self._last_state_change
            if elapsed >= self.config.timeout:
                logger.info(f"Circuit breaker '{self.name}' transitioning OPEN -> HALF_OPEN")
                self._transition_to(CircuitState.HALF_OPEN)
        return self._state

    def _transition_to(self, new_state: CircuitState) -> None:
        """Transition to a new state."""
        old_state = self._state
        self._state = new_state
        self._last_state_change = time.time()
        self._stats.state_changes += 1

        if new_state == CircuitState.HALF_OPEN:
            self._half_open_calls = 0

    def record_success(self) -> None:
        """Record a successful call."""
        self._stats.total_calls += 1
        self._stats.successful_calls += 1
        self._stats.consecutive_successes += 1
        self._stats.consecutive_failures = 0
        self._stats.last_success_time = time.time()

        if self._state == CircuitState.HALF_OPEN:
            if self._stats.consecutive_successes >= self.config.success_threshold:
                logger.info(f"Circuit breaker '{self.name}' HALF_OPEN -> CLOSED")
                self._transition_to(CircuitState.CLOSED)

    def record_failure(self, exception: Optional[Exception] = None) -> None:
        """Record a failed call."""
        self._stats.total_calls += 1
        self._stats.failed_calls += 1
        self._stats.consecutive_failures += 1
        self._stats.consecutive_successes = 0
        self._stats.last_failure_time = time.time()

        if self._state == CircuitState.HALF_OPEN:
            logger.info(f"Circuit breaker '{self.name}' HALF_OPEN -> OPEN (failure)")
            self._transition_to(CircuitState.OPEN)
            return

        if (
            self._state == CircuitState.CLOSED
            and self._stats.consecutive_failures >= self.config.failure_threshold
        ):
            logger.warning(
                f"Circuit breaker '{self.name}' CLOSED -> OPEN "
                f"(failures={self._stats.consecutive_failures})"
            )
            self._transition_to(CircuitState.OPEN)

    def can_execute(self) -> bool:
        """Check if a call can be executed."""
        if self._state == CircuitState.CLOSED:
            return True

        if self._state == CircuitState.OPEN:
            return False

        if self._state == CircuitState.HALF_OPEN:
            return self._half_open_calls < self.config.half_open_max_calls

        return False

    def before_call(self) -> None:
        """Called before executing a wrapped call."""
        if self.state == CircuitState.OPEN:
            self._stats.rejected_calls += 1
            raise CircuitBreakerOpenError(self.name, self.config.timeout)

        if self._state == CircuitState.HALF_OPEN:
            self._half_open_calls += 1

    def __enter__(self) -> "CircuitBreaker":
        """Context manager entry."""
        self.before_call()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Context manager exit."""
        if exc_type is None:
            self.record_success()
        else:
            should_record = True
            for exc_class in self.config.excluded_exceptions:
                if issubclass(exc_type, exc_class):
                    should_record = False
                    break
            if should_record:
                self.record_failure(exc_val)
        return False

    async def __aenter__(self) -> "CircuitBreaker":
        """Async context manager entry."""
        return self.__enter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Async context manager exit."""
        return self.__exit__(exc_type, exc_val, exc_tb)

    def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        """
        Execute a function with circuit breaker protection.

        Args:
            func: Function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Function result

        Raises:
            CircuitBreakerOpenError: If circuit is open
        """
        self.before_call()
        try:
            result = func(*args, **kwargs)
            self.record_success()
            return result
        except Exception as e:
            self.record_failure(e)
            raise

    async def call_async(self, coro) -> T:
        """
        Execute an async coroutine with circuit breaker protection.

        Args:
            coro: Coroutine to execute

        Returns:
            Coroutine result
        """
        self.before_call()
        try:
            result = await coro
            self.record_success()
            return result
        except Exception as e:
            self.record_failure(e)
            raise

    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        return {
            "name": self.name,
            "state": self.state.name,
            "total_calls": self._stats.total_calls,
            "successful_calls": self._stats.successful_calls,
            "failed_calls": self._stats.failed_calls,
            "rejected_calls": self._stats.rejected_calls,
            "failure_rate": self._stats.failure_rate,
            "consecutive_failures": self._stats.consecutive_failures,
            "consecutive_successes": self._stats.consecutive_successes,
            "state_changes": self._stats.state_changes,
            "time_in_state": time.time() - self._last_state_change,
        }

    def reset(self) -> None:
        """Reset the circuit breaker to closed state."""
        self._state = CircuitState.CLOSED
        self._stats = CircuitStats()
        self._half_open_calls = 0
        logger.info(f"Circuit breaker '{self.name}' reset")


class CircuitBreakerManager:
    """
    Manager for multiple circuit breakers.

    Example:
        >>> manager = CircuitBreakerManager()
        >>> cb = manager.get("payment_api")
        >>> with cb:
        ...     process_payment()
    """

    def __init__(self):
        """Initialize the circuit breaker manager."""
        self._breakers: Dict[str, CircuitBreaker] = {}
        logger.info("CircuitBreakerManager initialized")

    def get(
        self,
        name: str,
        config: Optional[CircuitConfig] = None,
        fallback: Optional[Callable[..., Any]] = None,
    ) -> CircuitBreaker:
        """Get or create a circuit breaker by name."""
        if name not in self._breakers:
            self._breakers[name] = CircuitBreaker(name, config, fallback)
        return self._breakers[name]

    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all circuit breakers."""
        return {name: cb.get_stats() for name, cb in self._breakers.items()}

    def reset_all(self) -> None:
        """Reset all circuit breakers."""
        for cb in self._breakers.values():
            cb.reset()
        logger.info("All circuit breakers reset")
