"""Circuit breaker pattern implementation.

This module provides circuit breaker functionality:
- Failure tracking and threshold detection
- State transitions (closed, open, half-open)
- Automatic recovery attempts
- Fallback handling

Example:
    >>> from actions.circuit_breaker_action import CircuitBreaker
    >>> cb = CircuitBreaker(failure_threshold=5)
    >>> result = cb.call(failing_function, fallback=fallback_func)
"""

from __future__ import annotations

import time
import threading
import logging
from typing import Any, Callable, Optional
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    """Configuration for a circuit breaker."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 60.0
    half_open_max_calls: int = 3
    exclude_exceptions: tuple[type, ...] = ()


class CircuitBreaker:
    """Circuit breaker for protecting against cascading failures.

    Attributes:
        name: Circuit breaker name.
    """

    def __init__(
        self,
        name: str = "circuit-breaker",
        failure_threshold: int = 5,
        success_threshold: int = 2,
        timeout: float = 60.0,
        half_open_max_calls: int = 3,
        exclude_exceptions: tuple[type, ...] = (),
    ) -> None:
        self.name = name
        self.config = CircuitBreakerConfig(
            failure_threshold=failure_threshold,
            success_threshold=success_threshold,
            timeout=timeout,
            half_open_max_calls=half_open_max_calls,
            exclude_exceptions=exclude_exceptions,
        )
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0
        self._lock = threading.RLock()
        logger.info(f"CircuitBreaker '{name}' initialized")

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            self._check_state_transition()
            return self._state

    def call(
        self,
        func: Callable[..., Any],
        *args: Any,
        fallback: Optional[Callable[..., Any]] = None,
        **kwargs: Any,
    ) -> Any:
        """Call a function through the circuit breaker.

        Args:
            func: Function to call.
            *args: Positional arguments for the function.
            fallback: Optional fallback function.
            **kwargs: Keyword arguments for the function.

        Returns:
            Function result or fallback value.

        Raises:
            Exception: If function fails and no fallback provided.
        """
        if self.state == CircuitState.OPEN:
            if fallback:
                logger.debug(f"Circuit OPEN, calling fallback for {func.__name__}")
                return fallback(*args, **kwargs)
            raise CircuitBreakerOpenError(f"Circuit breaker '{self.name}' is OPEN")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            if isinstance(e, self.config.exclude_exceptions):
                logger.debug(f"Excluded exception in {func.__name__}: {e}")
                return None
            self._on_failure()
            if fallback:
                logger.warning(f"{func.__name__} failed, calling fallback")
                return fallback(*args, **kwargs)
            raise

    def _on_success(self) -> None:
        """Handle successful call."""
        with self._lock:
            self._failure_count = 0
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._transition_to(CircuitState.CLOSED)
                    logger.info(f"Circuit '{self.name}' CLOSED (recovered)")

    def _on_failure(self) -> None:
        """Handle failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._state == CircuitState.HALF_OPEN:
                self._transition_to(CircuitState.OPEN)
                logger.warning(f"Circuit '{self.name}' OPEN (half-open failure)")
            elif self._failure_count >= self.config.failure_threshold:
                self._transition_to(CircuitState.OPEN)
                logger.warning(f"Circuit '{self.name}' OPEN (threshold reached)")

    def _check_state_transition(self) -> None:
        """Check if automatic state transition is needed."""
        if self._state == CircuitState.OPEN:
            if self._last_failure_time:
                elapsed = time.time() - self._last_failure_time
                if elapsed >= self.config.timeout:
                    self._transition_to(CircuitState.HALF_OPEN)

    def _transition_to(self, new_state: CircuitState) -> None:
        """Transition to a new state."""
        if self._state == new_state:
            return
        logger.info(f"Circuit '{self.name}' transition: {self._state.value} -> {new_state.value}")
        self._state = new_state
        if new_state == CircuitState.CLOSED:
            self._failure_count = 0
            self._success_count = 0
        elif new_state == CircuitState.HALF_OPEN:
            self._half_open_calls = 0
            self._success_count = 0

    def reset(self) -> None:
        """Reset the circuit breaker to closed state."""
        with self._lock:
            self._transition_to(CircuitState.CLOSED)
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time = None
            logger.info(f"Circuit '{self.name}' reset")

    def get_stats(self) -> dict[str, Any]:
        """Get circuit breaker statistics."""
        with self._lock:
            return {
                "name": self.name,
                "state": self.state.value,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "last_failure_time": self._last_failure_time,
            }


class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass


class CircuitBreakerRegistry:
    """Registry for managing multiple circuit breakers."""

    def __init__(self) -> None:
        self._breakers: dict[str, CircuitBreaker] = {}
        self._lock = threading.Lock()

    def register(
        self,
        name: str,
        **kwargs: Any,
    ) -> CircuitBreaker:
        """Register a circuit breaker.

        Args:
            name: Circuit breaker name.
            **kwargs: Configuration options.

        Returns:
            The registered circuit breaker.
        """
        with self._lock:
            if name in self._breakers:
                return self._breakers[name]
            cb = CircuitBreaker(name=name, **kwargs)
            self._breakers[name] = cb
            return cb

    def get(self, name: str) -> Optional[CircuitBreaker]:
        """Get a circuit breaker by name."""
        return self._breakers.get(name)

    def get_all_stats(self) -> dict[str, dict[str, Any]]:
        """Get statistics for all circuit breakers."""
        return {name: cb.get_stats() for name, cb in self._breakers.items()}
