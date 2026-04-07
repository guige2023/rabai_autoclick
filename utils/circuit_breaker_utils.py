"""
Circuit Breaker Pattern Implementation

Prevents cascading failures by failing fast when a service is unhealthy.
"""

from __future__ import annotations

import copy
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = auto()      # Normal operation
    OPEN = auto()        # Failing fast
    HALF_OPEN = auto()   # Testing recovery


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5          # Failures before opening
    success_threshold: int = 2        # Successes in half-open to close
    timeout_seconds: float = 30.0     # Time before trying half-open
    expected_exceptions: tuple[type[Exception], ...] = (Exception,)
    excluded_exceptions: tuple[type[Exception], ...] = ()


@dataclass
class CircuitBreakerMetrics:
    """Metrics for circuit breaker."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    state_changes: int = 0
    last_failure: float | None = None
    last_success: float | None = None
    consecutive_failures: int = 0
    consecutive_successes: int = 0


class CircuitBreaker(Generic[T]):
    """
    Circuit breaker implementation.

    Prevents repeated calls to a failing service by opening the circuit.
    """

    def __init__(self, config: CircuitBreakerConfig | None = None):
        self._config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._metrics = CircuitBreakerMetrics()
        self._last_state_change = time.time()
        self._lock = threading.RLock()

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                # Check if we should transition to half-open
                if time.time() - self._last_state_change >= self._config.timeout_seconds:
                    self._transition_to(CircuitState.HALF_OPEN)
            return self._state

    @property
    def metrics(self) -> CircuitBreakerMetrics:
        """Get circuit breaker metrics."""
        with self._lock:
            return copy.copy(self._metrics)

    def call(self, func: Callable[[], T], *args: Any, **kwargs: Any) -> T:
        """
        Execute a function through the circuit breaker.

        Args:
            func: Function to execute.
            *args: Positional arguments for the function.
            **kwargs: Keyword arguments for the function.

        Returns:
            The result of the function.

        Raises:
            CircuitBreakerOpen: If the circuit is open.
            Exception: If the function raises an exception.
        """
        # Check if circuit allows the call
        if not self._allow_request():
            self._metrics.rejected_calls += 1
            raise CircuitBreakerOpen(f"Circuit is {self.state.name}")

        self._metrics.total_calls += 1

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result

        except Exception as e:
            if self._is_expected_exception(e):
                self._on_failure()
                raise
            else:
                # Excluded exception - treat as success
                self._on_success()
                raise

    def _allow_request(self) -> bool:
        """Check if a request should be allowed."""
        with self._lock:
            if self._state == CircuitState.CLOSED:
                return True

            if self._state == CircuitState.OPEN:
                # Check timeout
                if time.time() - self._last_state_change >= self._config.timeout_seconds:
                    self._transition_to(CircuitState.HALF_OPEN)
                    return True
                return False

            # HALF_OPEN - allow limited requests
            return True

    def _on_success(self) -> None:
        """Handle successful call."""
        with self._lock:
            self._metrics.successful_calls += 1
            self._metrics.consecutive_successes += 1
            self._metrics.consecutive_failures = 0
            self._metrics.last_success = time.time()

            if self._state == CircuitState.HALF_OPEN:
                if self._metrics.consecutive_successes >= self._config.success_threshold:
                    self._transition_to(CircuitState.CLOSED)

    def _on_failure(self) -> None:
        """Handle failed call."""
        with self._lock:
            self._metrics.failed_calls += 1
            self._metrics.consecutive_failures += 1
            self._metrics.consecutive_successes = 0
            self._metrics.last_failure = time.time()

            if self._state == CircuitState.CLOSED:
                if self._metrics.consecutive_failures >= self._config.failure_threshold:
                    self._transition_to(CircuitState.OPEN)

            elif self._state == CircuitState.HALF_OPEN:
                self._transition_to(CircuitState.OPEN)

    def _transition_to(self, new_state: CircuitState) -> None:
        """Transition to a new state."""
        if self._state == new_state:
            return

        self._state = new_state
        self._last_state_change = time.time()
        self._metrics.state_changes += 1

        if new_state == CircuitState.HALF_OPEN:
            self._metrics.consecutive_successes = 0

        elif new_state == CircuitState.CLOSED:
            self._metrics.consecutive_failures = 0
            self._metrics.consecutive_successes = 0

    def _is_expected_exception(self, exception: Exception) -> bool:
        """Check if an exception should trigger the circuit breaker."""
        # Check excluded exceptions first
        for exc_type in self._config.excluded_exceptions:
            if isinstance(exception, exc_type):
                return False

        # Check expected exceptions
        for exc_type in self._config.expected_exceptions:
            if isinstance(exception, exc_type):
                return True

        return True

    def reset(self) -> None:
        """Manually reset the circuit breaker."""
        with self._lock:
            self._transition_to(CircuitState.CLOSED)
            self._metrics.consecutive_failures = 0
            self._metrics.consecutive_successes = 0


class CircuitBreakerOpen(Exception):
    """Exception raised when circuit breaker is open."""
    pass


class CircuitBreakerDecorator(Generic[T]):
    """
    Decorator for adding circuit breaker to a function.
    """

    def __init__(
        self,
        config: CircuitBreakerConfig | None = None,
        name: str = "",
    ):
        self._breaker = CircuitBreaker(config)
        self._name = name or "circuit_breaker"

    def __call__(self, func: Callable[..., T]) -> Callable[..., T]:
        """Decorate a function with circuit breaker."""

        def wrapper(*args: Any, **kwargs: Any) -> T:
            return self._breaker.call(func, *args, **kwargs)

        wrapper.breaker = self._breaker
        wrapper.__name__ = func.__name__
        return wrapper

    @property
    def breaker(self) -> CircuitBreaker[T]:
        """Access the underlying circuit breaker."""
        return self._breaker


def circuit_breaker(
    config: CircuitBreakerConfig | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator to add circuit breaker to a function.

    Usage:
        @circuit_breaker( CircuitBreakerConfig(failure_threshold=3) )
        def my_function():
            ...
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        cb = CircuitBreaker(config)

        def wrapper(*args: Any, **kwargs: Any) -> T:
            return cb.call(func, *args, **kwargs)

        wrapper.breaker = cb
        wrapper.__name__ = func.__name__
        return wrapper

    return decorator
