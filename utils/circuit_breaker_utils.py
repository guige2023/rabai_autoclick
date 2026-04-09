"""
Circuit Breaker Pattern Implementation.

Implements the circuit breaker pattern for handling distributed system failures
with configurable failure thresholds and automatic recovery.

Example:
    >>> breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)
    >>> with breaker:
    ...     call_remote_service()
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Optional, TypeVar

T = TypeVar('T')


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior."""
    failure_threshold: int = 5
    success_threshold: int = 3
    recovery_timeout: float = 60.0
    half_open_max_calls: int = 3
    excluded_exceptions: tuple[type[Exception], ...] = ()


@dataclass
class CircuitBreakerStats:
    """Statistics for circuit breaker monitoring."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    state_changes: int = 0
    last_failure_time: float = 0.0
    last_success_time: float = 0.0
    consecutive_failures: int = 0
    consecutive_successes: int = 0


class CircuitBreakerError(Exception):
    """Raised when circuit breaker is open."""
    pass


class CircuitBreaker:
    """
    Circuit breaker pattern implementation.

    Prevents cascading failures by failing fast when a service is experiencing
    problems. The circuit transitions between CLOSED, OPEN, and HALF_OPEN states.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        success_threshold: int = 3,
        recovery_timeout: float = 60.0,
        half_open_max_calls: int = 3,
        excluded_exceptions: tuple[type[Exception], ...] = ()
    ):
        """
        Initialize circuit breaker.

        Args:
            failure_threshold: Number of failures before opening circuit
            success_threshold: Number of successes in half-open to close circuit
            recovery_timeout: Seconds to wait before trying recovery
            half_open_max_calls: Max concurrent calls in half-open state
            excluded_exceptions: Exception types that don't count as failures
        """
        self._config = CircuitBreakerConfig(
            failure_threshold=failure_threshold,
            success_threshold=success_threshold,
            recovery_timeout=recovery_timeout,
            half_open_max_calls=half_open_max_calls,
            excluded_exceptions=excluded_exceptions
        )
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: float = 0
        self._half_open_calls = 0
        self._lock = threading.Lock()
        self._stats = CircuitBreakerStats()

    def __enter__(self):
        self._check_before_call()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self._record_failure(exc_type)
        else:
            self._record_success()
        return False

    def _check_before_call(self) -> None:
        """Check if call is allowed, raise if not."""
        with self._lock:
            if self._state == CircuitState.CLOSED:
                return

            if self._state == CircuitState.OPEN:
                if self._should_attempt_recovery():
                    self._transition_to_half_open()
                else:
                    self._stats.rejected_calls += 1
                    raise CircuitBreakerError("Circuit breaker is OPEN")

            if self._state == CircuitState.HALF_OPEN:
                if self._half_open_calls >= self._config.half_open_max_calls:
                    self._stats.rejected_calls += 1
                    raise CircuitBreakerError("Circuit breaker is HALF_OPEN, max calls reached")
                self._half_open_calls += 1

    def _should_attempt_recovery(self) -> bool:
        """Check if enough time has passed to attempt recovery."""
        if self._last_failure_time == 0:
            return True
        return (time.time() - self._last_failure_time) >= self._config.recovery_timeout

    def _transition_to_half_open(self) -> None:
        """Transition from OPEN to HALF_OPEN state."""
        self._state = CircuitState.HALF_OPEN
        self._half_open_calls = 1
        self._success_count = 0
        self._stats.state_changes += 1

    def _record_failure(self, exc_type: type[Exception]) -> None:
        """Record a failed call."""
        with self._lock:
            self._stats.failed_calls += 1
            self._stats.consecutive_failures += 1
            self._stats.consecutive_successes = 0
            self._last_failure_time = time.time()

            if exc_type in self._config.excluded_exceptions:
                return

            if self._state == CircuitState.HALF_OPEN:
                self._transition_to_open()
            elif self._state == CircuitState.CLOSED:
                self._failure_count += 1
                if self._failure_count >= self._config.failure_threshold:
                    self._transition_to_open()

    def _transition_to_open(self) -> None:
        """Transition to OPEN state."""
        self._state = CircuitState.OPEN
        self._stats.state_changes += 1

    def _record_success(self) -> None:
        """Record a successful call."""
        with self._lock:
            self._stats.successful_calls += 1
            self._stats.consecutive_successes += 1
            self._stats.consecutive_failures = 0
            self._last_success_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self._config.success_threshold:
                    self._transition_to_closed()

    def _transition_to_closed(self) -> None:
        """Transition from HALF_OPEN to CLOSED state."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._half_open_calls = 0
        self._stats.state_changes += 1

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        return self._state

    def get_stats(self) -> CircuitBreakerStats:
        """Get circuit breaker statistics."""
        with self._lock:
            return CircuitBreakerStats(
                total_calls=self._stats.total_calls,
                successful_calls=self._stats.successful_calls,
                failed_calls=self._stats.failed_calls,
                rejected_calls=self._stats.rejected_calls,
                state_changes=self._stats.state_changes,
                last_failure_time=self._last_failure_time,
                last_success_time=self._stats.last_success_time,
                consecutive_failures=self._stats.consecutive_failures,
                consecutive_successes=self._stats.consecutive_successes
            )

    def reset(self) -> None:
        """Manually reset the circuit breaker to closed state."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._half_open_calls = 0
            self._stats.state_changes += 1

    def allow_request(self) -> bool:
        """
        Check if a request is allowed without entering context manager.

        Returns:
            True if request is allowed, False otherwise
        """
        try:
            self._check_before_call()
            return True
        except CircuitBreakerError:
            return False


def circuit_breaker(
    failure_threshold: int = 5,
    success_threshold: int = 3,
    recovery_timeout: float = 60.0
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator to add circuit breaker to a function.

    Args:
        failure_threshold: Number of failures before opening circuit
        success_threshold: Number of successes in half-open to close
        recovery_timeout: Seconds to wait before trying recovery

    Returns:
        Decorated function with circuit breaker
    """
    breaker = CircuitBreaker(
        failure_threshold=failure_threshold,
        success_threshold=success_threshold,
        recovery_timeout=recovery_timeout
    )

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        def wrapper(*args: Any, **kwargs: Any) -> T:
            breaker._check_before_call()
            try:
                result = func(*args, **kwargs)
                breaker._record_success()
                return result
            except Exception as e:
                breaker._record_failure(type(e))
                raise
        return wrapper
    return decorator
