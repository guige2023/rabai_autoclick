"""Circuit breaker pattern implementation for fault-tolerant automation.

Prevents repeated execution of failing operations by opening
the circuit after a threshold of failures, and periodically
testing if the service has recovered.

Example:
    >>> from utils.circuit_breaker_utils import CircuitBreaker, CircuitOpen
    >>> cb = CircuitBreaker(failure_threshold=5, recovery_timeout=30)
    >>> with cb:
    ...     unreliable_action()
"""

from __future__ import annotations

import threading
import time
from enum import Enum
from typing import Callable, Optional, TypeVar

T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpen(Exception):
    """Raised when circuit is open and calls are rejected."""

    def __init__(self, recover_after: float) -> None:
        self.recover_after = recover_after
        super().__init__(f"Circuit is open until {time.ctime(recover_after)}")


class CircuitBreaker:
    """Circuit breaker for preventing cascading failures.

    States:
        CLOSED: Normal operation, requests pass through.
        OPEN: Too many failures, requests are rejected.
        HALF_OPEN: Testing if service recovered.

    Args:
        failure_threshold: Number of failures before opening circuit.
        recovery_timeout: Seconds before attempting recovery test.
        success_threshold: Successes needed in HALF_OPEN to close circuit.
        excluded_exceptions: Exception types that don't count as failures.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        success_threshold: int = 2,
        excluded_exceptions: tuple = (),
    ) -> None:
        if failure_threshold <= 0:
            raise ValueError("failure_threshold must be positive")
        if recovery_timeout <= 0:
            raise ValueError("recovery_timeout must be positive")

        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._success_threshold = success_threshold
        self._excluded_exceptions = excluded_exceptions

        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._state = CircuitState.CLOSED
        self._lock = threading.RLock()

    @property
    def state(self) -> CircuitState:
        """Current circuit state."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if self._should_attempt_recovery():
                    self._state = CircuitState.HALF_OPEN
                    self._success_count = 0
            return self._state

    def _should_attempt_recovery(self) -> bool:
        """Check if enough time has passed to attempt recovery."""
        if self._last_failure_time is None:
            return True
        return (time.monotonic() - self._last_failure_time) >= self._recovery_timeout

    def _record_success(self) -> None:
        """Record a successful call."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self._success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
            elif self._state == CircuitState.CLOSED:
                self._failure_count = max(0, self._failure_count - 1)

    def _record_failure(self, exc: Exception) -> None:
        """Record a failed call."""
        with self._lock:
            if isinstance(exc, self._excluded_exceptions):
                return

            self._failure_count += 1
            self._last_failure_time = time.monotonic()

            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
            elif self._failure_count >= self._failure_threshold:
                self._state = CircuitState.OPEN

    def call(self, fn: Callable[[], T], *args, **kwargs) -> T:
        """Execute a function through the circuit breaker.

        Args:
            fn: Function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Function result.

        Raises:
            CircuitOpen: If circuit is open.
        """
        if self.state == CircuitState.OPEN:
            raise CircuitOpen(
                self._last_failure_time + self._recovery_timeout
                if self._last_failure_time else 0
            )

        try:
            result = fn(*args, **kwargs)
            self._record_success()
            return result
        except Exception as e:
            self._record_failure(e)
            raise

    def __enter__(self) -> CircuitBreaker:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if exc_type is not None:
            self._record_failure(exc_val)
        else:
            self._record_success()
        return False

    def reset(self) -> None:
        """Manually reset the circuit breaker to closed state."""
        with self._lock:
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time = None
            self._state = CircuitState.CLOSED

    @property
    def failure_count(self) -> int:
        """Current failure count."""
        with self._lock:
            return self._failure_count


def circuit_breaker(
    failure_threshold: int = 5,
    recovery_timeout: float = 30.0,
) -> Callable[[Callable[[], T]], Callable[[], T]]:
    """Decorator to add circuit breaker to a function.

    Args:
        failure_threshold: Failures before opening circuit.
        recovery_timeout: Seconds before testing recovery.

    Returns:
        Decorated function with circuit breaker.
    """
    breaker: CircuitBreaker[ T ] = CircuitBreaker(
        failure_threshold=failure_threshold,
        recovery_timeout=recovery_timeout,
    )

    def decorator(fn: Callable[[], T]) -> Callable[[], T]:
        def wrapper(*args, **kwargs) -> T:
            return breaker.call(fn, *args, **kwargs)

        wrapper._circuit_breaker = breaker  # type: ignore
        wrapper._circuit_state = property(lambda self: breaker.state)  # type: ignore
        return wrapper

    return decorator
