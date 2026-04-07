"""Circuit breaker pattern utilities.

Prevents cascading failures by wrapping operations in a circuit breaker
that trips after repeated failures and auto-recovers.

Example:
    cb = CircuitBreaker(failure_threshold=5, recovery_timeout=60)
    with cb:
        call_remote_service()
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, ParamSpec, TypeVar

logger = logging.getLogger(__name__)

P = ParamSpec("P")
T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"    # Normal operation, requests pass through
    OPEN = "open"        # Failing fast, no requests allowed
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior."""
    failure_threshold: int = 5
    success_threshold: int = 2
    recovery_timeout: float = 60.0
    half_open_max_calls: int = 3
    excluded_exceptions: tuple[type[Exception], ...] = ()
    monitor_window: float = 60.0


class CircuitBreakerOpen(Exception):
    """Raised when circuit breaker is open and calls are rejected."""
    pass


class CircuitBreaker:
    """Circuit breaker implementation for fault tolerance.

    States:
    - CLOSED: Normal operation. Failures increment counter.
    - OPEN: After failure_threshold failures. All calls rejected.
    - HALF_OPEN: After recovery_timeout. Limited calls allowed to test recovery.
    """

    def __init__(self, config: CircuitBreakerConfig | None = None) -> None:
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: float | None = None
        self._last_state_change = time.time()
        self._lock = threading.RLock()

    @property
    def state(self) -> CircuitState:
        """Get current circuit state, auto-transitioning if needed."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self._transition_to(CircuitState.HALF_OPEN)
            return self._state

    def call(self, func: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
        """Execute a function with circuit breaker protection.

        Args:
            func: Function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Return value of the function.

        Raises:
            CircuitBreakerOpen: If circuit is open.
            Exception: Any exception raised by the function.
        """
        if not self._can_execute():
            raise CircuitBreakerOpen(
                f"Circuit breaker is open. State={self._state.value}"
            )

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            if self._is_excluded_exception(e):
                raise
            self._on_failure()
            raise

    def __enter__(self) -> "CircuitBreaker":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        """Context manager exit."""
        if exc_type is not None and not self._is_excluded_exception(exc_val):
            self._on_failure()
        return False

    def _can_execute(self) -> bool:
        """Check if execution is allowed in current state."""
        with self._lock:
            state = self.state
            if state == CircuitState.CLOSED:
                return True
            if state == CircuitState.HALF_OPEN:
                return self._half_open_calls < self.config.half_open_max_calls
            return False

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self._last_failure_time is None:
            return False
        return (time.time() - self._last_failure_time) >= self.config.recovery_timeout

    def _on_success(self) -> None:
        """Handle successful call."""
        with self._lock:
            self._failure_count = 0
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._transition_to(CircuitState.CLOSED)
                    logger.info("Circuit breaker reset to CLOSED")

    def _on_failure(self) -> None:
        """Handle failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                self._transition_to(CircuitState.OPEN)
                logger.warning("Circuit breaker reopened after HALF_OPEN failure")

            elif self._failure_count >= self.config.failure_threshold:
                self._transition_to(CircuitState.OPEN)
                logger.warning(
                    "Circuit breaker opened after %d failures",
                    self._failure_count,
                )

    def _transition_to(self, new_state: CircuitState) -> None:
        """Transition to a new state."""
        self._state = new_state
        self._last_state_change = time.time()

        if new_state == CircuitState.HALF_OPEN:
            self._half_open_calls = 0
            self._success_count = 0
        elif new_state == CircuitState.CLOSED:
            self._failure_count = 0
            self._success_count = 0

    def _is_excluded_exception(self, exc: Exception) -> bool:
        """Check if exception type is in excluded list."""
        return isinstance(exc, self.config.excluded_exceptions)

    def reset(self) -> None:
        """Manually reset the circuit breaker to closed state."""
        with self._lock:
            self._transition_to(CircuitState.CLOSED)
            logger.info("Circuit breaker manually reset")

    def get_stats(self) -> dict[str, Any]:
        """Get circuit breaker statistics."""
        with self._lock:
            return {
                "state": self._state.value,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "last_failure_time": self._last_failure_time,
                "last_state_change": self._last_state_change,
                "uptime_seconds": time.time() - self._last_state_change,
            }


class CircuitBreakerRegistry:
    """Registry for managing multiple circuit breakers by name."""

    def __init__(self) -> None:
        self._breakers: dict[str, CircuitBreaker] = {}
        self._lock = threading.Lock()

    def get(
        self,
        name: str,
        config: CircuitBreakerConfig | None = None,
    ) -> CircuitBreaker:
        """Get or create a circuit breaker by name."""
        with self._lock:
            if name not in self._breakers:
                self._breakers[name] = CircuitBreaker(config)
            return self._breakers[name]

    def all_stats(self) -> dict[str, dict[str, Any]]:
        """Get statistics for all registered circuit breakers."""
        with self._lock:
            return {name: cb.get_stats() for name, cb in self._breakers.items()}
