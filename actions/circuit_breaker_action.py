"""Circuit breaker action module.

Provides circuit breaker pattern implementation for fault tolerance
and graceful degradation of services.
"""

from __future__ import annotations

import time
import threading
from typing import Any, Callable, Optional
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 60.0
    half_open_max_calls: int = 3
    excluded_exceptions: tuple = ()


class CircuitBreaker:
    """Circuit breaker for fault tolerance."""

    def __init__(self, name: str, config: Optional[CircuitConfig] = None):
        """Initialize circuit breaker.

        Args:
            name: Circuit name
            config: Circuit configuration
        """
        self.name = name
        self.config = config or CircuitConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    logger.info(f"Circuit {self.name}: OPEN -> HALF_OPEN")
            return self._state

    def _should_attempt_reset(self) -> bool:
        """Check if should attempt reset."""
        if self._last_failure_time is None:
            return False
        return (time.time() - self._last_failure_time) >= self.config.timeout

    def is_available(self) -> bool:
        """Check if circuit allows requests."""
        return self.state != CircuitState.OPEN

    def record_success(self) -> None:
        """Record successful call."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                self._half_open_calls += 1
                if self._success_count >= self.config.success_threshold:
                    self._reset()
                    logger.info(f"Circuit {self.name}: HALF_OPEN -> CLOSED")
            else:
                self._failure_count = 0

    def record_failure(self, exception: Optional[Exception] = None) -> None:
        """Record failed call.

        Args:
            exception: Exception that caused failure
        """
        if exception and type(exception) in self.config.excluded_exceptions:
            return

        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                self._trip()
                logger.warning(f"Circuit {self.name}: HALF_OPEN -> OPEN (failure in half-open)")

            elif self._failure_count >= self.config.failure_threshold:
                self._trip()
                logger.warning(f"Circuit {self.name}: CLOSED -> OPEN ({self._failure_count} failures)")

    def _trip(self) -> None:
        """Trip circuit to OPEN state."""
        self._state = CircuitState.OPEN
        self._success_count = 0

    def _reset(self) -> None:
        """Reset circuit to CLOSED state."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = None
        self._half_open_calls = 0

    def get_status(self) -> dict[str, Any]:
        """Get circuit status.

        Returns:
            Dictionary with status info
        """
        with self._lock:
            return {
                "name": self.name,
                "state": self.state.value,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "last_failure_time": self._last_failure_time,
                "config": {
                    "failure_threshold": self.config.failure_threshold,
                    "success_threshold": self.config.success_threshold,
                    "timeout": self.config.timeout,
                },
            }


class CircuitBreakerRegistry:
    """Registry for managing multiple circuit breakers."""

    def __init__(self):
        """Initialize registry."""
        self._circuits: dict[str, CircuitBreaker] = {}
        self._lock = threading.Lock()

    def get_circuit(self, name: str) -> Optional[CircuitBreaker]:
        """Get circuit by name."""
        with self._lock:
            return self._circuits.get(name)

    def register_circuit(self, name: str, config: Optional[CircuitConfig] = None) -> CircuitBreaker:
        """Register new circuit.

        Args:
            name: Circuit name
            config: Circuit configuration

        Returns:
            CircuitBreaker instance
        """
        with self._lock:
            if name not in self._circuits:
                self._circuits[name] = CircuitBreaker(name, config)
            return self._circuits[name]

    def unregister_circuit(self, name: str) -> None:
        """Unregister circuit."""
        with self._lock:
            self._circuits.pop(name, None)

    def list_circuits(self) -> list[dict[str, Any]]:
        """List all circuits."""
        with self._lock:
            return [cb.get_status() for cb in self._circuits.values()]


def circuit_breaker(
    name: str,
    failure_threshold: int = 5,
    success_threshold: int = 2,
    timeout: float = 60.0,
):
    """Decorator for circuit breaker.

    Args:
        name: Circuit name
        failure_threshold: Failures before opening
        success_threshold: Successes before closing
        timeout: Timeout before attempting reset

    Returns:
        Decorated function
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        config = CircuitConfig(
            failure_threshold=failure_threshold,
            success_threshold=success_threshold,
            timeout=timeout,
        )
        breaker = CircuitBreaker(name, config)

        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if not breaker.is_available():
                raise CircuitOpenError(f"Circuit {name} is OPEN")

            try:
                result = func(*args, **kwargs)
                breaker.record_success()
                return result
            except Exception as e:
                breaker.record_failure(e)
                raise

        wrapper._circuit_breaker = breaker
        return wrapper

    return decorator


class CircuitOpenError(Exception):
    """Exception raised when circuit is open."""
    pass


def create_circuit_breaker(
    name: str,
    failure_threshold: int = 5,
    timeout: float = 60.0,
) -> CircuitBreaker:
    """Create circuit breaker instance.

    Args:
        name: Circuit name
        failure_threshold: Failures before opening
        timeout: Timeout before attempting reset

    Returns:
        CircuitBreaker instance
    """
    config = CircuitConfig(
        failure_threshold=failure_threshold,
        timeout=timeout,
    )
    return CircuitBreaker(name, config)
