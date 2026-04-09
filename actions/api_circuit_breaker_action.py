"""API Circuit Breaker action module.

Provides fault tolerance for API calls using the circuit breaker pattern.
When failures exceed a threshold, the circuit "opens" and fast-fails
subsequent requests, preventing cascade failures.
"""

import asyncio
import time
from enum import Enum
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing fast
    HALF_OPEN = "half_open"  # Testing recovery


class CircuitBreakerOpen(Exception):
    """Raised when circuit is open and requests are blocked."""

    def __init__(self, circuit_name: str, retry_after: float):
        self.circuit_name = circuit_name
        self.retry_after = retry_after
        super().__init__(
            f"Circuit '{circuit_name}' is OPEN. Retry after {retry_after:.1f}s"
        )


class CircuitBreaker:
    """Circuit breaker for API calls."""

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exceptions: tuple = (Exception,),
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exceptions = expected_exceptions

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._opened_at: Optional[float] = None

    @property
    def state(self) -> CircuitState:
        """Get current circuit state, checking for timeout transitions."""
        if self._state == CircuitState.OPEN:
            if self._opened_at is not None:
                elapsed = time.monotonic() - self._opened_at
                if elapsed >= self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
        return self._state

    def record_success(self) -> None:
        """Record a successful call."""
        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._opened_at = None

    def record_failure(self) -> None:
        """Record a failed call."""
        self._failure_count += 1
        self._last_failure_time = time.monotonic()

        if self._failure_count >= self.failure_threshold:
            self._state = CircuitState.OPEN
            self._opened_at = time.monotonic()

    def can_execute(self) -> bool:
        """Check if execution is allowed."""
        return self.state != CircuitState.OPEN

    def get_retry_after(self) -> float:
        """Get seconds until circuit might close."""
        if self._opened_at is None:
            return 0.0
        elapsed = time.monotonic() - self._opened_at
        return max(0.0, self.recovery_timeout - elapsed)


async def call_with_circuit_breaker(
    circuit: CircuitBreaker,
    func: Callable[..., Any],
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Execute a function with circuit breaker protection.

    Args:
        circuit: CircuitBreaker instance
        func: Async function to call
        *args: Positional arguments for func
        **kwargs: Keyword arguments for func

    Returns:
        Function result

    Raises:
        CircuitBreakerOpen: When circuit is open
    """
    if not circuit.can_execute():
        raise CircuitBreakerOpen(circuit.name, circuit.get_retry_after())

    try:
        result = await func(*args, **kwargs)
        circuit.record_success()
        return result
    except circuit.expected_exceptions as e:
        circuit.record_failure()
        raise e


class CircuitBreakerRegistry:
    """Global registry for circuit breakers."""

    def __init__(self):
        self._circuits: dict[str, CircuitBreaker] = {}

    def get_or_create(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
    ) -> CircuitBreaker:
        """Get existing or create new circuit breaker."""
        if name not in self._circuits:
            self._circuits[name] = CircuitBreaker(
                name=name,
                failure_threshold=failure_threshold,
                recovery_timeout=recovery_timeout,
            )
        return self._circuits[name]

    def get_status(self) -> dict[str, dict[str, Any]]:
        """Get status of all circuits."""
        return {
            name: {
                "state": circuit.state.value,
                "failure_count": circuit._failure_count,
                "last_failure": circuit._last_failure_time,
            }
            for name, circuit in self._circuits.items()
        }

    def reset_all(self) -> None:
        """Reset all circuits to closed state."""
        for circuit in self._circuits.values():
            circuit._state = CircuitState.CLOSED
            circuit._failure_count = 0
            circuit._opened_at = None


# Global registry instance
_registry = CircuitBreakerRegistry()


def get_circuit_registry() -> CircuitBreakerRegistry:
    """Get the global circuit registry."""
    return _registry
