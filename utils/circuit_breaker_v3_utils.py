"""
Circuit breaker pattern implementation for fault tolerance.

Provides a configurable circuit breaker that prevents cascading
failures by short-circuiting requests when a service is unhealthy.

Example:
    >>> from utils.circuit_breaker_v3_utils import CircuitBreaker
    >>> breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)
    >>> with breaker:
    ...     call_service()
"""

from __future__ import annotations

import asyncio
import threading
import time
from enum import Enum
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerError(Exception):
    """Raised when circuit breaker is open."""
    pass


class CircuitBreaker:
    """
    Thread-safe circuit breaker implementation.

    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Circuit is tripped, requests fail fast
    - HALF_OPEN: Testing if service has recovered

    Attributes:
        failure_threshold: Failures before opening circuit.
        recovery_timeout: Seconds before trying recovery.
        success_threshold: Successes in half-open to close.
        expected_exceptions: Exception types that count as failures.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        success_threshold: int = 2,
        expected_exceptions: tuple = (Exception,),
        name: Optional[str] = None,
    ) -> None:
        """
        Initialize the circuit breaker.

        Args:
            failure_threshold: Failures before opening circuit.
            recovery_timeout: Seconds before trying recovery.
            success_threshold: Successes in half-open to close.
            expected_exceptions: Exception types that count as failures.
            name: Optional name for monitoring.
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        self.expected_exceptions = expected_exceptions
        self.name = name or "CircuitBreaker"

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = threading.RLock()

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            self._check_state_transition()
            return self._state

    def _check_state_transition(self) -> None:
        """Check if state should transition based on timeout."""
        if self._state == CircuitState.OPEN and self._last_failure_time:
            if time.monotonic() - self._last_failure_time >= self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                self._success_count = 0

    def call(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any
    ) -> T:
        """
        Call a function through the circuit breaker.

        Args:
            func: Function to call.
            *args: Positional arguments for func.
            **kwargs: Keyword arguments for func.

        Returns:
            Function result.

        Raises:
            CircuitBreakerError: If circuit is open.
            Exception: If function raises an expected exception.
        """
        with self._lock:
            if self.state == CircuitState.OPEN:
                raise CircuitBreakerError(
                    f"Circuit '{self.name}' is OPEN. Service unavailable."
                )

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exceptions as e:
            self._on_failure()
            raise

    def __enter__(self) -> "CircuitBreaker":
        """Context manager entry."""
        if self.state == CircuitState.OPEN:
            raise CircuitBreakerError(
                f"Circuit '{self.name}' is OPEN. Service unavailable."
            )
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        """Context manager exit."""
        if exc_type is not None and issubclass(exc_type, self.expected_exceptions):
            self._on_failure()
            return False
        self._on_success()
        return True

    def _on_success(self) -> None:
        """Handle successful call."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0

    def _on_failure(self) -> None:
        """Handle failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()

            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                self._success_count = 0
            elif self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN

    def reset(self) -> None:
        """Reset the circuit breaker to closed state."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time = None

    def trip(self) -> None:
        """Force the circuit to open."""
        with self._lock:
            self._state = CircuitState.OPEN
            self._last_failure_time = time.monotonic()

    def half_open(self) -> None:
        """Manually put the circuit in half-open state."""
        with self._lock:
            self._state = CircuitState.HALF_OPEN
            self._success_count = 0

    def get_stats(self) -> dict:
        """Get circuit breaker statistics."""
        with self._lock:
            return {
                "name": self.name,
                "state": self._state.value,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "last_failure_time": self._last_failure_time,
            }


class AsyncCircuitBreaker:
    """
    Async circuit breaker for use in async contexts.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        success_threshold: int = 2,
        expected_exceptions: tuple = (Exception,),
        name: Optional[str] = None,
    ) -> None:
        """Initialize the async circuit breaker."""
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        self.expected_exceptions = expected_exceptions
        self.name = name or "AsyncCircuitBreaker"

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = asyncio.Lock()

    @property
    async def state(self) -> CircuitState:
        """Get current circuit state."""
        async with self._lock:
            await self._check_state_transition()
            return self._state

    async def _check_state_transition(self) -> None:
        """Check if state should transition."""
        if self._state == CircuitState.OPEN and self._last_failure_time:
            if time.monotonic() - self._last_failure_time >= self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                self._success_count = 0

    async def call(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any
    ) -> Any:
        """Call an async function through the circuit breaker."""
        async with self._lock:
            if self._state == CircuitState.OPEN:
                raise CircuitBreakerError(
                    f"Circuit '{self.name}' is OPEN. Service unavailable."
                )

        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            await self._on_success()
            return result
        except self.expected_exceptions as e:
            await self._on_failure()
            raise

    async def _on_success(self) -> None:
        """Handle successful call."""
        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0

    async def _on_failure(self) -> None:
        """Handle failed call."""
        async with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()

            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                self._success_count = 0
            elif self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN

    async def reset(self) -> None:
        """Reset the circuit breaker."""
        async with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time = None


class CircuitBreakerRegistry:
    """
    Registry for managing multiple circuit breakers.

    Allows centralized monitoring and management of
    circuit breakers by name.
    """

    def __init__(self) -> None:
        """Initialize the registry."""
        self._breakers: dict[str, CircuitBreaker] = {}
        self._lock = threading.Lock()

    def get(
        self,
        name: str,
        create: bool = True,
        **kwargs
    ) -> Optional[CircuitBreaker]:
        """
        Get a circuit breaker by name.

        Args:
            name: Circuit breaker name.
            create: Create if doesn't exist.
            **kwargs: Arguments for new circuit breaker.

        Returns:
            Circuit breaker instance or None.
        """
        with self._lock:
            if name in self._breakers:
                return self._breakers[name]
            if create:
                breaker = CircuitBreaker(name=name, **kwargs)
                self._breakers[name] = breaker
                return breaker
            return None

    def all_stats(self) -> dict:
        """Get statistics for all registered circuit breakers."""
        with self._lock:
            return {name: cb.get_stats() for name, cb in self._breakers.items()}

    def reset_all(self) -> None:
        """Reset all circuit breakers."""
        with self._lock:
            for cb in self._breakers.values():
                cb.reset()


def circuit_breaker(
    failure_threshold: int = 5,
    recovery_timeout: float = 60.0,
    **kwargs
) -> Callable:
    """
    Decorator to add circuit breaker to a function.

    Args:
        failure_threshold: Failures before opening circuit.
        recovery_timeout: Seconds before trying recovery.
        **kwargs: Additional arguments for CircuitBreaker.

    Returns:
        Decorated function.
    """
    breaker = CircuitBreaker(
        failure_threshold=failure_threshold,
        recovery_timeout=recovery_timeout,
        **kwargs
    )

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        def wrapper(*args: Any, **kwds: Any) -> T:
            return breaker.call(func, *args, **kwds)
        return wrapper
    return decorator
