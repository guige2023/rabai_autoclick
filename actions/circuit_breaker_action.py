"""
Circuit Breaker Action Module.

Provides circuit breaker pattern implementation for
failure protection and graceful degradation.

Author: rabai_autoclick team
"""

import time
import asyncio
import logging
from typing import (
    Optional, Dict, Any, Callable, Awaitable,
    List, Union
)
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 60.0
    half_open_max_calls: int = 3
    excluded_exceptions: List[type] = field(default_factory=list)
    name: str = "default"


@dataclass
class CircuitBreakerStats:
    """Statistics for circuit breaker."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    state_changes: int = 0
    last_state_change: float = 0.0
    consecutive_failures: int = 0
    consecutive_successes: int = 0

    @property
    def failure_rate(self) -> float:
        """Calculate failure rate."""
        if self.total_calls == 0:
            return 0.0
        return self.failed_calls / self.total_calls


class CircuitBreakerOpenError(Exception):
    """Exception raised when circuit breaker is open."""
    pass


class CircuitBreaker:
    """
    Circuit Breaker Implementation.

    Implements the circuit breaker pattern to prevent
    cascading failures and enable graceful degradation.

    Example:
        >>> cb = CircuitBreaker(failure_threshold=5, timeout=60)
        >>> await cb.call(failing_function)
    """

    def __init__(self, config: Optional[CircuitBreakerConfig] = None):
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._stats = CircuitBreakerStats()
        self._lock = asyncio.Lock()
        self._half_open_calls = 0

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        if self._state == CircuitState.OPEN:
            if self._should_attempt_reset():
                return CircuitState.HALF_OPEN
        return self._state

    def _should_attempt_reset(self) -> bool:
        """Check if circuit should attempt to reset."""
        elapsed = time.time() - self._stats.last_state_change
        return elapsed >= self.config.timeout

    async def _transition_to(self, new_state: CircuitState) -> None:
        """Transition to a new state."""
        if self._state == new_state:
            return

        logger.info(
            f"Circuit breaker '{self.config.name}' transitioning from "
            f"{self._state.value} to {new_state.value}"
        )

        self._state = new_state
        self._stats.state_changes += 1
        self._stats.last_state_change = time.time()

        if new_state == CircuitState.HALF_OPEN:
            self._half_open_calls = 0

    async def call(
        self,
        func: Callable[..., Awaitable[Any]],
        *args,
        fallback: Optional[Callable] = None,
        **kwargs,
    ) -> Any:
        """
        Call a function through the circuit breaker.

        Args:
            func: Async function to call
            *args: Positional arguments
            fallback: Optional fallback function
            **kwargs: Keyword arguments

        Returns:
            Function result

        Raises:
            CircuitBreakerOpenError: If circuit is open
        """
        async with self._lock:
            current_state = self.state

            if current_state == CircuitState.OPEN:
                self._stats.rejected_calls += 1
                if fallback:
                    logger.info(f"Circuit breaker '{self.config.name}' open, using fallback")
                    return await fallback(*args, **kwargs)
                raise CircuitBreakerOpenError(
                    f"Circuit breaker '{self.config.name}' is open"
                )

            if current_state == CircuitState.HALF_OPEN:
                if self._half_open_calls >= self.config.half_open_max_calls:
                    self._stats.rejected_calls += 1
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker '{self.config.name}' half_open limit reached"
                    )
                self._half_open_calls += 1

        self._stats.total_calls += 1

        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)

            await self._on_success()
            return result

        except Exception as e:
            if type(e) in self.config.excluded_exceptions:
                logger.warning(f"Exception {type(e).__name__} excluded from circuit breaker")
                raise

            await self._on_failure()
            raise

    async def _on_success(self) -> None:
        """Handle successful call."""
        async with self._lock:
            self._stats.successful_calls += 1
            self._stats.consecutive_failures = 0
            self._stats.consecutive_successes += 1

            if self._state == CircuitState.HALF_OPEN:
                if self._stats.consecutive_successes >= self.config.success_threshold:
                    await self._transition_to(CircuitState.CLOSED)
                    self._stats.consecutive_successes = 0

    async def _on_failure(self) -> None:
        """Handle failed call."""
        async with self._lock:
            self._stats.failed_calls += 1
            self._stats.consecutive_failures += 1
            self._stats.consecutive_successes = 0

            if self._state == CircuitState.HALF_OPEN:
                await self._transition_to(CircuitState.OPEN)

            elif self._state == CircuitState.CLOSED:
                if self._stats.consecutive_failures >= self.config.failure_threshold:
                    await self._transition_to(CircuitState.OPEN)

    def reset(self) -> None:
        """Manually reset the circuit breaker."""
        self._state = CircuitState.CLOSED
        self._stats = CircuitBreakerStats()
        logger.info(f"Circuit breaker '{self.config.name}' manually reset")

    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        return {
            "name": self.config.name,
            "state": self.state.value,
            "total_calls": self._stats.total_calls,
            "successful_calls": self._stats.successful_calls,
            "failed_calls": self._stats.failed_calls,
            "rejected_calls": self._stats.rejected_calls,
            "state_changes": self._stats.state_changes,
            "failure_rate": self._stats.failure_rate,
            "consecutive_failures": self._stats.consecutive_failures,
            "consecutive_successes": self._stats.consecutive_successes,
        }


class CircuitBreakerRegistry:
    """Registry for managing multiple circuit breakers."""

    def __init__(self):
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._lock = asyncio.Lock()

    def get_or_create(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None,
    ) -> CircuitBreaker:
        """Get existing or create new circuit breaker."""
        if name not in self._breakers:
            if config:
                config.name = name
            else:
                config = CircuitBreakerConfig(name=name)
            self._breakers[name] = CircuitBreaker(config)
        return self._breakers[name]

    def get(self, name: str) -> Optional[CircuitBreaker]:
        """Get circuit breaker by name."""
        return self._breakers.get(name)

    def remove(self, name: str) -> bool:
        """Remove circuit breaker from registry."""
        if name in self._breakers:
            del self._breakers[name]
            return True
        return False

    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all circuit breakers."""
        return {name: cb.get_stats() for name, cb in self._breakers.items()}


def circuit_breaker(
    failure_threshold: int = 5,
    success_threshold: int = 2,
    timeout: float = 60.0,
):
    """
    Decorator to add circuit breaker to a function.

    Args:
        failure_threshold: Number of failures before opening
        success_threshold: Number of successes before closing
        timeout: Time before attempting reset

    Example:
        >>> @circuit_breaker(failure_threshold=3)
        >>> async def unreliable_function():
        ...     pass
    """
    def decorator(func: Callable) -> Callable:
        config = CircuitBreakerConfig(
            failure_threshold=failure_threshold,
            success_threshold=success_threshold,
            timeout=timeout,
            name=func.__name__,
        )
        breaker = CircuitBreaker(config)

        @wraps(func)
        async def wrapper(*args, **kwargs):
            return await breaker.call(func, *args, **kwargs)

        wrapper.circuit_breaker = breaker
        return wrapper

    return decorator


class CircuitBreakerAction:
    """
    Circuit Breaker Action for automation workflows.

    Provides circuit breaker functionality for protecting
    automated workflows from cascading failures.

    Example:
        >>> action = CircuitBreakerAction()
        >>> cb = action.create_breaker("api_calls")
        >>> result = await cb.call(risky_api_call)
    """

    def __init__(self):
        self.registry = CircuitBreakerRegistry()

    def create_breaker(
        self,
        name: str,
        **kwargs,
    ) -> CircuitBreaker:
        """Create a new circuit breaker."""
        config = CircuitBreakerConfig(name=name, **kwargs)
        return CircuitBreaker(config)

    def get_breaker(self, name: str) -> Optional[CircuitBreaker]:
        """Get an existing circuit breaker."""
        return self.registry.get(name)

    async def protect(
        self,
        func: Callable[..., Awaitable[Any]],
        name: str = "default",
        fallback: Optional[Callable] = None,
        **kwargs,
    ) -> Any:
        """
        Protect a function call with circuit breaker.

        Args:
            func: Function to protect
            name: Circuit breaker name
            fallback: Optional fallback function
            **kwargs: Additional circuit breaker config

        Returns:
            Function result
        """
        breaker = self.registry.get_or_create(name, CircuitBreakerConfig(name=name, **kwargs))
        return await breaker.call(func, fallback=fallback)

    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all circuit breakers."""
        return self.registry.get_all_stats()
