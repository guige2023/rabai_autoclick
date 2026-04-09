"""Circuit breaker action for failure protection.

Provides circuit breaker pattern implementation with
half-open state and automatic recovery.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreaker:
    name: str
    state: CircuitState
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[float] = None
    opened_at: Optional[float] = None


class CircuitBreakerAction:
    """Circuit breaker for failure protection.

    Args:
        failure_threshold: Failures before opening circuit.
        success_threshold: Successes before closing circuit.
        timeout: Time before attempting half-open.
        half_open_max_calls: Max calls in half-open state.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        success_threshold: int = 3,
        timeout: float = 60.0,
        half_open_max_calls: int = 3,
    ) -> None:
        self._breakers: dict[str, CircuitBreaker] = {}
        self._failure_threshold = failure_threshold
        self._success_threshold = success_threshold
        self._timeout = timeout
        self._half_open_max_calls = half_open_max_calls
        self._half_open_calls: dict[str, int] = {}
        self._handlers: dict[str, list[Callable]] = {
            "on_open": [],
            "on_close": [],
            "on_half_open": [],
        }

    def get_breaker(self, name: str) -> CircuitBreaker:
        """Get or create a circuit breaker.

        Args:
            name: Circuit breaker name.

        Returns:
            Circuit breaker.
        """
        if name not in self._breakers:
            self._breakers[name] = CircuitBreaker(
                name=name,
                state=CircuitState.CLOSED,
            )
        return self._breakers[name]

    def is_available(self, name: str) -> bool:
        """Check if circuit breaker allows calls.

        Args:
            name: Circuit breaker name.

        Returns:
            True if calls are allowed.
        """
        breaker = self.get_breaker(name)

        if breaker.state == CircuitState.CLOSED:
            return True

        if breaker.state == CircuitState.OPEN:
            if breaker.last_failure_time:
                elapsed = time.time() - breaker.last_failure_time
                if elapsed >= self._timeout:
                    self._transition_to_half_open(name)
                    return True
            return False

        if breaker.state == CircuitState.HALF_OPEN:
            calls = self._half_open_calls.get(name, 0)
            if calls < self._half_open_max_calls:
                return True
            return False

        return False

    def record_success(self, name: str) -> None:
        """Record a successful call.

        Args:
            name: Circuit breaker name.
        """
        breaker = self.get_breaker(name)

        if breaker.state == CircuitState.HALF_OPEN:
            breaker.success_count += 1
            if breaker.success_count >= self._success_threshold:
                self._transition_to_closed(name)
        else:
            breaker.failure_count = 0

    def record_failure(self, name: str) -> None:
        """Record a failed call.

        Args:
            name: Circuit breaker name.
        """
        breaker = self.get_breaker(name)
        breaker.failure_count += 1
        breaker.last_failure_time = time.time()

        if breaker.state == CircuitState.HALF_OPEN:
            self._transition_to_open(name)
        elif breaker.failure_count >= self._failure_threshold:
            self._transition_to_open(name)

    def _transition_to_open(self, name: str) -> None:
        """Transition circuit to open state.

        Args:
            name: Circuit breaker name.
        """
        breaker = self.get_breaker(name)
        breaker.state = CircuitState.OPEN
        breaker.opened_at = time.time()
        breaker.success_count = 0

        for handler in self._handlers["on_open"]:
            try:
                handler(name)
            except Exception as e:
                logger.error(f"Handler error: {e}")

        logger.warning(f"Circuit breaker OPEN: {name}")

    def _transition_to_half_open(self, name: str) -> None:
        """Transition circuit to half-open state.

        Args:
            name: Circuit breaker name.
        """
        breaker = self.get_breaker(name)
        breaker.state = CircuitState.HALF_OPEN
        breaker.failure_count = 0
        breaker.success_count = 0
        self._half_open_calls[name] = 0

        for handler in self._handlers["on_half_open"]:
            try:
                handler(name)
            except Exception as e:
                logger.error(f"Handler error: {e}")

        logger.info(f"Circuit breaker HALF_OPEN: {name}")

    def _transition_to_closed(self, name: str) -> None:
        """Transition circuit to closed state.

        Args:
            name: Circuit breaker name.
        """
        breaker = self.get_breaker(name)
        breaker.state = CircuitState.CLOSED
        breaker.failure_count = 0
        breaker.success_count = 0
        breaker.opened_at = None

        if name in self._half_open_calls:
            del self._half_open_calls[name]

        for handler in self._handlers["on_close"]:
            try:
                handler(name)
            except Exception as e:
                logger.error(f"Handler error: {e}")

        logger.info(f"Circuit breaker CLOSED: {name}")

    def increment_half_open_calls(self, name: str) -> int:
        """Increment half-open call counter.

        Args:
            name: Circuit breaker name.

        Returns:
            Current call count.
        """
        if name not in self._half_open_calls:
            self._half_open_calls[name] = 0
        self._half_open_calls[name] += 1
        return self._half_open_calls[name]

    def execute(
        self,
        name: str,
        func: Callable,
        fallback: Optional[Callable] = None,
    ) -> Any:
        """Execute a function with circuit breaker protection.

        Args:
            name: Circuit breaker name.
            func: Function to execute.
            fallback: Fallback function.

        Returns:
            Function result or fallback result.
        """
        if not self.is_available(name):
            if fallback:
                return fallback()
            raise Exception(f"Circuit breaker is open: {name}")

        if self.get_breaker(name).state == CircuitState.HALF_OPEN:
            self.increment_half_open_calls(name)

        try:
            result = func()
            self.record_success(name)
            return result
        except Exception as e:
            self.record_failure(name)
            if fallback:
                return fallback()
            raise e

    def register_handler(self, event: str, handler: Callable) -> None:
        """Register a circuit breaker event handler.

        Args:
            event: Event type ('on_open', 'on_close', 'on_half_open').
            handler: Callback function.
        """
        if event in self._handlers:
            self._handlers[event].append(handler)

    def get_state(self, name: str) -> CircuitState:
        """Get current circuit state.

        Args:
            name: Circuit breaker name.

        Returns:
            Current state.
        """
        return self.get_breaker(name).state

    def reset(self, name: str) -> bool:
        """Reset a circuit breaker to closed state.

        Args:
            name: Circuit breaker name.

        Returns:
            True if reset.
        """
        if name in self._breakers:
            self._transition_to_closed(name)
            return True
        return False

    def get_stats(self) -> dict[str, Any]:
        """Get circuit breaker statistics.

        Returns:
            Dictionary with stats.
        """
        by_state = {
            "closed": sum(1 for b in self._breakers.values() if b.state == CircuitState.CLOSED),
            "open": sum(1 for b in self._breakers.values() if b.state == CircuitState.OPEN),
            "half_open": sum(1 for b in self._breakers.values() if b.state == CircuitState.HALF_OPEN),
        }

        return {
            "total_breakers": len(self._breakers),
            "by_state": by_state,
            "failure_threshold": self._failure_threshold,
            "success_threshold": self._success_threshold,
            "timeout": self._timeout,
        }
