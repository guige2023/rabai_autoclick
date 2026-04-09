"""
Automation Circuit Breaker Action Module.

Implements the circuit breaker pattern for automation workflows,
preventing cascading failures and enabling graceful degradation.
"""

from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum, auto
import asyncio
import logging
import time

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """States of a circuit breaker."""
    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


@dataclass
class CircuitBreakerConfig:
    """Configuration for a circuit breaker."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: float = 60.0
    half_open_max_calls: int = 3
    excluded_exceptions: List[type] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "failure_threshold": self.failure_threshold,
            "success_threshold": self.success_threshold,
            "timeout_seconds": self.timeout_seconds,
            "half_open_max_calls": self.half_open_max_calls,
        }


@dataclass
class CircuitEvent:
    """An event in the circuit breaker."""
    timestamp: datetime
    event_type: str
    state_from: Optional[CircuitState]
    state_to: CircuitState
    details: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "event_type": self.event_type,
            "from": self.state_from.name if self.state_from else None,
            "to": self.state_to.name,
            "details": self.details,
        }


class AutomationCircuitBreakerAction:
    """
    Implements the circuit breaker pattern.

    The circuit breaker monitors for failures and "trips" to prevent
    further calls when a threshold is reached, allowing systems to
    recover gracefully.

    Example:
        >>> cb = AutomationCircuitBreakerAction(failure_threshold=3)
        >>> try:
        ...     await cb.execute(failing_operation)
        ... except CircuitOpenError:
        ...     print("Circuit is open, using fallback")
    """

    def __init__(
        self,
        name: str = "default",
        config: Optional[CircuitBreakerConfig] = None,
    ):
        """
        Initialize the Circuit Breaker.

        Args:
            name: Circuit name.
            config: Circuit breaker configuration.
        """
        self.name = name
        self.config = config or CircuitBreakerConfig()

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0
        self._events: List[CircuitEvent] = []

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        if self._state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self._change_state(CircuitState.HALF_OPEN)
        return self._state

    def _should_attempt_reset(self) -> bool:
        """Check if should attempt to reset the circuit."""
        if self._last_failure_time is None:
            return True
        elapsed = time.time() - self._last_failure_time
        return elapsed >= self.config.timeout_seconds

    def _change_state(self, new_state: CircuitState, reason: str = "") -> None:
        """Change circuit state."""
        old_state = self._state

        if old_state != new_state:
            self._state = new_state
            self._record_event(
                event_type="state_change",
                state_from=old_state,
                state_to=new_state,
                details=reason,
            )

            logger.info(
                f"Circuit '{self.name}' state changed: "
                f"{old_state.name} -> {new_state.name} ({reason})"
            )

    def _record_event(
        self,
        event_type: str,
        state_from: Optional[CircuitState],
        state_to: CircuitState,
        details: str = "",
    ) -> None:
        """Record a circuit breaker event."""
        event = CircuitEvent(
            timestamp=datetime.now(timezone.utc),
            event_type=event_type,
            state_from=state_from,
            state_to=state_to,
            details=details,
        )
        self._events.append(event)

        if len(self._events) > 1000:
            self._events = self._events[-500:]

    async def execute(
        self,
        operation: Callable,
        fallback: Optional[Callable] = None,
    ) -> Any:
        """
        Execute an operation with circuit breaker protection.

        Args:
            operation: Async operation to execute.
            fallback: Optional fallback function.

        Returns:
            Operation result.

        Raises:
            RuntimeError: If circuit is open.
        """
        if self.state == CircuitState.OPEN:
            if fallback:
                return await fallback()

            raise CircuitOpenError(
                f"Circuit '{self.name}' is OPEN. "
                f"Try again in {self._get_remaining_timeout():.1f}s"
            )

        try:
            result = await operation()
            self._on_success()
            return result

        except Exception as e:
            if self._is_excluded_exception(e):
                raise

            self._on_failure()
            if fallback:
                return await fallback()
            raise

    def _on_success(self) -> None:
        """Handle successful execution."""
        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1

            if self._success_count >= self.config.success_threshold:
                self._change_state(
                    CircuitState.CLOSED,
                    f"Success threshold reached ({self._success_count})",
                )
                self._failure_count = 0
                self._success_count = 0
                self._half_open_calls = 0

        elif self._state == CircuitState.CLOSED:
            self._failure_count = 0

    def _on_failure(self) -> None:
        """Handle failed execution."""
        self._failure_count += 1
        self._last_failure_time = time.time()

        self._record_event(
            event_type="failure",
            state_from=self._state,
            state_to=self._state,
            details=f"Failure #{self._failure_count}",
        )

        if self._state == CircuitState.HALF_OPEN:
            self._change_state(
                CircuitState.OPEN,
                "Failure in half-open state",
            )
            self._half_open_calls = 0

        elif self._failure_count >= self.config.failure_threshold:
            self._change_state(
                CircuitState.OPEN,
                f"Failure threshold reached ({self._failure_count})",
            )

    def _is_excluded_exception(self, exc: Exception) -> bool:
        """Check if exception is excluded from circuit breaker."""
        for exc_type in self.config.excluded_exceptions:
            if isinstance(exc, exc_type):
                return True
        return False

    def _get_remaining_timeout(self) -> float:
        """Get remaining timeout in seconds."""
        if self._last_failure_time is None:
            return 0.0

        elapsed = time.time() - self._last_failure_time
        return max(0, self.config.timeout_seconds - elapsed)

    def is_closed(self) -> bool:
        """Check if circuit is closed."""
        return self.state == CircuitState.CLOSED

    def is_open(self) -> bool:
        """Check if circuit is open."""
        return self.state == CircuitState.OPEN

    def is_half_open(self) -> bool:
        """Check if circuit is half-open."""
        return self.state == CircuitState.HALF_OPEN

    def reset(self) -> None:
        """Manually reset the circuit breaker."""
        self._change_state(CircuitState.CLOSED, "Manual reset")
        self._failure_count = 0
        self._success_count = 0
        self._half_open_calls = 0
        self._last_failure_time = None

    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        return {
            "name": self.name,
            "state": self.state.name,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "last_failure_time": (
                datetime.fromtimestamp(self._last_failure_time).isoformat()
                if self._last_failure_time else None
            ),
            "config": self.config.to_dict(),
            "remaining_timeout": self._get_remaining_timeout(),
        }

    def get_events(
        self,
        limit: int = 100,
        event_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get recent circuit events."""
        events = self._events

        if event_type:
            events = [e for e in events if e.event_type == event_type]

        return [e.to_dict() for e in events[-limit:]]


class CircuitOpenError(Exception):
    """Raised when circuit is open."""
    pass


class CircuitBreakerRegistry:
    """Registry for managing multiple circuit breakers."""

    def __init__(self):
        """Initialize the registry."""
        self._breakers: Dict[str, AutomationCircuitBreakerAction] = {}

    def get_or_create(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None,
    ) -> AutomationCircuitBreakerAction:
        """
        Get existing or create new circuit breaker.

        Args:
            name: Circuit name.
            config: Optional configuration.

        Returns:
            Circuit breaker instance.
        """
        if name not in self._breakers:
            self._breakers[name] = AutomationCircuitBreakerAction(
                name=name,
                config=config,
            )
        return self._breakers[name]

    def get(self, name: str) -> Optional[AutomationCircuitBreakerAction]:
        """Get circuit breaker by name."""
        return self._breakers.get(name)

    def list_circuits(self) -> List[str]:
        """List all circuit names."""
        return list(self._breakers.keys())

    def reset_all(self) -> None:
        """Reset all circuit breakers."""
        for cb in self._breakers.values():
            cb.reset()


def create_circuit_breaker(
    name: str = "default",
    **kwargs,
) -> AutomationCircuitBreakerAction:
    """Factory function to create an AutomationCircuitBreakerAction."""
    return AutomationCircuitBreakerAction(name=name, **kwargs)
