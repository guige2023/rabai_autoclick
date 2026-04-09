"""
Circuit Breaker V2 Action Module

Provides enhanced circuit breaker pattern implementation for fault tolerance
in UI automation workflows. Supports half-open state, slow call detection,
and configurable recovery strategies.

Author: AI Agent
Version: 1.0.0
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Callable, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


class FailureReason(Enum):
    """Reason for circuit trip."""
    TIMEOUT = auto()
    ERROR = auto()
    PARTIAL_FAILURE = auto()
    SLOW_CALL = auto()


@dataclass
class CircuitEvent:
    """Circuit breaker event."""
    timestamp: float
    event_type: str
    state: CircuitState
    message: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class CircuitMetrics:
    """Circuit breaker metrics."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    timeout_calls: int = 0
    slow_calls: int = 0
    last_failure_time: Optional[float] = None
    last_success_time: Optional[float] = None
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    open_time: Optional[float] = None

    @property
    def failure_rate(self) -> float:
        """Calculate failure rate."""
        if self.total_calls == 0:
            return 0.0
        return self.failed_calls / self.total_calls

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.total_calls == 0:
            return 1.0
        return self.successful_calls / self.total_calls

    @property
    def avg_response_time(self) -> float:
        """Calculate average response time estimate."""
        return 0.0


@dataclass
class CircuitConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 60.0
    half_open_timeout: float = 30.0
    slow_call_threshold: float = 2.0
    slow_call_volume_threshold: int = 3
    permitted_volume: int = 10
    sliding_window_size: int = 100
    minimum_number_of_calls: int = 10
    wait_duration_in_open_state: float = 60.0
    record_failures: bool = True
    record_successes: bool = True


class CircuitBreaker:
    """
    Circuit breaker for fault tolerance.

    Example:
        >>> cb = CircuitBreaker(CircuitConfig(failure_threshold=3))
        >>> async with cb:
        ...     result = await my_function()
    """

    def __init__(self, config: Optional[CircuitConfig] = None, name: str = "default") -> None:
        self.config = config or CircuitConfig()
        self.name = name
        self._state = CircuitState.CLOSED
        self._metrics = CircuitMetrics()
        self._failure_times: list[float] = []
        self._success_times: list[float] = []
        self._call_durations: list[float] = []
        self._events: list[CircuitEvent] = []
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        return self._state

    @property
    def metrics(self) -> CircuitMetrics:
        """Get circuit metrics."""
        return self._metrics

    @property
    def is_closed(self) -> bool:
        """Check if circuit is closed."""
        return self._state == CircuitState.CLOSED

    @property
    def is_open(self) -> bool:
        """Check if circuit is open."""
        return self._state == CircuitState.OPEN

    @property
    def is_half_open(self) -> bool:
        """Check if circuit is half-open."""
        return self._state == CircuitState.HALF_OPEN

    async def call(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Execute function with circuit breaker protection."""
        if asyncio.iscoroutinefunction(func):
            return await self._call_async(func, *args, **kwargs)
        return self._call_sync(func, *args, **kwargs)

    async def _call_async(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Execute async function."""
        if not await self._can_execute():
            self._metrics.rejected_calls += 1
            raise CircuitOpenError(f"Circuit {self.name} is open")

        start_time = time.time()
        try:
            result = await asyncio.wait_for(
                func(*args, **kwargs),
                timeout=self.config.timeout,
            )
            await self._on_success(time.time() - start_time)
            return result
        except asyncio.TimeoutError:
            await self._on_timeout()
            raise
        except Exception as e:
            await self._on_error(e)
            raise

    def _call_sync(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Execute synchronous function."""
        if self._state == CircuitState.OPEN:
            self._metrics.rejected_calls += 1
            raise CircuitOpenError(f"Circuit {self.name} is open")

        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            self._record_success(time.time() - start_time)
            return result
        except Exception as e:
            self._record_error(e)
            raise

    async def _can_execute(self) -> bool:
        """Check if execution is allowed."""
        async with self._lock:
            if self._state == CircuitState.CLOSED:
                return True

            if self._state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    await self._to_half_open()
                    return True
                return False

            if self._state == CircuitState.HALF_OPEN:
                return True

            return False

    def _should_attempt_reset(self) -> bool:
        """Check if circuit should attempt reset."""
        if self._metrics.open_time is None:
            return True

        elapsed = time.time() - self._metrics.open_time
        return elapsed >= self.config.wait_duration_in_open_state

    async def _to_half_open(self) -> None:
        """Transition to half-open state."""
        self._state = CircuitState.HALF_OPEN
        self._record_event("STATE_CHANGE", CircuitState.HALF_OPEN, "Circuit entered half-open state")
        self._metrics.consecutive_failures = 0
        self._metrics.consecutive_successes = 0

    async def _on_success(self, duration: float) -> None:
        """Handle successful call."""
        async with self._lock:
            self._record_success(duration)

            if self._state == CircuitState.HALF_OPEN:
                self._metrics.consecutive_successes += 1
                if self._metrics.consecutive_successes >= self.config.success_threshold:
                    await self._to_closed()

    async def _on_error(self, error: Exception) -> None:
        """Handle failed call."""
        async with self._lock:
            self._record_failure(FailureReason.ERROR)

            if self._state == CircuitState.HALF_OPEN:
                await self._to_open(f"Error in half-open: {error}")

    async def _on_timeout(self) -> None:
        """Handle timeout."""
        async with self._lock:
            self._record_failure(FailureReason.TIMEOUT)

            if self._state == CircuitState.HALF_OPEN:
                await self._to_open("Timeout in half-open state")

    def _record_success(self, duration: float) -> None:
        """Record successful call."""
        current_time = time.time()

        if self.config.record_successes:
            self._success_times.append(current_time)
            if len(self._success_times) > self.config.sliding_window_size:
                self._success_times.pop(0)

        self._call_durations.append(duration)
        if len(self._call_durations) > self.config.sliding_window_size:
            self._call_durations.pop(0)

        self._metrics.total_calls += 1
        self._metrics.successful_calls += 1
        self._metrics.last_success_time = current_time
        self._metrics.consecutive_failures = 0

        if duration >= self.config.slow_call_threshold:
            self._metrics.slow_calls += 1

        self._record_event("SUCCESS", self._state, f"Call succeeded in {duration:.3f}s")

    def _record_failure(self, reason: FailureReason) -> None:
        """Record failed call."""
        current_time = time.time()

        if self.config.record_failures:
            self._failure_times.append(current_time)
            if len(self._failure_times) > self.config.sliding_window_size:
                self._failure_times.pop(0)

        self._metrics.total_calls += 1
        self._metrics.failed_calls += 1
        self._metrics.last_failure_time = current_time
        self._metrics.consecutive_failures += 1

        if reason == FailureReason.TIMEOUT:
            self._metrics.timeout_calls += 1

        if self._metrics.consecutive_failures >= self.config.failure_threshold:
            self._to_open_sync(f"Failure threshold reached: {self._metrics.consecutive_failures}")

        self._record_event(
            "FAILURE",
            self._state,
            f"Call failed: {reason.name}",
            {"reason": reason.name},
        )

    async def _to_open(self, message: str) -> None:
        """Transition to open state."""
        self._state = CircuitState.OPEN
        self._metrics.open_time = time.time()
        self._record_event("STATE_CHANGE", CircuitState.OPEN, message)

    def _to_open_sync(self, message: str) -> None:
        """Transition to open state (sync version)."""
        self._state = CircuitState.OPEN
        self._metrics.open_time = time.time()
        self._record_event("STATE_CHANGE", CircuitState.OPEN, message)

    async def _to_closed(self) -> None:
        """Transition to closed state."""
        self._state = CircuitState.CLOSED
        self._metrics.open_time = None
        self._metrics.consecutive_failures = 0
        self._metrics.consecutive_successes = 0
        self._record_event("STATE_CHANGE", CircuitState.CLOSED, "Circuit closed")

    def _record_event(
        self,
        event_type: str,
        state: CircuitState,
        message: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        """Record circuit event."""
        event = CircuitEvent(
            timestamp=time.time(),
            event_type=event_type,
            state=state,
            message=message,
            metadata=metadata or {},
        )
        self._events.append(event)
        if len(self._events) > 1000:
            self._events.pop(0)

    def get_events(self, limit: int = 100) -> list[CircuitEvent]:
        """Get recent circuit events."""
        return self._events[-limit:]

    def reset(self) -> None:
        """Reset circuit breaker to initial state."""
        self._state = CircuitState.CLOSED
        self._metrics = CircuitMetrics()
        self._failure_times.clear()
        self._success_times.clear()
        self._call_durations.clear()
        self._events.clear()
        self._record_event("RESET", CircuitState.CLOSED, "Circuit breaker reset")

    def __repr__(self) -> str:
        return f"CircuitBreaker(name={self.name}, state={self._state.name}, metrics={self._metrics})"


class CircuitOpenError(Exception):
    """Circuit is open error."""
    pass


class CircuitBreakerRegistry:
    """
    Registry for managing multiple circuit breakers.

    Example:
        >>> registry = CircuitBreakerRegistry()
        >>> cb = registry.get("payment_service")
        >>> await cb.call(payment_func)
    """

    def __init__(self) -> None:
        self._breakers: dict[str, CircuitBreaker] = {}
        self._configs: dict[str, CircuitConfig] = {}

    def register(
        self,
        name: str,
        config: Optional[CircuitConfig] = None,
    ) -> CircuitBreaker:
        """Register a circuit breaker."""
        if name in self._breakers:
            return self._breakers[name]

        self._configs[name] = config or CircuitConfig()
        self._breakers[name] = CircuitBreaker(self._configs[name], name)
        logger.info(f"Registered circuit breaker: {name}")
        return self._breakers[name]

    def get(self, name: str) -> Optional[CircuitBreaker]:
        """Get circuit breaker by name."""
        if name not in self._breakers:
            return self.register(name)
        return self._breakers[name]

    def unregister(self, name: str) -> None:
        """Unregister circuit breaker."""
        if name in self._breakers:
            del self._breakers[name]
        if name in self._configs:
            del self._configs[name]

    def list_breakers(self) -> list[str]:
        """List all registered circuit breakers."""
        return list(self._breakers.keys())

    def get_all_metrics(self) -> dict[str, CircuitMetrics]:
        """Get metrics for all circuit breakers."""
        return {name: cb.metrics for name, cb in self._breakers.items()}

    def reset_all(self) -> None:
        """Reset all circuit breakers."""
        for cb in self._breakers.values():
            cb.reset()

    def __repr__(self) -> str:
        return f"CircuitBreakerRegistry(breakers={len(self._breakers)})"
