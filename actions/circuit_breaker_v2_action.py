"""Circuit Breaker v2 Action Module.

Enhanced circuit breaker with state persistence and monitoring.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class CircuitState(Enum):
    """Enhanced circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"
    FORCED_OPEN = "forced_open"


@dataclass
class CircuitBreakerConfig:
    """Enhanced configuration."""
    failure_threshold: int = 5
    success_threshold: int = 3
    timeout_seconds: float = 60.0
    half_open_max_calls: int = 3
    slow_call_threshold: float = 2.0
    slow_call_percentage: float = 50.0
    sliding_window_size: int = 100


@dataclass
class CircuitBreakerEvent:
    """Circuit breaker event."""
    event_type: str
    state: CircuitState
    timestamp: float
    details: dict = field(default_factory=dict)


class CircuitBreakerV2(Generic[T]):
    """Enhanced circuit breaker with sliding window."""

    def __init__(
        self,
        name: str,
        config: CircuitBreakerConfig | None = None
    ) -> None:
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._slow_call_count = 0
        self._last_failure_time: float | None = None
        self._half_open_calls = 0
        self._slow_call_threshold = self.config.slow_call_threshold
        self._lock = asyncio.Lock()
        self._call_history: deque[tuple[float, bool, float]] = deque(maxlen=self.config.sliding_window_size)
        self._event_handlers: list[Callable[[CircuitBreakerEvent], Any]] = []

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        return self._state

    async def call(
        self,
        func: Callable[[], T | asyncio.coroutine],
        *args,
        **kwargs
    ) -> T:
        """Execute function through circuit breaker."""
        if not self._is_call_allowed():
            raise CircuitBreakerOpenError(
                f"Circuit breaker {self.name} is {self._state.value}",
                retry_after=self._get_retry_after()
            )
        start = time.monotonic()
        try:
            result = func(*args, **kwargs)
            if asyncio.iscoroutine(result):
                result = await result
            latency = time.monotonic() - start
            await self._on_success(latency)
            return result
        except Exception as e:
            await self._on_failure()
            raise

    def _is_call_allowed(self) -> bool:
        """Check if call is allowed."""
        if self._state == CircuitState.CLOSED:
            return True
        if self._state == CircuitState.HALF_OPEN:
            return self._half_open_calls < self.config.half_open_max_calls
        if self._state == CircuitState.FORCED_OPEN:
            return False
        if self._last_failure_time:
            if time.time() - self._last_failure_time >= self.config.timeout_seconds:
                self._transition_to(CircuitState.HALF_OPEN)
                return True
        return False

    async def _on_success(self, latency: float) -> None:
        """Handle successful call."""
        async with self._lock:
            self._call_history.append((time.time(), True, latency))
            if latency > self._slow_call_threshold:
                self._slow_call_count += 1
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._transition_to(CircuitState.CLOSED)
            self._failure_count = 0
            self._slow_call_count = 0

    async def _on_failure(self) -> None:
        """Handle failed call."""
        async with self._lock:
            self._call_history.append((time.time(), False, 0))
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._state == CircuitState.HALF_OPEN:
                self._transition_to(CircuitState.OPEN)
            elif self._failure_count >= self.config.failure_threshold:
                self._transition_to(CircuitState.OPEN)

    def _transition_to(self, new_state: CircuitState) -> None:
        """Transition to new state."""
        old_state = self._state
        self._state = new_state
        event = CircuitBreakerEvent(
            event_type=f"transition_{old_state.value}_to_{new_state.value}",
            state=new_state,
            timestamp=time.time()
        )
        for handler in self._event_handlers:
            if asyncio.iscoroutinefunction(handler):
                asyncio.create_task(handler(event))
            else:
                handler(event)
        if new_state == CircuitState.HALF_OPEN:
            self._half_open_calls = 0
            self._success_count = 0

    def _get_retry_after(self) -> float:
        """Get retry-after duration."""
        if self._last_failure_time:
            elapsed = time.time() - self._last_failure_time
            return max(0, self.config.timeout_seconds - elapsed)
        return self.config.timeout_seconds

    def on_event(self, handler: Callable[[CircuitBreakerEvent], Any]) -> None:
        """Register event handler."""
        self._event_handlers.append(handler)

    def get_metrics(self) -> dict[str, Any]:
        """Get circuit breaker metrics."""
        now = time.time()
        recent_calls = [c for c in self._call_history if now - c[0] < 60]
        recent_failures = sum(1 for c in recent_calls if not c[1])
        recent_success = sum(1 for c in recent_calls if c[1])
        slow_calls = sum(1 for c in recent_calls if c[2] > self._slow_call_threshold)
        return {
            "state": self._state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "recent_failure_rate": recent_failures / max(len(recent_calls), 1),
            "recent_success_rate": recent_success / max(len(recent_calls), 1),
            "slow_call_percentage": slow_calls / max(len(recent_calls), 1),
            "last_failure_time": self._last_failure_time,
        }


class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open."""
    def __init__(self, message: str, retry_after: float):
        super().__init__(message)
        self.retry_after = retry_after
