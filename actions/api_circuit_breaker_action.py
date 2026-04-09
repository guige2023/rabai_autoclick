"""API circuit breaker pattern implementation."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Coroutine, Optional


class CircuitState(str, Enum):
    """Circuit breaker state."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class CircuitConfig:
    """Configuration for circuit breaker."""

    name: str
    failure_threshold: int = 5  # Failures before opening
    success_threshold: int = 2  # Successes in half-open to close
    timeout_seconds: float = 60.0  # Time before trying half-open
    excluded_errors: list[str] = field(default_factory=list)
    on_state_change: Optional[Callable[[str, CircuitState, CircuitState], None]] = None
    on_rejected: Optional[Callable[[str], None]] = None


@dataclass
class CircuitStats:
    """Circuit breaker statistics."""

    name: str
    state: CircuitState
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    state_changed_at: Optional[datetime] = None
    last_failure_at: Optional[datetime] = None
    last_success_at: Optional[datetime] = None


@dataclass
class CircuitResult:
    """Result of circuit breaker operation."""

    success: bool
    state: CircuitState
    error: Optional[str] = None
    duration_ms: float = 0


class APICircuitBreakerAction:
    """Implements circuit breaker pattern for API calls."""

    def __init__(self, config: Optional[CircuitConfig] = None):
        """Initialize circuit breaker.

        Args:
            config: Circuit breaker configuration.
        """
        self._config = config or CircuitConfig(name="default")
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._stats = CircuitStats(name=self._config.name, state=self._state)
        self._lock = asyncio.Lock()

    def _change_state(self, new_state: CircuitState) -> None:
        """Change circuit state."""
        old_state = self._state
        self._state = new_state
        self._stats.state = new_state
        self._stats.state_changed_at = datetime.now()

        if new_state == CircuitState.HALF_OPEN:
            self._success_count = 0

        if self._config.on_state_change:
            self._config.on_state_change(self._config.name, old_state, new_state)

    def _should_allow_request(self) -> bool:
        """Check if request should be allowed."""
        if self._state == CircuitState.CLOSED:
            return True

        if self._state == CircuitState.OPEN:
            if self._last_failure_time:
                elapsed = time.time() - self._last_failure_time
                if elapsed >= self._config.timeout_seconds:
                    self._change_state(CircuitState.HALF_OPEN)
                    return True
            return False

        return True  # HALF_OPEN allows one test request

    async def call(
        self,
        func: Callable[..., Coroutine[Any, Any, Any]],
        *args: Any,
        **kwargs: Any,
    ) -> CircuitResult:
        """Call function with circuit breaker protection.

        Args:
            func: Async function to call.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            CircuitResult with outcome.
        """
        async with self._lock:
            if not self._should_allow_request():
                self._stats.rejected_calls += 1
                if self._config.on_rejected:
                    self._config.on_rejected(self._config.name)
                return CircuitResult(
                    success=False,
                    state=self._state,
                    error="Circuit breaker open",
                )

        start_time = time.time()
        try:
            result = await func(*args, **kwargs)
            duration_ms = (time.time() - start_time) * 1000

            async with self._lock:
                self._failure_count = 0
                self._stats.successful_calls += 1
                self._stats.last_success_at = datetime.now()

                if self._state == CircuitState.HALF_OPEN:
                    self._success_count += 1
                    if self._success_count >= self._config.success_threshold:
                        self._change_state(CircuitState.CLOSED)

            return CircuitResult(
                success=True,
                state=self._state,
                duration_ms=duration_ms,
            )

        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            error_str = str(e)

            async with self._lock:
                self._failure_count += 1
                self._last_failure_time = time.time()
                self._stats.failed_calls += 1
                self._stats.last_failure_at = datetime.now()

                if self._state == CircuitState.HALF_OPEN:
                    self._change_state(CircuitState.OPEN)
                elif self._failure_count >= self._config.failure_threshold:
                    if error_str not in self._config.excluded_errors:
                        self._change_state(CircuitState.OPEN)

            return CircuitResult(
                success=False,
                state=self._state,
                error=error_str,
                duration_ms=duration_ms,
            )

    def get_state(self) -> CircuitState:
        """Get current circuit state."""
        return self._state

    def get_stats(self) -> CircuitStats:
        """Get circuit breaker statistics."""
        self._stats.total_calls = self._stats.successful_calls + self._stats.failed_calls
        return self._stats

    def reset(self) -> None:
        """Reset circuit breaker to closed state."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = None
        self._stats = CircuitStats(name=self._config.name, state=self._state)

    def is_closed(self) -> bool:
        """Check if circuit is closed."""
        return self._state == CircuitState.CLOSED

    def is_open(self) -> bool:
        """Check if circuit is open."""
        return self._state == CircuitState.OPEN

    def is_half_open(self) -> bool:
        """Check if circuit is half-open."""
        return self._state == CircuitState.HALF_OPEN
