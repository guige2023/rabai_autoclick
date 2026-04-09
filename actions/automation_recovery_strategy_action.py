"""
Automation Recovery Strategy Action.

Implements various recovery strategies for failed automation
tasks: retry, fallback, circuit breaker, and graceful degradation.

Author: rabai_autoclick
License: MIT
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, Generic

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

logger = logging.getLogger(__name__)
T = TypeVar("T")


class RecoveryAction(Enum):
    """What to do after a failure."""
    RETRY = auto()
    RETRY_WITH_BACKOFF = auto()
    FALLBACK = auto()
    CIRCUIT_OPEN = auto()
    GRACEFUL_DEGRADATION = auto()
    ABORT = auto()
    ESCALATE = auto()


@dataclass
class RecoveryAttempt:
    """Record of a recovery attempt."""
    attempt_number: int
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None
    success: bool = False
    error: Optional[str] = None
    recovery_action: RecoveryAction = RecoveryAction.RETRY
    duration_ms: float = 0.0


@dataclass
class RecoveryStrategyConfig:
    """Configuration for a recovery strategy."""
    max_retries: int = 3
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    retry_on: Tuple[type, ...] = (Exception,)
    fallback_value: Optional[Any] = None
    fallback_fn: Optional[Callable[[Exception], Any]] = None
    circuit_threshold: int = 5
    circuit_timeout_seconds: float = 30.0


@dataclass
class CircuitBreakerState:
    """State of a circuit breaker."""
    failure_count: int = 0
    last_failure_time: Optional[datetime] = None
    state: str = "closed"  # closed, open, half_open
    opened_at: Optional[datetime] = None


class RecoveryStrategy:
    """Base class for recovery strategies."""

    def __init__(self, config: RecoveryStrategyConfig) -> None:
        self._config = config

    def execute(
        self,
        fn: Callable[[], T],
        fallback_fn: Optional[Callable[[Exception], T]] = None,
    ) -> T:
        """Execute function with recovery strategy."""
        raise NotImplementedError


class RetryStrategy(RecoveryStrategy):
    """Simple retry with fixed delay."""

    def execute(
        self,
        fn: Callable[[], T],
        fallback_fn: Optional[Callable[[Exception], T]] = None,
    ) -> T:
        last_error: Optional[Exception] = None
        for attempt in range(self._config.max_retries):
            try:
                return fn()
            except self._config.retry_on as exc:
                last_error = exc
                logger.warning("Attempt %d/%d failed: %s", attempt + 1,
                              self._config.max_retries, exc)
                if attempt < self._config.max_retries - 1:
                    delay = self._config.base_delay_seconds
                    if self._config.jitter:
                        delay *= (0.5 + random.random())
                    time.sleep(delay)

        if fallback_fn and last_error:
            logger.info("Using fallback after %d attempts", self._config.max_retries)
            return fallback_fn(last_error)
        raise last_error or Exception("All retries exhausted")


class ExponentialBackoffRetry(RecoveryStrategy):
    """Retry with exponential backoff and jitter."""

    def __init__(self, config: RecoveryStrategyConfig) -> None:
        super().__init__(config)
        self._attempts: List[RecoveryAttempt] = []

    def execute(
        self,
        fn: Callable[[], T],
        fallback_fn: Optional[Callable[[Exception], T]] = None,
    ) -> T:
        last_error: Optional[Exception] = None
        for attempt in range(self._config.max_retries):
            attempt_record = RecoveryAttempt(
                attempt_number=attempt + 1,
                recovery_action=RecoveryAction.RETRY_WITH_BACKOFF,
            )
            start = time.monotonic()
            try:
                result = fn()
                attempt_record.success = True
                attempt_record.completed_at = datetime.now(timezone.utc)
                attempt_record.duration_ms = (time.monotonic() - start) * 1000
                self._attempts.append(attempt_record)
                return result
            except self._config.retry_on as exc:
                last_error = exc
                attempt_record.error = str(exc)
                attempt_record.duration_ms = (time.monotonic() - start) * 1000
                self._attempts.append(attempt_record)
                logger.warning("Backoff attempt %d/%d failed: %s", attempt + 1,
                              self._config.max_retries, exc)

                if attempt < self._config.max_retries - 1:
                    delay = min(
                        self._config.base_delay_seconds * (self._config.exponential_base ** attempt),
                        self._config.max_delay_seconds,
                    )
                    if self._config.jitter:
                        delay *= (0.5 + random.random())
                    logger.info("Sleeping %.2fs before retry", delay)
                    time.sleep(delay)

        if fallback_fn and last_error:
            return fallback_fn(last_error)
        raise last_error

    def get_attempts(self) -> List[RecoveryAttempt]:
        return self._attempts


class CircuitBreakerRecovery:
    """
    Circuit breaker pattern for preventing cascading failures.

    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Failures exceeded threshold, requests fail fast
    - HALF_OPEN: Testing if service recovered
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        timeout_seconds: float = 30.0,
        half_open_max_calls: int = 3,
    ) -> None:
        self._failure_threshold = failure_threshold
        self._timeout = timeout_seconds
        self._half_open_max = half_open_max_calls
        self._state = CircuitBreakerState()
        self._half_open_calls = 0
        self._lock = asyncio.Lock()

    async def call(self, fn: Callable[[], T]) -> T:
        """Execute with circuit breaker protection."""
        async with self._lock:
            if self._state.state == "open":
                if self._should_attempt_reset():
                    self._state.state = "half_open"
                    self._half_open_calls = 0
                    logger.info("Circuit breaker entering HALF_OPEN state")
                else:
                    raise CircuitOpenError(
                        f"Circuit breaker open, retry after "
                        f"{(self._timeout - self._time_since_failure()):.0f}s"
                    )

        try:
            result = fn()
            if asyncio.iscoroutine(result):
                result = await result
            await self._on_success()
            return result
        except Exception as exc:
            await self._on_failure()
            raise

    def _should_attempt_reset(self) -> bool:
        if not self._state.last_failure_time:
            return True
        elapsed = (datetime.now(timezone.utc) - self._state.last_failure_time).total_seconds()
        return elapsed >= self._timeout

    def _time_since_failure(self) -> float:
        if not self._state.last_failure_time:
            return 0.0
        return (datetime.now(timezone.utc) - self._state.last_failure_time).total_seconds()

    async def _on_success(self) -> None:
        async with self._lock:
            if self._state.state == "half_open":
                self._half_open_calls += 1
                if self._half_open_calls >= self._half_open_max:
                    logger.info("Circuit breaker closing after successful half-open calls")
                    self._state = CircuitBreakerState(state="closed")
            elif self._state.state == "closed":
                self._state.failure_count = max(0, self._state.failure_count - 1)

    async def _on_failure(self) -> None:
        async with self._lock:
            self._state.failure_count += 1
            self._state.last_failure_time = datetime.now(timezone.utc)

            if self._state.state == "half_open":
                logger.warning("Half-open call failed, reopening circuit")
                self._state.state = "open"
                self._state.opened_at = datetime.now(timezone.utc)
            elif self._state.failure_count >= self._failure_threshold:
                logger.warning("Circuit breaker opening after %d failures",
                              self._state.failure_count)
                self._state.state = "open"
                self._state.opened_at = datetime.now(timezone.utc)

    def get_state(self) -> str:
        return self._state.state


class CircuitOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass


class GracefulDegradationRecovery:
    """
    Provides graceful degradation when primary functionality fails.
    Returns degraded but functional output instead of complete failure.
    """

    def __init__(self) -> None:
        self._degradation_levels: Dict[str, Callable[[], Any]] = {}
        self._current_level = 0

    def register_level(self, level: int, fn: Callable[[], Any]) -> None:
        """Register a degradation level (higher = more degraded)."""
        self._degradation_levels[level] = fn

    def execute(
        self,
        primary_fn: Callable[[], T],
        levels: Optional[List[int]] = None,
    ) -> T:
        """Execute primary function, fall back to degradation levels on failure."""
        try:
            self._current_level = 0
            return primary_fn()
        except Exception as exc:
            logger.warning("Primary function failed, attempting degradation: %s", exc)
            sorted_levels = sorted(self._degradation_levels.keys())
            for level in sorted_levels:
                if levels and level not in levels:
                    continue
                try:
                    logger.info("Attempting degradation level %d", level)
                    self._current_level = level
                    result = self._degradation_levels[level]()
                    logger.info("Degradation level %d succeeded", level)
                    return result
                except Exception:
                    continue
            raise exc

    def get_current_level(self) -> int:
        return self._current_level
