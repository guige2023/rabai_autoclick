"""Automation recovery action for error recovery and retry.

Implements sophisticated recovery strategies including
exponential backoff, circuit breaking, and state rollback.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class RecoveryStrategy(Enum):
    """Recovery strategies when failures occur."""
    RETRY = "retry"
    ROLLBACK = "rollback"
    FALLBACK = "fallback"
    SKIP = "skip"
    ABORT = "abort"


@dataclass
class RecoveryConfig:
    """Configuration for recovery behavior."""
    max_retries: int = 3
    initial_delay_ms: float = 100.0
    max_delay_ms: float = 5000.0
    backoff_multiplier: float = 2.0
    enable_rollback: bool = True
    enable_circuit_breaker: bool = True
    circuit_threshold: int = 5


@dataclass
class RecoveryState:
    """Current state of recovery mechanism."""
    retry_count: int = 0
    circuit_state: str = "closed"
    last_failure: Optional[float] = None
    consecutive_failures: int = 0


@dataclass
class RecoveryResult:
    """Result of a recovery operation."""
    success: bool
    recovered: bool
    strategy_used: RecoveryStrategy
    attempts: int
    total_time_ms: float
    error: Optional[str] = None


class AutomationRecoveryAction:
    """Handle errors and recover from failures.

    Args:
        config: Recovery configuration options.
        rollback_fn: Optional function to rollback state.

    Example:
        >>> recovery = AutomationRecoveryAction()
        >>> result = await recovery.execute_with_recovery(
        ...     fragile_operation,
        ...     fallback_fn=backup_operation,
        ... )
    """

    def __init__(
        self,
        config: Optional[RecoveryConfig] = None,
        rollback_fn: Optional[Callable[[], Any]] = None,
    ) -> None:
        self.config = config or RecoveryConfig()
        self.rollback_fn = rollback_fn
        self._state = RecoveryState()

    async def execute_with_recovery(
        self,
        operation: Callable[..., Any],
        fallback_fn: Optional[Callable[..., Any]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> RecoveryResult:
        """Execute an operation with recovery support.

        Args:
            operation: Operation to execute.
            fallback_fn: Fallback if all retries fail.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Recovery result with details.
        """
        import time
        start_time = time.time()
        attempts = 0
        last_error: Optional[Exception] = None

        while attempts < self.config.max_retries:
            attempts += 1

            if self._is_circuit_open():
                logger.warning("Circuit breaker is open, using fallback")
                return await self._use_fallback(
                    operation, fallback_fn, args, kwargs,
                    start_time, attempts, "Circuit breaker open"
                )

            try:
                if asyncio.iscoroutinefunction(operation):
                    result = await operation(*args, **kwargs)
                else:
                    result = operation(*args, **kwargs)

                self._on_success()
                return RecoveryResult(
                    success=True,
                    recovered=attempts > 1,
                    strategy_used=RecoveryStrategy.RETRY,
                    attempts=attempts,
                    total_time_ms=(time.time() - start_time) * 1000,
                )

            except Exception as e:
                last_error = e
                self._on_failure()
                logger.warning(
                    f"Operation attempt {attempts} failed: {e}"
                )

                if attempts < self.config.max_retries:
                    delay = self._calculate_delay(attempts)
                    await asyncio.sleep(delay / 1000.0)

        if fallback_fn and self.config.enable_rollback:
            return await self._attempt_rollback(
                operation, fallback_fn, args, kwargs, start_time, attempts
            )

        return RecoveryResult(
            success=False,
            recovered=False,
            strategy_used=RecoveryStrategy.ABORT,
            attempts=attempts,
            total_time_ms=(time.time() - start_time) * 1000,
            error=str(last_error),
        )

    async def _attempt_rollback(
        self,
        operation: Callable[..., Any],
        fallback_fn: Callable[..., Any],
        args: tuple,
        kwargs: dict,
        start_time: float,
        attempts: int,
    ) -> RecoveryResult:
        """Attempt to rollback and use fallback.

        Args:
            operation: Original operation.
            fallback_fn: Fallback function.
            args: Operation arguments.
            kwargs: Operation keyword arguments.
            start_time: Start timestamp.
            attempts: Number of attempts made.

        Returns:
            Recovery result.
        """
        if self.rollback_fn:
            try:
                logger.info("Attempting rollback before fallback")
                if asyncio.iscoroutinefunction(self.rollback_fn):
                    await self.rollback_fn()
                else:
                    self.rollback_fn()
            except Exception as e:
                logger.error(f"Rollback failed: {e}")

        try:
            if asyncio.iscoroutinefunction(fallback_fn):
                result = await fallback_fn(*args, **kwargs)
            else:
                result = fallback_fn(*args, **kwargs)

            return RecoveryResult(
                success=True,
                recovered=True,
                strategy_used=RecoveryStrategy.FALLBACK,
                attempts=attempts,
                total_time_ms=(time.time() - start_time) * 1000,
            )
        except Exception as e:
            return RecoveryResult(
                success=False,
                recovered=False,
                strategy_used=RecoveryStrategy.FALLBACK,
                attempts=attempts,
                total_time_ms=(time.time() - start_time) * 1000,
                error=str(e),
            )

    async def _use_fallback(
        self,
        operation: Callable[..., Any],
        fallback_fn: Optional[Callable[..., Any]],
        args: tuple,
        kwargs: dict,
        start_time: float,
        attempts: int,
        reason: str,
    ) -> RecoveryResult:
        """Use fallback when circuit is open.

        Args:
            operation: Original operation.
            fallback_fn: Fallback function.
            args: Operation arguments.
            kwargs: Operation keyword arguments.
            start_time: Start timestamp.
            attempts: Number of attempts.
            reason: Reason for fallback.

        Returns:
            Recovery result.
        """
        if not fallback_fn:
            return RecoveryResult(
                success=False,
                recovered=False,
                strategy_used=RecoveryStrategy.ABORT,
                attempts=attempts,
                total_time_ms=(time.time() - start_time) * 1000,
                error=reason,
            )

        try:
            if asyncio.iscoroutinefunction(fallback_fn):
                await fallback_fn(*args, **kwargs)
            else:
                fallback_fn(*args, **kwargs)

            return RecoveryResult(
                success=True,
                recovered=True,
                strategy_used=RecoveryStrategy.FALLBACK,
                attempts=attempts,
                total_time_ms=(time.time() - start_time) * 1000,
            )
        except Exception as e:
            return RecoveryResult(
                success=False,
                recovered=False,
                strategy_used=RecoveryStrategy.FALLBACK,
                attempts=attempts,
                total_time_ms=(time.time() - start_time) * 1000,
                error=str(e),
            )

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for retry with exponential backoff.

        Args:
            attempt: Current attempt number.

        Returns:
            Delay in milliseconds.
        """
        delay = self.config.initial_delay_ms * (
            self.config.backoff_multiplier ** (attempt - 1)
        )
        return min(delay, self.config.max_delay_ms)

    def _on_success(self) -> None:
        """Handle successful operation."""
        self._state.consecutive_failures = 0
        if self._state.circuit_state == "half_open":
            self._state.circuit_state = "closed"

    def _on_failure(self) -> None:
        """Handle failed operation."""
        self._state.consecutive_failures += 1
        self._state.last_failure = time.time()

        if self.config.enable_circuit_breaker:
            if self._state.consecutive_failures >= self.config.circuit_threshold:
                self._state.circuit_state = "open"
                logger.warning("Circuit breaker opened")

    def _is_circuit_open(self) -> bool:
        """Check if circuit breaker is open.

        Returns:
            True if circuit is open.
        """
        if not self.config.enable_circuit_breaker:
            return False

        if self._state.circuit_state == "open":
            if self._state.last_failure:
                reset_time = (
                    self._state.last_failure +
                    self.config.max_delay_ms / 1000 * 2
                )
                if time.time() > reset_time:
                    self._state.circuit_state = "half_open"
                    logger.info("Circuit breaker entering half-open state")
                    return False
            return True

        return False

    def reset(self) -> None:
        """Reset recovery state."""
        self._state = RecoveryState()

    def get_state(self) -> RecoveryState:
        """Get current recovery state.

        Returns:
            Current state.
        """
        return self._state
