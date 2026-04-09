"""
Action guard and guardrail utilities for safe automation.

This module provides utilities for adding safeguards to
automation actions including timeouts, retries, and validation.
"""

from __future__ import annotations

import time
import threading
from dataclasses import dataclass, field
from typing import Callable, Any, Optional, TypeVar, Generic
from enum import Enum, auto


class GuardType(Enum):
    """Types of action guards."""
    TIMEOUT = auto()
    RETRY = auto()
    VALIDATION = auto()
    RATE_LIMIT = auto()
    CONDITION = auto()


@dataclass
class GuardResult:
    """
    Result of a guarded action execution.

    Attributes:
        success: Whether the action succeeded.
        value: Return value if successful.
        error: Error message if failed.
        guards_triggered: List of guards that were triggered.
        duration: Execution time in seconds.
    """
    success: bool
    value: Any = None
    error: Optional[str] = None
    guards_triggered: list = field(default_factory=list)
    duration: float = 0.0


@dataclass
class GuardConfig:
    """Base configuration for a guard."""
    guard_type: GuardType
    enabled: bool = True


@dataclass
class TimeoutConfig(GuardConfig):
    """Timeout guard configuration."""
    guard_type: GuardType = GuardType.TIMEOUT
    timeout_seconds: float = 30.0


@dataclass
class RetryConfig(GuardConfig):
    """Retry guard configuration."""
    guard_type: GuardType = GuardType.RETRY
    max_attempts: int = 3
    backoff_multiplier: float = 2.0
    initial_delay: float = 0.1
    exceptions: tuple = (Exception,)


@dataclass
class ValidationConfig(GuardConfig):
    """Validation guard configuration."""
    guard_type: GuardType = GuardType.VALIDATION
    validator: Callable[[Any], bool] = None
    error_message: str = "Validation failed"


T = TypeVar("T")


class ActionGuard(Generic[T]):
    """
    Wrapper that adds guards to an action.

    Guards are executed in order and can short-circuit
    the action execution.
    """

    def __init__(self, action: Callable[[], T]) -> None:
        self._action = action
        self._guards: list = []
        self._timeout_handler: Optional[Callable] = None

    def with_timeout(self, seconds: float) -> ActionGuard[T]:
        """Add a timeout guard."""
        self._guards.append(TimeoutConfig(timeout_seconds=seconds))
        return self

    def with_retry(
        self,
        max_attempts: int = 3,
        exceptions: tuple = (Exception,),
    ) -> ActionGuard[T]:
        """Add a retry guard."""
        self._guards.append(RetryConfig(
            max_attempts=max_attempts,
            exceptions=exceptions,
        ))
        return self

    def with_validation(
        self,
        validator: Callable[[Any], bool],
        error_message: str = "Validation failed",
    ) -> ActionGuard[T]:
        """Add a validation guard."""
        self._guards.append(ValidationConfig(
            validator=validator,
            error_message=error_message,
        ))
        return self

    def on_timeout(self, handler: Callable[[], None]) -> ActionGuard[T]:
        """Set handler to call on timeout."""
        self._timeout_handler = handler
        return self

    def execute(self) -> GuardResult:
        """
        Execute the action with all guards.

        Returns GuardResult with success status and any guard triggers.
        """
        start_time = time.time()
        guards_triggered = []
        result: Any = None
        error: Optional[str] = None

        # Apply each guard type
        for guard in self._guards:
            if not guard.enabled:
                continue

            if isinstance(guard, TimeoutConfig):
                result = self._execute_with_timeout(
                    guard.timeout_seconds,
                    guards_triggered,
                )
                if result is ...:  # Timeout sentinel
                    error = f"Action timed out after {guard.timeout_seconds}s"
                    if self._timeout_handler:
                        self._timeout_handler()
                    return GuardResult(
                        success=False,
                        error=error,
                        guards_triggered=guards_triggered,
                        duration=time.time() - start_time,
                    )

            elif isinstance(guard, RetryConfig):
                result, attempt_error = self._execute_with_retry(guard, guards_triggered)
                if attempt_error:
                    error = attempt_error

            elif isinstance(guard, ValidationConfig):
                if result is not None:
                    try:
                        if not guard.validator(result):
                            error = guard.error_message
                            guards_triggered.append(GuardType.VALIDATION)
                    except Exception as e:
                        error = f"Validation error: {e}"
                        guards_triggered.append(GuardType.VALIDATION)

        if error:
            return GuardResult(
                success=False,
                error=error,
                guards_triggered=guards_triggered,
                duration=time.time() - start_time,
            )

        return GuardResult(
            success=True,
            value=result,
            guards_triggered=guards_triggered,
            duration=time.time() - start_time,
        )

    def _execute_with_timeout(
        self,
        timeout: float,
        guards_triggered: list,
    ) -> Any:
        """Execute action with timeout."""
        result: list = [None]
        finished: list = [False]

        def worker() -> None:
            try:
                result[0] = self._action()
            except Exception as e:
                result[0] = e
            finally:
                finished[0] = True

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join(timeout)

        if not finished[0]:
            guards_triggered.append(GuardType.TIMEOUT)
            return ...  # Timeout sentinel

        if isinstance(result[0], Exception):
            raise result[0]

        return result[0]

    def _execute_with_retry(
        self,
        config: RetryConfig,
        guards_triggered: list,
    ) -> tuple:
        """Execute action with retry logic."""
        last_error: Optional[Exception] = None
        delay = config.initial_delay

        for attempt in range(config.max_attempts):
            try:
                return self._action(), None
            except config.exceptions as e:
                last_error = e
                if attempt < config.max_attempts - 1:
                    time.sleep(delay)
                    delay *= config.backoff_multiplier

        guards_triggered.append(GuardType.RETRY)
        return None, f"Failed after {config.max_attempts} attempts: {last_error}"


def guarded(
    timeout: Optional[float] = None,
    retry_count: int = 0,
) -> Callable[[Callable], Callable]:
    """
    Decorator to add guards to a function.

    Usage:
        @guarded(timeout=5.0, retry_count=3)
        def my_function():
            ...
    """
    def decorator(func: Callable) -> Callable:
        guard = ActionGuard(func)

        if timeout is not None:
            guard.with_timeout(timeout)

        if retry_count > 0:
            guard.with_retry(max_attempts=retry_count)

        def wrapper() -> Any:
            return guard.execute()

        return wrapper
    return decorator


class GuardChain:
    """
    Chain multiple guards together.

    Provides a fluent API for building guard configurations.
    """

    def __init__(self) -> None:
        self._guards: list = []

    def timeout(self, seconds: float) -> GuardChain:
        """Add timeout guard."""
        self._guards.append(TimeoutConfig(timeout_seconds=seconds))
        return self

    def retry(
        self,
        max_attempts: int = 3,
        exceptions: tuple = (Exception,),
    ) -> GuardChain:
        """Add retry guard."""
        self._guards.append(RetryConfig(
            max_attempts=max_attempts,
            exceptions=exceptions,
        ))
        return self

    def validate(
        self,
        validator: Callable[[Any], bool],
        error_message: str = "Validation failed",
    ) -> GuardChain:
        """Add validation guard."""
        self._guards.append(ValidationConfig(
            validator=validator,
            error_message=error_message,
        ))
        return self

    def apply(self, action: Callable[[], Any]) -> ActionGuard:
        """Apply guards to an action."""
        guard = ActionGuard(action)
        guard._guards = self._guards.copy()
        return guard


class RateGuard:
    """
    Guard that limits action execution rate.

    Ensures minimum interval between executions.
    """

    def __init__(self, min_interval: float) -> None:
        self._min_interval = min_interval
        self._last_execution: float = 0.0
        self._lock = threading.Lock()

    def execute(self, action: Callable[[], Any]) -> Any:
        """Execute action if rate limit allows."""
        with self._lock:
            now = time.time()
            elapsed = now - self._last_execution

            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)

            self._last_execution = time.time()

        return action()

    @property
    def time_until_next_allowed(self) -> float:
        """Get time until next execution is allowed."""
        with self._lock:
            elapsed = time.time() - self._last_execution
            return max(0, self._min_interval - elapsed)
