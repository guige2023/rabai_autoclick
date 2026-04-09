"""
Error Recovery Action Module.

Provides automatic error recovery strategies for automation
failures including retry with backoff, fallback actions,
and graceful degradation.
"""

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


class RecoveryStrategy(Enum):
    """Recovery strategy types."""
    RETRY = "retry"
    FALLBACK = "fallback"
    SKIP = "skip"
    ABORT = "abort"
    IGNORE = "ignore"


@dataclass
class RecoveryAction:
    """A recovery action to execute."""
    strategy: RecoveryStrategy
    action: Optional[Callable[[], Any]] = None
    max_attempts: int = 3
    delay: float = 1.0
    backoff_multiplier: float = 2.0


@dataclass
class ErrorContext:
    """Context information about an error."""
    error: Exception
    error_type: str
    timestamp: float
    attempt: int
    metadata: dict = field(default_factory=dict)


class ErrorRecoveryManager:
    """Manages error recovery for automation actions."""

    def __init__(self):
        """Initialize error recovery manager."""
        self._handlers: dict[str, RecoveryAction] = {}
        self._error_log: list[ErrorContext] = []

    def register_handler(
        self,
        error_type: str,
        strategy: RecoveryStrategy,
        action: Optional[Callable[[], Any]] = None,
        max_attempts: int = 3,
        delay: float = 1.0,
        backoff_multiplier: float = 2.0,
    ) -> None:
        """
        Register an error handler.

        Args:
            error_type: Exception type name to handle.
            strategy: Recovery strategy.
            action: Optional recovery action callable.
            max_attempts: Maximum retry attempts.
            delay: Initial delay between retries.
            backoff_multiplier: Backoff multiplier for retries.
        """
        self._handlers[error_type] = RecoveryAction(
            strategy=strategy,
            action=action,
            max_attempts=max_attempts,
            delay=delay,
            backoff_multiplier=backoff_multiplier,
        )

    def execute_with_recovery(
        self,
        action: Callable[[], Any],
        error_types: Optional[list[str]] = None,
    ) -> tuple[Any, bool]:
        """
        Execute an action with error recovery.

        Args:
            action: Action to execute.
            error_types: List of error types to handle.

        Returns:
            Tuple of (result, success).
        """
        error_types = error_types or ["Exception"]
        attempt = 0

        while attempt < 10:
            try:
                result = action()
                return result, True
            except Exception as e:
                attempt += 1
                error_type = type(e).__name__
                ctx = ErrorContext(
                    error=e,
                    error_type=error_type,
                    timestamp=time.time(),
                    attempt=attempt,
                )
                self._error_log.append(ctx)

                handler = self._handlers.get(error_type)

                if handler is None and error_types:
                    for et in error_types:
                        if et in error_type or error_type in et:
                            handler = self._handlers.get(et)
                            break

                if handler is None:
                    return None, False

                if handler.strategy == RecoveryStrategy.IGNORE:
                    return None, True

                if handler.strategy == RecoveryStrategy.ABORT:
                    return None, False

                if handler.strategy == RecoveryStrategy.SKIP:
                    return None, True

                if handler.strategy == RecoveryStrategy.FALLBACK:
                    if handler.action:
                        try:
                            return handler.action(), True
                        except Exception:
                            pass
                    return None, False

                if handler.strategy == RecoveryStrategy.RETRY:
                    if attempt >= handler.max_attempts:
                        return None, False
                    sleep_time = handler.delay * (handler.backoff_multiplier ** (attempt - 1))
                    time.sleep(sleep_time)
                else:
                    return None, False

        return None, False

    def get_error_log(self) -> list[ErrorContext]:
        """
        Get the error log.

        Returns:
            List of ErrorContext objects.
        """
        return list(self._error_log)

    def clear_error_log(self) -> None:
        """Clear the error log."""
        self._error_log.clear()
