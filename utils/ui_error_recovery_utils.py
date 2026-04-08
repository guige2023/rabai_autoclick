"""UI Error Recovery Utilities.

Provides error handling and recovery strategies for UI automation.
Handles common error patterns and implements retry/recovery workflows.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Optional, TypeVar, Generic


class ErrorType(Enum):
    """Categories of UI automation errors."""

    ELEMENT_NOT_FOUND = auto()
    ELEMENT_DISABLED = auto()
    ELEMENT_OBSCURED = auto()
    TIMEOUT = auto()
    WINDOW_NOT_FOUND = auto()
    WINDOW_MINIMIZED = auto()
    PERMISSION_DENIED = auto()
    UNEXPECTED_POPUP = auto()
    ASSERTION_FAILED = auto()
    UNKNOWN = auto()


@dataclass
class RecoveryAction:
    """A single recovery action to attempt.

    Attributes:
        name: Human-readable name of the action.
        action_func: Callable that performs the recovery.
        delay_ms: Delay before executing this action.
        max_attempts: Maximum times to attempt this action.
    """

    name: str
    action_func: Callable[[], bool]
    delay_ms: int = 0
    max_attempts: int = 1


@dataclass
class ErrorContext:
    """Context information about an error.

    Attributes:
        error_type: Category of the error.
        message: Error message string.
        element_id: Element ID involved, if any.
        window_id: Window ID involved, if any.
        screenshot_path: Path to screenshot at error time.
        timestamp: When the error occurred.
    """

    error_type: ErrorType
    message: str
    element_id: Optional[str] = None
    window_id: Optional[str] = None
    screenshot_path: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class RecoveryResult:
    """Result of a recovery attempt.

    Attributes:
        success: Whether recovery succeeded.
        recovery_attempted: Name of recovery action attempted.
        recovery_time_ms: Time spent in recovery.
        final_error: Error type if recovery failed.
    """

    success: bool
    recovery_attempted: Optional[str] = None
    recovery_time_ms: int = 0
    final_error: Optional[ErrorType] = None


class RecoveryStrategy:
    """Base class for error recovery strategies.

    Implement this to create custom recovery logic.
    """

    def __init__(self, error_type: ErrorType):
        """Initialize the strategy.

        Args:
            error_type: Type of error this strategy handles.
        """
        self.error_type = error_type

    def get_recovery_actions(
        self,
        context: ErrorContext,
    ) -> list[RecoveryAction]:
        """Get recovery actions for the error context.

        Args:
            context: Error context information.

        Returns:
            List of RecoveryAction to attempt in order.
        """
        return []


class ElementNotFoundRecovery(RecoveryStrategy):
    """Recovery for element not found errors."""

    def __init__(self):
        """Initialize element not found recovery."""
        super().__init__(ErrorType.ELEMENT_NOT_FOUND)

    def get_recovery_actions(
        self,
        context: ErrorContext,
    ) -> list[RecoveryAction]:
        """Get recovery actions for element not found."""
        actions = []

        # Wait and retry
        actions.append(
            RecoveryAction(
                name="wait_and_retry",
                action_func=lambda: True,
                delay_ms=1000,
                max_attempts=3,
            )
        )

        # Refresh the page/view
        actions.append(
            RecoveryAction(
                name="refresh_view",
                action_func=lambda: True,
                delay_ms=500,
                max_attempts=1,
            )
        )

        return actions


class ElementObscuredRecovery(RecoveryStrategy):
    """Recovery for element obscured by other elements."""

    def __init__(self):
        """Initialize element obscured recovery."""
        super().__init__(ErrorType.ELEMENT_OBSCURED)

    def get_recovery_actions(
        self,
        context: ErrorContext,
    ) -> list[RecoveryAction]:
        """Get recovery actions for obscured element."""
        actions = []

        # Close popup and retry
        actions.append(
            RecoveryAction(
                name="dismiss_popups",
                action_func=lambda: True,
                delay_ms=300,
                max_attempts=2,
            )
        )

        # Bring window to front
        actions.append(
            RecoveryAction(
                name="bring_to_front",
                action_func=lambda: True,
                delay_ms=200,
                max_attempts=1,
            )
        )

        return actions


class TimeoutRecovery(RecoveryStrategy):
    """Recovery for timeout errors."""

    def __init__(self):
        """Initialize timeout recovery."""
        super().__init__(ErrorType.TIMEOUT)

    def get_recovery_actions(
        self,
        context: ErrorContext,
    ) -> list[RecoveryAction]:
        """Get recovery actions for timeout."""
        actions = []

        # Increase timeout and retry
        actions.append(
            RecoveryAction(
                name="increase_timeout",
                action_func=lambda: True,
                delay_ms=0,
                max_attempts=1,
            )
        )

        return actions


class ErrorRecoveryManager:
    """Manages error recovery for UI automation.

    Coordinates recovery strategies and executes recovery actions.

    Example:
        manager = ErrorRecoveryManager()
        manager.register_strategy(ElementNotFoundRecovery())
        result = manager.try_recover(context)
    """

    def __init__(self):
        """Initialize the recovery manager."""
        self._strategies: dict[ErrorType, RecoveryStrategy] = {}
        self._default_recovery: list[RecoveryAction] = []
        self._recovery_callbacks: list[Callable[[ErrorContext], None]] = []

    def register_strategy(self, strategy: RecoveryStrategy) -> None:
        """Register an error recovery strategy.

        Args:
            strategy: RecoveryStrategy to register.
        """
        self._strategies[strategy.error_type] = strategy

    def register_default_recovery(
        self,
        action: RecoveryAction,
    ) -> None:
        """Register a default recovery action.

        Args:
            action: RecoveryAction to use when no specific strategy exists.
        """
        self._default_recovery.append(action)

    def on_recovery_attempt(
        self,
        callback: Callable[[ErrorContext], None],
    ) -> None:
        """Register a callback for recovery attempts.

        Args:
            callback: Function to call when recovery is attempted.
        """
        self._recovery_callbacks.append(callback)

    def try_recover(
        self,
        context: ErrorContext,
    ) -> RecoveryResult:
        """Attempt to recover from an error.

        Args:
            context: Error context information.

        Returns:
            RecoveryResult indicating success or failure.
        """
        start_time = time.time()

        # Notify callbacks
        for callback in self._recovery_callbacks:
            try:
                callback(context)
            except Exception:
                pass

        # Get recovery actions
        strategy = self._strategies.get(context.error_type)
        if strategy:
            actions = strategy.get_recovery_actions(context)
        else:
            actions = self._default_recovery.copy()

        if not actions:
            return RecoveryResult(
                success=False,
                final_error=context.error_type,
            )

        # Execute recovery actions
        for action in actions:
            for attempt in range(action.max_attempts):
                if action.delay_ms > 0:
                    time.sleep(action.delay_ms / 1000.0)

                try:
                    success = action.action_func()
                    if success:
                        elapsed_ms = int((time.time() - start_time) * 1000)
                        return RecoveryResult(
                            success=True,
                            recovery_attempted=action.name,
                            recovery_time_ms=elapsed_ms,
                        )
                except Exception:
                    pass

        elapsed_ms = int((time.time() - start_time) * 1000)
        return RecoveryResult(
            success=False,
            recovery_attempted=actions[-1].name if actions else None,
            recovery_time_ms=elapsed_ms,
            final_error=context.error_type,
        )


T = TypeVar("T")


class RetryableOperation(Generic[T]):
    """Wrapper for operations that can be retried on failure.

    Example:
        operation = RetryableOperation(lambda: find_element("btn"))
        result = operation.execute(max_retries=3)
    """

    def __init__(
        self,
        func: Callable[[], T],
        error_types: Optional[list[type[Exception]]] = None,
    ):
        """Initialize the retryable operation.

        Args:
            func: Function to execute.
            error_types: Exception types that trigger retry.
        """
        self.func = func
        self.error_types = error_types or [Exception]

    def execute(
        self,
        max_retries: int = 3,
        base_delay_ms: int = 500,
        max_delay_ms: int = 5000,
        exponential_backoff: bool = True,
    ) -> T:
        """Execute the operation with retries.

        Args:
            max_retries: Maximum number of retry attempts.
            base_delay_ms: Base delay between retries.
            max_delay_ms: Maximum delay between retries.
            exponential_backoff: Use exponential backoff.

        Returns:
            Result of the function.

        Raises:
            The last exception if all retries fail.
        """
        last_error: Optional[Exception] = None
        delay_ms = base_delay_ms

        for attempt in range(max_retries + 1):
            try:
                return self.func()
            except Exception as e:
                last_error = e
                if not any(isinstance(e, et) for et in self.error_types):
                    raise

                if attempt < max_retries:
                    time.sleep(delay_ms / 1000.0)
                    if exponential_backoff:
                        delay_ms = min(delay_ms * 2, max_delay_ms)

        if last_error:
            raise last_error
        raise RuntimeError("Retryable operation failed")
