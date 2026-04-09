"""
Error Recovery Utilities for UI Automation.

This module provides utilities for error handling, recovery strategies,
and retry logic in automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
import traceback
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional, TypeVar


T = TypeVar('T')


class RecoveryStrategy(Enum):
    """Error recovery strategies."""
    RETRY = auto()
    FALLBACK = auto()
    SKIP = auto()
    ABORT = auto()
    FIGNORE = auto()


class ErrorSeverity(Enum):
    """Error severity levels."""
    INFO = 0
    WARNING = 1
    ERROR = 2
    CRITICAL = 3


@dataclass
class RecoveryAction:
    """
    A recovery action to execute.
    
    Attributes:
        name: Action name
        action_func: Callable to execute
        max_attempts: Maximum attempts
        timeout: Action timeout in seconds
        required: Whether action is required for continuation
    """
    name: str
    action_func: Callable[[], Any]
    max_attempts: int = 3
    timeout: float = 30.0
    required: bool = True


@dataclass
class ErrorContext:
    """
    Context information for an error.
    
    Attributes:
        error: The exception that occurred
        error_type: Type of error
        message: Error message
        severity: Error severity
        timestamp: When error occurred
        recovered: Whether error was recovered from
        recovery_attempts: Number of recovery attempts made
    """
    error: Optional[Exception]
    error_type: str
    message: str
    severity: ErrorSeverity = ErrorSeverity.ERROR
    timestamp: float = field(default_factory=time.time)
    recovered: bool = False
    recovery_attempts: int = 0
    stack_trace: str = ""
    context_data: dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if self.error:
            self.error_type = type(self.error).__name__
            self.message = str(self.error)
            self.stack_trace = traceback.format_exc()


@dataclass
class ErrorHandler:
    """
    Handles errors with recovery strategies.
    
    Example:
        handler = ErrorHandler()
        handler.register_recovery("TimeoutError", RecoveryStrategy.RETRY, max_attempts=3)
        result = handler.execute(some_function)
    """
    
    def __init__(self):
        self._strategies: dict[str, RecoveryStrategy] = {}
        self._recovery_actions: dict[str, list[RecoveryAction]] = {}
        self._error_handlers: dict[str, Callable[[ErrorContext], Any]] = {}
        self._error_history: list[ErrorContext] = []
    
    def register_strategy(
        self,
        error_type: str,
        strategy: RecoveryStrategy,
        max_attempts: Optional[int] = None
    ) -> None:
        """
        Register a recovery strategy for an error type.
        
        Args:
            error_type: Exception type name
            strategy: Recovery strategy to use
            max_attempts: Maximum retry attempts (for RETRY strategy)
        """
        self._strategies[error_type] = strategy
        
        if strategy == RecoveryStrategy.RETRY and max_attempts:
            self._recovery_actions.setdefault(error_type, [])
    
    def register_recovery_action(
        self,
        error_type: str,
        action: RecoveryAction
    ) -> None:
        """Register a recovery action for an error type."""
        self._recovery_actions.setdefault(error_type, []).append(action)
    
    def register_error_handler(
        self,
        error_type: str,
        handler_func: Callable[[ErrorContext], Any]
    ) -> None:
        """Register a custom error handler function."""
        self._error_handlers[error_type] = handler_func
    
    def execute(
        self,
        func: Callable[[], T],
        error_types: Optional[list[type]] = None,
        context_data: Optional[dict[str, Any]] = None
    ) -> 'ExecutionResult[T]':
        """
        Execute a function with error handling.
        
        Args:
            func: Function to execute
            error_types: List of error types to catch
            context_data: Additional context to store with errors
            
        Returns:
            ExecutionResult with value or error
        """
        error_types = error_types or [Exception]
        context_data = context_data or {}
        start_time = time.time()
        
        for attempt in range(self._get_max_attempts(error_types)):
            try:
                value = func()
                duration_ms = (time.time() - start_time) * 1000
                return ExecutionResult(
                    success=True,
                    value=value,
                    duration_ms=duration_ms
                )
            except Exception as e:
                error_ctx = self._create_error_context(e, attempt, context_data)
                
                strategy = self._get_strategy(type(e).__name__)
                
                if strategy == RecoveryStrategy.IGNORE:
                    duration_ms = (time.time() - start_time) * 1000
                    return ExecutionResult(
                        success=True,
                        value=None,
                        duration_ms=duration_ms,
                        error=error_ctx
                    )
                
                if strategy != RecoveryStrategy.RETRY:
                    duration_ms = (time.time() - start_time) * 1000
                    return ExecutionResult(
                        success=False,
                        duration_ms=duration_ms,
                        error=error_ctx
                    )
        
        duration_ms = (time.time() - start_time) * 1000
        return ExecutionResult(
            success=False,
            duration_ms=duration_ms,
            error=self._error_history[-1] if self._error_history else None
        )
    
    def _get_strategy(self, error_type: str) -> RecoveryStrategy:
        """Get the recovery strategy for an error type."""
        return self._strategies.get(error_type, RecoveryStrategy.ABORT)
    
    def _get_max_attempts(self, error_types: list[type]) -> int:
        """Get maximum attempts for error types."""
        max_attempts = 1
        for et in error_types:
            actions = self._recovery_actions.get(et.__name__, [])
            for action in actions:
                max_attempts = max(max_attempts, action.max_attempts)
        return max_attempts
    
    def _create_error_context(
        self,
        error: Exception,
        attempt: int,
        context_data: dict[str, Any]
    ) -> ErrorContext:
        """Create an error context from an exception."""
        ctx = ErrorContext(
            error=error,
            error_type=type(error).__name__,
            message=str(error),
            recovery_attempts=attempt,
            context_data=context_data
        )
        self._error_history.append(ctx)
        return ctx
    
    def get_error_history(self) -> list[ErrorContext]:
        """Get history of errors."""
        return list(self._error_history)
    
    def clear_history(self) -> None:
        """Clear error history."""
        self._error_history.clear()


@dataclass
class ExecutionResult(Generic[T]):
    """Result of executing a function with error handling."""
    success: bool
    value: Optional[T] = None
    duration_ms: float = 0.0
    error: Optional[ErrorContext] = None
    recovery_strategy: Optional[RecoveryStrategy] = None


class CircuitBreaker:
    """
    Circuit breaker pattern for preventing cascading failures.
    
    Example:
        breaker = CircuitBreaker(failure_threshold=5, timeout=60)
        result = breaker.execute(some_function)
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        timeout: float = 60.0,
        expected_exception: type = Exception
    ):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.expected_exception = expected_exception
        
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._state = CircuitState.CLOSED
    
    @property
    def state(self) -> 'CircuitState':
        """Get current circuit state."""
        if self._state == CircuitState.OPEN:
            if self._last_failure_time and \
               time.time() - self._last_failure_time >= self.timeout:
                self._state = CircuitState.HALF_OPEN
        return self._state
    
    def execute(self, func: Callable[[], T]) -> T:
        """
        Execute a function through the circuit breaker.
        
        Raises:
            CircuitOpenError: If circuit is open
            Exception: If function fails
        """
        if self.state == CircuitState.OPEN:
            raise CircuitOpenError("Circuit breaker is open")
        
        try:
            result = func()
            self._on_success()
            return result
        except self.expected_exception as e:
            self._on_failure()
            raise
    
    def _on_success(self) -> None:
        """Handle successful execution."""
        self._failure_count = 0
        self._state = CircuitState.CLOSED
    
    def _on_failure(self) -> None:
        """Handle failed execution."""
        self._failure_count += 1
        self._last_failure_time = time.time()
        
        if self._failure_count >= self.failure_threshold:
            self._state = CircuitState.OPEN
    
    def reset(self) -> None:
        """Reset the circuit breaker."""
        self._failure_count = 0
        self._last_failure_time = None
        self._state = CircuitState.CLOSED


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = auto()   # Normal operation
    OPEN = auto()     # Failing fast
    HALF_OPEN = auto()  # Testing recovery


class CircuitOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass
