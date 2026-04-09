"""Error context and diagnostic utilities.

Captures and enriches error information with context
for debugging automation failures.
"""

import sys
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Type


@dataclass
class ErrorContext:
    """Context information for an error.

    Attributes:
        error_type: Exception class name.
        message: Error message.
        timestamp: When error occurred.
        stack_trace: Formatted stack trace.
        locals: Local variables snapshot.
        user_data: Additional context data.
    """
    error_type: str
    message: str
    timestamp: datetime = field(default_factory=datetime.now)
    stack_trace: str = ""
    locals: Dict[str, Any] = field(default_factory=dict)
    user_data: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "error_type": self.error_type,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "stack_trace": self.stack_trace,
            "locals": self.locals,
            "user_data": self.user_data,
        }


class ErrorContextCapture:
    """Captures error context with filtering.

    Example:
        capture = ErrorContextCapture(max_locals=10)
        try:
            risky_operation()
        except Exception as e:
            ctx = capture.capture(e, {"operation": "backup"})
            log_error(ctx)
    """

    SENSITIVE_KEYS = frozenset({
        "password", "token", "secret", "api_key", "apikey",
        "auth", "credential", "private", "pwd", "passwd",
    })

    def __init__(
        self,
        max_locals: int = 20,
        max_depth: int = 3,
        strip_sensitive: bool = True,
    ) -> None:
        self.max_locals = max_locals
        self.max_depth = max_depth
        self.strip_sensitive = strip_sensitive

    def capture(
        self,
        exc: BaseException,
        **user_data: Any,
    ) -> ErrorContext:
        """Capture context from exception.

        Args:
            exc: The exception.
            **user_data: Additional context.

        Returns:
            ErrorContext with captured information.
        """
        ctx = ErrorContext(
            error_type=type(exc).__name__,
            message=str(exc),
            stack_trace=self._format_stack(),
            user_data=user_data,
        )

        if self.max_locals > 0:
            ctx.locals = self._capture_locals()

        return ctx

    def _format_stack(self) -> str:
        """Get formatted stack trace."""
        return "".join(traceback.format_exception(*sys.exc_info()))

    def _capture_locals(self) -> Dict[str, Any]:
        """Capture local variables from traceback."""
        locals_dict: Dict[str, Any] = {}

        for frame_summary in traceback.extract_stack():
            if len(locals_dict) >= self.max_locals:
                break

            for key, value in frame_summary.locals.items():
                if key in locals_dict:
                    continue

                if self.strip_sensitive and self._is_sensitive(key):
                    locals_dict[key] = "[REDACTED]"
                else:
                    locals_dict[key] = self._simplify(value)

        return dict(list(locals_dict.items())[:self.max_locals])

    def _is_sensitive(self, key: str) -> bool:
        """Check if variable name suggests sensitive data."""
        key_lower = key.lower()
        return any(s in key_lower for s in self.SENSITIVE_KEYS)

    def _simplify(self, value: Any, depth: int = 0) -> Any:
        """Simplify value to reduce size."""
        if depth >= self.max_depth:
            return type(value).__name__

        if isinstance(value, (str, int, float, bool, type(None))):
            return value

        if isinstance(value, bytes):
            return f"bytes({len(value)})"

        if isinstance(value, (list, tuple)):
            if len(value) > 10:
                return f"{type(value).__name__}({len(value)})"
            return [self._simplify(v, depth + 1) for v in value]

        if isinstance(value, dict):
            if len(value) > 10:
                return f"dict({len(value)})"
            return {
                k: self._simplify(v, depth + 1)
                for k, v in list(value.items())[:10]
            }

        return repr(value)[:100]


class ErrorHistory:
    """Maintains history of errors with deduplication.

    Example:
        history = ErrorHistory(max_size=100)
        history.record(error_context)
        if history.is_recurring("TimeoutError"):
            alert("Timeout errors recurring")
    """

    def __init__(self, max_size: int = 100) -> None:
        self.max_size = max_size
        self._errors: List[ErrorContext] = []
        self._error_counts: Dict[str, int] = {}

    def record(self, error: ErrorContext) -> None:
        """Record an error.

        Args:
            error: Error context to record.
        """
        self._errors.append(error)

        key = self._error_key(error)
        self._error_counts[key] = self._error_counts.get(key, 0) + 1

        if len(self._errors) > self.max_size:
            removed = self._errors.pop(0)
            removed_key = self._error_key(removed)
            self._error_counts[removed_key] = max(0, self._error_counts.get(removed_key, 1) - 1)

    def _error_key(self, error: ErrorContext) -> str:
        """Generate deduplication key for error."""
        return f"{error.error_type}:{error.message[:50]}"

    def count(self, error_type: Optional[str] = None) -> int:
        """Get error count.

        Args:
            error_type: Count for specific type. Total if None.

        Returns:
            Error count.
        """
        if error_type is None:
            return len(self._errors)

        return sum(
            1 for e in self._errors
            if e.error_type == error_type
        )

    def get_errors(
        self,
        error_type: Optional[str] = None,
        limit: int = 10,
    ) -> List[ErrorContext]:
        """Get recent errors.

        Args:
            error_type: Filter by type.
            limit: Maximum to return.

        Returns:
            List of errors, most recent first.
        """
        errors = self._errors
        if error_type:
            errors = [e for e in errors if e.error_type == error_type]
        return list(reversed(errors))[:limit]

    def is_recurring(self, error_type: str, threshold: int = 3) -> bool:
        """Check if error type is recurring.

        Args:
            error_type: Error type to check.
            threshold: Count to consider recurring.

        Returns:
            True if error occurred more than threshold times.
        """
        count = sum(1 for e in self._errors if e.error_type == error_type)
        return count >= threshold


def with_error_context(
    **user_data: Any,
) -> Callable:
    """Decorator to capture error context.

    Example:
        @with_error_context(operation="data_load")
        def load_data():
            ...
    """
    capture = ErrorContextCapture()

    def decorator(func: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                ctx = capture.capture(e, **user_data)
                ctx.user_data["function"] = func.__name__
                raise
        return wrapper
    return decorator
