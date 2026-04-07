"""Error handling utilities for RabAI AutoClick.

Provides:
- Error classes
- Error handling decorators
- Error aggregation
"""

from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional, Type, TypeVar, Union


T = TypeVar("T")


class RabAIError(Exception):
    """Base exception for RabAI AutoClick."""
    pass


class WorkflowError(RabAIError):
    """Error in workflow execution."""
    pass


class ActionError(RabAIError):
    """Error in action execution."""
    pass


class ConfigurationError(RabAIError):
    """Configuration error."""
    pass


class ValidationError(RabAIError):
    """Validation error."""
    pass


class TimeoutError(RabAIError):
    """Timeout error."""
    pass


class ResourceNotFoundError(RabAIError):
    """Resource not found error."""
    pass


@dataclass
class ErrorInfo:
    """Detailed error information."""
    error: Exception
    context: dict = field(default_factory=dict)
    timestamp: float = 0
    stack_trace: Optional[str] = None

    def __post_init__(self) -> None:
        import time
        if self.timestamp == 0:
            self.timestamp = time.time()
        if self.stack_trace is None:
            import traceback
            self.stack_trace = traceback.format_exc()


class ErrorHandler:
    """Central error handler.

    Collects and manages errors across the application.
    """

    def __init__(self) -> None:
        self._errors: List[ErrorInfo] = []
        self._max_errors = 100

    def add_error(
        self,
        error: Exception,
        context: Optional[dict] = None,
    ) -> ErrorInfo:
        """Add an error to the handler.

        Args:
            error: Exception that occurred.
            context: Additional context.

        Returns:
            ErrorInfo object.
        """
        info = ErrorInfo(
            error=error,
            context=context or {},
        )

        self._errors.append(info)

        if len(self._errors) > self._max_errors:
            self._errors.pop(0)

        return info

    def get_errors(
        self,
        error_type: Optional[Type[Exception]] = None,
    ) -> List[ErrorInfo]:
        """Get collected errors.

        Args:
            error_type: Filter by exception type.

        Returns:
            List of ErrorInfo objects.
        """
        if error_type is None:
            return self._errors.copy()

        return [
            e for e in self._errors
            if isinstance(e.error, error_type)
        ]

    def clear(self) -> None:
        """Clear all errors."""
        self._errors.clear()

    def __len__(self) -> int:
        """Get number of errors."""
        return len(self._errors)


def handle_errors(
    *exceptions: Type[Exception],
    default: Any = None,
    on_error: Optional[Callable[[Exception], None]] = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to handle specific exceptions.

    Args:
        *exceptions: Exception types to catch.
        default: Default value to return on error.
        on_error: Optional callback for error handling.

    Returns:
        Decorated function.
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        def wrapper(*args: Any, **kwargs: Any) -> T:
            try:
                return func(*args, **kwargs)
            except exceptions as e:
                if on_error:
                    on_error(e)
                return default
        return wrapper
    return decorator


def reraise(
    *exceptions: Type[Exception],
    into: Type[Exception],
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to reraise exceptions as different type.

    Args:
        *exceptions: Exception types to catch.
        into: Exception type to raise instead.

    Returns:
        Decorated function.
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        def wrapper(*args: Any, **kwargs: Any) -> T:
            try:
                return func(*args, **kwargs)
            except exceptions as e:
                raise into(str(e)) from e
        return wrapper
    return decorator


class ErrorAccumulator:
    """Accumulates multiple errors before raising.

    Useful for collecting validation errors.

    Usage:
        errors = ErrorAccumulator()
        errors.try_add("field1", lambda: validate_field1(value))
        errors.try_add("field2", lambda: validate_field2(value))
        errors.raise_if_any()
    """

    def __init__(self, error_type: Type[Exception] = ValueError) -> None:
        """Initialize error accumulator.

        Args:
            error_type: Type of exception to raise.
        """
        self._errors: List[Tuple[str, Exception]] = []
        self._error_type = error_type

    def add(self, context: str, error: Exception) -> None:
        """Add an error.

        Args:
            context: Context/location of error.
            error: The error.
        """
        self._errors.append((context, error))

       def try_add(self, context: str, func: Callable[[], Any]) -> None:
        """Try to execute function and add any error.

        Args:
            context: Context/location.
            func: Function to execute.
        """
        try:
            func()
        except Exception as e:
            self.add(context, e)

    def raise_if_any(self) -> None:
        """Raise exception if any errors accumulated.

        Raises:
            The configured error type with all errors as message.
        """
        if self._errors:
            error_messages = [
                f"{context}: {error}"
                for context, error in self._errors
            ]
            raise self._error_type("; ".join(error_messages))

    @property
    def has_errors(self) -> bool:
        """Check if any errors accumulated."""
        return len(self._errors) > 0

    def __len__(self) -> int:
        """Get number of errors."""
        return len(self._errors)


class Fallback:
    """Context manager for fallback on error.

    Usage:
        with Fallback(default_value, catch=ValueError):
            result = risky_operation()
    """

    def __init__(
        self,
        default: T,
        catch: Union[Type[Exception], tuple] = Exception,
    ) -> None:
        """Initialize fallback.

        Args:
            default: Default value to return on error.
            catch: Exception type(s) to catch.
        """
        self.default = default
        self.catch = catch
        self._error: Optional[Exception] = None

    def __enter__(self) -> T:
        """Enter context."""
        return self.default

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        """Exit context."""
        if exc_type is not None and issubclass(exc_type, self.catch):
            self._error = exc_val
            return True  # Suppress error
        return False

    @property
    def error(self) -> Optional[Exception]:
        """Get caught error if any."""
        return self._error


def suppress_errors(
    *exceptions: Type[Exception],
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to suppress specific exceptions.

    Args:
        *exceptions: Exception types to suppress.

    Returns:
        Decorated function.
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        def wrapper(*args: Any, **kwargs: Any) -> T:
            try:
                return func(*args, **kwargs)
            except exceptions:
                pass
        return wrapper
    return decorator


@dataclass
class ErrorContext:
    """Context for error handling with resources."""
    message: str
    code: Optional[str] = None
    details: Optional[dict] = None
    cause: Optional[Exception] = None


def format_error(error: Exception, include_traceback: bool = False) -> str:
    """Format exception as string.

    Args:
        error: Exception to format.
        include_traceback: Include stack trace.

    Returns:
        Formatted error string.
    """
    parts = [f"{error.__class__.__name__}: {error}"]

    if include_traceback:
        import traceback
        parts.append(traceback.format_exc())

    return "\n".join(parts)