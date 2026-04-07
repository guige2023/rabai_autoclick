"""Tests for error handling utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.error_handling import (
    RabAIError,
    WorkflowError,
    ActionError,
    ConfigurationError,
    ValidationError,
    TimeoutError,
    ResourceNotFoundError,
    ErrorInfo,
    ErrorHandler,
    ErrorAccumulator,
    Fallback,
    ErrorContext,
    handle_errors,
    reraise,
    suppress_errors,
    format_error,
)


class TestCustomExceptions:
    """Tests for custom exception classes."""

    def test_rabai_error(self) -> None:
        """Test RabAIError."""
        with pytest.raises(RabAIError):
            raise RabAIError("test")

    def test_workflow_error(self) -> None:
        """Test WorkflowError."""
        with pytest.raises(WorkflowError):
            raise WorkflowError("workflow failed")

    def test_action_error(self) -> None:
        """Test ActionError."""
        with pytest.raises(ActionError):
            raise ActionError("action failed")

    def test_configuration_error(self) -> None:
        """Test ConfigurationError."""
        with pytest.raises(ConfigurationError):
            raise ConfigurationError("bad config")

    def test_validation_error(self) -> None:
        """Test ValidationError."""
        with pytest.raises(ValidationError):
            raise ValidationError("invalid")

    def test_timeout_error(self) -> None:
        """Test TimeoutError."""
        with pytest.raises(TimeoutError):
            raise TimeoutError("timed out")

    def test_resource_not_found_error(self) -> None:
        """Test ResourceNotFoundError."""
        with pytest.raises(ResourceNotFoundError):
            raise ResourceNotFoundError("not found")

    def test_exception_inheritance(self) -> None:
        """Test all custom exceptions inherit from RabAIError."""
        assert issubclass(WorkflowError, RabAIError)
        assert issubclass(ActionError, RabAIError)
        assert issubclass(ConfigurationError, RabAIError)
        assert issubclass(ValidationError, RabAIError)
        assert issubclass(TimeoutError, RabAIError)
        assert issubclass(ResourceNotFoundError, RabAIError)


class TestErrorInfo:
    """Tests for ErrorInfo."""

    def test_create(self) -> None:
        """Test creating ErrorInfo."""
        error = ValueError("test error")
        info = ErrorInfo(error=error)
        assert info.error == error
        assert info.timestamp > 0
        assert info.stack_trace is not None

    def test_create_with_context(self) -> None:
        """Test creating ErrorInfo with context."""
        error = ValueError("test")
        info = ErrorInfo(error=error, context={"key": "value"})
        assert info.context == {"key": "value"}


class TestErrorHandler:
    """Tests for ErrorHandler."""

    def test_create(self) -> None:
        """Test creating error handler."""
        handler = ErrorHandler()
        assert len(handler) == 0

    def test_add_error(self) -> None:
        """Test adding error."""
        handler = ErrorHandler()
        info = handler.add_error(ValueError("test"))
        assert len(handler) == 1
        assert info.error is not None

    def test_add_error_with_context(self) -> None:
        """Test adding error with context."""
        handler = ErrorHandler()
        handler.add_error(ValueError("test"), {"field": "name"})
        errors = handler.get_errors()
        assert errors[0].context == {"field": "name"}

    def test_get_errors_all(self) -> None:
        """Test getting all errors."""
        handler = ErrorHandler()
        handler.add_error(ValueError("one"))
        handler.add_error(TypeError("two"))
        errors = handler.get_errors()
        assert len(errors) == 2

    def test_get_errors_filtered(self) -> None:
        """Test getting filtered errors."""
        handler = ErrorHandler()
        handler.add_error(ValueError("one"))
        handler.add_error(TypeError("two"))
        errors = handler.get_errors(ValueError)
        assert len(errors) == 1
        assert isinstance(errors[0].error, ValueError)

    def test_clear(self) -> None:
        """Test clearing errors."""
        handler = ErrorHandler()
        handler.add_error(ValueError("test"))
        handler.clear()
        assert len(handler) == 0

    def test_max_errors(self) -> None:
        """Test max errors limit."""
        handler = ErrorHandler()
        handler._max_errors = 3
        for i in range(5):
            handler.add_error(ValueError(str(i)))
        assert len(handler) == 3


class TestHandleErrors:
    """Tests for handle_errors decorator."""

    def test_no_error(self) -> None:
        """Test function with no errors."""
        @handle_errors(ValueError)
        def func():
            return 42
        assert func() == 42

    def test_catches_error(self) -> None:
        """Test catching error."""
        @handle_errors(ValueError, default=-1)
        def func():
            raise ValueError("bad")
        assert func() == -1

    def test_does_not_catch_other(self) -> None:
        """Test not catching other errors."""
        @handle_errors(ValueError, default=-1)
        def func():
            raise TypeError("bad")
        with pytest.raises(TypeError):
            func()

    def test_on_error_callback(self) -> None:
        """Test error callback."""
        callback_errors = []

        @handle_errors(ValueError, on_error=lambda e: callback_errors.append(e))
        def func():
            raise ValueError("bad")
        func()
        assert len(callback_errors) == 1


class TestReraise:
    """Tests for reraise decorator."""

    def test_reraise(self) -> None:
        """Test reraising exception."""
        @reraise(ValueError, into=RuntimeError)
        def func():
            raise ValueError("original")
        with pytest.raises(RuntimeError, match="original"):
            func()


class TestSuppressErrors:
    """Tests for suppress_errors decorator."""

    def test_no_error(self) -> None:
        """Test function with no errors."""
        @suppress_errors(ValueError)
        def func():
            return 42
        assert func() == 42

    def test_suppress_error(self) -> None:
        """Test suppressing error."""
        @suppress_errors(ValueError)
        def func():
            raise ValueError("suppressed")
        assert func() is None

    def test_does_not_suppress_other(self) -> None:
        """Test not suppressing other errors."""
        @suppress_errors(ValueError)
        def func():
            raise TypeError("not suppressed")
        with pytest.raises(TypeError):
            func()


class TestErrorAccumulator:
    """Tests for ErrorAccumulator."""

    def test_create(self) -> None:
        """Test creating error accumulator."""
        acc = ErrorAccumulator()
        assert len(acc) == 0
        assert acc.has_errors is False

    def test_add_error(self) -> None:
        """Test adding error."""
        acc = ErrorAccumulator()
        acc.add("field1", ValueError("invalid"))
        assert len(acc) == 1
        assert acc.has_errors is True

    def test_try_add_success(self) -> None:
        """Test try_add with success."""
        acc = ErrorAccumulator()
        acc.try_add("field1", lambda: 42)
        assert len(acc) == 0

    def test_try_add_failure(self) -> None:
        """Test try_add with failure."""
        acc = ErrorAccumulator()
        acc.try_add("field1", lambda: 1 / 0)
        assert len(acc) == 1

    def test_raise_if_any(self) -> None:
        """Test raising if errors."""
        acc = ErrorAccumulator()
        acc.add("field1", ValueError("error1"))
        acc.add("field2", ValueError("error2"))
        with pytest.raises(ValueError, match="field1"):
            acc.raise_if_any()

    def test_raise_if_any_empty(self) -> None:
        """Test raise_if_any with no errors."""
        acc = ErrorAccumulator()
        acc.raise_if_any()  # Should not raise


class TestFallback:
    """Tests for Fallback context manager."""

    def test_no_error(self) -> None:
        """Test fallback when no error."""
        with Fallback(default=42) as value:
            assert value == 42

    def test_catches_error(self) -> None:
        """Test catching error."""
        with Fallback(default=42) as value:
            raise ValueError("bad")
        assert value == 42

    def test_error_property(self) -> None:
        """Test error property."""
        fb = Fallback(default=42)
        with fb:
            raise ValueError("bad")
        assert isinstance(fb.error, ValueError)

    def test_does_not_catch_other(self) -> None:
        """Test not catching other errors."""
        with pytest.raises(TypeError):
            with Fallback(default=42, catch=ValueError):
                raise TypeError("bad")


class TestErrorContext:
    """Tests for ErrorContext."""

    def test_create(self) -> None:
        """Test creating ErrorContext."""
        ctx = ErrorContext(message="error occurred")
        assert ctx.message == "error occurred"
        assert ctx.code is None
        assert ctx.details is None

    def test_create_full(self) -> None:
        """Test creating ErrorContext with all fields."""
        cause = ValueError("cause")
        ctx = ErrorContext(
            message="error occurred",
            code="ERR001",
            details={"key": "value"},
            cause=cause,
        )
        assert ctx.message == "error occurred"
        assert ctx.code == "ERR001"
        assert ctx.details == {"key": "value"}
        assert ctx.cause == cause


class TestFormatError:
    """Tests for format_error function."""

    def test_format_simple(self) -> None:
        """Test formatting simple error."""
        error = ValueError("test error")
        result = format_error(error)
        assert "ValueError" in result
        assert "test error" in result

    def test_format_with_traceback(self) -> None:
        """Test formatting with traceback."""
        try:
            raise ValueError("test error")
        except ValueError as error:
            result = format_error(error, include_traceback=True)
            assert "ValueError" in result
            assert "Traceback" in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])