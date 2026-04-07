"""Tests for timeout utilities."""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.timeout import (
    TimeoutError,
    TimeoutResult,
    Timeout,
    timeout_decorator,
    timeout_call,
    TimeoutManager,
    RetryTimeout,
)


class TestTimeoutError:
    """Tests for TimeoutError."""

    def test_create(self) -> None:
        """Test creating error."""
        err = TimeoutError("test")
        assert str(err) == "test"


class TestTimeoutResult:
    """Tests for TimeoutResult."""

    def test_create_success(self) -> None:
        """Test creating success result."""
        result = TimeoutResult(success=True, value=42)
        assert result.success is True
        assert result.value == 42

    def test_create_failure(self) -> None:
        """Test creating failure result."""
        result = TimeoutResult(success=False, error="fail")
        assert result.success is False
        assert result.error == "fail"


class TestTimeout:
    """Tests for Timeout."""

    def test_context(self) -> None:
        """Test timeout context manager."""
        with Timeout(1.0) as t:
            pass
        assert t.elapsed >= 0

    def test_check_no_raise(self) -> None:
        """Test check without timeout."""
        with Timeout(1.0) as t:
            time.sleep(0.01)
            t.check()  # Should not raise


class TestTimeoutDecorator:
    """Tests for timeout_decorator."""

    def test_decorator_success(self) -> None:
        """Test decorated function success."""
        @timeout_decorator(1.0)
        def slow_func():
            return 42

        result = slow_func()
        assert result == 42

    def test_decorator_timeout(self) -> None:
        """Test decorated function timeout."""
        @timeout_decorator(0.1, default=-1)
        def slow_func():
            time.sleep(1)
            return 42

        result = slow_func()
        assert result == -1


class TestTimeoutCall:
    """Tests for timeout_call."""

    def test_success(self) -> None:
        """Test successful call."""
        result = timeout_call(lambda: 42, 1.0)
        assert result.success is True
        assert result.value == 42

    def test_timeout(self) -> None:
        """Test timeout."""
        result = timeout_call(lambda: time.sleep(1), 0.1)
        assert result.success is False
        assert "Timed out" in result.error

    def test_error(self) -> None:
        """Test error propagation."""
        def raise_error():
            raise ValueError("fail")

        result = timeout_call(raise_error, 1.0)
        assert result.success is False
        assert "fail" in result.error


class TestTimeoutManager:
    """Tests for TimeoutManager."""

    def test_create(self) -> None:
        """Test creating manager."""
        manager = TimeoutManager()
        assert len(manager._timeouts) == 0

    def test_set_cancel(self) -> None:
        """Test setting and cancelling timeout."""
        manager = TimeoutManager()
        called = []

        def callback():
            called.append(1)

        manager.set_timeout("test", 0.1, callback)
        manager.cancel("test")
        time.sleep(0.2)
        assert called == []


class TestRetryTimeout:
    """Tests for RetryTimeout."""

    def test_success_first_attempt(self) -> None:
        """Test success on first attempt."""
        retry = RetryTimeout(max_attempts=3, timeout_seconds=1.0)
        result = retry.execute(lambda: 42)
        assert result.success is True

    def test_retry_on_failure(self) -> None:
        """Test retry on failure."""
        attempts = []

        def failing():
            attempts.append(1)
            if len(attempts) < 3:
                raise ValueError("fail")
            return 42

        retry = RetryTimeout(max_attempts=3, timeout_seconds=1.0, backoff=0.1)
        result = retry.execute(failing)
        assert result.success is True
        assert len(attempts) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])