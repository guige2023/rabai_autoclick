"""Tests for decorator utilities."""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.decorators import (
    retry,
    timing,
    deprecated,
    cached,
    rate_limit,
    once,
    accepts,
    returns,
    debug,
    memoize,
)


class TestRetry:
    """Tests for retry decorator."""

    def test_retry_success(self) -> None:
        """Test retry succeeds on first try."""
        @retry(max_attempts=3, delay=0.01)
        def success():
            return "success"
        assert success() == "success"

    def test_retry_eventually_succeeds(self) -> None:
        """Test retry eventually succeeds."""
        attempts = [0]

        @retry(max_attempts=3, delay=0.01)
        def eventually_succeeds():
            attempts[0] += 1
            if attempts[0] < 2:
                raise ValueError("Not yet")
            return "success"

        assert eventually_succeeds() == "success"
        assert attempts[0] == 2

    def test_retry_fails(self) -> None:
        """Test retry fails after max attempts."""
        @retry(max_attempts=3, delay=0.01)
        def always_fails():
            raise ValueError("Always fails")

        with pytest.raises(ValueError):
            always_fails()


class TestTiming:
    """Tests for timing decorator."""

    def test_timing_returns_result(self) -> None:
        """Test timing returns function result."""
        @timing
        def slow_func():
            time.sleep(0.05)
            return 42
        assert slow_func() == 42


class TestDeprecated:
    """Tests for deprecated decorator."""

    def test_deprecated_warns(self) -> None:
        """Test deprecated decorator warns."""
        @deprecated("Use new_func instead")
        def old_func():
            return "old"

        with pytest.warns(DeprecationWarning, match="Use new_func instead"):
            result = old_func()
            assert result == "old"


class TestCached:
    """Tests for cached decorator."""

    def test_cached_caches(self) -> None:
        """Test cached decorator caches results."""
        call_count = [0]

        @cached
        def cached_func(x):
            call_count[0] += 1
            return x * 2

        assert cached_func(5) == 10
        assert cached_func(5) == 10
        assert call_count[0] == 1

    def test_cache_clear(self) -> None:
        """Test cache can be cleared."""
        @cached
        def cached_func(x):
            return x * 2

        cached_func(5)
        assert cached_func.cache == {((5,), ()): 10}
        cached_func.cache_clear()
        assert cached_func.cache == {}
        cached_func(5)
        assert cached_func.cache == {((5,), ()): 10}


class TestRateLimit:
    """Tests for rate_limit decorator."""

    def test_rate_limit_allows_calls(self) -> None:
        """Test rate limit allows calls within limit."""
        @rate_limit(calls=3, period=1.0)
        def limited_func():
            return "called"

        assert limited_func() == "called"
        assert limited_func() == "called"
        assert limited_func() == "called"


class TestOnce:
    """Tests for once decorator."""

    def test_once_executes_once(self) -> None:
        """Test once decorator executes only once."""
        call_count = [0]

        @once
        def once_func():
            call_count[0] += 1
            return "executed"

        assert once_func() == "executed"
        assert once_func() == "executed"
        assert call_count[0] == 1


class TestAccepts:
    """Tests for accepts decorator."""

    def test_accepts_valid(self) -> None:
        """Test accepts passes with valid types."""
        @accepts(str, int)
        def typed_func(a, b):
            return f"{a}-{b}"

        assert typed_func("hello", 42) == "hello-42"

    def test_accepts_invalid(self) -> None:
        """Test accepts raises with invalid types."""
        @accepts(str, int)
        def typed_func(a, b):
            return f"{a}-{b}"

        with pytest.raises(TypeError, match="Argument 1 must be int"):
            typed_func("hello", "world")


class TestReturns:
    """Tests for returns decorator."""

    def test_returns_valid(self) -> None:
        """Test returns passes with valid type."""
        @returns(int)
        def typed_func():
            return 42

        assert typed_func() == 42

    def test_returns_invalid(self) -> None:
        """Test returns raises with invalid type."""
        @returns(int)
        def typed_func():
            return "not an int"

        with pytest.raises(TypeError, match="Return type must be int"):
            typed_func()


class TestDebug:
    """Tests for debug decorator."""

    def test_debug_returns_result(self) -> None:
        """Test debug decorator returns result."""
        @debug
        def debug_func():
            return 42

        assert debug_func() == 42


class TestMemoize:
    """Tests for memoize decorator."""

    def test_memoize_caches(self) -> None:
        """Test memoize decorator caches results."""
        call_count = [0]

        @memoize
        def memo_func(x):
            call_count[0] += 1
            return x * 2

        assert memo_func(5) == 10
        assert memo_func(5) == 10
        assert call_count[0] == 1

    def test_memoize_different_args(self) -> None:
        """Test memoize with different arguments."""
        call_count = [0]

        @memoize
        def memo_func(x):
            call_count[0] += 1
            return x * 2

        memo_func(5)
        memo_func(6)
        assert call_count[0] == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])