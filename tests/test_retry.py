"""Tests for retry utilities."""

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.retry import (
    retry,
    retry_async,
    RetryError,
    CircuitBreaker,
    CircuitState,
    RateLimiter,
    rate_limit,
)


class TestRetry:
    """Tests for retry decorator."""

    def test_successful_on_first_attempt(self) -> None:
        """Test function succeeds on first try."""
        @retry(max_attempts=3)
        def succeed():
            return "success"

        result = succeed()
        assert result == "success"

    def test_retry_on_failure(self) -> None:
        """Test function is retried on failure."""
        attempts = []

        @retry(max_attempts=3, delay=0.01, backoff=1)
        def flaky():
            attempts.append(1)
            if len(attempts) < 3:
                raise ValueError("fail")
            return "success"

        result = flaky()
        assert result == "success"
        assert len(attempts) == 3

    def test_raises_after_max_attempts(self) -> None:
        """Test that RetryError is raised after max attempts."""
        @retry(max_attempts=2, delay=0.01)
        def always_fail():
            raise ValueError("always fails")

        with pytest.raises(RetryError) as exc_info:
            always_fail()

        assert exc_info.value.attempts == 2
        assert isinstance(exc_info.value.last_exception, ValueError)

    def test_specific_exception_handling(self) -> None:
        """Test retry only on specific exceptions."""
        attempts = []

        @retry(max_attempts=3, delay=0.01, exceptions=(ValueError,))
        def specific_fail():
            attempts.append(1)
            if len(attempts) < 2:
                raise ValueError("retry this")
            return "success"

        # TypeError should not be retried
        with pytest.raises(TypeError):
            @retry(max_attempts=3, delay=0.01, exceptions=(ValueError,))
            def other_fail():
                raise TypeError("don't retry")

            other_fail()


class TestCircuitBreaker:
    """Tests for CircuitBreaker."""

    def test_initial_state_closed(self) -> None:
        """Test circuit starts in closed state."""
        cb = CircuitBreaker()
        assert cb.state == CircuitState.CLOSED

    def test_opens_after_threshold(self) -> None:
        """Test circuit opens after failure threshold."""
        cb = CircuitBreaker(failure_threshold=3)

        for _ in range(3):
            with pytest.raises(Exception):
                cb.call(lambda: (_ for _ in ()).throw(Exception("fail")))

        assert cb.state == CircuitState.OPEN

    def test_success_resets_failure_count(self) -> None:
        """Test successful calls reset failure count."""
        cb = CircuitBreaker(failure_threshold=3)

        cb.call(lambda: "ok")
        assert cb._failure_count == 0

        try:
            cb.call(lambda: (_ for _ in ()).throw(Exception("fail")))
        except Exception:
            pass

        cb.call(lambda: "ok")
        cb.call(lambda: "ok")
        assert cb._failure_count == 0

    def test_reject_calls_when_open(self) -> None:
        """Test calls are rejected when circuit is open."""
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.1)

        with pytest.raises(Exception):
            cb.call(lambda: (_ for _ in ()).throw(Exception("fail")))

        assert cb.state == CircuitState.OPEN

        with pytest.raises(RuntimeError, match="OPEN"):
            cb.call(lambda: "ok")

    def test_half_open_after_timeout(self) -> None:
        """Test circuit goes to half-open after timeout."""
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.05)

        with pytest.raises(Exception):
            cb.call(lambda: (_ for _ in ()).throw(Exception("fail")))

        assert cb.state == CircuitState.OPEN

        time.sleep(0.1)
        assert cb.state == CircuitState.HALF_OPEN

    def test_half_open_to_closed_on_success(self) -> None:
        """Test circuit closes after successful half-open call."""
        cb = CircuitBreaker(
            failure_threshold=1,
            recovery_timeout=0.05,
            half_open_max_calls=1,
        )

        with pytest.raises(Exception):
            cb.call(lambda: (_ for _ in ()).throw(Exception("fail")))

        time.sleep(0.1)
        assert cb.state == CircuitState.HALF_OPEN

        cb.call(lambda: "success")
        assert cb.state == CircuitState.CLOSED

    def test_reset(self) -> None:
        """Test circuit reset."""
        cb = CircuitBreaker(failure_threshold=2)

        with pytest.raises(Exception):
            cb.call(lambda: (_ for _ in ()).throw(Exception("fail")))
        with pytest.raises(Exception):
            cb.call(lambda: (_ for _ in ()).throw(Exception("fail")))

        assert cb.state == CircuitState.OPEN

        cb.reset()
        assert cb.state == CircuitState.CLOSED
        assert cb._failure_count == 0


class TestRateLimiter:
    """Tests for RateLimiter."""

    def test_initial_tokens(self) -> None:
        """Test initial token availability."""
        limiter = RateLimiter(rate=10, capacity=10)
        assert limiter.available_tokens == 10

    def test_acquire_tokens(self) -> None:
        """Test acquiring tokens."""
        limiter = RateLimiter(rate=10, capacity=10)
        assert limiter.acquire(5)
        assert limiter.available_tokens == 5

    def test_acquire_blocks(self) -> None:
        """Test blocking acquire."""
        limiter = RateLimiter(rate=100, capacity=10)
        limiter.acquire(10)

        start = time.time()
        result = limiter.acquire(5, timeout=0.1)
        elapsed = time.time() - start

        assert result is False or elapsed >= 0.09

    def test_refill_tokens(self) -> None:
        """Test token refill over time."""
        limiter = RateLimiter(rate=100, capacity=10)
        limiter.acquire(10)

        time.sleep(0.05)
        tokens = limiter.available_tokens
        assert tokens > 0

    def test_non_blocking_acquire(self) -> None:
        """Test non-blocking acquire when no tokens."""
        limiter = RateLimiter(rate=1, capacity=1)
        limiter.acquire(1)

        result = limiter.acquire(1, blocking=False)
        assert result is False


class TestRateLimitDecorator:
    """Tests for rate_limit decorator."""

    def test_rate_limited_function(self) -> None:
        """Test function is rate limited."""
        call_count = 0

        @rate_limit(rate=1000)
        def limited_func():
            nonlocal call_count
            call_count += 1

        for _ in range(10):
            limited_func()

        assert call_count == 10

    def test_rate_limit_exceeded(self) -> None:
        """Test rate limit exceeded error."""
        call_count = 0

        @rate_limit(rate=0.1)
        def very_limited():
            nonlocal call_count
            call_count += 1

        very_limited()

        with pytest.raises(RuntimeError, match="Rate limit exceeded"):
            very_limited()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])