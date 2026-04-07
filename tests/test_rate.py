"""Tests for rate limiting utilities."""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.rate import (
    RateLimitResult,
    TokenBucket,
    SlidingWindow,
    LeakyBucket,
    AdaptiveRateLimiter,
    rate_limited,
)


class TestRateLimitResult:
    """Tests for RateLimitResult."""

    def test_allowed_result(self) -> None:
        """Test allowed result."""
        result = RateLimitResult(allowed=True, remaining=5)
        assert result.allowed is True
        assert result.remaining == 5
        assert result.retry_after is None

    def test_denied_result(self) -> None:
        """Test denied result."""
        result = RateLimitResult(allowed=False, remaining=0, retry_after=1.5)
        assert result.allowed is False
        assert result.remaining == 0
        assert result.retry_after == 1.5


class TestTokenBucket:
    """Tests for TokenBucket."""

    def test_create(self) -> None:
        """Test creating token bucket."""
        bucket = TokenBucket(rate=10, capacity=10)
        assert bucket.rate == 10
        assert bucket.capacity == 10

    def test_default_capacity(self) -> None:
        """Test default capacity equals rate."""
        bucket = TokenBucket(rate=5)
        assert bucket.capacity == 5

    def test_initial_tokens(self) -> None:
        """Test initial tokens equal capacity."""
        bucket = TokenBucket(rate=10, capacity=10)
        assert bucket.available == 10

    def test_consume_allowed(self) -> None:
        """Test consuming tokens when allowed."""
        bucket = TokenBucket(rate=10, capacity=10)
        result = bucket.consume(5)
        assert result.allowed is True
        assert result.remaining == 5

    def test_consume_exact(self) -> None:
        """Test consuming exact number of tokens."""
        bucket = TokenBucket(rate=10, capacity=10)
        result = bucket.consume(10)
        assert result.allowed is True
        assert result.remaining == 0

    def test_consume_denied(self) -> None:
        """Test consuming tokens when not enough."""
        bucket = TokenBucket(rate=10, capacity=5)
        result = bucket.consume(10)
        assert result.allowed is False
        assert result.remaining == 5
        assert result.retry_after is not None
        assert result.retry_after > 0

    def test_refill_tokens(self) -> None:
        """Test tokens refill over time."""
        bucket = TokenBucket(rate=100, capacity=10)
        bucket.consume(10)
        time.sleep(0.05)
        available = bucket.available
        assert available > 0

    def test_consume_multiple(self) -> None:
        """Test multiple consumes."""
        bucket = TokenBucket(rate=10, capacity=10)
        bucket.consume(3)
        bucket.consume(3)
        result = bucket.consume(3)
        assert result.allowed is True
        assert result.remaining == 1


class TestSlidingWindow:
    """Tests for SlidingWindow."""

    def test_create(self) -> None:
        """Test creating sliding window."""
        window = SlidingWindow(max_requests=10, window_seconds=1.0)
        assert window.max_requests == 10
        assert window.window_seconds == 1.0

    def test_allows_under_limit(self) -> None:
        """Test allows requests under limit."""
        window = SlidingWindow(max_requests=10, window_seconds=1.0)
        result = window.is_allowed()
        assert result.allowed is True
        assert result.remaining == 9

    def test_denies_over_limit(self) -> None:
        """Test denies requests over limit."""
        window = SlidingWindow(max_requests=2, window_seconds=1.0)
        window.is_allowed()
        window.is_allowed()
        result = window.is_allowed()
        assert result.allowed is False
        assert result.remaining == 0

    def test_retry_after_set(self) -> None:
        """Test retry_after is set when denied."""
        window = SlidingWindow(max_requests=1, window_seconds=1.0)
        window.is_allowed()
        result = window.is_allowed()
        assert result.allowed is False
        assert result.retry_after is not None
        assert result.retry_after > 0

    def test_current_count(self) -> None:
        """Test current count property."""
        window = SlidingWindow(max_requests=10, window_seconds=1.0)
        assert window.current_count == 0
        window.is_allowed()
        assert window.current_count == 1

    def test_window_expiry(self) -> None:
        """Test requests expire after window."""
        window = SlidingWindow(max_requests=1, window_seconds=0.1)
        window.is_allowed()
        time.sleep(0.15)
        result = window.is_allowed()
        assert result.allowed is True


class TestLeakyBucket:
    """Tests for LeakyBucket."""

    def test_create(self) -> None:
        """Test creating leaky bucket."""
        bucket = LeakyBucket(rate=5.0, capacity=10.0)
        assert bucket.rate == 5.0
        assert bucket.capacity == 10.0

    def test_add_allowed(self) -> None:
        """Test adding when not full."""
        bucket = LeakyBucket(rate=5.0, capacity=10.0)
        result = bucket.add(5.0)
        assert result is True
        assert bucket.level == 5.0

    def test_add_denied_when_full(self) -> None:
        """Test adding when full."""
        bucket = LeakyBucket(rate=1.0, capacity=5.0)
        bucket.add(5.0)
        result = bucket.add(1.0)
        assert result is False

    def test_leak_over_time(self) -> None:
        """Test bucket leaks over time."""
        bucket = LeakyBucket(rate=10.0, capacity=5.0)
        bucket.add(5.0)
        time.sleep(0.1)
        level = bucket.level
        assert level < 5.0

    def test_level_never_negative(self) -> None:
        """Test level never goes negative."""
        bucket = LeakyBucket(rate=5.0, capacity=10.0)
        time.sleep(0.1)
        assert bucket.level >= 0


class TestAdaptiveRateLimiter:
    """Tests for AdaptiveRateLimiter."""

    def test_create(self) -> None:
        """Test creating adaptive rate limiter."""
        limiter = AdaptiveRateLimiter(initial_rate=10.0)
        assert limiter.rate == 10.0
        assert limiter.min_rate == 0.1
        assert limiter.max_rate == 100

    def test_increase_on_success(self) -> None:
        """Test rate increases on success."""
        limiter = AdaptiveRateLimiter(initial_rate=10.0, increase_factor=1.5)
        limiter.record_success()
        assert limiter.rate == 15.0

    def test_decrease_on_failure(self) -> None:
        """Test rate decreases on failure."""
        limiter = AdaptiveRateLimiter(initial_rate=10.0, decrease_factor=0.5)
        limiter.record_failure()
        assert limiter.rate == 5.0

    def test_respects_max_rate(self) -> None:
        """Test rate respects max limit."""
        limiter = AdaptiveRateLimiter(initial_rate=50.0, max_rate=100, increase_factor=2.0)
        limiter.record_success()
        limiter.record_success()
        limiter.record_success()
        assert limiter.rate == 100

    def test_respects_min_rate(self) -> None:
        """Test rate respects min limit."""
        limiter = AdaptiveRateLimiter(initial_rate=1.0, min_rate=0.5, decrease_factor=0.1)
        limiter.record_failure()
        assert limiter.rate == 0.5

    def test_get_token_bucket(self) -> None:
        """Test getting token bucket."""
        limiter = AdaptiveRateLimiter(initial_rate=10.0)
        bucket = limiter.get_token_bucket()
        assert isinstance(bucket, TokenBucket)
        assert bucket.rate == 10.0


class TestRateLimitedDecorator:
    """Tests for rate_limited decorator."""

    def test_decorator_allows(self) -> None:
        """Test decorator allows calls within limit."""
        call_count = 0

        @rate_limited(calls=3, period=1.0)
        def my_func():
            nonlocal call_count
            call_count += 1

        my_func()
        my_func()
        my_func()
        assert call_count == 3

    def test_decorator_denies(self) -> None:
        """Test decorator denies calls over limit."""
        @rate_limited(calls=1, period=1.0)
        def my_func():
            return "called"

        my_func()
        with pytest.raises(RuntimeError, match="Rate limit exceeded"):
            my_func()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])