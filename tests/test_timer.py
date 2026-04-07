"""Tests for timer utilities."""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.timer import (
    Timer,
    timed,
    IntervalTimer,
    Debouncer,
    Throttler,
    PerformanceTimer,
)


class TestTimer:
    """Tests for Timer context manager."""

    def test_basic_timing(self) -> None:
        """Test basic timing functionality."""
        with Timer() as t:
            time.sleep(0.05)

        assert t.duration >= 0.05
        assert t.duration < 0.2

    def test_duration_property_before_exit(self) -> None:
        """Test duration property before context exit."""
        timer = Timer()
        timer.__enter__()
        time.sleep(0.01)

        assert timer.duration >= 0.01

        timer.__exit__(None, None, None)

    def test_elapsed_without_context(self) -> None:
        """Test getting elapsed time without context manager."""
        timer = Timer()
        time.sleep(0.02)

        assert timer.duration >= 0.02


class TestTimedDecorator:
    """Tests for timed decorator."""

    def test_timed_function(self) -> None:
        """Test function with timed decorator."""
        @timed
        def slow_func():
            time.sleep(0.02)
            return 42

        result = slow_func()
        assert result == 42


class TestIntervalTimer:
    """Tests for IntervalTimer."""

    def test_start_stop(self) -> None:
        """Test starting and stopping timer."""
        call_count = 0

        def callback():
            nonlocal call_count
            call_count += 1

        timer = IntervalTimer(0.02, callback)
        assert not timer.is_running

        timer.start()
        assert timer.is_running

        time.sleep(0.07)
        timer.stop()
        assert not timer.is_running

        assert call_count >= 2

    def test_multiple_start_noop(self) -> None:
        """Test multiple start calls are idempotent."""
        call_count = 0

        def callback():
            nonlocal call_count
            call_count += 1

        timer = IntervalTimer(0.1, callback)
        timer.start()
        timer.start()
        timer.start()

        time.sleep(0.05)
        timer.stop()

        assert call_count == 0


class TestDebouncer:
    """Tests for Debouncer."""

    def test_basic_debounce(self) -> None:
        """Test basic debouncing."""
        call_count = 0

        debouncer = Debouncer(0.05)

        @debouncer.debounce
        def debounced_func():
            nonlocal call_count
            call_count += 1

        debounced_func()
        debounced_func()
        debounced_func()

        time.sleep(0.1)
        assert call_count == 0

    def test_execute_cancels_pending(self) -> None:
        """Test execute cancels pending debounced calls."""
        call_count = 0

        debouncer = Debouncer(0.1)

        def func():
            nonlocal call_count
            call_count += 1

        debouncer.execute(func)
        debouncer.execute(func)

        assert call_count == 2


class TestThrottler:
    """Tests for Throttler."""

    def test_basic_throttle(self) -> None:
        """Test basic throttling."""
        call_times = []

        throttler = Throttler(rate=10)

        @throttler.throttle
        def throttled_func():
            call_times.append(time.time())

        for _ in range(5):
            throttled_func()

        assert len(call_times) == 5

    def test_throttle_rate(self) -> None:
        """Test throttle enforces rate limit."""
        call_count = 0

        throttler = Throttler(rate=5)

        @throttler.throttle
        def limited():
            nonlocal call_count
            call_count += 1

        start = time.time()
        for _ in range(3):
            limited()
        elapsed = time.time() - start

        assert elapsed >= 0.4


class TestPerformanceTimer:
    """Tests for PerformanceTimer."""

    def test_lap_timing(self) -> None:
        """Test lap timing."""
        timer = PerformanceTimer()

        time.sleep(0.02)
        lap1 = timer.lap()

        time.sleep(0.02)
        lap2 = timer.lap()

        assert lap1 >= 0.02
        assert lap2 >= 0.02

    def test_elapsed(self) -> None:
        """Test elapsed time."""
        timer = PerformanceTimer()
        time.sleep(0.03)

        assert timer.elapsed >= 0.03

    def test_reset(self) -> None:
        """Test timer reset."""
        timer = PerformanceTimer()
        time.sleep(0.01)
        timer.lap()

        timer.reset()
        time.sleep(0.01)
        lap = timer.lap()

        assert lap < 0.02


if __name__ == "__main__":
    pytest.main([__file__, "-v"])