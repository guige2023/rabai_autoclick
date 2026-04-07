"""Tests for monitoring utilities."""

import os
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.monitor import (
    PerformanceMetrics,
    HealthCheck,
    HealthChecker,
    PerformanceMonitor,
    ResourceTracker,
    Watchdog,
)


class TestPerformanceMetrics:
    """Tests for PerformanceMetrics."""

    def test_create(self) -> None:
        """Test creating metrics."""
        metrics = PerformanceMetrics(
            timestamp=1234567890.0,
            cpu_percent=10.5,
            memory_mb=100.0,
            thread_count=5,
            gc_counts=(10, 20, 30),
        )
        assert metrics.timestamp == 1234567890.0
        assert metrics.cpu_percent == 10.5
        assert metrics.memory_mb == 100.0
        assert metrics.thread_count == 5
        assert metrics.gc_counts == (10, 20, 30)


class TestHealthCheck:
    """Tests for HealthCheck."""

    def test_create_healthy(self) -> None:
        """Test creating healthy check."""
        check = HealthCheck(name="test", healthy=True, message="OK")
        assert check.name == "test"
        assert check.healthy is True
        assert check.message == "OK"
        assert check.latency_ms == 0

    def test_create_unhealthy(self) -> None:
        """Test creating unhealthy check."""
        check = HealthCheck(name="test", healthy=False, message="Error", latency_ms=50.0)
        assert check.healthy is False
        assert check.message == "Error"
        assert check.latency_ms == 50.0


class TestHealthChecker:
    """Tests for HealthChecker."""

    def test_create(self) -> None:
        """Test creating health checker."""
        checker = HealthChecker()
        assert checker.check_names == []

    def test_register_check(self) -> None:
        """Test registering a health check."""
        checker = HealthChecker()
        checker.register("test", lambda: HealthCheck(name="test", healthy=True))
        assert "test" in checker.check_names

    def test_check_all_healthy(self) -> None:
        """Test running all healthy checks."""
        checker = HealthChecker()
        checker.register("check1", lambda: HealthCheck(name="check1", healthy=True))
        checker.register("check2", lambda: HealthCheck(name="check2", healthy=True))
        all_healthy, results = checker.check_all()
        assert all_healthy is True
        assert len(results) == 2

    def test_check_all_unhealthy(self) -> None:
        """Test running checks when one fails."""
        checker = HealthChecker()
        checker.register("check1", lambda: HealthCheck(name="check1", healthy=True))
        checker.register("check2", lambda: HealthCheck(name="check2", healthy=False, message="Failed"))
        all_healthy, results = checker.check_all()
        assert all_healthy is False
        assert results[1].healthy is False

    def test_check_exception_handling(self) -> None:
        """Test exception handling in checks."""
        checker = HealthChecker()
        checker.register("bad", lambda: 1 / 0)
        all_healthy, results = checker.check_all()
        assert all_healthy is False
        assert results[0].healthy is False
        assert "ZeroDivisionError" in results[0].message


class TestPerformanceMonitor:
    """Tests for PerformanceMonitor."""

    def test_create(self) -> None:
        """Test creating performance monitor."""
        monitor = PerformanceMonitor(interval=30)
        assert monitor.interval == 30
        assert monitor.get_samples() == []

    def test_sample(self) -> None:
        """Test taking a sample."""
        monitor = PerformanceMonitor()
        monitor.sample()
        samples = monitor.get_samples()
        assert len(samples) == 1
        assert samples[0].timestamp > 0

    def test_start_stop(self) -> None:
        """Test starting and stopping monitor."""
        monitor = PerformanceMonitor(interval=0.1)
        monitor.start()
        time.sleep(0.3)
        monitor.stop()
        samples = monitor.get_samples()
        assert len(samples) >= 1

    def test_get_average_empty(self) -> None:
        """Test average with no samples."""
        monitor = PerformanceMonitor()
        avg = monitor.get_average()
        assert avg == {}

    def test_get_average(self) -> None:
        """Test average calculation."""
        monitor = PerformanceMonitor()
        monitor.sample()
        time.sleep(0.1)
        monitor.sample()
        avg = monitor.get_average()
        assert "avg_cpu" in avg
        assert "avg_memory" in avg


class TestResourceTracker:
    """Tests for ResourceTracker."""

    def test_create(self) -> None:
        """Test creating resource tracker."""
        tracker = ResourceTracker()
        assert tracker.get_snapshots() == []

    def test_take_snapshot(self) -> None:
        """Test taking a snapshot."""
        tracker = ResourceTracker()
        snapshot = tracker.take_snapshot("test")
        assert snapshot["timestamp"] > 0
        assert snapshot["label"] == "test"

    def test_multiple_snapshots(self) -> None:
        """Test multiple snapshots."""
        tracker = ResourceTracker()
        tracker.take_snapshot("one")
        tracker.take_snapshot("two")
        snapshots = tracker.get_snapshots()
        assert len(snapshots) == 2

    def test_clear(self) -> None:
        """Test clearing snapshots."""
        tracker = ResourceTracker()
        tracker.take_snapshot()
        tracker.clear()
        assert tracker.get_snapshots() == []


class TestWatchdog:
    """Tests for Watchdog."""

    def test_create(self) -> None:
        """Test creating watchdog."""
        watchdog = Watchdog(timeout=5.0)
        assert watchdog.timeout == 5.0

    def test_watch_success(self) -> None:
        """Test watching a successful operation."""
        watchdog = Watchdog(timeout=2.0)
        result = watchdog.watch(lambda: 42)
        assert result == 42

    def test_watch_with_args(self) -> None:
        """Test watching function with arguments."""
        watchdog = Watchdog(timeout=2.0)
        def add(a, b):
            return a + b
        result = watchdog.watch(add, 2, 3)
        assert result == 5

    def test_watch_timeout(self) -> None:
        """Test watching a timed out operation."""
        watchdog = Watchdog(timeout=0.1)
        def slow():
            time.sleep(1)
        with pytest.raises(TimeoutError, match="timed out"):
            watchdog.watch(slow)

    def test_watch_exception(self) -> None:
        """Test watching a failing function."""
        watchdog = Watchdog(timeout=2.0)
        def fail():
            raise ValueError("test error")
        with pytest.raises(ValueError, match="test error"):
            watchdog.watch(fail)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])