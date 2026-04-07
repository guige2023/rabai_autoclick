"""Tests for metrics utilities."""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.metrics import (
    Counter,
    Gauge,
    Histogram,
    Timer,
    MetricsCollector,
    metrics,
)


class TestCounter:
    """Tests for Counter."""

    def test_increment(self) -> None:
        """Test counter increment."""
        counter = Counter("test", initial_value=0)
        counter.inc()
        assert counter.value == 1

        counter.inc(5)
        assert counter.value == 6

    def test_decrement(self) -> None:
        """Test counter decrement."""
        counter = Counter("test", initial_value=10)
        counter.dec(3)
        assert counter.value == 7

    def test_set(self) -> None:
        """Test counter set."""
        counter = Counter("test")
        counter.set(100)
        assert counter.value == 100

    def test_reset(self) -> None:
        """Test counter reset."""
        counter = Counter("test", initial_value=50)
        counter.reset()
        assert counter.value == 0


class TestGauge:
    """Tests for Gauge."""

    def test_set(self) -> None:
        """Test gauge set."""
        gauge = Gauge("test", initial_value=0)
        gauge.set(42)
        assert gauge.value == 42

    def test_increment(self) -> None:
        """Test gauge increment."""
        gauge = Gauge("test", initial_value=10)
        gauge.inc(5)
        assert gauge.value == 15

    def test_decrement(self) -> None:
        """Test gauge decrement."""
        gauge = Gauge("test", initial_value=10)
        gauge.dec(3)
        assert gauge.value == 7

    def test_reset(self) -> None:
        """Test gauge reset."""
        gauge = Gauge("test", initial_value=100)
        gauge.reset()
        assert gauge.value == 0


class TestHistogram:
    """Tests for Histogram."""

    def test_observe(self) -> None:
        """Test histogram observation."""
        hist = Histogram("test", buckets=[0.1, 0.5, 1.0])
        hist.observe(0.3)
        hist.observe(0.7)

        assert hist.count == 2
        assert hist.sum == 1.0

    def test_mean(self) -> None:
        """Test histogram mean."""
        hist = Histogram("test")
        hist.observe(10)
        hist.observe(20)

        assert hist.mean == 15.0

    def test_min_max(self) -> None:
        """Test histogram min/max."""
        hist = Histogram("test")
        hist.observe(5)
        hist.observe(15)
        hist.observe(10)

        assert hist.min == 5
        assert hist.max == 15

    def test_bucket_counts(self) -> None:
        """Test bucket counting."""
        buckets = [1.0, 5.0, 10.0]
        hist = Histogram("test", buckets=buckets)

        hist.observe(0.5)
        hist.observe(3.0)
        hist.observe(7.0)

        counts = hist.bucket_counts()
        # Buckets are cumulative: 0.5 goes in bucket 1.0, 3.0 in 5.0, 7.0 in 10.0
        assert counts[1.0] == 1
        assert counts[5.0] == 2
        assert counts[10.0] == 3

    def test_reset(self) -> None:
        """Test histogram reset."""
        hist = Histogram("test")
        hist.observe(100)
        hist.reset()

        assert hist.count == 0
        assert hist.sum == 0.0


class TestTimerMetric:
    """Tests for Timer metric."""

    def test_time_context_manager(self) -> None:
        """Test timer as context manager."""
        timer = Timer("test")

        with timer.time():
            time.sleep(0.02)

        assert timer.count == 1
        assert timer.mean >= 0.02

    def test_observe_duration(self) -> None:
        """Test observing duration directly."""
        timer = Timer("test")
        timer.observe(0.5)

        assert timer.count == 1
        assert timer.mean == 0.5


class TestMetricsCollector:
    """Tests for MetricsCollector."""

    def test_get_counter(self) -> None:
        """Test getting a counter."""
        collector = MetricsCollector()
        counter = collector.counter("requests")

        counter.inc()
        assert counter.value == 1

        # Same counter returned on second call
        counter2 = collector.counter("requests")
        assert counter2.value == 1

    def test_get_gauge(self) -> None:
        """Test getting a gauge."""
        collector = MetricsCollector()
        gauge = collector.gauge("memory", value=100)

        assert gauge.value == 100

        gauge.inc(50)
        assert gauge.value == 150

    def test_get_histogram(self) -> None:
        """Test getting a histogram."""
        collector = MetricsCollector()
        hist = collector.histogram("latency")

        hist.observe(0.5)
        hist.observe(0.3)

        assert hist.count == 2

    def test_timed_decorator(self) -> None:
        """Test timed decorator."""
        collector = MetricsCollector()

        @collector.timed("function_duration")
        def slow_func():
            time.sleep(0.02)
            return 42

        result = slow_func()
        assert result == 42

        timer = collector.timer("function_duration")
        assert timer.count == 1

    def test_get_all_metrics(self) -> None:
        """Test getting all metrics."""
        collector = MetricsCollector()
        collector.counter("requests").inc()
        collector.gauge("memory", 1024)
        collector.histogram("latency").observe(0.5)

        all_metrics = collector.get_all_metrics()

        assert "requests" in all_metrics["counters"]
        assert "memory" in all_metrics["gauges"]
        assert "latency" in all_metrics["histograms"]

    def test_labels(self) -> None:
        """Test metrics with labels."""
        collector = MetricsCollector()

        c1 = collector.counter("requests", labels={"method": "GET"})
        c2 = collector.counter("requests", labels={"method": "POST"})

        c1.inc()
        c2.inc()
        c2.inc()

        assert c1.value == 1
        assert c2.value == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])