"""Tests for counter and tally utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.counter import (
    Counter,
    CounterStats,
    Tally,
    RateCounter,
    StatisticsCollector,
    get_collector,
    counter,
    tally,
    rate_counter,
)


class TestCounter:
    """Tests for Counter."""

    def test_create(self) -> None:
        """Test creating counter."""
        c = Counter("test", 0)
        assert c.name == "test"
        assert c.value == 0

    def test_increment(self) -> None:
        """Test incrementing."""
        c = Counter("test", 0)
        c.increment()
        assert c.value == 1
        c.increment(5)
        assert c.value == 6

    def test_decrement(self) -> None:
        """Test decrementing."""
        c = Counter("test", 10)
        c.decrement()
        assert c.value == 9

    def test_reset(self) -> None:
        """Test resetting."""
        c = Counter("test", 5)
        c.reset()
        assert c.value == 0
        c.reset(10)
        assert c.value == 10

    def test_get_stats(self) -> None:
        """Test getting stats."""
        c = Counter("test", 0)
        c.increment()
        c.increment()
        stats = c.get_stats()
        assert stats.name == "test"
        assert stats.value == 2


class TestTally:
    """Tests for Tally."""

    def test_create(self) -> None:
        """Test creating tally."""
        t = Tally("test")
        assert t.name == "test"

    def test_increment(self) -> None:
        """Test incrementing."""
        t = Tally("test")
        t.increment("a")
        t.increment("a")
        t.increment("b")
        assert t.get("a") == 2
        assert t.get("b") == 1

    def test_decrement(self) -> None:
        """Test decrementing."""
        t = Tally("test")
        t.set("a", 10)
        t.decrement("a", 3)
        assert t.get("a") == 7

    def test_total(self) -> None:
        """Test getting total."""
        t = Tally("test")
        t.increment("a", 2)
        t.increment("b", 3)
        assert t.total() == 5

    def test_categories(self) -> None:
        """Test getting categories."""
        t = Tally("test")
        t.increment("a")
        t.increment("b")
        cats = t.categories()
        assert "a" in cats
        assert "b" in cats

    def test_reset(self) -> None:
        """Test resetting."""
        t = Tally("test")
        t.increment("a")
        t.reset("a")
        assert t.get("a") == 0
        t.reset()
        assert t.total() == 0


class TestRateCounter:
    """Tests for RateCounter."""

    def test_create(self) -> None:
        """Test creating rate counter."""
        r = RateCounter("test", 60.0)
        assert r.name == "test"

    def test_record(self) -> None:
        """Test recording events."""
        r = RateCounter("test")
        r.record()
        r.record(5)
        assert r.total() == 6

    def test_rate(self) -> None:
        """Test getting rate."""
        r = RateCounter("test")
        r.record(10)
        rate = r.rate()
        assert rate >= 0


class TestStatisticsCollector:
    """Tests for StatisticsCollector."""

    def test_create(self) -> None:
        """Test creating collector."""
        c = StatisticsCollector("test")
        assert c._name == "test"

    def test_counter(self) -> None:
        """Test getting counter."""
        c = StatisticsCollector("test")
        counter = c.counter("requests")
        assert isinstance(counter, Counter)

    def test_tally(self) -> None:
        """Test getting tally."""
        c = StatisticsCollector("test")
        t = c.tally("status")
        assert isinstance(t, Tally)

    def test_rate_counter(self) -> None:
        """Test getting rate counter."""
        c = StatisticsCollector("test")
        r = c.rate_counter("events")
        assert isinstance(r, RateCounter)


class TestModuleFunctions:
    """Tests for module-level functions."""

    def test_get_collector(self) -> None:
        """Test getting global collector."""
        c = get_collector()
        assert c is not None

    def test_counter(self) -> None:
        """Test global counter function."""
        c = counter("test")
        assert isinstance(c, Counter)

    def test_tally_function(self) -> None:
        """Test global tally function."""
        t = tally("test")
        assert isinstance(t, Tally)

    def test_rate_counter_function(self) -> None:
        """Test global rate counter function."""
        r = rate_counter("test")
        assert isinstance(r, RateCounter)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])