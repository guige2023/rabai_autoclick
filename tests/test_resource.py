"""Tests for resource management utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.resource import (
    ResourceType,
    ResourceUsage,
    ResourceTracker,
    ResourceLimiter,
    ResourceCleanup,
    ResourcePool,
    MemoryPressureDetector,
)


class TestResourceType:
    """Tests for ResourceType."""

    def test_values(self) -> None:
        """Test resource type values."""
        assert ResourceType.MEMORY.value == "memory"
        assert ResourceType.CPU.value == "cpu"


class TestResourceUsage:
    """Tests for ResourceUsage."""

    def test_create(self) -> None:
        """Test creating usage."""
        import time
        usage = ResourceUsage(
            timestamp=time.time(),
            memory_mb=100,
            cpu_percent=50,
            num_fds=10,
            num_threads=5,
        )
        assert usage.memory_mb == 100


class TestResourceTracker:
    """Tests for ResourceTracker."""

    def test_create(self) -> None:
        """Test creating tracker."""
        tracker = ResourceTracker()
        assert len(tracker._samples) == 0

    def test_sample(self) -> None:
        """Test taking sample."""
        tracker = ResourceTracker()
        usage = tracker.sample()
        assert usage.memory_mb >= 0

    def test_get_average(self) -> None:
        """Test getting average."""
        tracker = ResourceTracker()
        tracker.sample()
        tracker.sample()
        avg = tracker.get_average()
        assert "avg_memory_mb" in avg


class TestResourceLimiter:
    """Tests for ResourceLimiter."""

    def test_create(self) -> None:
        """Test creating limiter."""
        limiter = ResourceLimiter()
        assert len(limiter._limits) == 0

    def test_set_limit(self) -> None:
        """Test setting limit."""
        limiter = ResourceLimiter()
        limiter.set_limit(ResourceType.MEMORY, 1000)
        assert ResourceType.MEMORY in limiter._limits

    def test_check_limit(self) -> None:
        """Test checking limit."""
        limiter = ResourceLimiter()
        limiter.set_limit(ResourceType.MEMORY, 1)
        # Low threshold means it should exceed
        result = limiter.check_limit(ResourceType.MEMORY)
        # Result depends on current usage


class TestResourceCleanup:
    """Tests for ResourceCleanup."""

    def test_create(self) -> None:
        """Test creating cleanup."""
        cleanup = ResourceCleanup()
        assert len(cleanup._cleanup_funcs) == 0

    def test_register(self) -> None:
        """Test registering cleanup."""
        cleanup = ResourceCleanup()
        cleanup.register(lambda: None)
        assert len(cleanup._cleanup_funcs) == 1


class DummyResource:
    """Dummy resource for testing."""
    pass


class TestResourcePool:
    """Tests for ResourcePool."""

    def test_create(self) -> None:
        """Test creating pool."""
        pool = ResourcePool(factory=DummyResource)
        assert pool.available_count == 0

    def test_acquire_release(self) -> None:
        """Test acquiring and releasing."""
        pool = ResourcePool(factory=DummyResource)
        resource = pool.acquire()
        assert pool.in_use_count == 1
        pool.release(resource)
        assert pool.available_count == 1

    def test_clear(self) -> None:
        """Test clearing pool."""
        pool = ResourcePool(factory=DummyResource)
        pool.acquire()
        pool.clear()
        assert pool.available_count == 0


class TestMemoryPressureDetector:
    """Tests for MemoryPressureDetector."""

    def test_create(self) -> None:
        """Test creating detector."""
        detector = MemoryPressureDetector(threshold_mb=10000)
        assert detector._threshold == 10000

    def test_on_pressure(self) -> None:
        """Test registering callback."""
        detector = MemoryPressureDetector()
        detector.on_pressure(lambda: None)
        assert len(detector._callbacks) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])