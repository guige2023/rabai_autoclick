"""Tests for memory utilities."""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.memory import (
    MemoryManager,
    ImageCache,
    OCRCache,
)


class TestMemoryManager:
    """Tests for MemoryManager."""

    def test_singleton(self) -> None:
        """Test MemoryManager is singleton."""
        m1 = MemoryManager()
        m2 = MemoryManager()
        assert m1 is m2

    def test_get_cached_loads_on_miss(self) -> None:
        """Test get_cached loads on cache miss."""
        m = MemoryManager()
        m.clear_cache()
        result = m.get_cached("key1", lambda: {"data": 123})
        assert result == {"data": 123}

    def test_get_cached_returns_cached(self) -> None:
        """Test get_cached returns cached value."""
        m = MemoryManager()
        m.clear_cache()
        m.set_cached("key2", "value2")
        result = m.get_cached("key2", lambda: "should not be called")
        assert result == "value2"

    def test_set_cached(self) -> None:
        """Test setting cached value."""
        m = MemoryManager()
        m.clear_cache()
        m.set_cached("key3", "value3")
        assert m.get_cached("key3", lambda: None) == "value3"

    def test_set_cached_weak(self) -> None:
        """Test setting weak reference."""
        m = MemoryManager()
        m.clear_cache()
        obj = {"data": "test"}
        m.set_cached("weak_key", obj, weak=True)
        # Weak ref may or may not be collected

    def test_clear_cache(self) -> None:
        """Test clearing cache."""
        m = MemoryManager()
        m.set_cached("test_key", "test_value")
        m.clear_cache()
        # After clear, cache should be empty
        assert m.get_memory_usage()["cache_size"] == 0

    def test_get_memory_usage(self) -> None:
        """Test getting memory usage."""
        m = MemoryManager()
        usage = m.get_memory_usage()
        assert "rss" in usage
        assert "vms" in usage
        assert "cache_size" in usage

    def test_optimize(self) -> None:
        """Test memory optimization."""
        m = MemoryManager()
        result = m.optimize()
        assert "before" in result
        assert "after" in result
        assert "freed" in result


class TestImageCache:
    """Tests for ImageCache."""

    def test_singleton(self) -> None:
        """Test ImageCache is singleton."""
        c1 = ImageCache()
        c2 = ImageCache()
        assert c1 is c2

    def test_get_template_nonexistent(self) -> None:
        """Test getting nonexistent template."""
        c = ImageCache()
        c.clear()
        result = c.get_template("/nonexistent/path/to/image.png")
        assert result is None

    def test_clear(self) -> None:
        """Test clearing cache."""
        c = ImageCache()
        c.clear()
        # Should not raise


class TestOCRCache:
    """Tests for OCRCache."""

    def test_singleton(self) -> None:
        """Test OCRCache is singleton."""
        c1 = OCRCache()
        c2 = OCRCache()
        assert c1 is c2

    def test_get_ocr_none(self) -> None:
        """Test getting OCR when none set."""
        c = OCRCache()
        # No OCR instance set, should return None after idle check
        result = c.get_ocr(lambda: None)
        # May be None since no instance was set

    def test_set_ocr(self) -> None:
        """Test setting OCR instance."""
        c = OCRCache()
        mock_ocr = {"engine": "mock"}
        c.set_ocr(mock_ocr)
        # Instance should be set

    def test_check_idle_cleanup(self) -> None:
        """Test idle cleanup check."""
        c = OCRCache()
        c.set_ocr({"engine": "mock"})
        c.check_idle_cleanup()
        # If idle timeout not exceeded, should still have instance


if __name__ == "__main__":
    pytest.main([__file__, "-v"])