"""Tests for cache utilities."""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.cache import (
    LRUCache,
    TTLCache,
    cached,
    Cache,
    CacheStats,
)


class TestLRUCache:
    """Tests for LRUCache."""

    def test_set_get(self) -> None:
        """Test basic set and get."""
        cache = LRUCache(max_size=3)
        cache.set("key", "value")
        assert cache.get("key") == "value"

    def test_get_missing(self) -> None:
        """Test get with missing key."""
        cache = LRUCache()
        assert cache.get("missing") is None
        assert cache.get("missing", "default") == "default"

    def test_eviction(self) -> None:
        """Test LRU eviction."""
        cache = LRUCache(max_size=3)
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)
        cache.set("d", 4)  # Should evict 'a'

        assert cache.get("a") is None
        assert cache.get("d") == 4

    def test_lru_order(self) -> None:
        """Test LRU reorders on access."""
        cache = LRUCache(max_size=3)
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)
        cache.get("a")  # Access 'a'
        cache.set("d", 4)  # Should evict 'b' (least recently used)

        assert cache.get("b") is None
        assert cache.get("a") == 1

    def test_update_existing(self) -> None:
        """Test updating existing key."""
        cache = LRUCache()
        cache.set("key", "value1")
        cache.set("key", "value2")
        assert cache.get("key") == "value2"

    def test_contains(self) -> None:
        """Test contains check."""
        cache = LRUCache()
        cache.set("key", "value")
        assert "key" in cache
        assert "other" not in cache

    def test_stats(self) -> None:
        """Test cache statistics."""
        cache = LRUCache()
        cache.set("a", 1)
        cache.get("a")
        cache.get("b")

        stats = cache.stats
        assert stats.hits == 1
        assert stats.misses == 1
        assert stats.total_requests == 2


class TestTTLCache:
    """Tests for TTLCache."""

    def test_set_get(self) -> None:
        """Test basic set and get."""
        cache = TTLCache(ttl=1)
        cache.set("key", "value")
        assert cache.get("key") == "value"

    def test_expiration(self) -> None:
        """Test TTL expiration."""
        cache = TTLCache(ttl=0.1)
        cache.set("key", "value")
        assert cache.get("key") == "value"

        time.sleep(0.15)
        assert cache.get("key") is None

    def test_custom_ttl(self) -> None:
        """Test custom TTL for specific item."""
        cache = TTLCache(ttl=1)
        cache.set("short", "value", ttl=0.05)

        time.sleep(0.1)
        assert cache.get("short") is None


class TestCachedDecorator:
    """Tests for cached decorator."""

    def test_basic_caching(self) -> None:
        """Test basic caching decorator."""
        call_count = [0]

        @cached(max_size=10)
        def expensive_func(x):
            call_count[0] += 1
            return x * 2

        assert expensive_func(5) == 10
        assert expensive_func(5) == 10
        assert call_count[0] == 1


class TestCache:
    """Tests for Cache interface."""

    def test_lru_cache_type(self) -> None:
        """Test Cache with LRU type."""
        cache = Cache(cache_type="lru", max_size=5)
        cache.set("key", "value")
        assert cache.get("key") == "value"

    def test_ttl_cache_type(self) -> None:
        """Test Cache with TTL type."""
        cache = Cache(cache_type="ttl", ttl=1, max_size=5)
        cache.set("key", "value")
        assert cache.get("key") == "value"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])