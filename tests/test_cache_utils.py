"""Tests for cache utilities."""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.cache_utils import (
    CacheEntry,
    LRUCache,
    TTLCache,
    memoize,
    cached,
    hashlib_md5,
    hashlib_sha256,
    CacheStats,
    in_memory_cache,
    lru_cacheDecorator,
    fifo_cache,
)


class TestCacheEntry:
    """Tests for CacheEntry class."""

    def test_cache_entry_no_ttl(self) -> None:
        """Test cache entry without TTL."""
        entry = CacheEntry("value")
        assert not entry.is_expired()

    def test_cache_entry_with_ttl(self) -> None:
        """Test cache entry with TTL."""
        entry = CacheEntry("value", ttl=0.1)
        assert not entry.is_expired()
        time.sleep(0.15)
        assert entry.is_expired()


class TestLRUCache:
    """Tests for LRUCache class."""

    def test_lru_cache_basic(self) -> None:
        """Test basic LRU cache operations."""
        cache = LRUCache(max_size=3)
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)
        assert cache.get("a") == 1
        assert cache.get("b") == 2
        assert cache.get("c") == 3

    def test_lru_cache_eviction(self) -> None:
        """Test LRU cache eviction."""
        cache = LRUCache(max_size=3)
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)
        cache.set("d", 4)
        assert cache.get("a") is None
        assert cache.get("d") == 4

    def test_lru_cache_delete(self) -> None:
        """Test deleting from LRU cache."""
        cache = LRUCache()
        cache.set("a", 1)
        assert cache.delete("a") is True
        assert cache.get("a") is None

    def test_lru_cache_clear(self) -> None:
        """Test clearing LRU cache."""
        cache = LRUCache()
        cache.set("a", 1)
        cache.set("b", 2)
        cache.clear()
        assert cache.size() == 0


class TestTTLCache:
    """Tests for TTLCache class."""

    def test_ttl_cache_basic(self) -> None:
        """Test basic TTL cache operations."""
        cache = TTLCache(default_ttl=1.0)
        cache.set("a", 1)
        assert cache.get("a") == 1

    def test_ttl_cache_expiration(self) -> None:
        """Test TTL cache expiration."""
        cache = TTLCache(default_ttl=0.1)
        cache.set("a", 1)
        assert cache.get("a") == 1
        time.sleep(0.15)
        assert cache.get("a") is None

    def test_ttl_cache_cleanup(self) -> None:
        """Test TTL cache cleanup."""
        cache = TTLCache(default_ttl=0.1)
        cache.set("a", 1)
        time.sleep(0.15)
        cleaned = cache.cleanup_expired()
        assert cleaned == 1
        assert cache.get("a") is None


class TestMemoize:
    """Tests for memoize decorator."""

    def test_memoize_basic(self) -> None:
        """Test basic memoization."""
        @memoize(ttl=1.0)
        def add(a, b):
            return a + b
        assert add(1, 2) == 3
        assert add(1, 2) == 3


class TestCached:
    """Tests for cached decorator."""

    def test_cached_basic(self) -> None:
        """Test basic caching."""
        call_count = 0

        @cached(ttl=1.0)
        def increment():
            nonlocal call_count
            call_count += 1
            return call_count

        assert increment() == 1
        assert increment() == 1
        assert call_count == 1


class TestHashlibMd5:
    """Tests for hashlib_md5 function."""

    def test_hashlib_md5(self) -> None:
        """Test MD5 hashing."""
        result = hashlib_md5("hello")
        assert len(result) == 32


class TestHashlibSha256:
    """Tests for hashlib_sha256 function."""

    def test_hashlib_sha256(self) -> None:
        """Test SHA256 hashing."""
        result = hashlib_sha256("hello")
        assert len(result) == 64


class TestCacheStats:
    """Tests for CacheStats class."""

    def test_cache_stats_basic(self) -> None:
        """Test basic cache statistics."""
        stats = CacheStats()
        stats.record_hit()
        stats.record_hit()
        stats.record_miss()
        assert stats.hits == 2
        assert stats.misses == 1
        assert stats.hit_rate() == 2/3

    def test_cache_stats_reset(self) -> None:
        """Test resetting cache statistics."""
        stats = CacheStats()
        stats.record_hit()
        stats.reset()
        assert stats.hits == 0


class TestInMemoryCache:
    """Tests for in_memory_cache function."""

    def test_in_memory_cache(self) -> None:
        """Test in-memory cache."""
        get, set, clear = in_memory_cache(max_size=3, ttl=1.0)
        set("a", 1)
        set("b", 2)
        assert get("a") == 1
        assert get("b") == 2
        clear()
        assert get("a") is None


class TestLruCacheDecorator:
    """Tests for lru_cacheDecorator function."""

    def test_lru_cache_decorator(self) -> None:
        """Test LRU cache decorator."""
        call_count = 0

        @lru_cacheDecorator(max_size=3)
        def get_value(x):
            nonlocal call_count
            call_count += 1
            return x * 2

        assert get_value(5) == 10
        assert get_value(5) == 10
        assert call_count == 1


class TestFifoCache:
    """Tests for fifo_cache function."""

    def test_fifo_cache_decorator(self) -> None:
        """Test FIFO cache decorator."""
        call_count = 0

        @fifo_cache(max_size=3)
        def get_value(x):
            nonlocal call_count
            call_count += 1
            return x * 2

        assert get_value(5) == 10
        assert get_value(5) == 10
        assert call_count == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
