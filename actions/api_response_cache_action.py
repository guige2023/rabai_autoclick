"""
API Response Cache Action Module.

Caching layer for API responses with TTL, eviction policies, and compression.
"""

import gzip
import hashlib
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional


@dataclass
class CacheEntry:
    """A single cache entry with metadata."""

    key: str
    value: bytes
    created_at: float
    last_accessed: float
    ttl: float
    hit_count: int = 0
    compressed: bool = False

    def is_expired(self) -> bool:
        """Check if entry has exceeded its TTL."""
        if self.ttl <= 0:
            return False
        return time.time() - self.created_at > self.ttl

    def access(self) -> bytes:
        """Record an access and return the value."""
        self.last_accessed = time.time()
        self.hit_count += 1
        return self.value


@dataclass
class CacheStats:
    """Statistics for cache performance monitoring."""

    hits: int = 0
    misses: int = 0
    evictions: int = 0
    expirations: int = 0
    total_bytes_saved: int = 0

    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate."""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0

    def to_dict(self) -> dict[str, Any]:
        """Export stats as dictionary."""
        return {
            "hits": self.hits,
            "misses": self.misses,
            "evictions": self.evictions,
            "expirations": self.expirations,
            "hit_rate": round(self.hit_rate, 4),
            "total_bytes_saved": self.total_bytes_saved,
        }


class APICache:
    """
    In-memory cache with optional disk persistence.

    Supports TTL, LRU eviction, compression, and statistics tracking.
    """

    def __init__(
        self,
        max_entries: int = 1000,
        default_ttl: float = 3600.0,
        compress_threshold: int = 1024,
        persistence_path: Optional[Path] = None,
    ) -> None:
        """
        Initialize the API cache.

        Args:
            max_entries: Maximum number of entries before eviction.
            default_ttl: Default time-to-live in seconds.
            compress_threshold: Min size in bytes to trigger compression.
            persistence_path: Optional path for disk persistence.
        """
        self._cache: dict[str, CacheEntry] = {}
        self._max_entries = max_entries
        self._default_ttl = default_ttl
        self._compress_threshold = compress_threshold
        self._persistence_path = persistence_path
        self._stats = CacheStats()
        self._access_order: list[str] = []

    def _make_key(self, prefix: str, *args: Any, **kwargs: Any) -> str:
        """Generate a cache key from arguments."""
        data = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True, default=str)
        digest = hashlib.sha256(data.encode()).hexdigest()[:16]
        return f"{prefix}:{digest}"

    def _compress(self, data: bytes) -> bytes:
        """Compress data using gzip."""
        return gzip.compress(data, compresslevel=6)

    def _decompress(self, data: bytes) -> bytes:
        """Decompress gzip data."""
        return gzip.decompress(data)

    def _evict_expired(self) -> int:
        """Remove all expired entries. Returns count of removed entries."""
        expired_keys = [
            k for k, v in self._cache.items() if v.is_expired()
        ]
        for key in expired_keys:
            self._evict(key)
            self._stats.expirations += 1
        return len(expired_keys)

    def _evict_lru(self) -> None:
        """Evict least recently used entry if over capacity."""
        if len(self._cache) < self._max_entries:
            return
        if not self._access_order:
            return
        lru_key = self._access_order.pop(0)
        if lru_key in self._cache:
            self._evict(lru_key)
            self._stats.evictions += 1

    def _evict(self, key: str) -> None:
        """Remove a specific key from cache."""
        if key in self._cache:
            del self._cache[key]
        if key in self._access_order:
            self._access_order.remove(key)

    def get(self, key: str) -> Optional[bytes]:
        """
        Retrieve a cached value.

        Args:
            key: Cache key to look up.

        Returns:
            Cached bytes or None if not found/expired.
        """
        if key not in self._cache:
            self._stats.misses += 1
            return None
        entry = self._cache[key]
        if entry.is_expired():
            self._evict(key)
            self._stats.expirations += 1
            self._stats.misses += 1
            return None
        value = entry.access()
        if entry.compressed:
            value = self._decompress(value)
        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)
        self._stats.hits += 1
        return value

    def set(
        self,
        key: str,
        value: bytes,
        ttl: Optional[float] = None,
    ) -> None:
        """
        Store a value in the cache.

        Args:
            key: Cache key.
            value: Bytes to store.
            ttl: Optional custom TTL (overrides default).
        """
        self._evict_lru()
        entry_ttl = ttl if ttl is not None else self._default_ttl
        compressed = len(value) >= self._compress_threshold
        if compressed:
            value = self._compress(value)
        self._cache[key] = CacheEntry(
            key=key,
            value=value,
            created_at=time.time(),
            last_accessed=time.time(),
            ttl=entry_ttl,
            compressed=compressed,
        )
        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)

    def invalidate(self, key: str) -> bool:
        """
        Remove a specific key from cache.

        Returns:
            True if key was present.
        """
        if key in self._cache:
            self._evict(key)
            return True
        return False

    def clear(self) -> int:
        """
        Clear all cached entries.

        Returns:
            Number of entries cleared.
        """
        count = len(self._cache)
        self._cache.clear()
        self._access_order.clear()
        return count

    def cached_call(
        self,
        func: Callable[..., bytes],
        *args: Any,
        cache_key: Optional[str] = None,
        ttl: Optional[float] = None,
        **kwargs: Any,
    ) -> bytes:
        """
       Execute a function with caching.

        Args:
            func: Function to call (should return bytes).
            *args: Positional arguments for func.
            cache_key: Optional explicit cache key.
            ttl: Optional TTL override.
            **kwargs: Keyword arguments for func.

        Returns:
            Bytes result from cache or fresh execution.
        """
        key = cache_key or self._make_key(func.__name__, *args, **kwargs)
        cached = self.get(key)
        if cached is not None:
            return cached
        result = func(*args, **kwargs)
        if isinstance(result, str):
            result = result.encode("utf-8")
        self.set(key, result, ttl=ttl)
        return result

    def stats(self) -> CacheStats:
        """Return current cache statistics."""
        return self._stats

    def save_to_disk(self) -> None:
        """Persist cache contents to disk."""
        if not self._persistence_path:
            return
        self._persistence_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "saved_at": time.time(),
            "entries": [
                {
                    "key": k,
                    "value": v.value.decode("latin-1"),
                    "created_at": v.created_at,
                    "last_accessed": v.last_accessed,
                    "ttl": v.ttl,
                    "compressed": v.compressed,
                }
                for k, v in self._cache.items()
            ],
        }
        with gzip.open(self._persistence_path, "wt", encoding="utf-8") as f:
            json.dump(data, f)

    def load_from_disk(self) -> int:
        """
        Load cache contents from disk.

        Returns:
            Number of entries loaded.
        """
        if not self._persistence_path or not self._persistence_path.exists():
            return 0
        try:
            with gzip.open(self._persistence_path, "rt", encoding="utf-8") as f:
                data = json.load(f)
            entries = data.get("entries", [])
            for entry in entries:
                value = entry["value"].encode("latin-1")
                self._cache[entry["key"]] = CacheEntry(
                    key=entry["key"],
                    value=value,
                    created_at=entry["created_at"],
                    last_accessed=entry["last_accessed"],
                    ttl=entry["ttl"],
                    compressed=entry.get("compressed", False),
                )
                self._access_order.append(entry["key"])
            return len(entries)
        except (json.JSONDecodeError, KeyError, gzip.BadGzipFile):
            return 0


def create_api_cache(
    max_entries: int = 1000,
    default_ttl: float = 3600.0,
    persistence_path: Optional[str] = None,
) -> APICache:
    """
    Factory function to create a configured API cache.

    Args:
        max_entries: Maximum cached entries.
        default_ttl: Default TTL in seconds.
        persistence_path: Optional path string for persistence.

    Returns:
        Configured APICache instance.
    """
    path = Path(persistence_path) if persistence_path else None
    cache = APICache(
        max_entries=max_entries,
        default_ttl=default_ttl,
        persistence_path=path,
    )
    return cache
