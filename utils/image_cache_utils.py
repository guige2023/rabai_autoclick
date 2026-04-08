"""Image cache for template matching and screen capture optimization.

Caches recently captured screen regions and template images to avoid
redundant captures and expensive image processing operations.
Implements LRU eviction when the cache exceeds a size limit.

Example:
    >>> from utils.image_cache_utils import ImageCache
    >>> cache = ImageCache(max_items=100, ttl=30.0)
    >>> cache.put("home_btn", image_data, region=(0, 0, 100, 50))
    >>> cached = cache.get("home_btn")
"""
from __future__ import annotations

import hashlib
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

__all__ = [
    "CacheEntry",
    "ImageCache",
]


@dataclass
class CacheEntry:
    """A cached image entry with metadata.

    Attributes:
        data: The cached image data (bytes or numpy array).
        region: The screen region (x, y, w, h) this image covers.
        timestamp: When this entry was created.
        ttl: Time-to-live in seconds.
        hash: SHA256 hash of the image data.
        hit_count: Number of times this entry was retrieved.
    """

    data: Any
    region: tuple[int, int, int, int] | None
    timestamp: float
    ttl: float
    hash: str
    hit_count: int = 0

    def is_expired(self, now: float | None = None) -> bool:
        """Check if this entry has exceeded its TTL."""
        if self.ttl <= 0:
            return False
        ts = now or time.time()
        return (ts - self.timestamp) > self.ttl


class ImageCache:
    """LRU cache for screen captures and template images.

    Thread-unsafe — use with external locking if needed in concurrent code.

    Example:
        >>> cache = ImageCache(max_items=50, ttl=60.0)
        >>> cache.put("template_login_btn", img_bytes)
        >>> entry = cache.get("template_login_btn")
        >>> if entry:
        ...     print(f"Cache hit! Used {entry.hit_count} times")
    """

    def __init__(
        self,
        max_items: int = 100,
        ttl: float = 60.0,
    ) -> None:
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._max_items = max_items
        self._ttl = ttl

    @staticmethod
    def compute_hash(data: bytes) -> str:
        """Compute a SHA256 hash of image data."""
        return hashlib.sha256(data).hexdigest()

    def put(
        self,
        key: str,
        data: Any,
        region: tuple[int, int, int, int] | None = None,
        ttl: float | None = None,
    ) -> None:
        """Store an image in the cache.

        Args:
            key: Unique key for this image.
            data: Image data (bytes, numpy array, etc.).
            region: Screen region (x, y, w, h) this image represents.
            ttl: Override the default TTL for this entry.
        """
        if isinstance(data, bytes):
            hash_str = self.compute_hash(data)
        else:
            hash_str = str(hash(data))

        entry = CacheEntry(
            data=data,
            region=region,
            timestamp=time.time(),
            ttl=ttl if ttl is not None else self._ttl,
            hash=hash_str,
        )

        # Evict if we're at capacity
        if key not in self._cache and len(self._cache) >= self._max_items:
            self._evict_lru()

        self._cache[key] = entry
        # Move to end (most recently used)
        self._cache.move_to_end(key)

    def get(self, key: str) -> CacheEntry | None:
        """Retrieve an image from the cache.

        Args:
            key: The cache key to look up.

        Returns:
            CacheEntry if found and not expired, else None.
        """
        entry = self._cache.get(key)
        if entry is None:
            return None
        if entry.is_expired():
            self._cache.pop(key, None)
            return None

        # Update hit count and move to end (most recently used)
        entry.hit_count += 1
        self._cache.move_to_end(key)
        return entry

    def get_data(self, key: str) -> Any | None:
        """Retrieve just the image data from the cache."""
        entry = self.get(key)
        return entry.data if entry else None

    def _evict_lru(self) -> None:
        """Remove the least recently used entry."""
        if self._cache:
            self._cache.popitem(last=False)

    def invalidate(self, key: str) -> bool:
        """Remove a specific entry from the cache.

        Args:
            key: The cache key to remove.

        Returns:
            True if the key was found and removed.
        """
        if key in self._cache:
            self._cache.pop(key)
            return True
        return False

    def clear(self) -> None:
        """Clear all entries from the cache."""
        self._cache.clear()

    def cleanup_expired(self) -> int:
        """Remove all expired entries.

        Returns:
            Number of entries removed.
        """
        now = time.time()
        expired = [
            k for k, e in self._cache.items() if e.is_expired(now)
        ]
        for k in expired:
            self._cache.pop(k, None)
        return len(expired)

    def stats(self) -> dict[str, Any]:
        """Return cache statistics.

        Returns:
            A dict with size, max_size, total_hits, and hit_rate.
        """
        total_hits = sum(e.hit_count for e in self._cache.values())
        size = len(self._cache)
        return {
            "size": size,
            "max_size": self._max_items,
            "total_hits": total_hits,
            "hit_rate": total_hits / size if size > 0 else 0.0,
        }

    def keys(self) -> list[str]:
        """Return all cache keys."""
        return list(self._cache.keys())
