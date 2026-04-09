"""
API Response Cache Eviction Action Module

Manages cache eviction policies for API responses.
"""

from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import time
import threading
import hashlib
import json


class EvictionPolicy(Enum):
    """Cache eviction policies."""
    LRU = "lru"  # Least Recently Used
    LFU = "lfu"  # Least Frequently Used
    FIFO = "fifo"  # First In First Out
    TTL = "ttl"  # Time To Live based
    MRU = "mru"  # Most Recently Used
    RANDOM = "random"  # Random eviction


@dataclass
class CacheEntry:
    """Single cache entry."""
    key: str
    value: Any
    created_at: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)
    access_count: int = 0
    ttl_seconds: Optional[float] = None
    size_bytes: int = 0
    tags: List[str] = field(default_factory=list)


@dataclass
class CacheStats:
    """Cache statistics."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    expirations: int = 0
    current_size: int = 0
    current_count: int = 0


class LRUCache:
    """Least Recently Used cache implementation."""

    def __init__(self, max_size: int):
        self.max_size = max_size
        self.cache: Dict[str, CacheEntry] = {}
        self.order: List[str] = []  # Most recent at end
        self.lock = threading.RLock()

    def get(self, key: str) -> Optional[Any]:
        """Get entry, updating access time."""
        with self.lock:
            if key not in self.cache:
                return None
            entry = self.cache[key]
            entry.last_accessed = time.time()
            entry.access_count += 1
            # Move to end (most recent)
            if key in self.order:
                self.order.remove(key)
            self.order.append(key)
            return entry.value

    def put(self, key: str, value: Any, ttl: Optional[float] = None, tags: Optional[List[str]] = None) -> None:
        """Put entry in cache."""
        with self.lock:
            if key in self.cache:
                entry = self.cache[key]
                entry.value = value
                entry.last_accessed = time.time()
                entry.ttl_seconds = ttl
                if tags:
                    entry.tags = tags
                # Move to end
                if key in self.order:
                    self.order.remove(key)
                self.order.append(key)
            else:
                entry = CacheEntry(
                    key=key,
                    value=value,
                    ttl_seconds=ttl,
                    tags=tags or [],
                    size_bytes=self._estimate_size(value)
                )
                self.cache[key] = entry
                self.order.append(key)
                self._evict_if_needed()

    def _evict_if_needed(self) -> None:
        """Evict oldest entry if over capacity."""
        while len(self.cache) > self.max_size and self.order:
            oldest_key = self.order.pop(0)
            self.cache.pop(oldest_key, None)

    def _estimate_size(self, value: Any) -> int:
        """Estimate size of value."""
        try:
            return len(json.dumps(value).encode())
        except:
            return 1024

    def invalidate(self, key: str) -> bool:
        """Invalidate specific key."""
        with self.lock:
            if key in self.cache:
                self.cache.pop(key)
                if key in self.order:
                    self.order.remove(key)
                return True
            return False

    def get_lru_key(self) -> Optional[str]:
        """Get least recently used key."""
        return self.order[0] if self.order else None

    def clear(self) -> None:
        """Clear all entries."""
        with self.lock:
            self.cache.clear()
            self.order.clear()


class LFUCache:
    """Least Frequently Used cache implementation."""

    def __init__(self, max_size: int):
        self.max_size = max_size
        self.cache: Dict[str, CacheEntry] = {}
        self.lock = threading.RLock()

    def get(self, key: str) -> Optional[Any]:
        """Get entry, incrementing access count."""
        with self.lock:
            if key not in self.cache:
                return None
            entry = self.cache[key]
            entry.access_count += 1
            entry.last_accessed = time.time()
            return entry.value

    def put(self, key: str, value: Any, ttl: Optional[float] = None, tags: Optional[List[str]] = None) -> None:
        """Put entry in cache."""
        with self.lock:
            if key in self.cache:
                entry = self.cache[key]
                entry.value = value
                entry.access_count += 1
                entry.last_accessed = time.time()
                entry.ttl_seconds = ttl
                if tags:
                    entry.tags = tags
            else:
                entry = CacheEntry(
                    key=key,
                    value=value,
                    ttl_seconds=ttl,
                    tags=tags or []
                )
                self.cache[key] = entry
                self._evict_if_needed()

    def _evict_if_needed(self) -> None:
        """Evict least frequently used entry."""
        while len(self.cache) > self.max_size:
            if not self.cache:
                break
            lfu_key = min(self.cache.keys(), key=lambda k: self.cache[k].access_count)
            self.cache.pop(lfu_key)

    def get_lfu_key(self) -> Optional[str]:
        """Get least frequently used key."""
        if not self.cache:
            return None
        return min(self.cache.keys(), key=lambda k: self.cache[k].access_count)


class TTLCache:
    """Time To Live based cache."""

    def __init__(self, default_ttl: float = 300):
        self.default_ttl = default_ttl
        self.cache: Dict[str, CacheEntry] = {}
        self.lock = threading.RLock()

    def get(self, key: str) -> Optional[Any]:
        """Get entry if not expired."""
        with self.lock:
            if key not in self.cache:
                return None
            entry = self.cache[key]
            age = time.time() - entry.created_at
            ttl = entry.ttl_seconds or self.default_ttl
            if age > ttl:
                self.cache.pop(key)
                return None
            entry.last_accessed = time.time()
            return entry.value

    def put(self, key: str, value: Any, ttl: Optional[float] = None, tags: Optional[List[str]] = None) -> None:
        """Put entry with optional TTL."""
        with self.lock:
            entry = CacheEntry(
                key=key,
                value=value,
                ttl_seconds=ttl or self.default_ttl,
                tags=tags or []
            )
            self.cache[key] = entry

    def cleanup_expired(self) -> int:
        """Remove all expired entries. Returns count."""
        with self.lock:
            now = time.time()
            expired = []
            for key, entry in self.cache.items():
                age = now - entry.created_at
                ttl = entry.ttl_seconds or self.default_ttl
                if age > ttl:
                    expired.append(key)
            for key in expired:
                self.cache.pop(key)
            return len(expired)


class TagBasedEviction:
    """Tag-based cache eviction."""

    def __init__(self):
        self.cache: Dict[str, CacheEntry] = {}
        self.tag_index: Dict[str, set] = {}  # tag -> set of keys
        self.lock = threading.RLock()

    def get(self, key: str) -> Optional[Any]:
        """Get entry."""
        with self.lock:
            if key not in self.cache:
                return None
            entry = self.cache[key]
            entry.last_accessed = time.time()
            entry.access_count += 1
            return entry.value

    def put(self, key: str, value: Any, tags: Optional[List[str]] = None) -> None:
        """Put entry with tags."""
        with self.lock:
            entry = CacheEntry(key=key, value=value, tags=tags or [])
            self.cache[key] = entry
            for tag in tags or []:
                if tag not in self.tag_index:
                    self.tag_index[tag] = set()
                self.tag_index[tag].add(key)

    def invalidate_by_tag(self, tag: str) -> int:
        """Invalidate all entries with a tag."""
        with self.lock:
            if tag not in self.tag_index:
                return 0
            keys = list(self.tag_index[tag])
            for key in keys:
                self.cache.pop(key, None)
            count = len(keys)
            self.tag_index.pop(tag, None)
            return count

    def get_by_tag(self, tag: str) -> List[Any]:
        """Get all entries with a tag."""
        with self.lock:
            if tag not in self.tag_index:
                return []
            keys = list(self.tag_index[tag])
            return [self.cache[k].value for k in keys if k in self.cache]


class ApiResponseCacheEvictionAction:
    """
    Manages cache eviction policies for API responses.

    Supports multiple eviction strategies: LRU, LFU, FIFO, TTL, and tag-based
    eviction for targeted cache invalidation.

    Example:
        manager = ApiResponseCacheEvictionAction(max_size=1000)
        manager.put("user_123", response_data, tags=["user", "profile"])
        result = manager.get("user_123")
        manager.evict_by_tag("user")  # Invalidate all user entries
    """

    def __init__(
        self,
        max_size: int = 10000,
        policy: EvictionPolicy = EvictionPolicy.LRU,
        default_ttl: float = 300
    ):
        """
        Initialize cache eviction manager.

        Args:
            max_size: Maximum number of entries
            policy: Default eviction policy
            default_ttl: Default TTL in seconds
        """
        self.max_size = max_size
        self.default_policy = policy
        self.stats = CacheStats()

        # Initialize caches for each policy
        self.lru_cache = LRUCache(max_size)
        self.lfu_cache = LFUCache(max_size)
        self.ttl_cache = TTLCache(default_ttl)
        self.tag_eviction = TagBasedEviction()

        self.fifo_order: List[Tuple[float, str]] = []  # (timestamp, key)
        self.lock = threading.RLock()

    def get(
        self,
        key: str,
        policy: Optional[EvictionPolicy] = None
    ) -> Optional[Any]:
        """
        Get cached response.

        Args:
            key: Cache key
            policy: Optional policy override

        Returns:
            Cached value or None
        """
        policy = policy or self.default_policy

        if policy == EvictionPolicy.LRU:
            return self.lru_cache.get(key)
        elif policy == EvictionPolicy.LFU:
            return self.lfu_cache.get(key)
        elif policy == EvictionPolicy.TTL:
            return self.ttl_cache.get(key)
        elif policy == EvictionPolicy.TAG:
            return self.tag_eviction.get(key)
        else:
            # Default to LRU
            return self.lru_cache.get(key)

    def put(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
        tags: Optional[List[str]] = None,
        policy: Optional[EvictionPolicy] = None
    ) -> None:
        """
        Put response in cache.

        Args:
            key: Cache key
            value: Response data
            ttl: Optional TTL override
            tags: Optional tags for tag-based eviction
            policy: Optional policy override
        """
        policy = policy or self.default_policy

        if policy == EvictionPolicy.LRU:
            self.lru_cache.put(key, value, ttl, tags)
        elif policy == EvictionPolicy.LFU:
            self.lfu_cache.put(key, value, ttl, tags)
        elif policy == EvictionPolicy.TTL:
            self.ttl_cache.put(key, value, ttl, tags)
        elif policy == EvictionPolicy.TAG:
            self.tag_eviction.put(key, value, tags)
        elif policy == EvictionPolicy.FIFO:
            with self.lock:
                self.fifo_order.append((time.time(), key))
                self.ttl_cache.put(key, value, ttl, tags)
                self._evict_fifo()
        else:
            self.lru_cache.put(key, value, ttl, tags)

        self.stats.current_count += 1
        self.stats.current_size += self._estimate_size(value)

    def _evict_fifo(self) -> None:
        """Evict oldest FIFO entry."""
        while len(self.fifo_order) > self.max_size:
            _, oldest_key = self.fifo_order.pop(0)
            self.ttl_cache.cache.pop(oldest_key, None)

    def _estimate_size(self, value: Any) -> int:
        """Estimate size of value in bytes."""
        try:
            return len(json.dumps(value).encode())
        except:
            return 1024

    def invalidate(self, key: str, policy: Optional[EvictionPolicy] = None) -> bool:
        """Invalidate specific key across all policies."""
        policy = policy or self.default_policy
        result = False

        if policy in (EvictionPolicy.LRU, EvictionPolicy.FIFO):
            result = self.lru_cache.invalidate(key) or result
        if policy == EvictionPolicy.LFU:
            result = self.lfu_cache.cache.pop(key, None) is not None or result
        if policy == EvictionPolicy.TTL:
            result = self.ttl_cache.cache.pop(key, None) is not None or result

        result = self.tag_eviction.cache.pop(key, None) is not None or result
        return result

    def evict_by_tag(self, tag: str) -> int:
        """Evict all entries with a specific tag."""
        count = self.tag_eviction.invalidate_by_tag(tag)
        self.stats.evictions += count
        return count

    def cleanup_expired(self) -> int:
        """Clean up expired TTL entries."""
        count = self.ttl_cache.cleanup_expired()
        self.stats.expirations += count
        return count

    def clear(self) -> None:
        """Clear all caches."""
        self.lru_cache.clear()
        self.lfu_cache.cache.clear()
        self.ttl_cache.cache.clear()
        self.tag_eviction.cache.clear()
        self.fifo_order.clear()
        self.stats = CacheStats()

    def get_stats(self) -> CacheStats:
        """Get cache statistics."""
        return self.stats

    def get_hit_rate(self) -> float:
        """Calculate cache hit rate."""
        total = self.stats.hits + self.stats.misses
        return self.stats.hits / total if total > 0 else 0.0
