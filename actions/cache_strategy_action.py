"""Cache strategy action module for RabAI AutoClick.

Provides caching mechanisms:
- MemoryCache: In-memory cache with TTL
- LRUCache: Least Recently Used cache
- LFUCache: Least Frequently Used cache
- TTLCache: Time-To-Live cache
- WriteThroughCache: Write-through caching
- WriteBehindCache: Write-behind caching
- CacheManager: Multi-tier cache management
"""

import time
import threading
import hashlib
from typing import Any, Dict, List, Optional, Callable, Tuple
from dataclasses import dataclass, field
from collections import OrderedDict
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CacheEvictionPolicy(Enum):
    """Cache eviction policies."""
    LRU = "lru"
    LFU = "lfu"
    FIFO = "fifo"
    LIFO = "lifo"
    TTL = "ttl"


@dataclass
class CacheEntry:
    """Cache entry with metadata."""
    key: str
    value: Any
    created_at: float
    accessed_at: float
    access_count: int = 0
    ttl: Optional[float] = None

    def is_expired(self) -> bool:
        """Check if entry is expired."""
        if self.ttl is None:
            return False
        return time.time() - self.created_at > self.ttl

    def touch(self):
        """Update access time and count."""
        self.accessed_at = time.time()
        self.access_count += 1


class MemoryCache:
    """Simple in-memory cache with TTL support."""

    def __init__(self, max_size: int = 1000, default_ttl: Optional[float] = None):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = threading.RLock()

    def get(self, key: str, default: Any = None) -> Any:
        """Get value from cache."""
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return default
            if entry.is_expired():
                del self._cache[key]
                return default
            entry.touch()
            return entry.value

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> bool:
        """Set value in cache."""
        with self._lock:
            if len(self._cache) >= self.max_size and key not in self._cache:
                self._evict()
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=time.time(),
                accessed_at=time.time(),
                ttl=ttl or self.default_ttl,
            )
            self._cache[key] = entry
            return True

    def delete(self, key: str) -> bool:
        """Delete key from cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def clear(self):
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()

    def _evict(self):
        """Evict an entry when cache is full."""
        if not self._cache:
            return
        oldest = min(self._cache.items(), key=lambda x: x[1].accessed_at)
        del self._cache[oldest[0]]

    def size(self) -> int:
        """Get current cache size."""
        with self._lock:
            return len(self._cache)

    def keys(self) -> List[str]:
        """Get all cache keys."""
        with self._lock:
            return list(self._cache.keys())


class LRUCache:
    """Least Recently Used cache."""

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self._cache: OrderedDict = OrderedDict()
        self._lock = threading.RLock()

    def get(self, key: str, default: Any = None) -> Any:
        """Get value, updating recency."""
        with self._lock:
            if key not in self._cache:
                return default
            self._cache.move_to_end(key)
            return self._cache[key]

    def set(self, key: str, value: Any) -> bool:
        """Set value in cache."""
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            self._cache[key] = value
            if len(self._cache) > self.max_size:
                self._cache.popitem(last=False)
            return True

    def delete(self, key: str) -> bool:
        """Delete key from cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def clear(self):
        """Clear cache."""
        with self._lock:
            self._cache.clear()


class LFUCache:
    """Least Frequently Used cache."""

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self._cache: Dict[str, Tuple[Any, int]] = {}
        self._lock = threading.RLock()

    def get(self, key: str, default: Any = None) -> Any:
        """Get value and increment frequency."""
        with self._lock:
            if key not in self._cache:
                return default
            value, count = self._cache[key]
            self._cache[key] = (value, count + 1)
            return value

    def set(self, key: str, value: Any) -> bool:
        """Set value in cache."""
        with self._lock:
            if key in self._cache:
                _, count = self._cache[key]
                self._cache[key] = (value, count)
            else:
                if len(self._cache) >= self.max_size:
                    self._evict()
                self._cache[key] = (value, 1)
            return True

    def _evict(self):
        """Evict least frequently used item."""
        if not self._cache:
            return
        lfu_key = min(self._cache.items(), key=lambda x: x[1][1])[0]
        del self._cache[lfu_key]

    def delete(self, key: str) -> bool:
        """Delete key from cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def clear(self):
        """Clear cache."""
        with self._lock:
            self._cache.clear()


class TTLCache:
    """Time-To-Live cache with automatic expiration."""

    def __init__(self, default_ttl: float = 300.0):
        self.default_ttl = default_ttl
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._lock = threading.RLock()

    def get(self, key: str, default: Any = None) -> Any:
        """Get value if not expired."""
        with self._lock:
            if key not in self._cache:
                return default
            value, expiry = self._cache[key]
            if time.time() > expiry:
                del self._cache[key]
                return default
            return value

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> bool:
        """Set value with TTL."""
        with self._lock:
            ttl_value = ttl if ttl is not None else self.default_ttl
            expiry = time.time() + ttl_value
            self._cache[key] = (value, expiry)
            return True

    def delete(self, key: str) -> bool:
        """Delete key from cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def clear(self):
        """Clear expired entries."""
        with self._lock:
            now = time.time()
            expired = [k for k, (_, exp) in self._cache.items() if now > exp]
            for k in expired:
                del self._cache[k]


class WriteBehindCache:
    """Write-behind cache that batches writes."""

    def __init__(self, backend: Callable, flush_interval: float = 5.0, batch_size: int = 100):
        self.backend = backend
        self.flush_interval = flush_interval
        self.batch_size = batch_size
        self._write_buffer: Dict[str, Any] = {}
        self._lock = threading.RLock()
        self._last_flush = time.time()

    def get(self, key: str, default: Any = None) -> Any:
        """Get value from cache or backend."""
        with self._lock:
            if key in self._write_buffer:
                return self._write_buffer[key]
        try:
            value = self.backend(key)
            self.set(key, value)
            return value
        except Exception:
            return default

    def set(self, key: str, value: Any):
        """Set value in write buffer."""
        with self._lock:
            self._write_buffer[key] = value
            self._maybe_flush()

    def _maybe_flush(self):
        """Flush if buffer is full or interval elapsed."""
        now = time.time()
        should_flush = (
            len(self._write_buffer) >= self.batch_size or
            now - self._last_flush >= self.flush_interval
        )
        if should_flush:
            self.flush()

    def flush(self):
        """Flush write buffer to backend."""
        with self._lock:
            for key, value in self._write_buffer.items():
                try:
                    self.backend(key, value)
                except Exception:
                    pass
            self._write_buffer.clear()
            self._last_flush = time.time()


class CacheManager:
    """Multi-tier cache manager."""

    def __init__(self):
        self.tiers: List[Tuple[str, Any]] = []
        self._lock = threading.RLock()

    def add_tier(self, name: str, cache: Any):
        """Add a cache tier."""
        with self._lock:
            self.tiers.append((name, cache))

    def get(self, key: str, default: Any = None) -> Any:
        """Get from first tier, fall through to lower tiers."""
        with self._lock:
            for name, cache in self.tiers:
                try:
                    value = cache.get(key)
                    if value is not None:
                        return value
                except Exception:
                    pass
            return default

    def set(self, key: str, value: Any):
        """Set in all tiers."""
        with self._lock:
            for name, cache in self.tiers:
                try:
                    cache.set(key, value)
                except Exception:
                    pass

    def delete(self, key: str):
        """Delete from all tiers."""
        with self._lock:
            for name, cache in self.tiers:
                try:
                    cache.delete(key)
                except Exception:
                    pass

    def clear(self):
        """Clear all tiers."""
        with self._lock:
            for name, cache in self.tiers:
                try:
                    cache.clear()
                except Exception:
                    pass


class CacheStrategyAction(BaseAction):
    """Cache strategy action for automation."""
    action_type = "cache_strategy"
    display_name = "缓存策略"
    description = "多种缓存策略管理"

    def __init__(self):
        super().__init__()
        self._caches: Dict[str, Any] = {}
        self._managers: Dict[str, CacheManager] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "get")
            cache_name = params.get("name", "default")
            key = params.get("key", "")
            value = params.get("value")
            strategy = params.get("strategy", "lru")

            if operation == "create":
                return self._create_cache(cache_name, strategy, params)
            elif operation == "get":
                return self._get_from_cache(cache_name, key)
            elif operation == "set":
                return self._set_in_cache(cache_name, key, value)
            elif operation == "delete":
                return self._delete_from_cache(cache_name, key)
            elif operation == "clear":
                return self._clear_cache(cache_name)
            elif operation == "stats":
                return self._get_cache_stats(cache_name)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Cache error: {str(e)}")

    def _create_cache(self, name: str, strategy: str, params: Dict) -> ActionResult:
        """Create a new cache."""
        max_size = params.get("max_size", 1000)
        ttl = params.get("ttl")

        if strategy == "lru":
            cache = LRUCache(max_size=max_size)
        elif strategy == "lfu":
            cache = LFUCache(max_size=max_size)
        elif strategy == "memory":
            cache = MemoryCache(max_size=max_size, default_ttl=ttl)
        elif strategy == "ttl":
            cache = TTLCache(default_ttl=ttl or 300.0)
        else:
            return ActionResult(success=False, message=f"Unknown strategy: {strategy}")

        self._caches[name] = cache
        return ActionResult(success=True, message=f"Cache '{name}' created with {strategy} strategy")

    def _get_from_cache(self, name: str, key: str) -> ActionResult:
        """Get value from cache."""
        cache = self._caches.get(name)
        if cache is None:
            return ActionResult(success=False, message=f"Cache '{name}' not found")

        value = cache.get(key)
        return ActionResult(
            success=True,
            message="Cache hit" if value is not None else "Cache miss",
            data={"key": key, "value": value, "hit": value is not None},
        )

    def _set_in_cache(self, name: str, key: str, value: Any) -> ActionResult:
        """Set value in cache."""
        cache = self._caches.get(name)
        if cache is None:
            return ActionResult(success=False, message=f"Cache '{name}' not found")

        cache.set(key, value)
        return ActionResult(success=True, message=f"Value set for key '{key}'")

    def _delete_from_cache(self, name: str, key: str) -> ActionResult:
        """Delete from cache."""
        cache = self._caches.get(name)
        if cache is None:
            return ActionResult(success=False, message=f"Cache '{name}' not found")

        deleted = cache.delete(key)
        return ActionResult(success=deleted, message=f"Key '{key}' deleted" if deleted else f"Key '{key}' not found")

    def _clear_cache(self, name: str) -> ActionResult:
        """Clear cache."""
        cache = self._caches.get(name)
        if cache is None:
            return ActionResult(success=False, message=f"Cache '{name}' not found")

        cache.clear()
        return ActionResult(success=True, message=f"Cache '{name}' cleared")

    def _get_cache_stats(self, name: str) -> ActionResult:
        """Get cache statistics."""
        cache = self._caches.get(name)
        if cache is None:
            return ActionResult(success=False, message=f"Cache '{name}' not found")

        size = cache.size() if hasattr(cache, "size") else 0
        return ActionResult(
            success=True,
            message="Cache stats",
            data={"name": name, "size": size, "strategy": type(cache).__name__},
        )
