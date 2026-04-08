"""Cache action module for RabAI AutoClick.

Provides caching utilities:
- LRUCache: LRU cache implementation
- TTLCache: TTL-based cache
- CacheManager: Manage multiple caches
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
import threading
import time
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class LRUCache:
    """LRU (Least Recently Used) cache."""

    def __init__(self, max_size: int = 100):
        self.max_size = max_size
        self._cache: Dict[str, Any] = {}
        self._access_order: List[str] = []
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        with self._lock:
            if key in self._cache:
                self._hits += 1
                self._access_order.remove(key)
                self._access_order.append(key)
                return self._cache[key]
            self._misses += 1
            return None

    def put(self, key: str, value: Any) -> None:
        """Put value in cache."""
        with self._lock:
            if key in self._cache:
                self._access_order.remove(key)
            elif len(self._cache) >= self.max_size:
                oldest = self._access_order.pop(0)
                del self._cache[oldest]

            self._cache[key] = value
            self._access_order.append(key)

    def has(self, key: str) -> bool:
        """Check if key exists."""
        with self._lock:
            return key in self._cache

    def delete(self, key: str) -> bool:
        """Delete a key."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                self._access_order.remove(key)
                return True
            return False

    def clear(self) -> None:
        """Clear cache."""
        with self._lock:
            self._cache.clear()
            self._access_order.clear()

    def size(self) -> int:
        """Get cache size."""
        with self._lock:
            return len(self._cache)

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            total = self._hits + self._misses
            hit_rate = self._hits / total if total > 0 else 0.0
            return {
                "size": len(self._cache),
                "max_size": self.max_size,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": hit_rate,
            }


class TTLCache:
    """TTL (Time To Live) cache."""

    def __init__(self, ttl: float = 300.0, max_size: int = 100):
        self.ttl = ttl
        self.max_size = max_size
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        with self._lock:
            if key not in self._cache:
                self._misses += 1
                return None

            value, expires_at = self._cache[key]

            if time.time() > expires_at:
                del self._cache[key]
                self._misses += 1
                return None

            self._hits += 1
            return value

    def put(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """Put value in cache."""
        with self._lock:
            self._cleanup_expired()

            if len(self._cache) >= self.max_size:
                first_key = next(iter(self._cache))
                del self._cache[first_key]

            ttl = ttl or self.ttl
            expires_at = time.time() + ttl
            self._cache[key] = (value, expires_at)

    def has(self, key: str) -> bool:
        """Check if key exists and is valid."""
        with self._lock:
            if key not in self._cache:
                return False
            _, expires_at = self._cache[key]
            if time.time() > expires_at:
                del self._cache[key]
                return False
            return True

    def delete(self, key: str) -> bool:
        """Delete a key."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def clear(self) -> None:
        """Clear cache."""
        with self._lock:
            self._cache.clear()

    def size(self) -> int:
        """Get cache size."""
        with self._lock:
            self._cleanup_expired()
            return len(self._cache)

    def _cleanup_expired(self) -> None:
        """Remove expired entries."""
        now = time.time()
        expired = [k for k, (_, exp) in self._cache.items() if now > exp]
        for k in expired:
            del self._cache[k]

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            self._cleanup_expired()
            total = self._hits + self._misses
            hit_rate = self._hits / total if total > 0 else 0.0
            return {
                "size": len(self._cache),
                "max_size": self.max_size,
                "ttl": self.ttl,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": hit_rate,
            }


class CacheManager:
    """Manage multiple caches."""

    def __init__(self):
        self._caches: Dict[str, Any] = {}
        self._lock = threading.RLock()

    def create_lru(self, name: str, max_size: int = 100) -> LRUCache:
        """Create LRU cache."""
        with self._lock:
            cache = LRUCache(max_size)
            self._caches[name] = cache
            return cache

    def create_ttl(self, name: str, ttl: float = 300.0, max_size: int = 100) -> TTLCache:
        """Create TTL cache."""
        with self._lock:
            cache = TTLCache(ttl, max_size)
            self._caches[name] = cache
            return cache

    def get_cache(self, name: str) -> Optional[Any]:
        """Get a cache."""
        with self._lock:
            return self._caches.get(name)

    def delete_cache(self, name: str) -> bool:
        """Delete a cache."""
        with self._lock:
            if name in self._caches:
                del self._caches[name]
                return True
            return False

    def list_caches(self) -> List[str]:
        """List all caches."""
        with self._lock:
            return list(self._caches.keys())


class CacheAction(BaseAction):
    """Cache action."""
    action_type = "cache"
    display_name = "缓存管理"
    description = "LRU和TTL缓存"

    def __init__(self):
        super().__init__()
        self._manager = CacheManager()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "get")

            if operation == "create_lru":
                return self._create_lru(params)
            elif operation == "create_ttl":
                return self._create_ttl(params)
            elif operation == "get":
                return self._get(params)
            elif operation == "put":
                return self._put(params)
            elif operation == "delete":
                return self._delete(params)
            elif operation == "clear":
                return self._clear(params)
            elif operation == "stats":
                return self._stats(params)
            elif operation == "list":
                return self._list_caches()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Cache error: {str(e)}")

    def _create_lru(self, params: Dict[str, Any]) -> ActionResult:
        """Create LRU cache."""
        name = params.get("name", "default")
        max_size = params.get("max_size", 100)

        cache = self._manager.create_lru(name, max_size)

        return ActionResult(success=True, message=f"LRU cache created: {name}", data={"name": name})

    def _create_ttl(self, params: Dict[str, Any]) -> ActionResult:
        """Create TTL cache."""
        name = params.get("name", "default")
        ttl = params.get("ttl", 300.0)
        max_size = params.get("max_size", 100)

        cache = self._manager.create_ttl(name, ttl, max_size)

        return ActionResult(success=True, message=f"TTL cache created: {name}", data={"name": name})

    def _get(self, params: Dict[str, Any]) -> ActionResult:
        """Get from cache."""
        cache_name = params.get("cache_name", "default")
        key = params.get("key")

        if not key:
            return ActionResult(success=False, message="key is required")

        cache = self._manager.get_cache(cache_name)
        if not cache:
            return ActionResult(success=False, message=f"Cache not found: {cache_name}")

        value = cache.get(key)

        return ActionResult(success=value is not None, message="Found" if value is not None else "Not found", data={"key": key, "value": value})

    def _put(self, params: Dict[str, Any]) -> ActionResult:
        """Put into cache."""
        cache_name = params.get("cache_name", "default")
        key = params.get("key")
        value = params.get("value")
        ttl = params.get("ttl")

        if not key:
            return ActionResult(success=False, message="key is required")

        cache = self._manager.get_cache(cache_name)
        if not cache:
            return ActionResult(success=False, message=f"Cache not found: {cache_name}")

        cache.put(key, value, ttl)

        return ActionResult(success=True, message=f"Cached: {key}")

    def _delete(self, params: Dict[str, Any]) -> ActionResult:
        """Delete from cache."""
        cache_name = params.get("cache_name", "default")
        key = params.get("key")

        if not key:
            return ActionResult(success=False, message="key is required")

        cache = self._manager.get_cache(cache_name)
        if not cache:
            return ActionResult(success=False, message=f"Cache not found: {cache_name}")

        success = cache.delete(key)

        return ActionResult(success=success, message="Deleted" if success else "Key not found")

    def _clear(self, params: Dict[str, Any]) -> ActionResult:
        """Clear cache."""
        cache_name = params.get("cache_name", "default")

        cache = self._manager.get_cache(cache_name)
        if not cache:
            return ActionResult(success=False, message=f"Cache not found: {cache_name}")

        cache.clear()

        return ActionResult(success=True, message=f"Cache cleared: {cache_name}")

    def _stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get cache stats."""
        cache_name = params.get("cache_name", "default")

        cache = self._manager.get_cache(cache_name)
        if not cache:
            return ActionResult(success=False, message=f"Cache not found: {cache_name}")

        stats = cache.get_stats()

        return ActionResult(success=True, message="Stats retrieved", data={"name": cache_name, "stats": stats})

    def _list_caches(self) -> ActionResult:
        """List all caches."""
        caches = self._manager.list_caches()

        return ActionResult(success=True, message=f"{len(caches)} caches", data={"caches": caches})
