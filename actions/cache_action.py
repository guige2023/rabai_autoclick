"""
Cache utilities - TTL cache, LRU, memoization, cache invalidation, distributed cache simulation.
"""
from typing import Any, Dict, Hashable, Optional, Callable
import time
import hashlib
import logging
import threading

logger = logging.getLogger(__name__)


class BaseAction:
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


class TTLCache:
    """Simple in-memory TTL cache."""

    def __init__(self, default_ttl: int = 300) -> None:
        self._cache: Dict[str, tuple] = {}
        self._default_ttl = default_ttl
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key in self._cache:
                value, expiry = self._cache[key]
                if expiry > time.time():
                    return value
                del self._cache[key]
            return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        with self._lock:
            expiry = time.time() + (ttl or self._default_ttl)
            self._cache[key] = (value, expiry)

    def delete(self, key: str) -> bool:
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def clear(self) -> int:
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            return count

    def keys(self) -> list:
        with self._lock:
            now = time.time()
            valid_keys = [k for k, (_, exp) in self._cache.items() if exp > now]
            return valid_keys


class LRUCache:
    """Simple LRU cache with max size."""

    def __init__(self, max_size: int = 100) -> None:
        self._cache: Dict[str, Any] = {}
        self._order: list = []
        self._max_size = max_size
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key in self._cache:
                self._order.remove(key)
                self._order.append(key)
                return self._cache[key]
            return None

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            if key in self._cache:
                self._order.remove(key)
            elif len(self._cache) >= self._max_size:
                oldest = self._order.pop(0)
                del self._cache[oldest]
            self._cache[key] = value
            self._order.append(key)

    def delete(self, key: str) -> bool:
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                self._order.remove(key)
                return True
            return False

    def clear(self) -> int:
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            self._order.clear()
            return count


class CacheAction(BaseAction):
    """Cache operations.

    Provides TTL cache, LRU cache, memoization, cache stats.
    """

    def __init__(self) -> None:
        self._ttl_cache = TTLCache()
        self._lru_cache = LRUCache()

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "ttl_get")
        key = params.get("key", "")

        try:
            if operation == "ttl_get":
                if not key:
                    return {"success": False, "error": "key required"}
                value = self._ttl_cache.get(key)
                return {"success": True, "key": key, "found": value is not None, "value": value}

            elif operation == "ttl_set":
                if not key:
                    return {"success": False, "error": "key required"}
                value = params.get("value")
                ttl = int(params.get("ttl", 300))
                self._ttl_cache.set(key, value, ttl)
                return {"success": True, "key": key, "ttl": ttl}

            elif operation == "ttl_delete":
                deleted = self._ttl_cache.delete(key)
                return {"success": True, "key": key, "deleted": deleted}

            elif operation == "ttl_clear":
                count = self._ttl_cache.clear()
                return {"success": True, "cleared": count}

            elif operation == "ttl_keys":
                keys = self._ttl_cache.keys()
                return {"success": True, "keys": keys, "count": len(keys)}

            elif operation == "lru_get":
                if not key:
                    return {"success": False, "error": "key required"}
                value = self._lru_cache.get(key)
                return {"success": True, "key": key, "found": value is not None, "value": value}

            elif operation == "lru_set":
                if not key:
                    return {"success": False, "error": "key required"}
                value = params.get("value")
                self._lru_cache.set(key, value)
                return {"success": True, "key": key}

            elif operation == "lru_delete":
                deleted = self._lru_cache.delete(key)
                return {"success": True, "key": key, "deleted": deleted}

            elif operation == "lru_clear":
                count = self._lru_cache.clear()
                return {"success": True, "cleared": count}

            elif operation == "memoize":
                if not key:
                    return {"success": False, "error": "key required"}
                fn_name = params.get("fn", "unknown")
                value = params.get("value")
                self._lru_cache.set(f"memo:{key}", value)
                return {"success": True, "key": key, "fn": fn_name, "cached": True}

            elif operation == "stats":
                ttl_keys = self._ttl_cache.keys()
                return {"success": True, "ttl_cache_size": len(ttl_keys), "lru_cache_size": 0, "ttl_keys": ttl_keys}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"CacheAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    return CacheAction().execute(context, params)
