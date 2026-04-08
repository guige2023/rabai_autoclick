"""Data caching action module for RabAI AutoClick.

Provides data caching operations:
- MemoryCacheAction: In-memory cache operations
- LruCacheAction: LRU cache implementation
- CacheTTLAction: Cache with TTL support
- CacheStatsAction: Cache statistics
"""

import time
from typing import Any, Dict, List, Optional, Tuple
from threading import Lock

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CacheStore:
    """Shared cache storage."""

    def __init__(self):
        self._lock = Lock()
        self._store: Dict[str, Dict] = {}
        self._lru_order: List[str] = []

    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        with self._lock:
            self._store[key] = {
                "value": value,
                "created_at": time.time(),
                "ttl": ttl,
                "access_count": 0,
            }
            if key in self._lru_order:
                self._lru_order.remove(key)
            self._lru_order.append(key)

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key not in self._store:
                return None
            entry = self._store[key]
            if entry["ttl"]:
                if time.time() - entry["created_at"] > entry["ttl"]:
                    del self._store[key]
                    if key in self._lru_order:
                        self._lru_order.remove(key)
                    return None
            entry["access_count"] += 1
            if key in self._lru_order:
                self._lru_order.remove(key)
            self._lru_order.append(key)
            return entry["value"]

    def delete(self, key: str) -> bool:
        with self._lock:
            if key in self._store:
                del self._store[key]
                if key in self._lru_order:
                    self._lru_order.remove(key)
                return True
            return False

    def clear(self):
        with self._lock:
            self._store.clear()
            self._lru_order.clear()

    def stats(self) -> Dict:
        with self._lock:
            total_entries = len(self._store)
            total_accesses = sum(e["access_count"] for e in self._store.values())
            expired = sum(1 for e in self._store.values() if e["ttl"] and time.time() - e["created_at"] > e["ttl"])
            return {
                "total_entries": total_entries,
                "total_accesses": total_accesses,
                "expired_entries": expired,
            }


_cache = CacheStore()


class MemoryCacheAction(BaseAction):
    """In-memory cache operations."""
    action_type = "memory_cache"
    display_name = "内存缓存"
    description = "内存缓存操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "get")
            key = params.get("key", "")
            value = params.get("value", None)
            ttl = params.get("ttl", None)

            if action == "set":
                if not key:
                    return ActionResult(success=False, message="key is required")
                _cache.set(key, value, ttl)
                return ActionResult(success=True, message=f"Set cache key '{key}'", data={"key": key, "ttl": ttl})

            elif action == "get":
                if not key:
                    return ActionResult(success=False, message="key is required")
                cached_value = _cache.get(key)
                found = cached_value is not None
                return ActionResult(
                    success=True,
                    message=f"{'Found' if found else 'Not found'} cache key '{key}'",
                    data={"key": key, "value": cached_value, "found": found},
                )

            elif action == "delete":
                if not key:
                    return ActionResult(success=False, message="key is required")
                deleted = _cache.delete(key)
                return ActionResult(success=True, message=f"Deleted cache key '{key}': {deleted}", data={"deleted": deleted})

            elif action == "clear":
                _cache.clear()
                return ActionResult(success=True, message="Cache cleared")

            elif action == "stats":
                stats = _cache.stats()
                return ActionResult(success=True, message="Cache stats", data=stats)

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"MemoryCache error: {e}")


class LruCacheAction(BaseAction):
    """LRU cache implementation."""
    action_type = "lru_cache"
    display_name = "LRU缓存"
    description = "LRU缓存实现"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "get")
            key = params.get("key", "")
            value = params.get("value", None)
            max_size = params.get("max_size", 100)

            if action == "set":
                if not key:
                    return ActionResult(success=False, message="key is required")
                _cache.set(key, value)
                with _cache._lock:
                    while len(_cache._lru_order) > max_size:
                        oldest = _cache._lru_order[0]
                        del _cache._store[oldest]
                        _cache._lru_order.pop(0)
                return ActionResult(success=True, message=f"LRU set key '{key}' (max {max_size})", data={"key": key, "max_size": max_size})

            elif action == "get":
                if not key:
                    return ActionResult(success=False, message="key is required")
                cached_value = _cache.get(key)
                return ActionResult(
                    success=True,
                    message=f"LRU get key '{key}': {'hit' if cached_value is not None else 'miss'}",
                    data={"key": key, "value": cached_value, "hit": cached_value is not None},
                )

            elif action == "evict_lru":
                with _cache._lock:
                    if _cache._lru_order:
                        oldest = _cache._lru_order[0]
                        del _cache._store[oldest]
                        _cache._lru_order.pop(0)
                        return ActionResult(success=True, message=f"Evicted LRU key '{oldest}'", data={"evicted_key": oldest})
                return ActionResult(success=True, message="No keys to evict")

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"LruCache error: {e}")


class CacheTTLAction(BaseAction):
    """Cache with TTL support."""
    action_type = "cache_ttl"
    display_name: "TTL缓存"
    description: "带TTL的缓存"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "set")
            key = params.get("key", "")
            value = params.get("value", None)
            ttl = params.get("ttl", 300)
            refresh = params.get("refresh", False)

            if action == "set":
                if not key:
                    return ActionResult(success=False, message="key is required")
                _cache.set(key, value, ttl)
                return ActionResult(success=True, message=f"Set TTL cache key '{key}' (ttl={ttl}s)", data={"key": key, "ttl": ttl})

            elif action == "get":
                if not key:
                    return ActionResult(success=False, message="key is required")
                cached_value = _cache.get(key)
                if cached_value is None:
                    return ActionResult(success=True, message=f"TTL cache miss for '{key}'", data={"key": key, "found": False})
                with _cache._lock:
                    entry = _cache._store.get(key, {})
                    created = entry.get("created_at", 0)
                    ttl_val = entry.get("ttl", 0)
                    remaining = max(0, ttl_val - (time.time() - created))
                return ActionResult(
                    success=True,
                    message=f"TTL cache hit for '{key}' ({remaining:.0f}s remaining)",
                    data={"key": key, "value": cached_value, "found": True, "ttl_remaining": round(remaining, 2)},
                )

            elif action == "touch":
                if not key:
                    return ActionResult(success=False, message="key is required")
                with _cache._lock:
                    if key in _cache._store:
                        _cache._store[key]["created_at"] = time.time()
                        return ActionResult(success=True, message=f"TTL refreshed for '{key}'", data={"key": key})
                return ActionResult(success=False, message=f"Key '{key}' not found")

            elif action == "expire":
                if not key:
                    return ActionResult(success=False, message="key is required")
                _cache.delete(key)
                return ActionResult(success=True, message=f"Expired key '{key}'")

            elif action == "cleanup":
                with _cache._lock:
                    expired_keys = []
                    for k, entry in list(_cache._store.items()):
                        if entry["ttl"] and time.time() - entry["created_at"] > entry["ttl"]:
                            expired_keys.append(k)
                            del _cache._store[k]
                            if k in _cache._lru_order:
                                _cache._lru_order.remove(k)
                return ActionResult(success=True, message=f"Cleaned up {len(expired_keys)} expired entries", data={"cleaned": len(expired_keys)})

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"CacheTTL error: {e}")


class CacheStatsAction(BaseAction):
    """Cache statistics."""
    action_type = "cache_stats"
    display_name: "缓存统计"
    description: "缓存统计信息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "stats")
            key = params.get("key", None)

            if action == "stats":
                stats = _cache.stats()
                with _cache._lock:
                    total_entries = len(_cache._store)
                    entries_list = []
                    for k, entry in _cache._store.items():
                        age = time.time() - entry["created_at"]
                        ttl = entry.get("ttl")
                        entries_list.append({
                            "key": k,
                            "age_seconds": round(age, 2),
                            "ttl": ttl,
                            "ttl_remaining": max(0, ttl - age) if ttl else None,
                            "access_count": entry["access_count"],
                        })
                    entries_list.sort(key=lambda x: x["access_count"], reverse=True)
                    top_accessed = entries_list[:10]
                return ActionResult(
                    success=True,
                    message="Cache statistics",
                    data={
                        "stats": stats,
                        "total_entries": total_entries,
                        "top_accessed": top_accessed,
                    },
                )

            elif action == "key_stats":
                if not key:
                    return ActionResult(success=False, message="key is required")
                with _cache._lock:
                    if key not in _cache._store:
                        return ActionResult(success=False, message=f"Key '{key}' not found")
                    entry = _cache._store[key]
                    age = time.time() - entry["created_at"]
                    ttl = entry.get("ttl")
                    return ActionResult(
                        success=True,
                        message=f"Key stats for '{key}'",
                        data={
                            "key": key,
                            "value": entry["value"],
                            "age_seconds": round(age, 2),
                            "ttl": ttl,
                            "ttl_remaining": max(0, ttl - age) if ttl else None,
                            "access_count": entry["access_count"],
                        },
                    )

            elif action == "hit_rate":
                with _cache._lock:
                    stats = _cache.stats()
                    total = stats["total_accesses"]
                    return ActionResult(
                        success=True,
                        message=f"Cache hit rate stats",
                        data={
                            "total_accesses": total,
                            "entries": stats["total_entries"],
                        },
                    )

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"CacheStats error: {e}")
