"""Cache manager action module for RabAI AutoClick.

Provides caching operations:
- CacheGetAction: Get cached value
- CacheSetAction: Set cached value
- CacheDeleteAction: Delete cached value
- CacheStatsAction: Get cache statistics
"""

import threading
import time
import uuid
import hashlib
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class CacheEntry:
    """Represents a cache entry."""
    key: str
    value: Any
    created_at: datetime
    accessed_at: datetime
    ttl_seconds: Optional[float] = None
    access_count: int = 0
    hit_count: int = 0


class CacheManager:
    """Thread-safe in-memory cache manager."""
    def __init__(self, max_size: int = 1000):
        self._cache: Dict[str, CacheEntry] = {}
        self._max_size = max_size
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key in self._cache:
                entry = self._cache[key]
                if entry.ttl_seconds:
                    age = (datetime.utcnow() - entry.created_at).total_seconds()
                    if age > entry.ttl_seconds:
                        del self._cache[key]
                        self._misses += 1
                        return None
                entry.accessed_at = datetime.utcnow()
                entry.access_count += 1
                entry.hit_count += 1
                self._hits += 1
                return entry.value
            self._misses += 1
            return None

    def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> None:
        with self._lock:
            if len(self._cache) >= self._max_size and key not in self._cache:
                self._evict_lru()
            self._cache[key] = CacheEntry(
                key=key,
                value=value,
                created_at=datetime.utcnow(),
                accessed_at=datetime.utcnow(),
                ttl_seconds=ttl_seconds
            )

    def delete(self, key: str) -> bool:
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self._hits = 0
            self._misses = 0

    def _evict_lru(self) -> None:
        if not self._cache:
            return
        lru_key = min(self._cache.keys(), key=lambda k: self._cache[k].accessed_at)
        del self._cache[lru_key]

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            total = self._hits + self._misses
            hit_rate = self._hits / total if total > 0 else 0.0
            return {
                "size": len(self._cache),
                "max_size": self._max_size,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": hit_rate,
                "total_requests": total
            }

    def get_keys(self, pattern: Optional[str] = None) -> List[str]:
        with self._lock:
            keys = list(self._cache.keys())
            if pattern:
                import fnmatch
                keys = fnmatch.filter(keys, pattern)
            return keys


_cache = CacheManager()


class CacheGetAction(BaseAction):
    """Get a value from cache."""
    action_type = "cache_get"
    display_name = "缓存获取"
    description = "从缓存获取值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", "")
            default = params.get("default", None)

            if not key:
                return ActionResult(success=False, message="key is required")

            value = _cache.get(key)
            if value is not None:
                return ActionResult(
                    success=True,
                    message=f"Cache hit for '{key}'",
                    data={"key": key, "value": value, "hit": True}
                )
            else:
                return ActionResult(
                    success=True,
                    message=f"Cache miss for '{key}'",
                    data={"key": key, "value": default, "hit": False}
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Cache get failed: {str(e)}")


class CacheSetAction(BaseAction):
    """Set a value in cache."""
    action_type = "cache_set"
    display_name = "缓存设置"
    description = "设置缓存值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", "")
            value = params.get("value", None)
            ttl_seconds = params.get("ttl_seconds", None)
            if_exists = params.get("if_exists", False)

            if not key:
                return ActionResult(success=False, message="key is required")

            if if_exists and _cache.get(key) is None:
                return ActionResult(success=False, message=f"Key '{key}' does not exist")

            _cache.set(key, value, ttl_seconds=ttl_seconds)

            return ActionResult(
                success=True,
                message=f"Cache set for '{key}'" + (f" (TTL: {ttl_seconds}s)" if ttl_seconds else ""),
                data={"key": key, "ttl_seconds": ttl_seconds}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Cache set failed: {str(e)}")


class CacheDeleteAction(BaseAction):
    """Delete a value from cache."""
    action_type = "cache_delete"
    display_name = "缓存删除"
    description = "删除缓存值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", "")
            pattern = params.get("pattern", None)

            if not key and not pattern:
                return ActionResult(success=False, message="key or pattern is required")

            deleted = 0
            if key:
                if _cache.delete(key):
                    deleted = 1
            if pattern:
                keys = _cache.get_keys(pattern=pattern)
                for k in keys:
                    if _cache.delete(k):
                        deleted += 1

            return ActionResult(
                success=True,
                message=f"Deleted {deleted} cache entries",
                data={"deleted": deleted}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Cache delete failed: {str(e)}")


class CacheStatsAction(BaseAction):
    """Get cache statistics."""
    action_type = "cache_stats"
    display_name = "缓存统计"
    description = "获取缓存统计"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stats = _cache.get_stats()
            keys = _cache.get_keys()
            return ActionResult(
                success=True,
                message=f"Cache stats: {stats['size']} entries, {stats['hit_rate']:.2%} hit rate",
                data={**stats, "keys": keys[:100]}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Cache stats failed: {str(e)}")
