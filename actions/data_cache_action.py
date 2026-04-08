"""Data cache action module for RabAI AutoClick.

Provides data caching operations:
- CacheSetAction: Set cache entry
- CacheGetAction: Get cache entry
- CacheDeleteAction: Delete cache entry
- CacheClearAction: Clear all cache
- CacheStatsAction: Get cache statistics
"""

import time
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CacheSetAction(BaseAction):
    """Set a cache entry."""
    action_type = "cache_set"
    display_name = "缓存设置"
    description = "设置缓存条目"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", "")
            value = params.get("value", None)
            ttl = params.get("ttl", 3600)
            tags = params.get("tags", [])

            if not key:
                return ActionResult(success=False, message="key is required")

            if not hasattr(context, "data_cache"):
                context.data_cache = {}

            context.data_cache[key] = {
                "key": key,
                "value": value,
                "ttl": ttl,
                "tags": tags,
                "created_at": time.time(),
                "expires_at": time.time() + ttl,
                "hits": 0,
            }

            return ActionResult(
                success=True,
                data={"key": key, "ttl": ttl, "tags": tags},
                message=f"Cached key '{key}' with TTL={ttl}s",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cache set failed: {e}")


class CacheGetAction(BaseAction):
    """Get a cache entry."""
    action_type = "cache_get"
    display_name = "缓存获取"
    description = "获取缓存条目"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", "")
            if not key:
                return ActionResult(success=False, message="key is required")

            cache = getattr(context, "data_cache", {})
            if key not in cache:
                return ActionResult(success=True, data={"key": key, "found": False, "value": None}, message=f"Cache miss: '{key}'")

            entry = cache[key]
            if entry.get("expires_at", 0) < time.time():
                del cache[key]
                return ActionResult(success=True, data={"key": key, "found": False, "expired": True}, message=f"Cache expired: '{key}'")

            entry["hits"] += 1
            return ActionResult(
                success=True,
                data={"key": key, "found": True, "value": entry["value"], "hits": entry["hits"]},
                message=f"Cache hit: '{key}' (hits={entry['hits']})",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cache get failed: {e}")


class CacheDeleteAction(BaseAction):
    """Delete a cache entry."""
    action_type = "cache_delete"
    display_name = "缓存删除"
    description = "删除缓存条目"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", "")
            if not key:
                return ActionResult(success=False, message="key is required")

            cache = getattr(context, "data_cache", {})
            found = key in cache
            if found:
                del cache[key]

            return ActionResult(success=True, data={"key": key, "deleted": found}, message=f"Deleted key '{key}'")
        except Exception as e:
            return ActionResult(success=False, message=f"Cache delete failed: {e}")


class CacheClearAction(BaseAction):
    """Clear all cache entries."""
    action_type = "cache_clear"
    display_name = "清空缓存"
    description = "清空所有缓存"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pattern = params.get("pattern", "")
            reason = params.get("reason", "manual_clear")

            cache = getattr(context, "data_cache", {})
            if pattern:
                keys_to_delete = [k for k in cache if pattern in k]
                for k in keys_to_delete:
                    del cache[k]
                cleared = len(keys_to_delete)
            else:
                cleared = len(cache)
                cache.clear()

            return ActionResult(
                success=True,
                data={"cleared_count": cleared, "pattern": pattern or "all"},
                message=f"Cleared {cleared} cache entries",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cache clear failed: {e}")


class CacheStatsAction(BaseAction):
    """Get cache statistics."""
    action_type = "cache_stats"
    display_name = "缓存统计"
    description = "获取缓存统计"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            cache = getattr(context, "data_cache", {})
            now = time.time()

            active = sum(1 for e in cache.values() if e.get("expires_at", 0) > now)
            expired = sum(1 for e in cache.values() if e.get("expires_at", 0) <= now)
            total_hits = sum(e.get("hits", 0) for e in cache.values())

            return ActionResult(
                success=True,
                data={"total_entries": len(cache), "active": active, "expired": expired, "total_hits": total_hits},
                message=f"Cache stats: {active} active, {expired} expired, {total_hits} total hits",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cache stats failed: {e}")
