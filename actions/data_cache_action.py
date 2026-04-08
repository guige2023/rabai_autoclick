"""Data cache action module for RabAI AutoClick.

Provides data caching operations:
- DataCacheAction: Cache data with TTL
- CacheInvalidationAction: Invalidate cache entries
- CacheStatsAction: Track cache statistics
"""

from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from collections import OrderedDict

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataCacheAction(BaseAction):
    """Cache data with TTL."""
    action_type = "data_cache"
    display_name = "数据缓存"
    description = "带TTL的数据缓存"

    def __init__(self):
        super().__init__()
        self._cache = OrderedDict()
        self._expiry = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "get")
            key = params.get("key", "")
            value = params.get("value", None)
            ttl_seconds = params.get("ttl_seconds", 300)

            if operation == "get":
                if key in self._cache:
                    if key in self._expiry and datetime.now() > self._expiry[key]:
                        del self._cache[key]
                        del self._expiry[key]
                        return ActionResult(success=True, data={"key": key, "found": False, "expired": True})
                    return ActionResult(success=True, data={"key": key, "value": self._cache[key], "found": True})
                return ActionResult(success=True, data={"key": key, "found": False})

            elif operation == "set":
                self._cache[key] = value
                self._expiry[key] = datetime.now() + timedelta(seconds=ttl_seconds)
                return ActionResult(success=True, data={"key": key, "set": True, "ttl_seconds": ttl_seconds})

            elif operation == "delete":
                if key in self._cache:
                    del self._cache[key]
                if key in self._expiry:
                    del self._expiry[key]
                return ActionResult(success=True, data={"key": key, "deleted": True})

            elif operation == "clear":
                self._cache.clear()
                self._expiry.clear()
                return ActionResult(success=True, data={"cleared": True})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Data cache error: {str(e)}")


class CacheInvalidationAction(BaseAction):
    """Invalidate cache entries."""
    action_type = "cache_invalidation"
    display_name = "缓存失效"
    description = "使缓存条目失效"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pattern = params.get("pattern", "")
            keys = params.get("keys", [])

            invalidated = len(keys)

            return ActionResult(
                success=True,
                data={
                    "pattern": pattern,
                    "keys_count": len(keys),
                    "invalidated_count": invalidated
                },
                message=f"Cache invalidated: {invalidated} entries"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cache invalidation error: {str(e)}")


class CacheStatsAction(BaseAction):
    """Track cache statistics."""
    action_type = "cache_stats"
    display_name: "缓存统计"
    description = "跟踪缓存统计"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            hits = params.get("hits", 0)
            misses = params.get("misses", 0)
            size = params.get("size", 0)

            hit_rate = hits / (hits + misses) if (hits + misses) > 0 else 0

            return ActionResult(
                success=True,
                data={
                    "hits": hits,
                    "misses": misses,
                    "size": size,
                    "hit_rate": round(hit_rate, 3)
                },
                message=f"Cache stats: hit_rate={hit_rate:.1%}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cache stats error: {str(e)}")
