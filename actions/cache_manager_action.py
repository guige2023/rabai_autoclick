"""Cache management action module for RabAI AutoClick.

Provides caching operations:
- CacheSetAction: Set cache value
- CacheGetAction: Get cache value
- CacheDeleteAction: Delete cache entry
- CacheClearAction: Clear all cache
- CacheExistsAction: Check if key exists
- CacheTTLAction: Get/set TTL
- CacheStatsAction: Cache statistics
- CacheWarmAction: Warm cache with data
"""

import json
import os
import pickle
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CacheManager:
    """In-memory cache storage."""
    
    _cache: Dict[str, Dict[str, Any]] = {}
    
    @classmethod
    def set(cls, key: str, value: Any, ttl: int = 3600) -> None:
        """Set a cache value."""
        cls._cache[key] = {
            "value": value,
            "expires_at": time.time() + ttl if ttl > 0 else float("inf"),
            "created_at": time.time()
        }
    
    @classmethod
    def get(cls, key: str) -> Optional[Any]:
        """Get a cache value."""
        if key not in cls._cache:
            return None
        
        entry = cls._cache[key]
        if time.time() > entry["expires_at"]:
            del cls._cache[key]
            return None
        
        return entry["value"]
    
    @classmethod
    def delete(cls, key: str) -> bool:
        """Delete a cache entry."""
        if key in cls._cache:
            del cls._cache[key]
            return True
        return False
    
    @classmethod
    def exists(cls, key: str) -> bool:
        """Check if key exists and is not expired."""
        return cls.get(key) is not None
    
    @classmethod
    def clear(cls) -> int:
        """Clear all cache entries."""
        count = len(cls._cache)
        cls._cache.clear()
        return count
    
    @classmethod
    def keys(cls) -> List[str]:
        """Get all cache keys."""
        cls._cleanup()
        return list(cls._cache.keys())
    
    @classmethod
    def _cleanup(cls) -> None:
        """Remove expired entries."""
        now = time.time()
        expired = [k for k, v in cls._cache.items() if now > v["expires_at"]]
        for k in expired:
            del cls._cache[k]
    
    @classmethod
    def stats(cls) -> Dict[str, Any]:
        """Get cache statistics."""
        cls._cleanup()
        return {
            "size": len(cls._cache),
            "keys": list(cls._cache.keys()),
            "memory_usage_estimate": len(str(cls._cache))
        }


class CacheSetAction(BaseAction):
    """Set a cache value."""
    action_type = "cache_set"
    display_name = "设置缓存"
    description = "设置缓存值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", "")
            value = params.get("value")
            ttl = params.get("ttl", 3600)
            
            if not key:
                return ActionResult(success=False, message="key is required")
            
            CacheManager.set(key, value, ttl)
            
            return ActionResult(
                success=True,
                message=f"Cache set: {key}",
                data={"key": key, "ttl": ttl}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cache set failed: {str(e)}")


class CacheGetAction(BaseAction):
    """Get a cache value."""
    action_type = "cache_get"
    display_name = "获取缓存"
    description = "获取缓存值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", "")
            
            if not key:
                return ActionResult(success=False, message="key is required")
            
            value = CacheManager.get(key)
            
            if value is None:
                return ActionResult(success=False, message=f"Cache miss: {key}")
            
            return ActionResult(
                success=True,
                message=f"Cache hit: {key}",
                data={"key": key, "value": value}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cache get failed: {str(e)}")


class CacheDeleteAction(BaseAction):
    """Delete a cache entry."""
    action_type = "cache_delete"
    display_name = "删除缓存"
    description = "删除缓存条目"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", "")
            pattern = params.get("pattern", "")
            
            if not key and not pattern:
                return ActionResult(success=False, message="key or pattern required")
            
            if pattern:
                import fnmatch
                keys_to_delete = [k for k in CacheManager.keys() if fnmatch.fnmatch(k, pattern)]
                for k in keys_to_delete:
                    CacheManager.delete(k)
                return ActionResult(
                    success=True,
                    message=f"Deleted {len(keys_to_delete)} keys matching {pattern}",
                    data={"pattern": pattern, "deleted": len(keys_to_delete)}
                )
            else:
                deleted = CacheManager.delete(key)
                return ActionResult(
                    success=True,
                    message=f"Deleted: {key}" if deleted else f"Not found: {key}",
                    data={"key": key, "deleted": deleted}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Cache delete failed: {str(e)}")


class CacheClearAction(BaseAction):
    """Clear all cache."""
    action_type = "cache_clear"
    display_name = "清空缓存"
    description = "清空所有缓存"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            count = CacheManager.clear()
            
            return ActionResult(
                success=True,
                message=f"Cleared {count} cache entries",
                data={"cleared": count}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cache clear failed: {str(e)}")


class CacheExistsAction(BaseAction):
    """Check if cache key exists."""
    action_type = "cache_exists"
    display_name = "缓存存在检查"
    description = "检查缓存键是否存在"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", "")
            
            if not key:
                return ActionResult(success=False, message="key is required")
            
            exists = CacheManager.exists(key)
            
            return ActionResult(
                success=exists,
                message=f"Cache {'hit' if exists else 'miss'}: {key}",
                data={"key": key, "exists": exists}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cache exists check failed: {str(e)}")


class CacheTTLAction(BaseAction):
    """Get or set TTL for cache entry."""
    action_type = "cache_ttl"
    display_name = "缓存TTL"
    description = "获取或设置缓存TTL"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", "")
            ttl = params.get("ttl")
            
            if not key:
                return ActionResult(success=False, message="key is required")
            
            CacheManager._cleanup()
            
            if key not in CacheManager._cache:
                return ActionResult(success=False, message=f"Key not found: {key}")
            
            if ttl is not None:
                entry = CacheManager._cache[key]
                entry["expires_at"] = time.time() + ttl if ttl > 0 else float("inf")
                return ActionResult(
                    success=True,
                    message=f"Set TTL for {key}: {ttl}s",
                    data={"key": key, "ttl": ttl}
                )
            else:
                entry = CacheManager._cache[key]
                remaining = max(0, entry["expires_at"] - time.time())
                return ActionResult(
                    success=True,
                    message=f"TTL for {key}: {remaining}s",
                    data={"key": key, "ttl": remaining}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Cache TTL failed: {str(e)}")


class CacheStatsAction(BaseAction):
    """Get cache statistics."""
    action_type = "cache_stats"
    display_name = "缓存统计"
    description = "获取缓存统计"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stats = CacheManager.stats()
            
            return ActionResult(
                success=True,
                message=f"Cache stats: {stats['size']} entries",
                data=stats
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cache stats failed: {str(e)}")


class CacheWarmAction(BaseAction):
    """Warm cache with data."""
    action_type = "cache_warm"
    display_name = "预热缓存"
    description = "预热缓存数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            prefix = params.get("prefix", "")
            default_ttl = params.get("ttl", 3600)
            
            if not items:
                return ActionResult(success=False, message="items required")
            
            warmed = 0
            for item in items:
                key = item.get("key", "")
                value = item.get("value")
                ttl = item.get("ttl", default_ttl)
                
                if prefix:
                    key = f"{prefix}:{key}"
                
                CacheManager.set(key, value, ttl)
                warmed += 1
            
            return ActionResult(
                success=True,
                message=f"Warmed cache with {warmed} items",
                data={"warmed": warmed, "items": len(items)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cache warm failed: {str(e)}")
