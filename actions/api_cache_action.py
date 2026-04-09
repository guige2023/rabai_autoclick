"""API cache management action module for RabAI AutoClick.

Provides caching operations for API responses:
- CacheLookupAction: Look up cached responses
- CacheStoreAction: Store API responses in cache
- CacheInvalidateAction: Invalidate cached entries
- CacheStatsAction: Get cache statistics
- CacheWarmingAction: Warm cache with frequently used data
"""

from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
import hashlib
import json

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class LRUCache:
    """Simple LRU cache implementation."""
    
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self._cache: Dict[str, Dict] = {}
        self._access_order: List[str] = []
    
    def get(self, key: str) -> Optional[Dict]:
        if key in self._cache:
            self._access_order.remove(key)
            self._access_order.append(key)
            return self._cache[key]
        return None
    
    def set(self, key: str, value: Dict) -> None:
        if key in self._cache:
            self._access_order.remove(key)
        elif len(self._cache) >= self.max_size:
            oldest = self._access_order.pop(0)
            del self._cache[oldest]
        
        self._cache[key] = value
        self._access_order.append(key)
    
    def delete(self, key: str) -> bool:
        if key in self._cache:
            del self._cache[key]
            self._access_order.remove(key)
            return True
        return False
    
    def clear(self) -> int:
        count = len(self._cache)
        self._cache.clear()
        self._access_order.clear()
        return count
    
    def get_stats(self) -> Dict:
        return {
            "size": len(self._cache),
            "max_size": self.max_size,
            "utilization": len(self._cache) / self.max_size if self.max_size > 0 else 0
        }


class TTLCache:
    """Cache with time-to-live support."""
    
    def __init__(self, default_ttl: int = 3600):
        self.default_ttl = default_ttl
        self._cache: Dict[str, Dict] = {}
        self._expiry: Dict[str, datetime] = {}
    
    def get(self, key: str) -> Optional[Dict]:
        if key not in self._cache:
            return None
        
        if datetime.now() > self._expiry[key]:
            self.delete(key)
            return None
        
        return self._cache[key]
    
    def set(self, key: str, value: Dict, ttl: Optional[int] = None) -> None:
        self._cache[key] = value
        ttl = ttl or self.default_ttl
        self._expiry[key] = datetime.now() + timedelta(seconds=ttl)
    
    def delete(self, key: str) -> bool:
        if key in self._cache:
            del self._cache[key]
            del self._expiry[key]
            return True
        return False
    
    def clear(self) -> int:
        count = len(self._cache)
        self._cache.clear()
        self._expiry.clear()
        return count
    
    def cleanup(self) -> int:
        now = datetime.now()
        expired_keys = [k for k, v in self._expiry.items() if now > v]
        for key in expired_keys:
            self.delete(key)
        return len(expired_keys)


class CacheLookupAction(BaseAction):
    """Look up cached responses."""
    action_type = "cache_lookup"
    display_name = "缓存查询"
    description = "查询缓存的API响应"
    
    def __init__(self):
        super().__init__()
        self._cache = LRUCache(max_size=1000)
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            cache_key = params.get("cache_key")
            if not cache_key:
                key_parts = {
                    "endpoint": params.get("endpoint"),
                    "params": params.get("params"),
                    "user": params.get("user_id")
                }
                cache_key = self._generate_key(key_parts)
            
            cached = self._cache.get(cache_key)
            
            if cached:
                return ActionResult(
                    success=True,
                    message="Cache hit",
                    data={
                        "cache_hit": True,
                        "cache_key": cache_key,
                        "cached_at": cached.get("cached_at"),
                        "data": cached.get("data")
                    }
                )
            else:
                return ActionResult(
                    success=True,
                    message="Cache miss",
                    data={
                        "cache_hit": False,
                        "cache_key": cache_key
                    }
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _generate_key(self, parts: Dict) -> str:
        content = json.dumps(parts, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()[:16]


class CacheStoreAction(BaseAction):
    """Store API responses in cache."""
    action_type = "cache_store"
    display_name = "缓存存储"
    description = "将API响应存储到缓存"
    
    def __init__(self):
        super().__init__()
        self._cache = LRUCache(max_size=1000)
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            cache_key = params.get("cache_key")
            data = params.get("data")
            ttl = params.get("ttl")
            
            if not cache_key:
                key_parts = {
                    "endpoint": params.get("endpoint"),
                    "params": params.get("params"),
                    "user": params.get("user_id")
                }
                cache_key = self._generate_key(key_parts)
            
            cache_entry = {
                "data": data,
                "cached_at": datetime.now().isoformat(),
                "ttl": ttl,
                "hits": 0
            }
            
            self._cache.set(cache_key, cache_entry)
            
            return ActionResult(
                success=True,
                message="Data cached successfully",
                data={
                    "cache_key": cache_key,
                    "cached_at": cache_entry["cached_at"],
                    "ttl": ttl
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _generate_key(self, parts: Dict) -> str:
        content = json.dumps(parts, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()[:16]


class CacheInvalidateAction(BaseAction):
    """Invalidate cached entries."""
    action_type = "cache_invalidate"
    display_name = "缓存失效"
    description = "使缓存条目失效"
    
    def __init__(self):
        super().__init__()
        self._cache = LRUCache(max_size=1000)
        self._ttl_cache = TTLCache()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "delete")
            
            if operation == "delete":
                cache_key = params.get("cache_key")
                if not cache_key:
                    return ActionResult(success=False, message="cache_key is required")
                
                deleted = self._cache.delete(cache_key)
                ttl_deleted = self._ttl_cache.delete(cache_key)
                
                return ActionResult(
                    success=True,
                    message=f"Cache entry deleted: {deleted or ttl_deleted}",
                    data={
                        "cache_key": cache_key,
                        "deleted": deleted or ttl_deleted
                    }
                )
            
            elif operation == "clear":
                lru_count = self._cache.clear()
                ttl_count = self._ttl_cache.clear()
                
                return ActionResult(
                    success=True,
                    message="Cache cleared",
                    data={
                        "lru_entries_cleared": lru_count,
                        "ttl_entries_cleared": ttl_count
                    }
                )
            
            elif operation == "cleanup":
                cleaned = self._ttl_cache.cleanup()
                
                return ActionResult(
                    success=True,
                    message=f"Cleaned up {cleaned} expired entries",
                    data={"cleaned_entries": cleaned}
                )
            
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class CacheStatsAction(BaseAction):
    """Get cache statistics."""
    action_type = "cache_stats"
    display_name = "缓存统计"
    description = "获取缓存统计信息"
    
    def __init__(self):
        super().__init__()
        self._cache = LRUCache(max_size=1000)
        self._ttl_cache = TTLCache()
        self._hits = 0
        self._misses = 0
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            self._ttl_cache.cleanup()
            
            lru_stats = self._cache.get_stats()
            
            total_requests = self._hits + self._misses
            hit_rate = self._hits / total_requests if total_requests > 0 else 0
            
            return ActionResult(
                success=True,
                message="Cache statistics retrieved",
                data={
                    "lru_cache": lru_stats,
                    "ttl_cache_size": len(self._ttl_cache._cache),
                    "hits": self._hits,
                    "misses": self._misses,
                    "hit_rate": hit_rate,
                    "total_requests": total_requests
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def record_hit(self) -> None:
        self._hits += 1
    
    def record_miss(self) -> None:
        self._misses += 1


class CacheWarmingAction(BaseAction):
    """Warm cache with frequently used data."""
    action_type = "cache_warming"
    display_name = "缓存预热"
    description = "预热缓存常用数据"
    
    def __init__(self):
        super().__init__()
        self._warm_data: List[Dict] = []
        self._cache = LRUCache(max_size=1000)
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "warm")
            
            if operation == "add":
                data = params.get("data")
                priority = params.get("priority", 1)
                
                self._warm_data.append({
                    "data": data,
                    "priority": priority,
                    "added_at": datetime.now().isoformat()
                })
                
                return ActionResult(
                    success=True,
                    message="Data added for warming",
                    data={"warm_data_count": len(self._warm_data)}
                )
            
            elif operation == "warm":
                max_items = params.get("max_items", 100)
                fetch_fn = params.get("fetch_fn")
                
                sorted_data = sorted(self._warm_data, key=lambda x: x["priority"], reverse=True)
                warmed = 0
                failed = 0
                
                for item in sorted_data[:max_items]:
                    cache_key = self._generate_key(item["data"])
                    
                    if callable(fetch_fn):
                        try:
                            fetched_data = fetch_fn(item["data"])
                            self._cache.set(cache_key, {
                                "data": fetched_data,
                                "cached_at": datetime.now().isoformat()
                            })
                            warmed += 1
                        except Exception:
                            failed += 1
                    else:
                        self._cache.set(cache_key, {
                            "data": item["data"],
                            "cached_at": datetime.now().isoformat()
                        })
                        warmed += 1
                
                return ActionResult(
                    success=True,
                    message=f"Cache warming complete",
                    data={
                        "warmed_count": warmed,
                        "failed_count": failed,
                        "total_warm_data": len(self._warm_data)
                    }
                )
            
            elif operation == "clear":
                count = len(self._warm_data)
                self._warm_data.clear()
                
                return ActionResult(
                    success=True,
                    message=f"Cleared {count} warm data entries",
                    data={"cleared_count": count}
                )
            
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _generate_key(self, data: Any) -> str:
        content = json.dumps(data, sort_keys=True) if isinstance(data, (dict, list)) else str(data)
        return hashlib.sha256(content.encode()).hexdigest()[:16]
