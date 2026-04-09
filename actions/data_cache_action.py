"""Data caching and cache management action module for RabAI AutoClick.

Provides:
- DataCacheAction: In-memory data caching
- DataCacheEvictionAction: Cache eviction policies
- DataCacheWarmerAction: Cache warming strategies
- CacheStrategyAction: Advanced caching strategies
"""

import time
import json
import hashlib
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime, timedelta
from enum import Enum
import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EvictionPolicy(str, Enum):
    """Cache eviction policies."""
    LRU = "lru"
    LFU = "lfu"
    FIFO = "fifo"
    TTL = "ttl"
    RANDOM = "random"


class DataCacheAction(BaseAction):
    """In-memory data caching."""
    action_type = "data_cache"
    display_name = "数据缓存"
    description = "内存数据缓存"

    def __init__(self):
        super().__init__()
        self._cache: Dict[str, Dict] = {}
        self._hits = 0
        self._misses = 0

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "get")
            key = params.get("key", "")

            if operation == "set":
                if not key:
                    return ActionResult(success=False, message="key required")

                self._cache[key] = {
                    "value": params.get("value"),
                    "created_at": time.time(),
                    "ttl": params.get("ttl"),
                    "access_count": 0,
                    "last_accessed": time.time()
                }
                return ActionResult(success=True, data={"key": key}, message=f"Cached: {key}")

            elif operation == "get":
                if key not in self._cache:
                    self._misses += 1
                    return ActionResult(success=False, message=f"Cache miss: {key}")

                entry = self._cache[key]
                if entry["ttl"]:
                    age = time.time() - entry["created_at"]
                    if age > entry["ttl"]:
                        del self._cache[key]
                        self._misses += 1
                        return ActionResult(success=False, message=f"Cache expired: {key}")

                entry["access_count"] += 1
                entry["last_accessed"] = time.time()
                self._hits += 1

                return ActionResult(
                    success=True,
                    data={
                        "key": key,
                        "value": entry["value"],
                        "hits": entry["access_count"],
                        "age_seconds": round(time.time() - entry["created_at"], 2)
                    }
                )

            elif operation == "delete":
                if key in self._cache:
                    del self._cache[key]
                return ActionResult(success=True, message=f"Deleted: {key}")

            elif operation == "clear":
                count = len(self._cache)
                self._cache = {}
                return ActionResult(success=True, data={"cleared": count})

            elif operation == "stats":
                total = self._hits + self._misses
                hit_rate = self._hits / total if total > 0 else 0
                return ActionResult(
                    success=True,
                    data={
                        "size": len(self._cache),
                        "hits": self._hits,
                        "misses": self._misses,
                        "hit_rate": round(hit_rate, 4),
                        "total_requests": total
                    }
                )

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={
                        "keys": list(self._cache.keys()),
                        "count": len(self._cache)
                    }
                )

            elif operation == "exists":
                exists = key in self._cache
                if exists and self._cache[key].get("ttl"):
                    age = time.time() - self._cache[key]["created_at"]
                    exists = age <= self._cache[key]["ttl"]
                return ActionResult(success=True, data={"key": key, "exists": exists})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Cache error: {str(e)}")


class DataCacheEvictionAction(BaseAction):
    """Cache eviction policies."""
    action_type = "data_cache_eviction"
    display_name = "缓存淘汰"
    description = "缓存淘汰策略"

    def __init__(self):
        super().__init__()
        self._cache: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "evict")
            policy = params.get("policy", EvictionPolicy.LRU.value)
            max_size = params.get("max_size", 1000)

            if operation == "put":
                key = params.get("key", "")
                value = params.get("value")
                self._cache[key] = {
                    "value": value,
                    "created_at": time.time(),
                    "access_count": 0,
                    "last_accessed": time.time(),
                    "ttl": params.get("ttl")
                }

                if len(self._cache) > max_size:
                    evicted = self._evict(policy, max_size)
                    return ActionResult(success=True, data={"evicted": evicted})
                return ActionResult(success=True, data={"key": key})

            elif operation == "evict":
                evicted = self._evict(policy, max_size)
                return ActionResult(success=True, data={"evicted": evicted, "policy": policy, "remaining": len(self._cache)})

            elif operation == "evict_expired":
                now = time.time()
                expired = []
                for key, entry in list(self._cache.items()):
                    if entry.get("ttl"):
                        if now - entry["created_at"] > entry["ttl"]:
                            del self._cache[key]
                            expired.append(key)
                return ActionResult(success=True, data={"expired": expired, "count": len(expired)})

            elif operation == "stats":
                return ActionResult(
                    success=True,
                    data={
                        "size": len(self._cache),
                        "max_size": max_size,
                        "policy": policy,
                        "fill_rate": round(len(self._cache) / max_size, 4)
                    }
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Eviction error: {str(e)}")

    def _evict(self, policy: str, max_size: int) -> List[str]:
        evicted = []
        while len(self._cache) > max_size:
            if policy == EvictionPolicy.LRU.value:
                lru_key = min(self._cache.keys(), key=lambda k: self._cache[k]["last_accessed"])
                del self._cache[lru_key]
                evicted.append(lru_key)
            elif policy == EvictionPolicy.LFU.value:
                lfu_key = min(self._cache.keys(), key=lambda k: self._cache[k]["access_count"])
                del self._cache[lfu_key]
                evicted.append(lfu_key)
            elif policy == EvictionPolicy.FIFO.value:
                fifo_key = min(self._cache.keys(), key=lambda k: self._cache[k]["created_at"])
                del self._cache[fifo_key]
                evicted.append(fifo_key)
            elif policy == EvictionPolicy.RANDOM.value:
                import random
                rand_key = random.choice(list(self._cache.keys()))
                del self._cache[rand_key]
                evicted.append(rand_key)
            else:
                break
        return evicted


class DataCacheWarmerAction(BaseAction):
    """Cache warming strategies."""
    action_type = "data_cache_warmer"
    display_name = "缓存预热"
    description = "缓存预热策略"

    def __init__(self):
        super().__init__()
        self._warm_data: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "warm")
            strategy = params.get("strategy", "eager")

            if operation == "add_warm_entry":
                key = params.get("key", "")
                value = params.get("value")
                priority = params.get("priority", 0)
                self._warm_data[key] = {
                    "value": value,
                    "priority": priority,
                    "added_at": time.time()
                }
                return ActionResult(success=True, data={"key": key, "priority": priority})

            elif operation == "warm":
                target_cache = params.get("target_cache", {})
                max_entries = params.get("max_entries", 100)
                warmed = 0

                sorted_entries = sorted(self._warm_data.items(), key=lambda x: x[1]["priority"], reverse=True)

                for key, entry in sorted_entries[:max_entries]:
                    target_cache[key] = entry["value"]
                    warmed += 1

                return ActionResult(
                    success=True,
                    data={
                        "warmed": warmed,
                        "strategy": strategy,
                        "total_available": len(self._warm_data)
                    }
                )

            elif operation == "adaptive":
                frequent_keys = params.get("frequent_keys", [])
                recent_keys = params.get("recent_keys", [])
                target_cache = params.get("target_cache", {})

                combined = list(set(frequent_keys[:50] + recent_keys[:50]))
                warmed = 0
                for key in combined:
                    if key in self._warm_data:
                        target_cache[key] = self._warm_data[key]["value"]
                        warmed += 1

                return ActionResult(
                    success=True,
                    data={
                        "warmed": warmed,
                        "combined_keys": len(combined)
                    }
                )

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={"entries": list(self._warm_data.keys()), "count": len(self._warm_data)}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Cache warmer error: {str(e)}")


class CacheStrategyAction(BaseAction):
    """Advanced caching strategies."""
    action_type = "cache_strategy"
    display_name = "缓存策略"
    description = "高级缓存策略"

    def __init__(self):
        super().__init__()
        self._cache_layers: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "strategize")

            if operation == "setup":
                layer_name = params.get("layer_name", "default")
                layers = params.get("layers", ["memory", "disk"])

                self._cache_layers[layer_name] = {
                    "name": layer_name,
                    "layers": layers,
                    "created_at": time.time(),
                    "hit_counts": {layer: 0 for layer in layers}
                }
                return ActionResult(success=True, data={"layer": layer_name, "layers": layers})

            elif operation == "get":
                layer_name = params.get("layer_name", "default")
                key = params.get("key", "")

                if layer_name not in self._cache_layers:
                    return ActionResult(success=False, message=f"Layer '{layer_name}' not found")

                layers = self._cache_layers[layer_name]["layers"]
                for layer in layers:
                    self._cache_layers[layer_name]["hit_counts"][layer] += 1

                return ActionResult(
                    success=True,
                    data={
                        "layer": layer_name,
                        "layers_checked": len(layers),
                        "total_hits": sum(self._cache_layers[layer_name]["hit_counts"].values())
                    }
                )

            elif operation == "invalidate":
                layer_name = params.get("layer_name", "default")
                key = params.get("key", "")
                return ActionResult(success=True, message=f"Invalidated '{key}' in '{layer_name}'")

            elif operation == "stats":
                return ActionResult(
                    success=True,
                    data={
                        "layers": {
                            name: {"layers": info["layers"], "hits": info["hit_counts"]}
                            for name, info in self._cache_layers.items()
                        }
                    }
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Strategy error: {str(e)}")
