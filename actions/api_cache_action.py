"""API cache action module for RabAI AutoClick.

Provides API caching operations:
- APICacheAction: Cache API responses
- CacheInvalidatorAction: Invalidate cached entries
- CacheManagerAction: Manage cache configuration
- CacheStrategyAction: Configure cache strategies
"""

import time
import hashlib
from typing import Any, Dict, List, Optional, Union, Callable
from datetime import datetime, timedelta

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class APICacheAction(BaseAction):
    """Cache API responses."""
    action_type = "api_cache"
    display_name = "API缓存"
    description = "缓存API响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "get")
            cache_key = params.get("cache_key", "")
            cache_ttl = params.get("ttl", 300)
            cache_data = params.get("data")
            namespace = params.get("namespace", "default")

            if not hasattr(context, "_api_cache"):
                context._api_cache = {}
                context._api_cache_meta = {}

            cache_ns = f"{namespace}:{cache_key}"
            if not cache_ns:
                cache_ns = f"{namespace}:{self._generate_key(str(cache_data) if cache_data else '')}"

            if action == "get":
                if cache_ns in context._api_cache:
                    entry = context._api_cache[cache_ns]
                    meta = context._api_cache_meta.get(cache_ns, {})

                    if meta.get("expires_at", 0) > time.time():
                        return ActionResult(
                            success=True,
                            data={
                                "cached": True,
                                "data": entry,
                                "cache_key": cache_ns,
                                "age_seconds": int(time.time() - meta.get("created_at", time.time())),
                                "hit": True
                            },
                            message=f"Cache hit: {cache_ns}"
                        )
                    else:
                        del context._api_cache[cache_ns]
                        if cache_ns in context._api_cache_meta:
                            del context._api_cache_meta[cache_ns]

                return ActionResult(
                    success=True,
                    data={
                        "cached": False,
                        "cache_key": cache_ns,
                        "hit": False
                    },
                    message=f"Cache miss: {cache_ns}"
                )

            elif action == "set":
                if cache_data is None:
                    return ActionResult(success=False, message="data is required for set action")

                context._api_cache[cache_ns] = cache_data
                context._api_cache_meta[cache_ns] = {
                    "created_at": time.time(),
                    "expires_at": time.time() + cache_ttl,
                    "ttl": cache_ttl,
                    "hits": 0
                }

                return ActionResult(
                    success=True,
                    data={
                        "cached": True,
                        "cache_key": cache_ns,
                        "ttl": cache_ttl,
                        "size": len(str(cache_data))
                    },
                    message=f"Cached: {cache_ns} (TTL: {cache_ttl}s)"
                )

            elif action == "delete":
                if cache_ns in context._api_cache:
                    del context._api_cache[cache_ns]
                    if cache_ns in context._api_cache_meta:
                        del context._api_cache_meta[cache_ns]

                return ActionResult(
                    success=True,
                    data={"deleted": cache_ns},
                    message=f"Deleted cache: {cache_ns}"
                )

            elif action == "clear":
                count = len(context._api_cache)
                context._api_cache.clear()
                context._api_cache_meta.clear()

                return ActionResult(
                    success=True,
                    data={"cleared": True, "entries_removed": count},
                    message=f"Cleared {count} cache entries"
                )

            elif action == "stats":
                total_entries = len(context._api_cache)
                expired = sum(1 for m in context._api_cache_meta.values() if m.get("expires_at", 0) < time.time())

                return ActionResult(
                    success=True,
                    data={
                        "total_entries": total_entries,
                        "expired_entries": expired,
                        "active_entries": total_entries - expired,
                        "namespace": namespace
                    },
                    message=f"Cache stats: {total_entries} entries, {expired} expired"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"API cache error: {str(e)}")

    def _generate_key(self, data: str) -> str:
        return hashlib.md5(data.encode()).hexdigest()[:16]

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"cache_key": "", "ttl": 300, "data": None, "namespace": "default"}


class CacheInvalidatorAction(BaseAction):
    """Invalidate cached entries."""
    action_type = "api_cache_invalidator"
    display_name = "缓存失效器"
    description = "使缓存条目失效"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "invalidate")
            pattern = params.get("pattern", "")
            namespace = params.get("namespace", "default")
            older_than = params.get("older_than")

            if not hasattr(context, "_api_cache"):
                context._api_cache = {}
                context._api_cache_meta = {}

            if action == "invalidate":
                if not pattern:
                    return ActionResult(success=False, message="pattern is required for invalidate action")

                import re
                regex = re.compile(pattern)
                keys_to_delete = [k for k in context._api_cache.keys() if regex.search(k)]

                for key in keys_to_delete:
                    del context._api_cache[key]
                    if key in context._api_cache_meta:
                        del context._api_cache_meta[key]

                return ActionResult(
                    success=True,
                    data={
                        "invalidated_count": len(keys_to_delete),
                        "pattern": pattern
                    },
                    message=f"Invalidated {len(keys_to_delete)} entries matching '{pattern}'"
                )

            elif action == "invalidate_by_ttl":
                ttl_threshold = params.get("ttl_threshold", 60)
                now = time.time()

                keys_to_delete = []
                for key, meta in context._api_cache_meta.items():
                    expires_at = meta.get("expires_at", 0)
                    remaining_ttl = expires_at - now
                    if remaining_ttl < ttl_threshold:
                        keys_to_delete.append(key)

                for key in keys_to_delete:
                    del context._api_cache[key]
                    if key in context._api_cache_meta:
                        del context._api_cache_meta[key]

                return ActionResult(
                    success=True,
                    data={
                        "invalidated_count": len(keys_to_delete),
                        "ttl_threshold": ttl_threshold
                    },
                    message=f"Invalidated {len(keys_to_delete)} entries with TTL < {ttl_threshold}s"
                )

            elif action == "invalidate_by_age":
                if older_than is None:
                    return ActionResult(success=False, message="older_than is required for invalidate_by_age action")

                age_threshold = time.time() - older_than
                keys_to_delete = []

                for key, meta in context._api_cache_meta.items():
                    created_at = meta.get("created_at", 0)
                    if created_at < age_threshold:
                        keys_to_delete.append(key)

                for key in keys_to_delete:
                    del context._api_cache[key]
                    if key in context._api_cache_meta:
                        del context._api_cache_meta[key]

                return ActionResult(
                    success=True,
                    data={
                        "invalidated_count": len(keys_to_delete),
                        "older_than": older_than
                    },
                    message=f"Invalidated {len(keys_to_delete)} entries older than {older_than}s"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Cache invalidator error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"pattern": "", "namespace": "default", "older_than": None, "ttl_threshold": 60}


class CacheManagerAction(BaseAction):
    """Manage cache configuration."""
    action_type = "api_cache_manager"
    display_name = "缓存管理器"
    description = "管理缓存配置"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "config")
            max_size = params.get("max_size", 1000)
            default_ttl = params.get("default_ttl", 300)
            eviction_policy = params.get("eviction_policy", "lru")

            if not hasattr(context, "_cache_config"):
                context._cache_config = {
                    "max_size": max_size,
                    "default_ttl": default_ttl,
                    "eviction_policy": eviction_policy,
                    "enabled": True
                }

            if action == "config":
                return ActionResult(
                    success=True,
                    data={
                        "config": context._cache_config,
                        "updated": True
                    },
                    message=f"Cache config: max_size={max_size}, default_ttl={default_ttl}, policy={eviction_policy}"
                )

            elif action == "enable":
                context._cache_config["enabled"] = True
                return ActionResult(
                    success=True,
                    data={"enabled": True},
                    message="Cache enabled"
                )

            elif action == "disable":
                context._cache_config["enabled"] = False
                return ActionResult(
                    success=True,
                    data={"enabled": False},
                    message="Cache disabled"
                )

            elif action == "evict":
                if not hasattr(context, "_api_cache"):
                    return ActionResult(success=True, data={"evicted": 0}, message="Cache empty, nothing to evict")

                current_size = len(context._api_cache)
                if current_size <= max_size:
                    return ActionResult(
                        success=True,
                        data={"evicted": 0, "current_size": current_size},
                        message=f"No eviction needed: {current_size}/{max_size}"
                    )

                evict_count = current_size - max_size
                keys = list(context._api_cache.keys())

                if eviction_policy == "lru":
                    meta_list = [(k, context._api_cache_meta.get(k, {}).get("created_at", 0)) for k in keys]
                    meta_list.sort(key=lambda x: x[1])
                    keys_to_evict = [k for k, _ in meta_list[:evict_count]]
                else:
                    keys_to_evict = keys[:evict_count]

                for key in keys_to_evict:
                    del context._api_cache[key]
                    if key in context._api_cache_meta:
                        del context._api_cache_meta[key]

                return ActionResult(
                    success=True,
                    data={
                        "evicted": len(keys_to_evict),
                        "current_size": len(context._api_cache),
                        "policy": eviction_policy
                    },
                    message=f"Evicted {len(keys_to_evict)} entries using {eviction_policy}"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Cache manager error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"max_size": 1000, "default_ttl": 300, "eviction_policy": "lru"}


class CacheStrategyAction(BaseAction):
    """Configure cache strategies."""
    action_type = "api_cache_strategy"
    display_name = "缓存策略"
    description = "配置缓存策略"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "apply")
            strategy = params.get("strategy", "cache_first")
            url_pattern = params.get("url_pattern", "*")
            ttl = params.get("ttl")

            strategies = {
                "cache_first": {
                    "description": "Try cache first, fallback to network",
                    "ttl_default": 300,
                    "skip_on_error": False
                },
                "network_first": {
                    "description": "Try network first, cache response",
                    "ttl_default": 60,
                    "skip_on_error": True
                },
                "stale_while_revalidate": {
                    "description": "Return stale, revalidate in background",
                    "ttl_default": 600,
                    "stale_ttl": 3600,
                    "skip_on_error": False
                },
                "cache_only": {
                    "description": "Cache only, no network",
                    "ttl_default": 3600,
                    "skip_on_error": True
                },
                "network_only": {
                    "description": "Network only, no cache",
                    "ttl_default": 0,
                    "skip_on_error": False
                }
            }

            if action == "apply":
                if strategy not in strategies:
                    return ActionResult(success=False, message=f"Unknown strategy: {strategy}")

                strategy_config = strategies[strategy]
                if ttl:
                    strategy_config["ttl_default"] = ttl

                return ActionResult(
                    success=True,
                    data={
                        "strategy": strategy,
                        "config": strategy_config,
                        "url_pattern": url_pattern
                    },
                    message=f"Applied '{strategy}' strategy for '{url_pattern}'"
                )

            elif action == "list":
                return ActionResult(
                    success=True,
                    data={
                        "strategies": strategies,
                        "count": len(strategies)
                    },
                    message=f"Available strategies: {list(strategies.keys())}"
                )

            elif action == "match":
                request_url = params.get("request_url", "")
                matching_strategies = []

                for strat, config in strategies.items():
                    if url_pattern == "*" or url_pattern in request_url:
                        matching_strategies.append({"strategy": strat, "config": config})

                return ActionResult(
                    success=True,
                    data={
                        "matching": matching_strategies,
                        "request_url": request_url,
                        "pattern": url_pattern
                    },
                    message=f"Found {len(matching_strategies)} matching strategies"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Cache strategy error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"strategy": "cache_first", "url_pattern": "*", "ttl": None, "request_url": ""}
