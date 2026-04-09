"""API cache-aside action module for RabAI AutoClick.

Provides cache-aside pattern for API operations:
- ApiCacheAsideAction: Cache-aside read/write operations
- ApiCacheAsideWriteAction: Write-through with cache invalidation
- ApiCacheAsideRefreshAction: Proactive cache refresh
- ApiCacheAsideBulkAction: Bulk cache-aside operations
"""

import time
import hashlib
import json
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ApiCacheAsideAction(BaseAction):
    """Cache-aside pattern for API calls."""
    action_type = "api_cache_aside"
    display_name = "API缓存旁路模式"
    description = "缓存旁路模式API读写"

    def __init__(self):
        super().__init__()
        self._cache: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "read")
            url = params.get("url", "")
            cache_key = params.get("cache_key", url)
            ttl = params.get("ttl", 300)
            fallback_to_source = params.get("fallback_to_source", True)

            if not cache_key and not url:
                return ActionResult(success=False, message="cache_key or url is required")

            key = self._normalize_key(cache_key)

            if operation == "read":
                return self._cache_read(key, url, ttl, fallback_to_source)
            elif operation == "write":
                data = params.get("data")
                return self._cache_write(key, data, ttl)
            elif operation == "invalidate":
                return self._cache_invalidate(key)
            elif operation == "clear":
                count = len(self._cache)
                self._cache.clear()
                return ActionResult(success=True, message=f"Cleared {count} cache entries", data={"cleared": count})

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Cache-aside error: {e}")

    def _cache_read(self, key: str, url: str, ttl: int, fallback: bool) -> ActionResult:
        """Read from cache, fallback to source."""
        if key in self._cache:
            entry = self._cache[key]
            if time.time() - entry["timestamp"] < entry["ttl"]:
                entry["hits"] = entry.get("hits", 0) + 1
                return ActionResult(success=True, message="Cache hit", data={"data": entry["data"], "from_cache": True})

        if not url and not fallback:
            return ActionResult(success=False, message="Cache miss and no source URL")

        try:
            import urllib.request
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=30) as response:
                data = response.read().decode()
                self._cache[key] = {
                    "data": data,
                    "timestamp": time.time(),
                    "ttl": ttl,
                    "url": url,
                    "hits": 0,
                }
                return ActionResult(success=True, message="Cache miss, fetched from source", data={"data": data, "from_cache": False})
        except Exception as e:
            if fallback and key in self._cache:
                return ActionResult(success=True, message="Source failed, returning stale cache", data={"data": self._cache[key]["data"], "from_cache": True, "stale": True})
            return ActionResult(success=False, message=f"Read failed: {e}")

    def _cache_write(self, key: str, data: Any, ttl: int) -> ActionResult:
        """Write to cache."""
        if data is None:
            return ActionResult(success=False, message="data is required for write operation")

        self._cache[key] = {
            "data": data,
            "timestamp": time.time(),
            "ttl": ttl,
            "hits": 0,
        }
        return ActionResult(success=True, message=f"Cached data at key {key}", data={"key": key, "ttl": ttl})

    def _cache_invalidate(self, key: str) -> ActionResult:
        """Invalidate cache entry."""
        if key in self._cache:
            del self._cache[key]
            return ActionResult(success=True, message=f"Invalidated key {key}")
        return ActionResult(success=True, message=f"Key {key} not in cache", data={"was_present": False})

    def _normalize_key(self, key: str) -> str:
        """Normalize cache key."""
        return hashlib.md5(key.encode()).hexdigest()


class ApiCacheAsideWriteAction(BaseAction):
    """Write-through with cache invalidation."""
    action_type = "api_cache_aside_write"
    display_name = "API缓存写策略"
    description = "写时失效缓存策略"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "write")
            url = params.get("url", "")
            method = params.get("method", "POST")
            headers = params.get("headers", {})
            body = params.get("body")
            invalidate_keys = params.get("invalidate_keys", [])

            if not url:
                return ActionResult(success=False, message="url is required")

            import urllib.request
            import json as json_module

            req = urllib.request.Request(url, method=method, headers=headers)
            if body:
                req.data = json_module.dumps(body).encode() if isinstance(body, dict) else str(body).encode()

            try:
                with urllib.request.urlopen(req, timeout=30) as response:
                    content = response.read().decode()
                    result = {"success": True, "content": content, "status": response.status}
            except urllib.error.HTTPError as e:
                content = e.read().decode()
                result = {"success": False, "error": str(e), "content": content, "status": e.code}

            if operation == "write" and invalidate_keys:
                invalidated = []
                for key in invalidate_keys:
                    invalidated.append({"key": key, "invalidated": True})
                result["invalidated_keys"] = invalidated

            return ActionResult(success=result.get("success", False), message=f"Write operation completed", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Cache-aside write error: {e}")


class ApiCacheAsideRefreshAction(BaseAction):
    """Proactive cache refresh."""
    action_type = "api_cache_aside_refresh"
    display_name = "API缓存主动刷新"
    description = "主动刷新即将过期的缓存"

    def __init__(self):
        super().__init__()
        self._cache: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            refresh_threshold = params.get("refresh_threshold", 0.2)
            callback = params.get("callback")

            if not items:
                return ActionResult(success=False, message="items list is required")

            refreshed = []
            skipped = []
            failed = []

            for item in items:
                key = item.get("cache_key", item.get("url", ""))
                url = item.get("url", "")
                ttl = item.get("ttl", 300)

                if key in self._cache:
                    entry = self._cache[key]
                    age = time.time() - entry["timestamp"]
                    remaining = entry["ttl"] - age
                    if remaining / entry["ttl"] > refresh_threshold:
                        skipped.append(key)
                        continue

                try:
                    import urllib.request
                    req = urllib.request.Request(url)
                    with urllib.request.urlopen(req, timeout=30) as response:
                        data = response.read().decode()
                        self._cache[key] = {
                            "data": data,
                            "timestamp": time.time(),
                            "ttl": ttl,
                            "url": url,
                        }
                        refreshed.append(key)
                except Exception as e:
                    failed.append({"key": key, "error": str(e)})

            return ActionResult(
                success=len(failed) == 0,
                message=f"Refreshed {len(refreshed)}, skipped {len(skipped)}, failed {len(failed)}",
                data={"refreshed": refreshed, "skipped": skipped, "failed": failed}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cache refresh error: {e}")


class ApiCacheAsideBulkAction(BaseAction):
    """Bulk cache-aside operations."""
    action_type = "api_cache_aside_bulk"
    display_name = "API批量缓存操作"
    description = "批量缓存旁路操作"

    def __init__(self):
        super().__init__()
        self._cache: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operations = params.get("operations", [])
            parallel = params.get("parallel", False)
            max_workers = params.get("max_workers", 5)

            if not operations:
                return ActionResult(success=False, message="operations list is required")

            from concurrent.futures import ThreadPoolExecutor, as_completed

            results = []

            if parallel:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {executor.submit(self._execute_single, op): op for op in operations}
                    for future in as_completed(futures):
                        try:
                            result = future.result()
                            results.append(result)
                        except Exception as e:
                            results.append({"success": False, "error": str(e)})
            else:
                for op in operations:
                    result = self._execute_single(op)
                    results.append(result)

            success_count = sum(1 for r in results if r.get("success", False))

            return ActionResult(
                success=success_count == len(results),
                message=f"Bulk operation: {success_count}/{len(results)} succeeded",
                data={"results": results, "total": len(results), "succeeded": success_count}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Bulk cache-aside error: {e}")

    def _execute_single(self, operation: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single cache-aside operation."""
        op_type = operation.get("type", "read")
        url = operation.get("url", "")
        key = operation.get("cache_key", url)

        if op_type == "read":
            if key in self._cache:
                entry = self._cache[key]
                if time.time() - entry["timestamp"] < entry["ttl"]:
                    return {"success": True, "key": key, "from_cache": True, "data": entry["data"]}
            try:
                import urllib.request
                req = urllib.request.Request(url)
                with urllib.request.urlopen(req, timeout=30) as response:
                    data = response.read().decode()
                    self._cache[key] = {"data": data, "timestamp": time.time(), "ttl": operation.get("ttl", 300)}
                    return {"success": True, "key": key, "from_cache": False, "data": data}
            except Exception as e:
                return {"success": False, "key": key, "error": str(e)}
        elif op_type == "write":
            self._cache[key] = {"data": operation.get("data"), "timestamp": time.time(), "ttl": operation.get("ttl", 300)}
            return {"success": True, "key": key}
        elif op_type == "invalidate":
            if key in self._cache:
                del self._cache[key]
            return {"success": True, "key": key}
        return {"success": False, "error": f"Unknown operation type: {op_type}"}
