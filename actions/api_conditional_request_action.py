"""API conditional request action module for RabAI AutoClick.

Provides conditional HTTP request operations:
- ApiConditionalRequestAction: Conditional GET requests (If-None-Match, If-Modified-Since)
- ApiETagAction: ETag-based cache validation
- ApiLastModifiedAction: Last-Modified based conditional requests
- ApiFreshnessCheckAction: Check cache freshness before request
"""

import time
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ApiConditionalRequestAction(BaseAction):
    """Conditional HTTP requests with ETag/Last-Modified."""
    action_type = "api_conditional_request"
    display_name = "API条件请求"
    description = "条件HTTP请求(ETag/Last-Modified)"

    def __init__(self):
        super().__init__()
        self._etag_store: Dict[str, str] = {}
        self._lm_store: Dict[str, str] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            method = params.get("method", "GET")
            use_etag = params.get("use_etag", True)
            use_last_modified = params.get("use_last_modified", True)
            force_refresh = params.get("force_refresh", False)
            store_key = params.get("store_key", url)

            if not url:
                return ActionResult(success=False, message="url is required")

            import urllib.request

            req = urllib.request.Request(url, method=method)

            if not force_refresh:
                if use_etag and store_key in self._etag_store:
                    req.add_header("If-None-Match", self._etag_store[store_key])
                if use_last_modified and store_key in self._lm_store:
                    req.add_header("If-Modified-Since", self._lm_store[store_key])

            try:
                with urllib.request.urlopen(req, timeout=30) as response:
                    etag = response.headers.get("ETag")
                    last_modified = response.headers.get("Last-Modified")
                    content = response.read().decode()

                    if etag:
                        self._etag_store[store_key] = etag
                    if last_modified:
                        self._lm_store[store_key] = last_modified

                    return ActionResult(
                        success=True,
                        message="Request succeeded (200 OK)",
                        data={"content": content, "status": response.status, "etag": etag, "last_modified": last_modified, "from_cache": False}
                    )
            except urllib.error.HTTPError as e:
                if e.code == 304:
                    return ActionResult(
                        success=True,
                        message="Not Modified (304) - use cached data",
                        data={"status": 304, "from_cache": True, "etag": self._etag_store.get(store_key), "last_modified": self._lm_store.get(store_key)}
                    )
                return ActionResult(success=False, message=f"HTTP {e.code}: {e.reason}", data={"status": e.code})

        except Exception as e:
            return ActionResult(success=False, message=f"Conditional request error: {e}")


class ApiETagAction(BaseAction):
    """ETag-based cache validation."""
    action_type = "api_etag"
    display_name = "API ETag验证"
    description = "基于ETag的缓存验证"

    def __init__(self):
        super().__init__()
        self._etag_store: Dict[str, str] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "validate")
            url = params.get("url", "")
            etag = params.get("etag")
            store_key = params.get("store_key", url)

            if operation == "store":
                if not etag:
                    return ActionResult(success=False, message="etag is required")
                self._etag_store[store_key] = etag
                return ActionResult(success=True, message=f"ETag stored for {store_key}", data={"etag": etag})

            elif operation == "get":
                stored_etag = self._etag_store.get(store_key)
                return ActionResult(success=True, message=f"ETag for {store_key}: {stored_etag}", data={"etag": stored_etag, "has_etag": store_key in self._etag_store})

            elif operation == "validate":
                if not url:
                    return ActionResult(success=False, message="url is required")

                stored_etag = self._etag_store.get(store_key)
                if not stored_etag:
                    return ActionResult(success=True, message="No ETag stored, request full response", data={"has_etag": False, "should_request": True})

                import urllib.request
                req = urllib.request.Request(url)
                req.add_header("If-None-Match", stored_etag)

                try:
                    with urllib.request.urlopen(req, timeout=30) as response:
                        new_etag = response.headers.get("ETag")
                        content = response.read().decode()
                        if new_etag:
                            self._etag_store[store_key] = new_etag
                        return ActionResult(success=True, message="Content changed, return new", data={"changed": True, "etag": new_etag, "from_cache": False})
                except urllib.error.HTTPError as e:
                    if e.code == 304:
                        return ActionResult(success=True, message="Content unchanged (304)", data={"changed": False, "from_cache": True})
                    return ActionResult(success=False, message=f"HTTP {e.code}", data={"status": e.code})

            elif operation == "clear":
                if store_key and store_key in self._etag_store:
                    del self._etag_store[store_key]
                return ActionResult(success=True, message=f"Cleared ETag for {store_key}")

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"ETag action error: {e}")


class ApiLastModifiedAction(BaseAction):
    """Last-Modified based conditional requests."""
    action_type = "api_last_modified"
    display_name = "API最后修改时间验证"
    description = "基于Last-Modified的条件请求"

    def __init__(self):
        super().__init__()
        self._lm_store: Dict[str, str] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "validate")
            url = params.get("url", "")
            last_modified = params.get("last_modified")
            store_key = params.get("store_key", url)

            if operation == "store":
                if not last_modified:
                    return ActionResult(success=False, message="last_modified is required")
                self._lm_store[store_key] = last_modified
                return ActionResult(success=True, message=f"Last-Modified stored for {store_key}", data={"last_modified": last_modified})

            elif operation == "get":
                stored_lm = self._lm_store.get(store_key)
                return ActionResult(success=True, message=f"Last-Modified for {store_key}: {stored_lm}", data={"last_modified": stored_lm})

            elif operation == "validate":
                if not url:
                    return ActionResult(success=False, message="url is required")

                stored_lm = self._lm_store.get(store_key)
                if not stored_lm:
                    return ActionResult(success=True, message="No Last-Modified stored", data={"has_last_modified": False, "should_request": True})

                import urllib.request
                req = urllib.request.Request(url)
                req.add_header("If-Modified-Since", stored_lm)

                try:
                    with urllib.request.urlopen(req, timeout=30) as response:
                        new_lm = response.headers.get("Last-Modified")
                        content = response.read().decode()
                        if new_lm:
                            self._lm_store[store_key] = new_lm
                        return ActionResult(success=True, message="Content updated", data={"changed": True, "last_modified": new_lm})
                except urllib.error.HTTPError as e:
                    if e.code == 304:
                        return ActionResult(success=True, message="Not modified since", data={"changed": False, "from_cache": True})
                    return ActionResult(success=False, message=f"HTTP {e.code}", data={"status": e.code})

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Last-Modified action error: {e}")


class ApiFreshnessCheckAction(BaseAction):
    """Check cache freshness before making requests."""
    action_type = "api_freshness_check"
    display_name = "API缓存新鲜度检查"
    description = "请求前检查缓存新鲜度"

    def __init__(self):
        super().__init__()
        self._cache_times: Dict[str, float] = {}
        self._cache_ttls: Dict[str, int] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "check")
            store_key = params.get("store_key")
            url = params.get("url")
            ttl = params.get("ttl", 300)

            if operation == "store":
                if not store_key:
                    return ActionResult(success=False, message="store_key required")
                self._cache_times[store_key] = time.time()
                self._cache_ttls[store_key] = ttl
                return ActionResult(success=True, message=f"Cached at {self._cache_times[store_key]}")

            elif operation == "check":
                if not store_key:
                    return ActionResult(success=False, message="store_key required")
                if store_key not in self._cache_times:
                    return ActionResult(success=True, message="Not cached", data={"fresh": False, "should_request": True})

                age = time.time() - self._cache_times[store_key]
                max_age = self._cache_ttls.get(store_key, ttl)
                fresh = age < max_age

                return ActionResult(
                    success=True,
                    message=f"Cache {'fresh' if fresh else 'stale'} (age={age:.1f}s, max={max_age}s)",
                    data={"fresh": fresh, "age": age, "max_age": max_age, "should_request": not fresh}
                )

            elif operation == "clear":
                if store_key and store_key in self._cache_times:
                    del self._cache_times[store_key]
                    if store_key in self._cache_ttls:
                        del self._cache_ttls[store_key]
                return ActionResult(success=True, message=f"Cleared cache for {store_key}")

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Freshness check error: {e}")
