"""API prefetch action module for RabAI AutoClick.

Provides API prefetching operations:
- ApiPrefetchAction: Prefetch and cache related resources
- ApiPrefetchHintsAction: Use HTTP/2 server hints for prefetch
- ApiPrefetchBatchAction: Batch prefetch multiple URLs
- ApiPrefetchSchedulerAction: Schedule prefetch operations
"""

import time
from typing import Any, Dict, List, Optional, Set
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ApiPrefetchAction(BaseAction):
    """Prefetch and cache related resources."""
    action_type = "api_prefetch"
    display_name = "API预取"
    description = "预取并缓存相关资源"

    def __init__(self):
        super().__init__()
        self._prefetch_cache: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "prefetch")
            urls = params.get("urls", [])
            base_url = params.get("base_url", "")
            related_paths = params.get("related_paths", [])
            ttl = params.get("ttl", 300)
            parallel = params.get("parallel", True)

            if operation == "prefetch":
                if not urls and not base_url:
                    return ActionResult(success=False, message="urls or base_url required")

                if base_url and related_paths:
                    urls = [base_url.rstrip("/") + "/" + p.lstrip("/") for p in related_paths]

                fetched = 0
                failed = []

                if parallel:
                    with ThreadPoolExecutor(max_workers=min(len(urls), 5)) as executor:
                        futures = {executor.submit(self._fetch_url, url, ttl): url for url in urls}
                        for future in as_completed(futures):
                            url = futures[future]
                            try:
                                result = future.result()
                                if result.get("success"):
                                    fetched += 1
                                else:
                                    failed.append({"url": url, "error": result.get("error")})
                            except Exception as e:
                                failed.append({"url": url, "error": str(e)})
                else:
                    for url in urls:
                        result = self._fetch_url(url, ttl)
                        if result.get("success"):
                            fetched += 1
                        else:
                            failed.append({"url": url, "error": result.get("error")})

                return ActionResult(
                    success=fetched > 0,
                    message=f"Prefetched {fetched}/{len(urls)} URLs",
                    data={"fetched": fetched, "failed": failed, "total": len(urls)}
                )

            elif operation == "get":
                url = params.get("url")
                if not url:
                    return ActionResult(success=False, message="url required")

                if url in self._prefetch_cache:
                    entry = self._prefetch_cache[url]
                    age = time.time() - entry["timestamp"]
                    if age < entry["ttl"]:
                        return ActionResult(success=True, message="Cache hit", data={"data": entry["data"], "from_cache": True, "age": age})
                    return ActionResult(success=False, message="Cache expired", data={"from_cache": False})

                return ActionResult(success=False, message="Not prefetched", data={"from_cache": False})

            elif operation == "clear":
                count = len(self._prefetch_cache)
                self._prefetch_cache.clear()
                return ActionResult(success=True, message=f"Cleared {count} prefetch entries")

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Prefetch error: {e}")

    def _fetch_url(self, url: str, ttl: int) -> Dict[str, Any]:
        """Fetch and cache a URL."""
        try:
            import urllib.request
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=10) as response:
                data = response.read()
                self._prefetch_cache[url] = {
                    "data": data,
                    "timestamp": time.time(),
                    "ttl": ttl,
                    "status": response.status,
                }
                return {"success": True, "url": url, "status": response.status}
        except Exception as e:
            return {"success": False, "url": url, "error": str(e)}


class ApiPrefetchHintsAction(BaseAction):
    """Use HTTP/2 server hints for prefetch."""
    action_type = "api_prefetch_hints"
    display_name = "API预取提示"
    description = "使用HTTP/2服务器提示预取"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            html_content = params.get("html_content")
            base_url = params.get("base_url", "")
            prefetch_patterns = params.get("prefetch_patterns", ["preload", "prefetch", "preconnect"])
            dns_prefetch = params.get("dns_prefetch", True)
            timeout = params.get("timeout", 5)

            if not html_content and not base_url:
                return ActionResult(success=False, message="html_content or base_url required")

            if html_content:
                import re
                hints = []

                for pattern in prefetch_patterns:
                    if pattern == "preload":
                        matches = re.findall(r'<link[^>]+rel=["\']preload["\'][^>]+href=["\']([^"\']+)["\']', html_content, re.I)
                        hints.extend([{"type": "preload", "url": m} for m in matches])
                    elif pattern == "prefetch":
                        matches = re.findall(r'<link[^>]+rel=["\'prefetch["\'][^>]+href=["\']([^"\']+)["\']', html_content, re.I)
                        hints.extend([{"type": "prefetch", "url": m} for m in matches])

                dns_pattern = r'<link[^>]+rel=["\'dns-prefetch["\'][^>]+href=["\']([^"\']+)["\']'
                dns_matches = re.findall(dns_pattern, html_content, re.I)
                hints.extend([{"type": "dns-prefetch", "url": m} for m in dns_matches])

                prefetch_urls = [h["url"] for h in hints if h["type"] in ("preload", "prefetch")]

                if base_url and prefetch_urls:
                    prefetch_urls = [u if u.startswith("http") else base_url.rstrip("/") + "/" + u.lstrip("/") for u in prefetch_urls]

                return ActionResult(
                    success=True,
                    message=f"Found {len(hints)} resource hints",
                    data={"hints": hints, "prefetch_urls": prefetch_urls, "count": len(hints)}
                )

            return ActionResult(success=False, message="No HTML content provided")
        except Exception as e:
            return ActionResult(success=False, message=f"Prefetch hints error: {e}")


class ApiPrefetchBatchAction(BaseAction):
    """Batch prefetch multiple URLs."""
    action_type = "api_prefetch_batch"
    display_name = "API批量预取"
    description = "批量预取多个URL"

    def __init__(self):
        super().__init__()
        self._batch_cache: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            urls = params.get("urls", [])
            batch_size = params.get("batch_size", 10)
            ttl = params.get("ttl", 300)
            dedup = params.get("deduplicate", True)

            if not urls:
                return ActionResult(success=False, message="urls list is required")

            if dedup:
                unique_urls = list(dict.fromkeys(urls))
            else:
                unique_urls = urls

            results = []
            for i in range(0, len(unique_urls), batch_size):
                batch = unique_urls[i:i + batch_size]
                batch_results = self._fetch_batch(batch, ttl)
                results.extend(batch_results)

            success_count = sum(1 for r in results if r.get("success"))
            return ActionResult(
                success=success_count == len(unique_urls),
                message=f"Batch prefetch: {success_count}/{len(unique_urls)} succeeded",
                data={"results": results, "total": len(unique_urls), "succeeded": success_count}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch prefetch error: {e}")

    def _fetch_batch(self, urls: List[str], ttl: int) -> List[Dict[str, Any]]:
        """Fetch a batch of URLs."""
        with ThreadPoolExecutor(max_workers=min(len(urls), 5)) as executor:
            futures = {executor.submit(self._fetch_single, url, ttl): url for url in urls}
            return [future.result() for future in as_completed(futures)]

    def _fetch_single(self, url: str, ttl: int) -> Dict[str, Any]:
        """Fetch a single URL."""
        try:
            import urllib.request
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=30) as response:
                data = response.read()
                self._batch_cache[url] = {
                    "data": data,
                    "timestamp": time.time(),
                    "ttl": ttl,
                    "status": response.status,
                }
                return {"success": True, "url": url, "size": len(data)}
        except Exception as e:
            return {"success": False, "url": url, "error": str(e)}


class ApiPrefetchSchedulerAction(BaseAction):
    """Schedule prefetch operations."""
    action_type = "api_prefetch_scheduler"
    display_name = "API预取调度器"
    description = "调度预取操作"

    def __init__(self):
        super().__init__()
        self._scheduled_prefetchs: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "schedule")
            prefetch_id = params.get("prefetch_id")
            urls = params.get("urls", [])
            schedule_time = params.get("schedule_time")
            interval = params.get("interval")
            ttl = params.get("ttl", 300)

            if operation == "schedule":
                if not urls or not prefetch_id:
                    return ActionResult(success=False, message="prefetch_id and urls required")

                prefetch = {
                    "urls": urls,
                    "schedule_time": schedule_time,
                    "interval": interval,
                    "ttl": ttl,
                    "scheduled_at": datetime.now().isoformat(),
                    "last_run": None,
                    "next_run": schedule_time,
                    "active": True,
                }

                if schedule_time:
                    try:
                        from datetime import datetime as dt
                        prefetch["next_run"] = dt.fromisoformat(schedule_time).isoformat()
                    except ValueError:
                        pass

                self._scheduled_prefetchs[prefetch_id] = prefetch
                return ActionResult(success=True, message=f"Scheduled prefetch '{prefetch_id}'", data={"prefetch_id": prefetch_id})

            elif operation == "list":
                return ActionResult(success=True, message=f"{len(self._scheduled_prefetchs)} scheduled", data={"scheduled": self._scheduled_prefetchs})

            elif operation == "cancel":
                if prefetch_id and prefetch_id in self._scheduled_prefetchs:
                    del self._scheduled_prefetchs[prefetch_id]
                    return ActionResult(success=True, message=f"Cancelled '{prefetch_id}'")
                return ActionResult(success=False, message="Not found")

            elif operation == "trigger":
                if prefetch_id and prefetch_id in self._scheduled_prefetchs:
                    prefetch = self._scheduled_prefetchs[prefetch_id]
                    prefetch["last_run"] = datetime.now().isoformat()
                    return ActionResult(success=True, message=f"Triggered '{prefetch_id}'", data={"urls": prefetch["urls"]})
                return ActionResult(success=False, message="Not found")

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Prefetch scheduler error: {e}")
