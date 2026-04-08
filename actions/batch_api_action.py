"""Batch API operations action module for RabAI AutoClick.

Provides batch API operations:
- BatchApiCallAction: Execute multiple API calls in parallel
- BatchApiRetryAction: Retry failed API calls with backoff
- BatchApiThrottleAction: Throttle API calls to respect rate limits
- ApiBatchCollectorAction: Collect and aggregate responses
"""

import hashlib
import json
import time
import urllib.request
import urllib.parse
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional
from threading import Lock

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BatchApiCallAction(BaseAction):
    """Execute multiple API calls in parallel."""
    action_type = "batch_api_call"
    display_name = "批量API调用"
    description = "并行执行多个API调用"

    def __init__(self):
        super().__init__()
        self._executor = None

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            calls = params.get("calls", [])
            max_workers = params.get("max_workers", 5)
            timeout = params.get("timeout", 30)
            fail_fast = params.get("fail_fast", False)

            if not calls:
                return ActionResult(success=False, message="calls is required")

            results = []
            completed = 0
            total = len(calls)

            def execute_call(call_def: Dict) -> Dict:
                url = call_def.get("url", "")
                method = call_def.get("method", "GET").upper()
                headers = call_def.get("headers", {})
                body = call_def.get("body", None)
                call_id = call_def.get("id", str(time.time()))

                try:
                    req = urllib.request.Request(url, method=method)
                    for key, value in headers.items():
                        req.add_header(key, value)
                    if body:
                        body_bytes = json.dumps(body).encode("utf-8") if isinstance(body, dict) else body.encode("utf-8")
                        req.data = body_bytes
                        req.add_header("Content-Type", "application/json")
                    with urllib.request.urlopen(req, timeout=timeout) as response:
                        content = response.read()
                        return {
                            "id": call_id,
                            "success": True,
                            "status": response.status,
                            "body": json.loads(content.decode("utf-8")) if content else None,
                        }
                except Exception as e:
                    return {
                        "id": call_id,
                        "success": False,
                        "error": str(e),
                    }

            self._executor = ThreadPoolExecutor(max_workers=max_workers)
            futures = {self._executor.submit(execute_call, call): call for call in calls}

            for future in as_completed(futures):
                if fail_fast and not results:
                    for f in futures:
                        f.cancel()
                    result = future.result()
                    if not result.get("success", False):
                        return ActionResult(success=False, message=f"Fail-fast: {result.get('error')}", data=result)
                results.append(future.result())
                completed += 1

            self._executor.shutdown(wait=False)
            success_count = sum(1 for r in results if r.get("success", False))
            return ActionResult(
                success=success_count == total,
                message=f"{success_count}/{total} calls succeeded",
                data={"results": results, "total": total, "succeeded": success_count},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"BatchApiCall error: {e}")


class BatchApiRetryAction(BaseAction):
    """Retry failed API calls with exponential backoff."""
    action_type = "batch_api_retry"
    display_name = "批量API重试"
    description = "对失败的API调用进行指数退避重试"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            calls = params.get("calls", [])
            max_retries = params.get("max_retries", 3)
            base_delay = params.get("base_delay", 1.0)
            timeout = params.get("timeout", 30)

            if not calls:
                return ActionResult(success=False, message="calls is required")

            results = []

            for call in calls:
                url = call.get("url", "")
                method = call.get("method", "GET").upper()
                headers = call.get("headers", {})
                body = call.get("body", None)
                call_id = call.get("id", str(time.time()))
                last_error = None

                for attempt in range(max_retries + 1):
                    try:
                        req = urllib.request.Request(url, method=method)
                        for key, value in headers.items():
                            req.add_header(key, value)
                        if body and attempt == 0:
                            body_bytes = json.dumps(body).encode("utf-8") if isinstance(body, dict) else body.encode("utf-8")
                            req.data = body_bytes
                            req.add_header("Content-Type", "application/json")
                        with urllib.request.urlopen(req, timeout=timeout) as response:
                            content = response.read()
                            results.append({
                                "id": call_id,
                                "success": True,
                                "status": response.status,
                                "attempts": attempt + 1,
                                "body": json.loads(content.decode("utf-8")) if content else None,
                            })
                            break
                    except Exception as e:
                        last_error = str(e)
                        if attempt < max_retries:
                            delay = base_delay * (2 ** attempt)
                            time.sleep(delay)
                        else:
                            results.append({
                                "id": call_id,
                                "success": False,
                                "error": last_error,
                                "attempts": attempt + 1,
                            })

            success_count = sum(1 for r in results if r.get("success", False))
            return ActionResult(
                success=success_count == len(calls),
                message=f"{success_count}/{len(calls)} calls succeeded after retry",
                data={"results": results},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"BatchApiRetry error: {e}")


class BatchApiThrottleAction(BaseAction):
    """Throttle API calls to respect rate limits."""
    action_type = "batch_api_throttle"
    display_name = "API限流调用"
    description = "限流批量API调用"

    def __init__(self):
        super().__init__()
        self._lock = Lock()
        self._call_times: List[float] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            calls = params.get("calls", [])
            rate_limit = params.get("rate_limit", 10)
            window_seconds = params.get("window_seconds", 60)
            timeout = params.get("timeout", 30)

            if not calls:
                return ActionResult(success=False, message="calls is required")

            results = []

            def throttle_wait():
                now = time.time()
                with self._lock:
                    self._call_times = [t for t in self._call_times if now - t < window_seconds]
                    if len(self._call_times) >= rate_limit:
                        sleep_time = window_seconds - (now - self._call_times[0])
                        if sleep_time > 0:
                            time.sleep(sleep_time)
                            now = time.time()
                            self._call_times = [t for t in self._call_times if now - t < window_seconds]
                    self._call_times.append(now)

            for call in calls:
                throttle_wait()
                url = call.get("url", "")
                method = call.get("method", "GET").upper()
                headers = call.get("headers", {})
                body = call.get("body", None)
                call_id = call.get("id", str(time.time()))

                try:
                    req = urllib.request.Request(url, method=method)
                    for key, value in headers.items():
                        req.add_header(key, value)
                    if body:
                        body_bytes = json.dumps(body).encode("utf-8") if isinstance(body, dict) else body.encode("utf-8")
                        req.data = body_bytes
                        req.add_header("Content-Type", "application/json")
                    with urllib.request.urlopen(req, timeout=timeout) as response:
                        content = response.read()
                        results.append({
                            "id": call_id,
                            "success": True,
                            "status": response.status,
                        })
                except Exception as e:
                    results.append({
                        "id": call_id,
                        "success": False,
                        "error": str(e),
                    })

            success_count = sum(1 for r in results if r.get("success", False))
            return ActionResult(
                success=success_count == len(calls),
                message=f"{success_count}/{len(calls)} throttled calls succeeded",
                data={"results": results},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"BatchApiThrottle error: {e}")


class ApiBatchCollectorAction(BaseAction):
    """Collect and aggregate responses from multiple API calls."""
    action_type = "api_batch_collector"
    display_name = "API响应收集器"
    description = "收集并聚合多个API响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            responses = params.get("responses", [])
            group_by = params.get("group_by", None)
            aggregate = params.get("aggregate", False)

            if not responses:
                return ActionResult(success=False, message="responses is required")

            collected = {"total": len(responses), "items": responses}

            if group_by:
                grouped: Dict[str, List] = {}
                for resp in responses:
                    key = resp.get(group_by, "unknown")
                    if key not in grouped:
                        grouped[key] = []
                    grouped[key].append(resp)
                collected["grouped"] = grouped

            if aggregate:
                total_success = sum(1 for r in responses if r.get("success", False))
                total_failure = len(responses) - total_success
                collected["summary"] = {
                    "total": len(responses),
                    "success": total_success,
                    "failure": total_failure,
                }

            return ActionResult(success=True, message="Batch collected", data=collected)
        except Exception as e:
            return ActionResult(success=False, message=f"ApiBatchCollector error: {e}")
