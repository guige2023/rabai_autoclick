"""API response handling action module for RabAI AutoClick.

Provides API response processing operations:
- ResponseParserAction: Parse API responses
- ResponseTransformAction: Transform API responses
- PaginationHandlerAction: Handle paginated API responses
- ErrorHandlerAction: Handle API errors
"""

import json
import re
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse, parse_qs

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ResponseParserAction(BaseAction):
    """Parse API responses."""
    action_type = "response_parser"
    display_name = "响应解析"
    description = "解析API响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            response = params.get("response", {})
            format = params.get("format", "json")
            extract_path = params.get("extract_path", None)

            if isinstance(response, str):
                if format == "json":
                    try:
                        response = json.loads(response)
                    except json.JSONDecodeError:
                        return ActionResult(success=False, message="Invalid JSON response")

            if extract_path:
                extracted = self._extract_by_path(response, extract_path)
                return ActionResult(success=True, message=f"Extracted from path {extract_path}", data={"data": extracted})

            if isinstance(response, dict):
                data_field = response.get("data") or response.get("result") or response.get("items") or response
                return ActionResult(success=True, message="Response parsed", data={"data": data_field, "raw": response})

            return ActionResult(success=True, message="Response parsed", data={"data": response})
        except Exception as e:
            return ActionResult(success=False, message=f"ResponseParser error: {e}")

    def _extract_by_path(self, obj: Any, path: str) -> Any:
        parts = path.strip("/").split("/")
        current = obj
        for part in parts:
            if not part:
                continue
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list) and part.isdigit():
                idx = int(part)
                current = current[idx] if idx < len(current) else None
            else:
                return None
        return current


class ResponseTransformAction(BaseAction):
    """Transform API responses."""
    action_type = "response_transform"
    display_name = "响应转换"
    description = "转换API响应格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            response = params.get("response", {})
            transformations = params.get("transformations", [])
            output_format = params.get("output_format", "unchanged")

            if isinstance(response, str):
                try:
                    response = json.loads(response)
                except Exception:
                    pass

            if not transformations:
                return ActionResult(success=True, message="No transformations", data={"data": response})

            if isinstance(response, dict):
                for t in transformations:
                    t_type = t.get("type", "rename")
                    if t_type == "rename":
                        old_key = t.get("old_key")
                        new_key = t.get("new_key")
                        if old_key in response:
                            response[new_key] = response.pop(old_key)
                    elif t_type == "remove":
                        key = t.get("key")
                        if key in response:
                            del response[key]
                    elif t_type == "add":
                        key = t.get("key")
                        value = t.get("value")
                        response[key] = value
                    elif t_type == "flatten":
                        prefix = t.get("prefix", "")
                        nested = t.get("nested", {})
                        for nk, nv in nested.items():
                            response[f"{prefix}{nk}"] = nv

            elif isinstance(response, list):
                transformed = []
                for item in response:
                    if isinstance(item, dict):
                        for t in transformations:
                            t_type = t.get("type", "rename")
                            if t_type == "rename":
                                old_key = t.get("old_key")
                                new_key = t.get("new_key")
                                if old_key in item:
                                    item[new_key] = item.pop(old_key)
                    transformed.append(item)
                response = transformed

            return ActionResult(success=True, message="Response transformed", data={"data": response})
        except Exception as e:
            return ActionResult(success=False, message=f"ResponseTransform error: {e}")


class PaginationHandlerAction(BaseAction):
    """Handle paginated API responses."""
    action_type = "pagination_handler"
    display_name = "分页处理"
    description = "处理分页API响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "handle")
            pages = params.get("pages", [])
            pagination_config = params.get("pagination_config", {})
            cursor = params.get("cursor", None)
            limit = params.get("limit", 100)

            if action == "handle":
                page_field = pagination_config.get("page_field", "page")
                per_page_field = pagination_config.get("per_page_field", "per_page")
                total_field = pagination_config.get("total_field", "total")
                next_field = pagination_config.get("next_field", "next_cursor")
                data_field = pagination_config.get("data_field", "data")

                all_items = []
                for page in pages:
                    page_data = page.get(data_field, page.get("items", page))
                    if isinstance(page_data, list):
                        all_items.extend(page_data)

                has_more = any(page.get(next_field) or page.get("has_more", False) for page in pages)
                next_cursor = None
                for page in reversed(pages):
                    if page.get(next_field):
                        next_cursor = page.get(next_field)
                        break

                total = 0
                for page in pages:
                    if total_field in page:
                        total = page.get(total_field)
                        break

                return ActionResult(
                    success=True,
                    message=f"Collected {len(all_items)} items from {len(pages)} pages",
                    data={
                        "items": all_items,
                        "total": total,
                        "page_count": len(pages),
                        "has_more": has_more,
                        "next_cursor": next_cursor,
                    },
                )

            elif action == "merge":
                if not pages:
                    return ActionResult(success=False, message="No pages to merge")

                merged = []
                for page in pages:
                    if isinstance(page, list):
                        merged.extend(page)
                    elif isinstance(page, dict):
                        data = page.get("data", page.get("items", page))
                        if isinstance(data, list):
                            merged.extend(data)

                return ActionResult(
                    success=True,
                    message=f"Merged {len(merged)} items from {len(pages)} pages",
                    data={"items": merged, "count": len(merged), "page_count": len(pages)},
                )

            elif action == "slice":
                items = params.get("items", [])
                start = params.get("start", 0)
                end = params.get("end", limit)

                sliced = items[start:end]
                return ActionResult(
                    success=True,
                    message=f"Sliced items [{start}:{end}]: {len(sliced)} items",
                    data={"items": sliced, "count": len(sliced), "has_more": end < len(items)},
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"PaginationHandler error: {e}")


class ErrorHandlerAction(BaseAction):
    """Handle API errors."""
    action_type = "error_handler"
    display_name = "错误处理"
    description = "处理API错误"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            error = params.get("error", {})
            error_mapping = params.get("error_mapping", {})
            retry_config = params.get("retry_config", {})
            fallback = params.get("fallback", None)

            if isinstance(error, str):
                error = {"message": error, "code": "UNKNOWN"}

            code = error.get("code", error.get("status", "UNKNOWN"))
            message = error.get("message", str(error))
            status = error.get("status", error.get("http_status", 0))

            error_type = self._classify_error(code, status, message)

            should_retry = self._should_retry(error_type, retry_config)

            if error_mapping:
                mapped = error_mapping.get(code) or error_mapping.get(error_type)
                if mapped:
                    message = mapped

            response = {
                "error_type": error_type,
                "code": code,
                "message": message,
                "status": status,
                "should_retry": should_retry,
            }

            if should_retry and retry_config:
                response["retry_after"] = self._compute_retry_delay(error_type, retry_config)

            if fallback and not should_retry:
                response["fallback_used"] = fallback

            return ActionResult(
                success=False,
                message=f"Error handled: {error_type}",
                data=response,
            )
        except Exception as e:
            return ActionResult(success=False, message=f"ErrorHandler error: {e}")

    def _classify_error(self, code: str, status: int, message: str) -> str:
        if status == 400:
            return "BAD_REQUEST"
        elif status == 401:
            return "UNAUTHORIZED"
        elif status == 403:
            return "FORBIDDEN"
        elif status == 404:
            return "NOT_FOUND"
        elif status == 429:
            return "RATE_LIMITED"
        elif 400 <= status < 500:
            return "CLIENT_ERROR"
        elif status == 500:
            return "SERVER_ERROR"
        elif 500 <= status < 600:
            return "SERVER_ERROR"
        elif "timeout" in message.lower():
            return "TIMEOUT"
        elif "network" in message.lower() or "connection" in message.lower():
            return "NETWORK_ERROR"
        elif "auth" in code.lower() or "token" in code.lower():
            return "AUTH_ERROR"
        return "UNKNOWN_ERROR"

    def _should_retry(self, error_type: str, config: Dict) -> bool:
        retry_on = config.get("retry_on", ["RATE_LIMITED", "TIMEOUT", "NETWORK_ERROR", "SERVER_ERROR"])
        return error_type in retry_on

    def _compute_retry_delay(self, error_type: str, config: Dict) -> int:
        base_delay = config.get("base_delay", 1)
        if error_type == "RATE_LIMITED":
            return config.get("rate_limit_delay", 60)
        return min(base_delay * (2 ** config.get("attempt", 0)), config.get("max_delay", 300))
