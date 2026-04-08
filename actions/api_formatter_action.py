"""API formatter action module for RabAI AutoClick.

Provides API response/request formatting:
- ResponseFormatterAction: Format API responses
- RequestFormatterAction: Format API requests
- HeaderFormatterAction: Format HTTP headers
- ErrorFormatterAction: Format error responses
- PaginationFormatterAction: Format paginated responses
"""

from typing import Any, Dict, List, Optional, Union
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ResponseFormatterAction(BaseAction):
    """Format API responses."""
    action_type = "api_response_formatter"
    display_name = "响应格式化器"
    description = "格式化API响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            response = params.get("response", {})
            format_type = params.get("format_type", "standard")
            include_metadata = params.get("include_metadata", True)
            wrap_in_envelope = params.get("wrap_in_envelope", False)
            envelope_key = params.get("envelope_key", "data")

            if isinstance(response, dict):
                formatted = dict(response)
            else:
                formatted = {"value": response}

            if format_type == "standard":
                if "data" not in formatted and wrap_in_envelope:
                    formatted = {envelope_key: formatted}
                if include_metadata:
                    formatted["_meta"] = {
                        "timestamp": datetime.now().isoformat(),
                        "format": format_type,
                        "status": "success"
                    }

            elif format_type == "graphql":
                formatted = {
                    "data": response.get("data", response) if isinstance(response, dict) else {"result": response}
                }
                if include_metadata:
                    formatted["_meta"] = {"format": "graphql"}

            elif format_type == "rest":
                if "status" not in formatted:
                    formatted["status"] = "ok"
                if "timestamp" not in formatted and include_metadata:
                    formatted["timestamp"] = datetime.now().isoformat()

            elif format_type == "odata":
                formatted = {
                    "value": response if isinstance(response, list) else [response]
                }
                if include_metadata:
                    formatted["@odata.context"] = params.get("odata_context", "")

            return ActionResult(
                success=True,
                data={
                    "formatted": formatted,
                    "format_type": format_type,
                    "include_metadata": include_metadata
                },
                message=f"Formatted response as {format_type}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Response formatter error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["response"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"format_type": "standard", "include_metadata": True, "wrap_in_envelope": False, "envelope_key": "data", "odata_context": ""}


class RequestFormatterAction(BaseAction):
    """Format API requests."""
    action_type = "api_request_formatter"
    display_name = "请求格式化器"
    description = "格式化API请求"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            request = params.get("request", {})
            format_type = params.get("format_type", "standard")
            remove_empty = params.get("remove_empty", True)
            sort_keys = params.get("sort_keys", False)
            add_defaults = params.get("add_defaults", True)

            if isinstance(request, dict):
                formatted = dict(request)
            else:
                formatted = {"value": request}

            if remove_empty:
                formatted = {k: v for k, v in formatted.items() if v is not None and v != "" and v != []}

            if sort_keys:
                formatted = dict(sorted(formatted.items()))

            if add_defaults:
                if "Content-Type" not in formatted.get("headers", {}):
                    if "headers" not in formatted:
                        formatted["headers"] = {}
                    formatted["headers"]["Content-Type"] = "application/json"

            if format_type == "rest":
                method = formatted.get("method", "GET")
                if method in ("POST", "PUT", "PATCH"):
                    if "body" in formatted and isinstance(formatted["body"], dict):
                        formatted["body"] = {k: v for k, v in formatted["body"].items() if v is not None}

            return ActionResult(
                success=True,
                data={
                    "formatted": formatted,
                    "format_type": format_type,
                    "keys_count": len(formatted)
                },
                message=f"Formatted request: {len(formatted)} keys"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Request formatter error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["request"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"format_type": "standard", "remove_empty": True, "sort_keys": False, "add_defaults": True}


class HeaderFormatterAction(BaseAction):
    """Format HTTP headers."""
    action_type = "api_header_formatter"
    display_name = "头部格式化器"
    description = "格式化HTTP头部"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            headers = params.get("headers", {})
            case_style = params.get("case_style", "canonical")
            add_standard = params.get("add_standard", True)
            remove_empty = params.get("remove_empty", True)

            formatted = {}

            for key, value in headers.items():
                if remove_empty and (value is None or value == ""):
                    continue

                if case_style == "lower":
                    formatted_key = key.lower()
                elif case_style == "upper":
                    formatted_key = key.upper()
                elif case_style == "title":
                    formatted_key = key.title()
                else:
                    formatted_key = key

                formatted[formatted_key] = value

            if add_standard:
                standard_headers = {
                    "User-Agent": "RabAI-AutoClick/1.0",
                    "Accept": "application/json",
                    "Accept-Encoding": "gzip, deflate"
                }
                for key, value in standard_headers.items():
                    if key not in formatted:
                        formatted[key] = value

            return ActionResult(
                success=True,
                data={
                    "headers": formatted,
                    "count": len(formatted),
                    "case_style": case_style
                },
                message=f"Formatted {len(formatted)} headers in {case_style} case"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Header formatter error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["headers"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"case_style": "canonical", "add_standard": True, "remove_empty": True}


class ErrorFormatterAction(BaseAction):
    """Format error responses."""
    action_type = "api_error_formatter"
    display_name = "错误格式化器"
    description = "格式化错误响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            error = params.get("error", {})
            format_type = params.get("format_type", "standard")
            include_trace = params.get("include_trace", False)
            status_code = params.get("status_code", 500)

            error_message = error.get("message", str(error)) if isinstance(error, dict) else str(error)
            error_code = error.get("code", "INTERNAL_ERROR") if isinstance(error, dict) else "INTERNAL_ERROR"

            if format_type == "standard":
                formatted = {
                    "error": {
                        "message": error_message,
                        "code": error_code,
                        "status": status_code
                    }
                }
                if include_trace and isinstance(error, dict):
                    formatted["error"]["trace"] = error.get("trace", "")

            elif format_type == "detailed":
                formatted = {
                    "error": {
                        "message": error_message,
                        "code": error_code,
                        "status": status_code,
                        "timestamp": datetime.now().isoformat(),
                        "details": error.get("details", {}) if isinstance(error, dict) else {}
                    }
                }
                if include_trace:
                    import traceback
                    formatted["error"]["stack_trace"] = traceback.format_exc()

            elif format_type == "simple":
                formatted = {
                    "message": error_message,
                    "code": error_code
                }

            elif format_type == "rfc7807":
                formatted = {
                    "type": f"https://api.example.com/errors/{error_code.lower()}",
                    "title": error_code,
                    "status": status_code,
                    "detail": error_message,
                    "instance": params.get("instance", "/")
                }

            return ActionResult(
                success=False,
                data={
                    "error": formatted,
                    "status_code": status_code,
                    "format_type": format_type
                },
                message=f"Formatted error: {error_code}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error formatter error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["error"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"format_type": "standard", "include_trace": False, "status_code": 500, "instance": "/"}


class PaginationFormatterAction(BaseAction):
    """Format paginated responses."""
    action_type = "api_pagination_formatter"
    display_name = "分页格式化器"
    description = "格式化分页响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            page = params.get("page", 1)
            per_page = params.get("per_page", 20)
            total = params.get("total")
            format_type = params.get("format_type", "offset")

            if not isinstance(items, list):
                items = [items]

            if total is None:
                total = len(items)

            total_pages = (total + per_page - 1) // per_page if per_page > 0 else 0
            has_next = page < total_pages
            has_prev = page > 1

            if format_type == "offset":
                formatted = {
                    "data": items,
                    "pagination": {
                        "offset": (page - 1) * per_page,
                        "limit": per_page,
                        "total": total,
                        "count": len(items)
                    }
                }

            elif format_type == "page":
                formatted = {
                    "data": items,
                    "pagination": {
                        "page": page,
                        "per_page": per_page,
                        "total": total,
                        "total_pages": total_pages,
                        "has_next": has_next,
                        "has_prev": has_prev
                    }
                }

            elif format_type == "cursor":
                next_cursor = f"cursor_page_{page + 1}" if has_next else None
                prev_cursor = f"cursor_page_{page - 1}" if has_prev else None
                formatted = {
                    "data": items,
                    "pagination": {
                        "next_cursor": next_cursor,
                        "prev_cursor": prev_cursor,
                        "has_more": has_next,
                        "count": len(items)
                    }
                }

            elif format_type == "link":
                base_url = params.get("base_url", "")
                links = {}
                if has_next:
                    links["next"] = f"{base_url}?page={page + 1}&per_page={per_page}"
                if has_prev:
                    links["prev"] = f"{base_url}?page={page - 1}&per_page={per_page}"
                links["first"] = f"{base_url}?page=1&per_page={per_page}"
                links["last"] = f"{base_url}?page={total_pages}&per_page={per_page}"

                formatted = {
                    "data": items,
                    "pagination": {
                        "total": total,
                        "count": len(items)
                    },
                    "_links": links
                }

            return ActionResult(
                success=True,
                data={
                    "formatted": formatted,
                    "format_type": format_type,
                    "page": page,
                    "total_pages": total_pages,
                    "items_count": len(items)
                },
                message=f"Formatted pagination: page {page}/{total_pages}, {len(items)} items"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pagination formatter error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["items"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"page": 1, "per_page": 20, "total": None, "format_type": "offset", "base_url": ""}
