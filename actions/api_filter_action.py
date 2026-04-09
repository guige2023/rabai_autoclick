"""API Filter Action Module.

Provides request/response filtering, transformation, and routing
capabilities for API interactions.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Pattern, Union

logger = logging.getLogger(__name__)


class FilterType(Enum):
    """Filter types."""
    REQUEST_HEADER = "request_header"
    RESPONSE_HEADER = "response_header"
    REQUEST_BODY = "request_body"
    RESPONSE_BODY = "response_body"
    QUERY_PARAM = "query_param"
    ROUTE = "route"
    RATE_LIMIT = "rate_limit"
    AUTH = "auth"


@dataclass
class FilterRule:
    """Represents a filter rule."""
    name: str
    filter_type: FilterType
    pattern: Optional[str] = None
    regex: Optional[Pattern] = None
    priority: int = 0
    enabled: bool = True
    config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FilterResult:
    """Result of filter processing."""
    filter_name: str
    filter_type: FilterType
    success: bool
    modified: bool = False
    error: Optional[str] = None


class HeaderFilter:
    """Filter for modifying headers."""

    def __init__(self):
        self._add_headers: Dict[str, str] = {}
        self._remove_headers: List[str] = []
        self._transform_headers: Dict[str, Callable[[str], str]] = {}

    def add_header(self, key: str, value: str) -> None:
        """Add a header to be added."""
        self._add_headers[key] = value

    def remove_header(self, key: str) -> None:
        """Remove a header."""
        if key not in self._remove_headers:
            self._remove_headers.append(key)

    def transform_header(self, key: str, transform_fn: Callable[[str], str]) -> None:
        """Register a header transformation function."""
        self._transform_headers[key] = transform_fn

    async def apply_request(self, headers: Dict[str, str]) -> Dict[str, str]:
        """Apply header filters to request headers."""
        result = dict(headers)

        # Remove headers
        for key in self._remove_headers:
            result.pop(key, None)

        # Add headers
        result.update(self._add_headers)

        # Transform headers
        for key, fn in self._transform_headers.items():
            if key in result:
                result[key] = fn(result[key])

        return result

    async def apply_response(self, headers: Dict[str, str]) -> Dict[str, str]:
        """Apply header filters to response headers."""
        return await self.apply_request(headers)


class QueryParamFilter:
    """Filter for query parameters."""

    def __init__(self):
        self._add_params: Dict[str, str] = {}
        self._remove_params: List[str] = []
        self._rename_params: Dict[str, str] = {}

    def add_param(self, key: str, value: str) -> None:
        """Add a query parameter."""
        self._add_params[key] = value

    def remove_param(self, key: str) -> None:
        """Remove a query parameter."""
        if key not in self._remove_params:
            self._remove_params.append(key)

    def rename_param(self, old_key: str, new_key: str) -> None:
        """Rename a query parameter."""
        self._rename_params[old_key] = new_key

    async def apply(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Apply query parameter filters."""
        result = dict(params)

        # Remove params
        for key in self._remove_params:
            result.pop(key, None)

        # Rename params
        for old_key, new_key in self._rename_params.items():
            if old_key in result:
                result[new_key] = result.pop(old_key)

        # Add params
        result.update(self._add_params)

        return result


class BodyFilter:
    """Filter for request/response bodies."""

    def __init__(self):
        self._field_transforms: Dict[str, Callable[[Any], Any]] = {}
        self._field_renames: Dict[str, str] = {}
        self._field_removes: List[str] = []

    def transform_field(self, field_path: str, transform_fn: Callable[[Any], Any]) -> None:
        """Register a field transformation."""
        self._field_transforms[field_path] = transform_fn

    def rename_field(self, old_path: str, new_path: str) -> None:
        """Register a field rename."""
        self._field_renames[old_path] = new_path

    def remove_field(self, field_path: str) -> None:
        """Register a field for removal."""
        if field_path not in self._field_removes:
            self._field_removes.append(field_path)

    async def apply(self, body: Any) -> Any:
        """Apply body filters."""
        if isinstance(body, dict):
            result = dict(body)

            # Remove fields
            for field_path in self._field_removes:
                result.pop(field_path, None)

            # Rename fields
            for old_path, new_path in self._field_renames.items():
                if old_path in result:
                    result[new_path] = result.pop(old_path)

            # Transform fields
            for field_path, fn in self._field_transforms.items():
                if field_path in result:
                    result[field_path] = fn(result[field_path])

            return result

        return body


class RouteFilter:
    """Filter for routing requests."""

    def __init__(self):
        self._routes: Dict[str, str] = {}  # pattern -> target
        self._default_target: Optional[str] = None

    def add_route(self, path_pattern: str, target: str) -> None:
        """Add a route mapping."""
        self._routes[path_pattern] = target

    def set_default(self, target: str) -> None:
        """Set the default route target."""
        self._default_target = target

    async def route(self, path: str) -> Optional[str]:
        """Route a path to its target."""
        for pattern, target in self._routes.items():
            if re.match(pattern, path):
                return target
        return self._default_target


class APIFilterChain:
    """Chain of filters applied in order."""

    def __init__(self):
        self._filters: List[tuple[int, FilterRule, Callable]] = []
        self._header_filter = HeaderFilter()
        self._param_filter = QueryParamFilter()
        self._body_filter = BodyFilter()
        self._route_filter = RouteFilter()

    def add_filter(
        self,
        rule: FilterRule,
        handler: Callable
    ) -> None:
        """Add a filter to the chain."""
        self._filters.append((rule.priority, rule, handler))
        self._filters.sort(key=lambda x: x[0])

    async def process_request(
        self,
        url: str,
        method: str,
        headers: Dict[str, str],
        params: Optional[Dict[str, Any]],
        body: Optional[Any]
    ) -> Dict[str, Any]:
        """Process a request through the filter chain."""
        # Apply header filter
        filtered_headers = await self._header_filter.apply_request(headers)

        # Apply param filter
        filtered_params = await self._param_filter.apply(params or {})

        # Apply body filter
        filtered_body = await self._body_filter.apply(body)

        # Apply route filter
        route_target = await self._route_filter.route(url)

        return {
            "url": route_target or url,
            "method": method,
            "headers": filtered_headers,
            "params": filtered_params,
            "body": filtered_body
        }

    async def process_response(
        self,
        status_code: int,
        headers: Dict[str, str],
        body: Any
    ) -> Dict[str, Any]:
        """Process a response through the filter chain."""
        filtered_headers = await self._header_filter.apply_response(headers)
        filtered_body = await self._body_filter.apply(body)

        return {
            "status_code": status_code,
            "headers": filtered_headers,
            "body": filtered_body
        }


class APIFilterAction:
    """Main action class for API filtering."""

    def __init__(self):
        self._chain = APIFilterChain()
        self._rules: Dict[str, FilterRule] = {}

    def add_header(self, key: str, value: str) -> None:
        """Add a header to be injected."""
        self._chain._header_filter.add_header(key, value)

    def remove_header(self, key: str) -> None:
        """Remove a header."""
        self._chain._header_filter.remove_header(key)

    def add_query_param(self, key: str, value: str) -> None:
        """Add a query parameter."""
        self._chain._param_filter.add_param(key, value)

    def remove_query_param(self, key: str) -> None:
        """Remove a query parameter."""
        self._chain._param_filter.remove_param(key)

    def add_route(self, path_pattern: str, target: str) -> None:
        """Add a route mapping."""
        self._chain._route_filter.add_route(path_pattern, target)

    def transform_field(self, field_path: str, transform_fn: Callable[[Any], Any]) -> None:
        """Register a field transformation."""
        self._chain._body_filter.transform_field(field_path, transform_fn)

    async def execute(
        self,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the API filter action.

        Args:
            context: Dictionary containing:
                - operation: Operation to perform
                - url: Request URL
                - method: HTTP method
                - headers: Request headers
                - params: Query parameters
                - body: Request body

        Returns:
            Dictionary with filtered request/response.
        """
        operation = context.get("operation", "filter_request")

        if operation == "filter_request":
            result = await self._chain.process_request(
                url=context.get("url", ""),
                method=context.get("method", "GET"),
                headers=context.get("headers", {}),
                params=context.get("params"),
                body=context.get("body")
            )
            return {
                "success": True,
                "filtered": result
            }

        elif operation == "filter_response":
            result = await self._chain.process_response(
                status_code=context.get("status_code", 200),
                headers=context.get("headers", {}),
                body=context.get("body")
            )
            return {
                "success": True,
                "filtered": result
            }

        elif operation == "add_header":
            self.add_header(
                context.get("key", ""),
                context.get("value", "")
            )
            return {"success": True}

        elif operation == "remove_header":
            self.remove_header(context.get("key", ""))
            return {"success": True}

        elif operation == "add_query_param":
            self.add_query_param(
                context.get("key", ""),
                context.get("value", "")
            )
            return {"success": True}

        elif operation == "remove_query_param":
            self.remove_query_param(context.get("key", ""))
            return {"success": True}

        elif operation == "add_route":
            self.add_route(
                context.get("pattern", ""),
                context.get("target", "")
            )
            return {"success": True}

        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
