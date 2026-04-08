"""API Gateway Proxy Action Module.

Handles API gateway proxying, request routing,
header manipulation, and response transformation.
"""

from __future__ import annotations

import sys
import os
import time
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class RouteRule:
    """A routing rule for the proxy."""
    path_pattern: str
    upstream_url: str
    methods: List[str] = field(default_factory=lambda: ["GET", "POST"])
    strip_path: bool = False
    add_prefix: str = ""


@dataclass
class HeaderTransform:
    """Header transformation rule."""
    header_name: str
    action: str
    value: str = ""


class APIGatewayProxyAction(BaseAction):
    """
    API Gateway proxy and routing.

    Handles request proxying, header transformations,
    URL rewrites, and response modifications.

    Example:
        proxy = APIGatewayProxyAction()
        result = proxy.execute(ctx, {"action": "add_route", "path": "/api/*", "upstream": "http://backend:8080"})
    """
    action_type = "api_gateway_proxy"
    display_name = "API网关代理"
    description = "API网关代理：路由、头转换、URL重写"

    def __init__(self) -> None:
        super().__init__()
        self._routes: List[RouteRule] = []
        self._request_transforms: List[HeaderTransform] = []
        self._response_transforms: List[HeaderTransform] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "add_route":
                return self._add_route(params)
            elif action == "remove_route":
                return self._remove_route(params)
            elif action == "proxy_request":
                return self._proxy_request(params)
            elif action == "transform_request":
                return self._transform_request(params)
            elif action == "transform_response":
                return self._transform_response(params)
            elif action == "list_routes":
                return self._list_routes(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Proxy error: {str(e)}")

    def _add_route(self, params: Dict[str, Any]) -> ActionResult:
        path = params.get("path", "")
        upstream = params.get("upstream", "")
        methods = params.get("methods", ["GET", "POST"])

        if not path or not upstream:
            return ActionResult(success=False, message="path and upstream are required")

        route = RouteRule(path_pattern=path, upstream_url=upstream, methods=methods)
        self._routes.append(route)

        return ActionResult(success=True, message=f"Route added: {path} -> {upstream}")

    def _remove_route(self, params: Dict[str, Any]) -> ActionResult:
        path = params.get("path", "")
        self._routes = [r for r in self._routes if r.path_pattern != path]
        return ActionResult(success=True, message=f"Route removed: {path}")

    def _proxy_request(self, params: Dict[str, Any]) -> ActionResult:
        path = params.get("path", "/")
        method = params.get("method", "GET")
        headers = params.get("headers", {})
        body = params.get("body")

        route = self._find_route(path, method)

        if not route:
            return ActionResult(success=False, message=f"No route found for {method} {path}")

        upstream_url = self._build_upstream_url(route, path)

        return ActionResult(success=True, message=f"Proxying to {upstream_url}", data={"upstream_url": upstream_url, "route": route.path_pattern})

    def _transform_request(self, params: Dict[str, Any]) -> ActionResult:
        headers = params.get("headers", {})
        transforms = params.get("transforms", [])

        for t in transforms:
            header_name = t.get("header_name", "")
            action = t.get("action", "add")
            value = t.get("value", "")

            if action == "add":
                headers[header_name] = value
            elif action == "remove":
                headers.pop(header_name, None)
            elif action == "update":
                if header_name in headers:
                    headers[header_name] = value

        return ActionResult(success=True, message="Request transformed", data={"headers": headers})

    def _transform_response(self, params: Dict[str, Any]) -> ActionResult:
        status_code = params.get("status_code", 200)
        headers = params.get("headers", {})
        body = params.get("body")

        return ActionResult(success=True, message="Response transformed", data={"status_code": status_code, "headers": headers})

    def _list_routes(self, params: Dict[str, Any]) -> ActionResult:
        routes = [{"path": r.path_pattern, "upstream": r.upstream_url, "methods": r.methods} for r in self._routes]
        return ActionResult(success=True, data={"routes": routes, "count": len(routes)})

    def _find_route(self, path: str, method: str) -> Optional[RouteRule]:
        for route in self._routes:
            if self._path_matches(route.path_pattern, path) and method in route.methods:
                return route
        return None

    def _path_matches(self, pattern: str, path: str) -> bool:
        if pattern == path:
            return True
        if pattern.endswith("*"):
            prefix = pattern[:-1]
            return path.startswith(prefix)
        return False

    def _build_upstream_url(self, route: RouteRule, path: str) -> str:
        upstream = route.upstream_url.rstrip("/")
        remaining_path = path

        if route.strip_path:
            pattern_parts = route.path_pattern.rstrip("/*").split("/")
            path_parts = remaining_path.split("/")
            if len(path_parts) >= len(pattern_parts):
                remaining_path = "/".join(path_parts[len(pattern_parts):])

        if route.add_prefix:
            remaining_path = f"{route.add_prefix}/{remaining_path}"

        return f"{upstream}/{remaining_path.lstrip('/')}"
