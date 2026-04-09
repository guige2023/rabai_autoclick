"""API Gateway Action Module.

Provides API gateway capabilities including routing, rate limiting,
request/response transformation, and backend aggregation.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class RoutingStrategy(Enum):
    """Route selection strategies."""
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    WEIGHTED = "weighted"
    LEAST_LOADED = "least_loaded"
    CONSISTENT_HASH = "consistent_hash"


@dataclass
class Route:
    """An API route definition."""
    path_pattern: str
    backend_url: str
    methods: List[str] = field(default_factory=lambda: ["GET"])
    timeout: float = 30.0
    weight: float = 1.0
    retries: int = 1
    strip_path: bool = False
    add_prefix: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    priority: int = 0

    _compiled_pattern: re.Pattern = field(init=False, repr=False)

    def __post_init__(self) -> None:
        """Compile the path pattern for matching."""
        self._compiled_pattern = re.compile(self.path_pattern)

    def matches(self, path: str, method: str) -> bool:
        """Check if this route matches the given path and method."""
        if method.upper() not in [m.upper() for m in self.methods]:
            return False
        return self._compiled_pattern.match(path) is not None


@dataclass
class RouteMatch:
    """Result of route matching."""
    route: Route
    path_params: Dict[str, str]
    backend_path: str


@dataclass
class GatewayStats:
    """Gateway statistics."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    routed_requests: int = 0
    transformed_requests: int = 0
    transformed_responses: int = 0
    route_not_found: int = 0
    backend_errors: int = 0
    avg_latency_ms: float = 0.0


class RequestTransformer:
    """Transform requests before forwarding."""

    def __init__(self):
        self._header_mappings: Dict[str, str] = {}
        self._add_headers: Dict[str, str] = {}
        self._remove_headers: List[str] = []
        self._path_replacements: List[Tuple[str, str]] = []

    def add_header_mapping(self, from_header: str, to_header: str) -> None:
        """Map one header to another."""
        self._header_mappings[from_header.lower()] = to_header

    def add_header(self, name: str, value: str) -> None:
        """Add a header to all requests."""
        self._add_headers[name] = value

    def remove_header(self, name: str) -> None:
        """Remove a header from requests."""
        self._remove_headers.append(name.lower())

    def add_path_replacement(self, pattern: str, replacement: str) -> None:
        """Add path replacement rule."""
        self._path_replacements.append((pattern, replacement))

    def transform_request(
        self, path: str, headers: Dict[str, str], body: Optional[bytes]
    ) -> Tuple[str, Dict[str, str], Optional[bytes]]:
        """Transform the request."""
        # Transform headers
        new_headers = {}
        for k, v in headers.items():
            if k.lower() not in self._remove_headers:
                new_key = self._header_mappings.get(k.lower(), k)
                new_headers[new_key] = v

        # Add static headers
        new_headers.update(self._add_headers)

        # Transform path
        new_path = path
        for pattern, replacement in self._path_replacements:
            new_path = re.sub(pattern, replacement, new_path)

        return new_path, new_headers, body


class ResponseTransformer:
    """Transform responses before returning."""

    def __init__(self):
        self._header_mappings: Dict[str, str] = {}
        self._add_headers: Dict[str, str] = {}
        self._remove_headers: List[str] = []
        self._response_template: Optional[Dict[str, Any]] = None

    def add_header_mapping(self, from_header: str, to_header: str) -> None:
        """Map response header."""
        self._header_mappings[from_header.lower()] = to_header

    def add_header(self, name: str, value: str) -> None:
        """Add header to responses."""
        self._add_headers[name] = value

    def remove_header(self, name: str) -> None:
        """Remove header from responses."""
        self._remove_headers.append(name.lower())

    def set_response_template(self, template: Dict[str, Any]) -> None:
        """Set a response template to wrap responses."""
        self._response_template = template

    def transform_response(
        self, status_code: int, headers: Dict[str, str], body: Any
    ) -> Tuple[int, Dict[str, str], Any]:
        """Transform the response."""
        # Transform headers
        new_headers = {}
        for k, v in headers.items():
            if k.lower() not in self._remove_headers:
                new_key = self._header_mappings.get(k.lower(), k)
                new_headers[new_key] = v
        new_headers.update(self._add_headers)

        # Apply template if set
        if self._response_template:
            body = dict(self._response_template)
            body["data"] = body

        return status_code, new_headers, body


class APIGatewayAction(BaseAction):
    """API Gateway Action for routing and transformation.

    Provides routing, rate limiting, request/response transformation,
    and backend aggregation for API gateway functionality.

    Examples:
        >>> action = APIGatewayAction()
        >>> result = action.execute(ctx, {
        ...     "path": "/api/users/123",
        ...     "method": "GET",
        ...     "routes": [
        ...         {"path_pattern": "/api/users/(\\\\d+)", "backend_url": "http://user-svc:8000"}
        ...     ]
        ... })
    """

    action_type = "api_gateway"
    display_name = "API网关"
    description = "API路由、限流、请求响应转换、后端聚合"

    def __init__(self):
        super().__init__()
        self._routes: List[Route] = []
        self._round_robin_counters: Dict[str, int] = {}
        self._request_transformer = RequestTransformer()
        self._response_transformer = ResponseTransformer()
        self._stats = GatewayStats()
        self._stats_lock = __import__("threading").RLock()

    def add_route(self, route: Union[Route, Dict[str, Any]]) -> None:
        """Add a route to the gateway."""
        if isinstance(route, dict):
            route = Route(**route)
        # Sort by priority (higher first)
        self._routes.append(route)
        self._routes.sort(key=lambda r: r.priority, reverse=True)

    def configure_request_transform(self, config: Dict[str, Any]) -> None:
        """Configure request transformation."""
        for mapping in config.get("header_mappings", []):
            self._request_transformer.add_header_mapping(
                mapping["from"], mapping["to"]
            )
        for name, value in config.get("add_headers", {}).items():
            self._request_transformer.add_header(name, value)
        for name in config.get("remove_headers", []):
            self._request_transformer.remove_header(name)
        for rule in config.get("path_replacements", []):
            self._request_transformer.add_path_replacement(
                rule["pattern"], rule["replacement"]
            )

    def configure_response_transform(self, config: Dict[str, Any]) -> None:
        """Configure response transformation."""
        for mapping in config.get("header_mappings", []):
            self._response_transformer.add_header_mapping(
                mapping["from"], mapping["to"]
            )
        for name, value in config.get("add_headers", {}).items():
            self._response_transformer.add_header(name, value)
        for name in config.get("remove_headers", []):
            self._response_transformer.remove_header(name)
        if "response_template" in config:
            self._response_transformer.set_response_template(
                config["response_template"]
            )

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute API gateway routing.

        Args:
            context: Execution context.
            params: Dict with keys:
                - path: Request path
                - method: HTTP method (default: GET)
                - routes: List of route definitions (if not pre-configured)
                - headers: Request headers
                - body: Request body (optional)
                - route_name: Specific route to use (optional)
                - strategy: Routing strategy (optional)

        Returns:
            ActionResult with routed response data.
        """
        import urllib.request
        import urllib.error

        path = params.get("path", "/")
        method = params.get("method", "GET").upper()
        headers = params.get("headers", {})
        body = params.get("body")
        routes_config = params.get("routes", [])

        # Add routes if provided
        if routes_config:
            for r in routes_config:
                if isinstance(r, Route):
                    self.add_route(r)
                else:
                    self.add_route(Route(**r))

        # Find matching route
        match = self._match_route(path, method)
        if match is None:
            with self._stats_lock:
                self._stats.route_not_found += 1
            return ActionResult(
                success=False,
                message=f"No route found for {method} {path}",
                data={"path": path, "method": method}
            )

        route = match.route

        # Transform request
        backend_path = match.backend_path
        if route.strip_path:
            # Remove matched prefix from path
            backend_path = re.sub(route.path_pattern, "", path, count=1)
        if route.add_prefix:
            backend_path = route.add_prefix + backend_path

        transformed_path, transformed_headers, transformed_body = \
            self._request_transformer.transform_request(
                backend_path, headers, body
            )

        # Build backend URL
        backend_url = route.backend_url.rstrip("/") + "/" + transformed_path.lstrip("/")

        with self._stats_lock:
            self._stats.total_requests += 1
            self._stats.routed_requests += 1

        # Make backend request
        try:
            req = urllib.request.Request(
                backend_url,
                headers=transformed_headers,
                method=method,
            )
            if transformed_body:
                if isinstance(transformed_body, dict):
                    import urllib.parse
                    req.data = urllib.parse.urlencode(transformed_body).encode()
                elif isinstance(transformed_body, str):
                    req.data = transformed_body.encode()

            with urllib.request.urlopen(req, timeout=route.timeout) as response:
                content = response.read()
                status = response.status
                resp_headers = dict(response.headers)

                # Transform response
                status, resp_headers, resp_body = \
                    self._response_transformer.transform_response(
                        status, resp_headers, content
                    )

                try:
                    resp_data = json.loads(resp_body)
                except (json.JSONDecodeError, TypeError):
                    resp_data = content.decode(errors="replace")

                with self._stats_lock:
                    self._stats.successful_requests += 1

                return ActionResult(
                    success=True,
                    message=f"Routed to {route.backend_url}",
                    data={
                        "status_code": status,
                        "data": resp_data,
                        "backend_url": route.backend_url,
                        "route": route.path_pattern,
                    }
                )

        except urllib.error.HTTPError as e:
            with self._stats_lock:
                self._stats.failed_requests += 1
                self._stats.backend_errors += 1
            return ActionResult(
                success=False,
                message=f"Backend error {e.code}: {e.reason}",
                data={"status_code": e.code, "backend": route.backend_url}
            )

        except Exception as e:
            with self._stats_lock:
                self._stats.failed_requests += 1
                self._stats.backend_errors += 1
            return ActionResult(
                success=False,
                message=f"Gateway error: {str(e)}",
                data={"backend": route.backend_url}
            )

    def _match_route(self, path: str, method: str) -> Optional[RouteMatch]:
        """Match a request to a route."""
        for route in self._routes:
            match = route._compiled_pattern.match(path)
            if match:
                path_params = match.groupdict()
                return RouteMatch(
                    route=route,
                    path_params=path_params,
                    backend_path=path,
                )
        return None

    def get_stats(self) -> Dict[str, Any]:
        """Get gateway statistics."""
        with self._stats_lock:
            total = self._stats.total_requests
            return {
                "total_requests": total,
                "successful_requests": self._stats.successful_requests,
                "failed_requests": self._stats.failed_requests,
                "routed_requests": self._stats.routed_requests,
                "route_not_found": self._stats.route_not_found,
                "backend_errors": self._stats.backend_errors,
                "success_rate": (
                    self._stats.successful_requests / total if total > 0 else 0
                ),
            }

    def get_required_params(self) -> List[str]:
        return ["path"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "method": "GET",
            "headers": {},
            "body": None,
            "routes": [],
            "route_name": "",
            "strategy": "round_robin",
        }
