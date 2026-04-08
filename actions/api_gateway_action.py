"""API gateway action module for RabAI AutoClick.

Provides API gateway operations:
- APIGateway: Simple API gateway
- RequestRouter: Route requests to backends
- RequestValidator: Validate API requests
- ResponseFormatter: Format API responses
- MiddlewareChain: API middleware chain
- RateLimitMiddleware: Rate limiting middleware
"""

import time
import json
import hashlib
import hmac
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HTTPMethod(Enum):
    """HTTP methods."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"
    OPTIONS = "OPTIONS"
    HEAD = "HEAD"


@dataclass
class Route:
    """API route definition."""
    path: str
    method: HTTPMethod
    handler: Callable
    middleware: List[Callable] = field(default_factory=list)
    auth_required: bool = False
    rate_limit: Optional[float] = None
    validators: List[Callable] = None


@dataclass
class APIRequest:
    """API request."""
    method: str
    path: str
    headers: Dict[str, str]
    query_params: Dict[str, str]
    body: Any
    source_ip: str = ""
    user_agent: str = ""


@dataclass
class APIResponse:
    """API response."""
    status_code: int
    headers: Dict[str, str] = field(default_factory=dict)
    body: Any = None
    error: Optional[str] = None


class RequestValidator:
    """Validate API requests."""

    def __init__(self):
        self._validators: Dict[str, Callable] = {}

    def register(self, name: str, validator: Callable[[APIRequest], Tuple[bool, str]]):
        """Register a validator."""
        self._validators[name] = validator

    def validate(self, request: APIRequest, validator_names: List[str]) -> Tuple[bool, str]:
        """Validate request with validators."""
        for name in validator_names:
            if name not in self._validators:
                continue
            valid, message = self._validators[name](request)
            if not valid:
                return False, message
        return True, ""


class ResponseFormatter:
    """Format API responses."""

    @staticmethod
    def format_success(
        data: Any,
        message: str = "Success",
        meta: Optional[Dict] = None,
        status_code: int = 200,
    ) -> APIResponse:
        """Format success response."""
        body = {
            "success": True,
            "message": message,
            "data": data,
        }
        if meta:
            body["meta"] = meta

        return APIResponse(
            status_code=status_code,
            headers={"Content-Type": "application/json"},
            body=body,
        )

    @staticmethod
    def format_error(
        error: str,
        status_code: int = 400,
        details: Optional[Dict] = None,
    ) -> APIResponse:
        """Format error response."""
        body = {
            "success": False,
            "error": error,
        }
        if details:
            body["details"] = details

        return APIResponse(
            status_code=status_code,
            headers={"Content-Type": "application/json"},
            body=body,
        )

    @staticmethod
    def format_paginated(
        data: List,
        page: int,
        page_size: int,
        total: int,
    ) -> APIResponse:
        """Format paginated response."""
        total_pages = (total + page_size - 1) // page_size

        return APIResponse(
            status_code=200,
            headers={"Content-Type": "application/json"},
            body={
                "success": True,
                "data": data,
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total": total,
                    "total_pages": total_pages,
                    "has_next": page < total_pages,
                    "has_prev": page > 1,
                },
            },
        )


class MiddlewareChain:
    """Middleware chain for request processing."""

    def __init__(self):
        self._middleware: List[Callable] = []

    def use(self, middleware: Callable) -> "MiddlewareChain":
        """Add middleware to chain."""
        self._middleware.append(middleware)
        return self

    def process(self, request: APIRequest, handler: Callable) -> APIResponse:
        """Process request through middleware chain."""
        def chain(index: int) -> APIResponse:
            if index >= len(self._middleware):
                return handler(request)

            middleware = self._middleware[index]

            def next_handler(req: APIRequest) -> APIResponse:
                return chain(index + 1)

            return middleware(req, next_handler)

        return chain(0)


class RequestRouter:
    """Route requests to handlers."""

    def __init__(self):
        self._routes: Dict[Tuple[str, str], Route] = {}
        self._wildcard_routes: List[Route] = []

    def register(
        self,
        path: str,
        method: str,
        handler: Callable,
        middleware: Optional[List[Callable]] = None,
        auth_required: bool = False,
        rate_limit: Optional[float] = None,
    ) -> bool:
        """Register a route."""
        try:
            method_enum = HTTPMethod[method.upper()]
        except KeyError:
            return False

        route = Route(
            path=path,
            method=method_enum,
            handler=handler,
            middleware=middleware or [],
            auth_required=auth_required,
            rate_limit=rate_limit,
        )

        if "*" in path:
            self._wildcard_routes.append(route)
        else:
            self._routes[(path, method.upper())] = route

        return True

    def route(self, request: APIRequest) -> Optional[Route]:
        """Find route for request."""
        key = (request.path, request.method.upper())
        if key in self._routes:
            return self._routes[key]

        for route in self._wildcard_routes:
            if self._match_path(route.path, request.path):
                return route

        return None

    def _match_path(self, pattern: str, path: str) -> bool:
        """Match path against pattern."""
        if pattern.endswith("*"):
            return path.startswith(pattern[:-1])
        return pattern == path


class APIGateway:
    """Simple API gateway."""

    def __init__(self):
        self.router = RequestRouter()
        self.validator = RequestValidator()
        self.middleware_chain = MiddlewareChain()
        self._rate_limiters: Dict[str, List[float]] = {}

    def handle(self, request: APIRequest) -> APIResponse:
        """Handle incoming request."""
        route = self.router.route(request)

        if not route:
            return ResponseFormatter.format_error("Not Found", 404)

        if route.auth_required:
            if not self._check_auth(request):
                return ResponseFormatter.format_error("Unauthorized", 401)

        if route.rate_limit:
            if not self._check_rate_limit(request.source_ip, route.rate_limit):
                return ResponseFormatter.format_error("Rate Limited", 429)

        def handler(req: APIRequest) -> APIResponse:
            try:
                result = route.handler(req)
                if isinstance(result, APIResponse):
                    return result
                return ResponseFormatter.format_success(result)
            except Exception as e:
                return ResponseFormatter.format_error(str(e), 500)

        if route.middleware:
            def final_handler(req: APIRequest) -> APIResponse:
                return handler(req)

            current_handler = final_handler
            for mw in reversed(route.middleware):
                next_h = current_handler
                def wrapped(req, next_handler=next_h):
                    return mw(req, next_handler)
                current_handler = wrapped

            return current_handler(request)
        else:
            return handler(request)

    def _check_auth(self, request: APIRequest) -> bool:
        """Check request authentication."""
        auth_header = request.headers.get("Authorization", "")
        return bool(auth_header)

    def _check_rate_limit(self, ip: str, limit: float) -> bool:
        """Check rate limit for IP."""
        now = time.time()
        if ip not in self._rate_limiters:
            self._rate_limiters[ip] = []

        self._rate_limiters[ip] = [t for t in self._rate_limiters[ip] if now - t < 1.0]

        if len(self._rate_limiters[ip]) >= limit:
            return False

        self._rate_limiters[ip].append(now)
        return True


class APIGatewayAction(BaseAction):
    """API gateway action."""
    action_type = "api_gateway"
    display_name = "API网关"
    description = "API网关和路由"

    def __init__(self):
        super().__init__()
        self._gateways: Dict[str, APIGateway] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "route")
            gateway_name = params.get("gateway", "default")

            if gateway_name not in self._gateways:
                self._gateways[gateway_name] = APIGateway()

            gateway = self._gateways[gateway_name]

            if operation == "register":
                return self._register_route(gateway, params)
            elif operation == "route":
                return self._route_request(gateway, params)
            elif operation == "validate":
                return self._validate_request(gateway, params)
            elif operation == "list":
                return self._list_routes(gateway)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Gateway error: {str(e)}")

    def _register_route(self, gateway: APIGateway, params: Dict) -> ActionResult:
        """Register a route."""
        path = params.get("path")
        method = params.get("method", "GET")
        handler = params.get("handler")
        middleware = params.get("middleware", [])
        auth_required = params.get("auth_required", False)
        rate_limit = params.get("rate_limit")

        if not path or not handler:
            return ActionResult(success=False, message="path and handler are required")

        success = gateway.router.register(
            path=path,
            method=method,
            handler=handler,
            middleware=middleware,
            auth_required=auth_required,
            rate_limit=rate_limit,
        )

        return ActionResult(
            success=success,
            message=f"Route {method} {path} registered" if success else "Failed to register route",
        )

    def _route_request(self, gateway: APIGateway, params: Dict) -> ActionResult:
        """Route a request."""
        request = APIRequest(
            method=params.get("method", "GET"),
            path=params.get("path", "/"),
            headers=params.get("headers", {}),
            query_params=params.get("query_params", {}),
            body=params.get("body"),
            source_ip=params.get("source_ip", "127.0.0.1"),
            user_agent=params.get("user_agent", ""),
        )

        response = gateway.handle(request)

        return ActionResult(
            success=response.status_code < 400,
            message=f"HTTP {response.status_code}",
            data={
                "status_code": response.status_code,
                "headers": response.headers,
                "body": response.body,
                "error": response.error,
            },
        )

    def _validate_request(self, gateway: APIGateway, params: Dict) -> ActionResult:
        """Validate a request."""
        request = APIRequest(
            method=params.get("method", "GET"),
            path=params.get("path", "/"),
            headers=params.get("headers", {}),
            query_params=params.get("query_params", {}),
            body=params.get("body"),
        )

        validators = params.get("validators", [])
        valid, message = gateway.validator.validate(request, validators)

        return ActionResult(
            success=valid,
            message=message if not valid else "Valid",
        )

    def _list_routes(self, gateway: APIGateway) -> ActionResult:
        """List all routes."""
        routes = []
        for (path, method), route in gateway.router._routes.items():
            routes.append({
                "path": path,
                "method": method,
                "auth_required": route.auth_required,
                "rate_limit": route.rate_limit,
            })

        return ActionResult(
            success=True,
            message=f"{len(routes)} routes",
            data={"routes": routes},
        )
