"""
API Gateway Action Module.

Provides API gateway functionality with routing, load balancing,
authentication, rate limiting, and request transformation.

Author: rabai_autoclick team
"""

import time
import logging
from typing import (
    Optional, Dict, Any, List, Callable, Awaitable,
    Union, Pattern
)
from dataclasses import dataclass, field
from enum import Enum
from urllib.parse import urlparse
import re

logger = logging.getLogger(__name__)


class AuthType(Enum):
    """Authentication types."""
    NONE = "none"
    API_KEY = "api_key"
    BEARER = "bearer"
    BASIC = "basic"
    OAUTH2 = "oauth2"
    CUSTOM = "custom"


@dataclass
class RouteConfig:
    """Configuration for a route."""
    path: str
    method: str
    handler: Callable[..., Awaitable[Any]]
    auth_type: AuthType = AuthType.NONE
    auth_config: Dict[str, Any] = field(default_factory=dict)
    rate_limit: Optional[int] = None
    rate_window: int = 60
    timeout: Optional[float] = None
    cache_ttl: Optional[int] = None
    transform_request: Optional[Callable] = None
    transform_response: Optional[Callable] = None
    middlewares: List[Callable] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RequestContext:
    """Request context passed through the gateway."""
    method: str
    path: str
    query_params: Dict[str, Any]
    headers: Dict[str, str]
    body: Optional[Any] = None
    path_params: Dict[str, str] = field(default_factory=dict)
    state: Dict[str, Any] = field(default_factory=dict)
    auth_data: Optional[Dict[str, Any]] = None


@dataclass
class ResponseContext:
    """Response context from the gateway."""
    status_code: int = 200
    headers: Dict[str, str] = field(default_factory=dict)
    body: Any = None
    error: Optional[str] = None
    cached: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RateLimitEntry:
    """Rate limit tracking entry."""
    count: int = 0
    window_start: float = 0


class APIGatewayAction:
    """
    API Gateway Implementation.

    Provides routing, authentication, rate limiting,
    caching, and request/response transformation.

    Example:
        >>> gateway = APIGatewayAction()
        >>> gateway.add_route("/api/users", "GET", get_users_handler)
        >>> gateway.add_middleware(auth_middleware)
        >>> await gateway.handle(request)
    """

    def __init__(self, base_url: str = ""):
        self.base_url = base_url
        self._routes: Dict[str, RouteConfig] = {}
        self._route_patterns: List[tuple] = []
        self._middlewares: List[Callable] = []
        self._rate_limits: Dict[str, RateLimitEntry] = {}
        self._cache: Dict[str, tuple] = {}
        self._auth_handlers: Dict[AuthType, Callable] = {}
        self._error_handler: Optional[Callable] = None
        self._request_id_header = "X-Request-ID"

    def add_route(
        self,
        path: str,
        method: str,
        handler: Callable[..., Awaitable[Any]],
        **kwargs,
    ) -> "APIGatewayAction":
        """
        Add a route to the gateway.

        Args:
            path: Route path pattern
            method: HTTP method
            handler: Request handler
            **kwargs: Route configuration

        Returns:
            Self for chaining
        """
        route_key = f"{method.upper()}:{path}"
        route_config = RouteConfig(path=path, method=method.upper(), handler=handler, **kwargs)
        self._routes[route_key] = route_config

        if self._has_path_params(path):
            pattern = self._compile_path_pattern(path)
            self._route_patterns.append((pattern, route_config))

        logger.info(f"Added route: {method.upper()} {path}")
        return self

    def _has_path_params(self, path: str) -> bool:
        """Check if path contains path parameters."""
        return "{" in path and "}" in path

    def _compile_path_pattern(self, path: str) -> Pattern:
        """Compile path pattern to regex."""
        pattern = re.sub(r"\{(\w+)\}", r"(?P<\1>[^/]+)", path)
        pattern = f"^{pattern}$"
        return re.compile(pattern)

    def get(self, path: str, handler: Callable, **kwargs) -> "APIGatewayAction":
        """Add GET route."""
        return self.add_route(path, "GET", handler, **kwargs)

    def post(self, path: str, handler: Callable, **kwargs) -> "APIGatewayAction":
        """Add POST route."""
        return self.add_route(path, "POST", handler, **kwargs)

    def put(self, path: str, handler: Callable, **kwargs) -> "APIGatewayAction":
        """Add PUT route."""
        return self.add_route(path, "PUT", handler, **kwargs)

    def delete(self, path: str, handler: Callable, **kwargs) -> "APIGatewayAction":
        """Add DELETE route."""
        return self.add_route(path, "DELETE", handler, **kwargs)

    def add_middleware(self, middleware: Callable) -> "APIGatewayAction":
        """Add a middleware function."""
        self._middlewares.append(middleware)
        return self

    def set_auth_handler(self, auth_type: AuthType, handler: Callable) -> None:
        """Set authentication handler for auth type."""
        self._auth_handlers[auth_type] = handler

    def set_error_handler(self, handler: Callable) -> None:
        """Set global error handler."""
        self._error_handler = handler

    def _match_route(self, method: str, path: str) -> Optional[tuple]:
        """Match request to a route."""
        route_key = f"{method.upper()}:{path}"
        if route_key in self._routes:
            return self._routes[route_key], {}

        for pattern, route in self._route_patterns:
            match = pattern.match(path)
            if match:
                return route, match.groupdict()

        return None

    async def handle(
        self,
        request: RequestContext,
    ) -> ResponseContext:
        """
        Handle an incoming request.

        Args:
            request: Request context

        Returns:
            ResponseContext
        """
        response = ResponseContext()
        request_id = request.headers.get(self._request_id_header, str(time.time()))
        request.state["request_id"] = request_id

        try:
            route_match = self._match_route(request.method, request.path)
            if not route_match:
                response.status_code = 404
                response.error = f"Route not found: {request.method} {request.path}"
                return response

            route, path_params = route_match
            request.path_params = path_params

            response = await self._process_middlewares(request, response)

            if response.error:
                return response

            if route.auth_type != AuthType.NONE:
                auth_result = await self._authenticate(request, route)
                if not auth_result:
                    response.status_code = 401
                    response.error = "Authentication failed"
                    return response
                request.auth_data = auth_result

            if route.rate_limit:
                if not await self._check_rate_limit(request, route):
                    response.status_code = 429
                    response.error = "Rate limit exceeded"
                    return response

            if route.cache_ttl:
                cache_key = self._get_cache_key(request, route)
                cached = self._get_from_cache(cache_key)
                if cached:
                    response.body = cached
                    response.cached = True
                    return response

            if route.transform_request:
                request = route.transform_request(request) or request

            if route.timeout:
                response.body = await self._execute_with_timeout(
                    route.handler, request, route.timeout
                )
            else:
                response.body = await route.handler(request)

            if route.transform_response:
                response.body = route.transform_response(response.body) or response.body

            if route.cache_ttl:
                self._set_cache(cache_key, response.body, route.cache_ttl)

            response.status_code = 200

        except Exception as e:
            logger.error(f"Request handling error: {e}")
            if self._error_handler:
                response = await self._error_handler(request, e)
            else:
                response.status_code = 500
                response.error = str(e)

        return response

    async def _process_middlewares(
        self,
        request: RequestContext,
        response: ResponseContext,
    ) -> ResponseContext:
        """Process middleware chain."""
        for middleware in self._middlewares:
            try:
                result = middleware(request, response)
                if hasattr(result, "__await__"):
                    result = await result
                if result is not None and isinstance(result, ResponseContext):
                    response = result
            except Exception as e:
                logger.error(f"Middleware error: {e}")
                response.error = str(e)
                return response

            if response.error:
                return response

        return response

    async def _authenticate(
        self,
        request: RequestContext,
        route: RouteConfig,
    ) -> Optional[Dict[str, Any]]:
        """Authenticate request."""
        handler = self._auth_handlers.get(route.auth_type)
        if handler:
            return await handler(request, route.auth_config)
        return None

    async def _check_rate_limit(
        self,
        request: RequestContext,
        route: RouteConfig,
    ) -> bool:
        """Check rate limit for request."""
        key = f"{request.path}:{request.headers.get('X-Forwarded-For', 'unknown')}"
        now = time.time()

        if key not in self._rate_limits:
            self._rate_limits[key] = RateLimitEntry(window_start=now)

        entry = self._rate_limits[key]

        if now - entry.window_start >= route.rate_window:
            entry.count = 0
            entry.window_start = now

        entry.count += 1

        return entry.count <= route.rate_limit

    def _get_cache_key(
        self,
        request: RequestContext,
        route: RouteConfig,
    ) -> str:
        """Generate cache key for request."""
        return f"{request.method}:{request.path}:{request.query_params}"

    def _get_from_cache(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        if key in self._cache:
            value, expiry = self._cache[key]
            if time.time() < expiry:
                return value
            del self._cache[key]
        return None

    def _set_cache(self, key: str, value: Any, ttl: int) -> None:
        """Set value in cache."""
        self._cache[key] = (value, time.time() + ttl)

    async def _execute_with_timeout(
        self,
        handler: Callable,
        request: RequestContext,
        timeout: float,
    ) -> Any:
        """Execute handler with timeout."""
        import asyncio
        try:
            return await asyncio.wait_for(handler(request), timeout=timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(f"Handler timeout after {timeout}s")

    def get_stats(self) -> Dict[str, Any]:
        """Get gateway statistics."""
        return {
            "total_routes": len(self._routes),
            "route_patterns": len(self._route_patterns),
            "middlewares": len(self._middlewares),
            "cache_entries": len(self._cache),
            "rate_limit_entries": len(self._rate_limits),
        }
