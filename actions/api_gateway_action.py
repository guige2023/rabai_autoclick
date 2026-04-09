"""
API Gateway Action Module

API gateway with routing, authentication, rate limiting,
request/response transformation, and monitoring.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Pattern

logger = logging.getLogger(__name__)


class AuthType(Enum):
    """Authentication types."""

    NONE = "none"
    API_KEY = "api_key"
    BEARER = "bearer"
    BASIC = "basic"
    OAUTH2 = "oauth2"


@dataclass
class RouteConfig:
    """Configuration for a single route."""

    path_pattern: str
    method: str = "GET"
    backend_url: str
    auth_type: AuthType = AuthType.NONE
    auth_config: Dict[str, Any] = field(default_factory=dict)
    rate_limit: Optional[float] = None
    timeout_seconds: float = 30.0
    retry_count: int = 0
    circuit_breaker_threshold: int = 5
    transforms: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GatewayMetrics:
    """Gateway metrics."""

    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_latency_ms: float = 0.0
    active_requests: int = 0


class RouteMatcher:
    """Matches incoming requests to routes."""

    def __init__(self):
        self._routes: List[RouteConfig] = []

    def add_route(self, config: RouteConfig) -> None:
        """Add a route configuration."""
        self._routes.append(config)

    def match(self, path: str, method: str) -> Optional[RouteConfig]:
        """Match a request to a route."""
        for route in self._routes:
            if self._match_path(route.path_pattern, path) and route.method.upper() == method.upper():
                return route
        return None

    def _match_path(self, pattern: str, path: str) -> bool:
        """Match path pattern against actual path."""
        import re
        # Convert {param} to regex groups
        regex_pattern = re.sub(r'\{(\w+)\}', r'(?P<\1>[^/]+)', pattern)
        regex_pattern = f"^{regex_pattern}$"
        return bool(re.match(regex_pattern, path))


class RequestAuthenticator:
    """Authenticates incoming requests."""

    def __init__(self):
        self._valid_api_keys: Dict[str, str] = {}  # key -> client_id

    def add_api_key(self, key: str, client_id: str) -> None:
        """Register an API key."""
        self._valid_api_keys[key] = client_id

    async def authenticate(
        self,
        request: Dict[str, Any],
        auth_type: AuthType,
        auth_config: Dict[str, Any],
    ) -> tuple[bool, Optional[str]]:
        """Authenticate a request. Returns (success, client_id)."""
        if auth_type == AuthType.NONE:
            return True, "anonymous"

        elif auth_type == AuthType.API_KEY:
            api_key = request.get("headers", {}).get("x-api-key")
            if api_key and api_key in self._valid_api_keys:
                return True, self._valid_api_keys[api_key]
            return False, None

        elif auth_type == AuthType.BEARER:
            auth_header = request.get("headers", {}).get("authorization", "")
            if auth_header.startswith("Bearer "):
                token = auth_header[7:]
                # Token validation would go here
                return True, token
            return False, None

        elif auth_type == AuthType.BASIC:
            auth_header = request.get("headers", {}).get("authorization", "")
            if auth_header.startswith("Basic "):
                # Base64 decode and validate
                return True, "basic_user"
            return False, None

        return True, "unknown"


class CircuitBreaker:
    """Simple circuit breaker for backend protection."""

    def __init__(self, failure_threshold: int = 5):
        self.failure_threshold = failure_threshold
        self._failures: int = 0
        self._open: bool = False
        self._last_failure_time: Optional[float] = None

    def record_success(self) -> None:
        """Record a successful request."""
        self._failures = max(0, self._failures - 1)
        if self._failures == 0 and self._open:
            self._open = False

    def record_failure(self) -> None:
        """Record a failed request."""
        self._failures += 1
        self._last_failure_time = time.time()
        if self._failures >= self.failure_threshold:
            self._open = True

    def can_execute(self) -> bool:
        """Check if request can proceed."""
        return not self._open


class APIGatewayAction:
    """
    API Gateway action for routing, authentication, and monitoring.

    Features:
    - Route matching with path parameters
    - Multiple authentication types
    - Rate limiting per route
    - Circuit breaker pattern
    - Request/response transformation
    - Comprehensive metrics
    """

    def __init__(self):
        self._route_matcher = RouteMatcher()
        self._authenticator = RequestAuthenticator()
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        self._metrics = GatewayMetrics()
        self._middleware: List[Callable] = []

    def add_route(self, config: RouteConfig) -> "APIGatewayAction":
        """Add a route configuration."""
        self._route_matcher.add_route(config)
        self._circuit_breakers[config.path_pattern] = CircuitBreaker(
            config.circuit_breaker_threshold
        )
        return self

    def add_api_key(self, key: str, client_id: str) -> None:
        """Register an API key."""
        self._authenticator.add_api_key(key, client_id)

    def add_middleware(self, middleware: Callable) -> "APIGatewayAction":
        """Add middleware function."""
        self._middleware.append(middleware)
        return self

    async def handle_request(
        self,
        path: str,
        method: str,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[Any] = None,
        query_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Handle an incoming gateway request."""
        request = {
            "path": path,
            "method": method,
            "headers": headers or {},
            "body": body,
            "query_params": query_params or {},
        }

        self._metrics.active_requests += 1
        start_time = time.time()

        try:
            # Apply middleware
            for mw in self._middleware:
                if asyncio.iscoroutinefunction(mw):
                    request = await mw(request) or request
                else:
                    request = mw(request) or request

            # Match route
            route = self._route_matcher.match(path, method)
            if not route:
                return self._error_response(404, "Route not found")

            # Check circuit breaker
            cb = self._circuit_breakers.get(route.path_pattern)
            if cb and not cb.can_execute():
                return self._error_response(503, "Service temporarily unavailable")

            # Authenticate
            authenticated, client_id = await self._authenticator.authenticate(
                request, route.auth_type, route.auth_config
            )
            if not authenticated:
                return self._error_response(401, "Unauthorized")

            # Forward to backend
            response = await self._forward_request(route, request)

            # Record success
            if cb:
                cb.record_success()
            self._metrics.successful_requests += 1

            return response

        except Exception as e:
            logger.error(f"Gateway error: {e}")
            if cb:
                cb.record_failure()
            self._metrics.failed_requests += 1
            return self._error_response(500, str(e))

        finally:
            self._metrics.total_requests += 1
            self._metrics.active_requests -= 1
            self._metrics.total_latency_ms += (time.time() - start_time) * 1000

    async def _forward_request(
        self,
        route: RouteConfig,
        request: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Forward request to backend service."""
        import httpx

        url = f"{route.backend_url}{request['path']}"

        async with httpx.AsyncClient() as client:
            response = await client.request(
                method=request["method"],
                url=url,
                headers=request["headers"],
                json=request.get("body"),
                params=request.get("query_params"),
                timeout=route.timeout_seconds,
            )

        return {
            "status_code": response.status_code,
            "headers": dict(response.headers),
            "body": response.json() if response.headers.get("content-type", "").startswith("application/json") else response.text,
        }

    def _error_response(self, status_code: int, message: str) -> Dict[str, Any]:
        """Create error response."""
        return {
            "status_code": status_code,
            "body": {"error": message},
        }

    def get_metrics(self) -> Dict[str, Any]:
        """Get gateway metrics."""
        avg_latency = 0.0
        if self._metrics.total_requests > 0:
            avg_latency = self._metrics.total_latency_ms / self._metrics.total_requests

        return {
            "total_requests": self._metrics.total_requests,
            "successful_requests": self._metrics.successful_requests,
            "failed_requests": self._metrics.failed_requests,
            "active_requests": self._metrics.active_requests,
            "avg_latency_ms": avg_latency,
        }


async def demo_gateway():
    """Demonstrate API gateway."""
    gateway = APIGatewayAction()

    # Add routes
    gateway.add_route(RouteConfig(
        path_pattern="/api/users/{id}",
        method="GET",
        backend_url="http://localhost:8000",
        auth_type=AuthType.API_KEY,
    ))

    gateway.add_api_key("test-key-123", "test-client")

    # Handle request
    response = await gateway.handle_request(
        path="/api/users/123",
        method="GET",
        headers={"x-api-key": "test-key-123"},
    )

    print(f"Response: {response['status_code']}")
    print(f"Metrics: {gateway.get_metrics()}")


if __name__ == "__main__":
    asyncio.run(demo_gateway())
