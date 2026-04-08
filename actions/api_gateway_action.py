"""
API Gateway Action Module.

Provides API gateway functionality including routing, rate limiting,
 authentication, and request/response transformation.
"""

from __future__ import annotations

import time
import hashlib
from typing import Any, Callable, Optional, Union
from dataclasses import dataclass, field
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


@dataclass
class Route:
    """A single API route definition."""
    path: str
    method: str
    handler: Callable
    auth_required: bool = False
    rate_limit: Optional[int] = None
    timeout: float = 30.0
    response_transformer: Optional[Callable[[Any], Any]] = None


@dataclass
class RateLimitConfig:
    """Rate limiting configuration."""
    requests_per_second: int = 100
    requests_per_minute: int = 1000
    requests_per_hour: int = 10000
    burst_size: int = 200


@dataclass
class GatewayResult:
    """Result of a gateway request."""
    status_code: int
    data: Any
    headers: dict[str, str] = field(default_factory=dict)
    duration_ms: float = 0.0


class APIGatewayAction:
    """
    API Gateway with routing, auth, rate limiting, and transformation.

    Acts as a facade for multiple backend services with consistent
    handling for authentication, rate limiting, and response formatting.

    Example:
        gateway = APIGatewayAction()
        gateway.add_route("/users", "GET", get_users_handler, auth_required=True)
        gateway.add_route("/users/{id}", "GET", get_user_handler)
        result = await gateway.handle_request(method="GET", path="/users", token="abc")
    """

    def __init__(
        self,
        rate_limit_config: Optional[RateLimitConfig] = None,
        auth_handler: Optional[Callable[[str], Optional[dict[str, Any]]]] = None,
    ) -> None:
        self._routes: list[Route] = []
        self._route_map: dict[tuple[str, str], Route] = {}
        self.rate_limit_config = rate_limit_config or RateLimitConfig()
        self.auth_handler = auth_handler
        self._rate_limit_counters: dict[str, list[float]] = defaultdict(list)

    def add_route(
        self,
        path: str,
        method: str,
        handler: Callable,
        auth_required: bool = False,
        rate_limit: Optional[int] = None,
        timeout: float = 30.0,
        response_transformer: Optional[Callable[[Any], Any]] = None,
    ) -> "APIGatewayAction":
        """Add a route to the gateway."""
        route = Route(
            path=path,
            method=method.upper(),
            handler=handler,
            auth_required=auth_required,
            rate_limit=rate_limit,
            timeout=timeout,
            response_transformer=response_transformer,
        )
        self._routes.append(route)
        key = (method.upper(), path)
        self._route_map[key] = route
        return self

    def get_route(self, method: str, path: str) -> Optional[Route]:
        """Find a matching route for the request."""
        key = (method.upper(), path)
        if key in self._route_map:
            return self._route_map[key]

        for route in self._routes:
            if self._match_path(route.path, path):
                return route
        return None

    def _match_path(self, route_path: str, request_path: str) -> bool:
        """Match a route path against a request path."""
        route_parts = route_path.strip("/").split("/")
        req_parts = request_path.strip("/").split("/")

        if len(route_parts) != len(req_parts):
            return False

        for rp, rq in zip(route_parts, req_parts):
            if rp.startswith("{") and rp.endswith("}"):
                continue
            if rp != rq:
                return False
        return True

    async def handle_request(
        self,
        method: str,
        path: str,
        headers: Optional[dict[str, str]] = None,
        body: Optional[Any] = None,
        params: Optional[dict[str, Any]] = None,
        token: Optional[str] = None,
        client_id: Optional[str] = None,
    ) -> GatewayResult:
        """Handle an incoming API request."""
        import asyncio
        start_time = time.monotonic()
        headers = headers or {}

        route = self.get_route(method, path)
        if not route:
            return GatewayResult(
                status_code=404,
                data={"error": "Route not found"},
                duration_ms=(time.monotonic() - start_time) * 1000,
            )

        if route.auth_required:
            auth_result = self._authenticate(token, client_id, headers)
            if not auth_result:
                return GatewayResult(
                    status_code=401,
                    data={"error": "Unauthorized"},
                    duration_ms=(time.monotonic() - start_time) * 1000,
                )

        rate_limit_key = client_id or token or "anonymous"
        if not self._check_rate_limit(rate_limit_key, route.rate_limit):
            return GatewayResult(
                status_code=429,
                data={"error": "Rate limit exceeded"},
                duration_ms=(time.monotonic() - start_time) * 1000,
            )

        try:
            kwargs: dict[str, Any] = {}
            if body is not None:
                kwargs["body"] = body
            if params:
                kwargs["params"] = params

            if asyncio.iscoroutinefunction(route.handler):
                data = await asyncio.wait_for(route.handler(**kwargs), timeout=route.timeout)
            else:
                data = route.handler(**kwargs)

            if route.response_transformer:
                data = route.response_transformer(data)

            return GatewayResult(
                status_code=200,
                data=data,
                headers={"Content-Type": "application/json"},
                duration_ms=(time.monotonic() - start_time) * 1000,
            )

        except asyncio.TimeoutError:
            return GatewayResult(
                status_code=504,
                data={"error": "Gateway timeout"},
                duration_ms=(time.monotonic() - start_time) * 1000,
            )
        except Exception as e:
            logger.error(f"Gateway handler error: {e}")
            return GatewayResult(
                status_code=500,
                data={"error": str(e)},
                duration_ms=(time.monotonic() - start_time) * 1000,
            )

    def _authenticate(
        self,
        token: Optional[str],
        client_id: Optional[str],
        headers: dict[str, str],
    ) -> bool:
        """Authenticate a request."""
        if self.auth_handler:
            return self.auth_handler(token or "", client_id) is not None
        return token is not None or client_id is not None

    def _check_rate_limit(self, key: str, route_limit: Optional[int] = None) -> bool:
        """Check if a request is within rate limits."""
        now = time.monotonic()
        limit = route_limit or self.rate_limit_config.requests_per_second

        self._rate_limit_counters[key] = [
            t for t in self._rate_limit_counters[key] if now - t < 1.0
        ]

        if len(self._rate_limit_counters[key]) >= limit:
            return False

        self._rate_limit_counters[key].append(now)
        return True

    def list_routes(self) -> list[Route]:
        """List all registered routes."""
        return self._routes.copy()
