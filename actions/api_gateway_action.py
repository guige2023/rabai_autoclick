"""API Gateway Action Module.

API gateway with routing, load balancing, and middleware support.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from .workflow_routing_action import RoutingStrategy, RouteTarget, WorkflowRouter


class MiddlewareType(Enum):
    """Middleware types."""
    AUTH = "auth"
    RATE_LIMIT = "rate_limit"
    LOGGING = "logging"
    TRANSFORM = "transform"
    VALIDATION = "validation"


@dataclass
class Middleware:
    """API middleware."""
    name: str
    middleware_type: MiddlewareType
    handler: Callable
    priority: int = 0


@dataclass
class APIRoute:
    """API route definition."""
    path: str
    method: str
    handler: Callable
    middlewares: list[str] = field(default_factory=list)
    timeout: float = 30.0


@dataclass
class GatewayRequest:
    """Gateway request."""
    path: str
    method: str
    headers: dict
    body: Any = None
    query_params: dict = field(default_factory=dict)


@dataclass
class GatewayResponse:
    """Gateway response."""
    status_code: int
    body: Any
    headers: dict = field(default_factory=dict)
    latency_ms: float = 0.0


class APIGateway:
    """API gateway with routing and middleware."""

    def __init__(self) -> None:
        self._routes: dict[str, APIRoute] = {}
        self._middleware: dict[str, Middleware] = {}
        self._router: WorkflowRouter | None = None
        self._lock = asyncio.Lock()

    def add_route(self, route: APIRoute) -> None:
        """Add an API route."""
        key = f"{route.method}:{route.path}"
        self._routes[key] = route

    def add_middleware(self, middleware: Middleware) -> None:
        """Add middleware."""
        self._middleware[middleware.name] = middleware

    def set_load_balancer(self, strategy: RoutingStrategy) -> None:
        """Set load balancing strategy."""
        self._router = WorkflowRouter(strategy)

    async def handle_request(self, request: GatewayRequest) -> GatewayResponse:
        """Handle incoming gateway request."""
        start = time.monotonic()
        key = f"{request.method}:{request.path}"
        route = self._routes.get(key)
        if not route:
            return GatewayResponse(
                status_code=404,
                body={"error": "Route not found"},
                latency_ms=(time.monotonic() - start) * 1000
            )
        middlewares = sorted(
            [self._middleware[m] for m in route.middlewares if m in self._middleware],
            key=lambda m: m.priority
        )
        for mw in middlewares:
            if asyncio.iscoroutinefunction(mw.handler):
                result = await mw.handler(request)
            else:
                result = mw.handler(request)
            if result is not None and result is not True:
                return GatewayResponse(
                    status_code=400,
                    body={"error": f"Middleware {mw.name} rejected request"},
                    latency_ms=(time.monotonic() - start) * 1000
                )
        try:
            result = route.handler(request)
            if asyncio.iscoroutine(result):
                result = await asyncio.wait_for(result, timeout=route.timeout)
            return GatewayResponse(
                status_code=200,
                body=result,
                latency_ms=(time.monotonic() - start) * 1000
            )
        except asyncio.TimeoutError:
            return GatewayResponse(
                status_code=504,
                body={"error": "Gateway timeout"},
                latency_ms=(time.monotonic() - start) * 1000
            )
        except Exception as e:
            return GatewayResponse(
                status_code=500,
                body={"error": str(e)},
                latency_ms=(time.monotonic() - start) * 1000
            )
