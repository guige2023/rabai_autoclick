"""
API Gateway Action - API Gateway with routing and load balancing.

This module provides API gateway capabilities including request
routing, load balancing, and request/response transformation.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable
from enum import Enum


class RoutingStrategy(Enum):
    """Routing strategies for API gateway."""
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    WEIGHTED = "weighted"
    LEAST_LATENCY = "least_latency"


@dataclass
class Route:
    """A route configuration."""
    path_prefix: str
    target_url: str
    methods: list[str] = field(default_factory=lambda: ["GET"])
    timeout: float = 30.0
    retry_count: int = 3


@dataclass
class GatewayConfig:
    """Configuration for API gateway."""
    routes: list[Route] = field(default_factory=list)
    default_timeout: float = 30.0
    rate_limit: int = 100


@dataclass
class GatewayResult:
    """Result of gateway request."""
    success: bool
    status_code: int | None = None
    data: Any = None
    error: str | None = None
    duration_ms: float = 0.0
    route: str | None = None


class APIRouter:
    """Routes requests to appropriate backends."""
    
    def __init__(self, routes: list[Route]) -> None:
        self.routes = routes
    
    def match_route(self, path: str, method: str) -> Route | None:
        """Match a route for the given path and method."""
        for route in self.routes:
            if path.startswith(route.path_prefix) and method in route.methods:
                return route
        return None


class APIGatewayAction:
    """
    API Gateway action for routing requests.
    
    Example:
        gateway = APIGatewayAction()
        gateway.add_route("/api/users", "https://users.example.com")
        result = await gateway.handle_request("GET", "/api/users")
    """
    
    def __init__(self, config: GatewayConfig | None = None) -> None:
        self.config = config or GatewayConfig()
        self.router = APIRouter(self.config.routes)
        self._latencies: dict[str, list[float]] = {}
    
    def add_route(self, path_prefix: str, target_url: str, methods: list[str] | None = None) -> None:
        """Add a route to the gateway."""
        route = Route(path_prefix=path_prefix, target_url=target_url, methods=methods or ["GET"])
        self.config.routes.append(route)
        self.router = APIRouter(self.config.routes)
    
    async def handle_request(self, method: str, path: str, **kwargs) -> GatewayResult:
        """Handle an incoming request."""
        start_time = time.time()
        route = self.router.match_route(path, method)
        
        if not route:
            return GatewayResult(success=False, error="No route found", duration_ms=0.0)
        
        target_url = f"{route.target_url.rstrip('/')}{path}"
        
        try:
            import aiohttp
            timeout = aiohttp.ClientTimeout(total=route.timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.request(method, target_url, **kwargs) as response:
                    try:
                        data = await response.json()
                    except Exception:
                        data = await response.text()
                    latency = (time.time() - start_time) * 1000
                    self._record_latency(route.target_url, latency)
                    return GatewayResult(
                        success=response.status < 400,
                        status_code=response.status,
                        data=data,
                        duration_ms=latency,
                        route=route.path_prefix,
                    )
        except Exception as e:
            return GatewayResult(success=False, error=str(e), duration_ms=(time.time() - start_time) * 1000, route=route.path_prefix)
    
    def _record_latency(self, target: str, latency: float) -> None:
        """Record latency for a target."""
        if target not in self._latencies:
            self._latencies[target] = []
        self._latencies[target].append(latency)
        if len(self._latencies[target]) > 100:
            self._latencies[target] = self._latencies[target][-100:]


__all__ = ["RoutingStrategy", "Route", "GatewayConfig", "GatewayResult", "APIRouter", "APIGatewayAction"]
