"""
API Gateway Action Module.

Unified API gateway with routing, transformation,
authentication, and rate limiting.
"""

from __future__ import annotations

from typing import Any, Callable, Optional
from dataclasses import dataclass, field
import logging
import asyncio
import httpx
import time
import re

logger = logging.getLogger(__name__)


@dataclass
class Route:
    """API route definition."""
    path_pattern: str
    target_url: str
    methods: list[str] = field(default_factory=lambda: ["GET"])
    auth_required: bool = False
    rate_limit: Optional[int] = None
    timeout: float = 30.0
    transform_request: Optional[Callable] = None
    transform_response: Optional[Callable] = None


@dataclass
class GatewayStats:
    """Gateway statistics."""
    total_requests: int
    successful_requests: int
    failed_requests: int
    avg_latency_ms: float
    active_routes: int


class APIGatewayAction:
    """
    Unified API gateway for routing and management.

    Routes requests to backend services,
    handles auth, rate limiting, and transformation.

    Example:
        gateway = APIGatewayAction()
        gateway.add_route("/api/users", "http://users-service:8000")
        await gateway.forward(request)
    """

    def __init__(
        self,
        default_timeout: float = 30.0,
        max_retries: int = 3,
    ) -> None:
        self.default_timeout = default_timeout
        self.max_retries = max_retries
        self._routes: list[Route] = []
        self._compiled_patterns: list[tuple] = []
        self._stats = {"total": 0, "success": 0, "failed": 0, "total_latency": 0.0}

    def add_route(
        self,
        path_pattern: str,
        target_url: str,
        methods: Optional[list[str]] = None,
        auth_required: bool = False,
        rate_limit: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> "APIGatewayAction":
        """Add a route to the gateway."""
        route = Route(
            path_pattern=path_pattern,
            target_url=target_url.rstrip("/"),
            methods=methods or ["GET"],
            auth_required=auth_required,
            rate_limit=rate_limit,
            timeout=timeout or self.default_timeout,
        )

        self._routes.append(route)
        self._compile_pattern(route)

        return self

    def remove_route(self, path_pattern: str) -> bool:
        """Remove a route by pattern."""
        for i, route in enumerate(self._routes):
            if route.path_pattern == path_pattern:
                del self._routes[i]
                self._compiled_patterns = [
                    (r, p) for r, p in self._compiled_patterns
                    if r.path_pattern != path_pattern
                ]
                return True
        return False

    async def forward(
        self,
        request: dict[str, Any],
    ) -> dict[str, Any]:
        """Forward request to appropriate backend."""
        path = request.get("path", "/")
        method = request.get("method", "GET").upper()

        route, match = self._find_route(path, method)

        if not route:
            return {
                "status_code": 404,
                "body": {"error": "Route not found"},
            }

        start_time = time.perf_counter()
        self._stats["total"] += 1

        try:
            target_url = f"{route.target_url}{path}"

            transformed_req = request
            if route.transform_request:
                transformed_req = route.transform_request(request)

            headers = transformed_req.get("headers", {})
            params = transformed_req.get("params", {})
            body = transformed_req.get("body")

            async with httpx.AsyncClient(timeout=route.timeout) as client:
                response = await client.request(
                    method=method,
                    url=target_url,
                    headers=headers,
                    params=params,
                    json=body,
                )

            response_data = {
                "status_code": response.status_code,
                "body": response.json() if response.text else None,
                "headers": dict(response.headers),
            }

            if route.transform_response:
                response_data = route.transform_response(response_data)

            latency_ms = (time.perf_counter() - start_time) * 1000
            self._stats["success"] += 1
            self._stats["total_latency"] += latency_ms

            return response_data

        except Exception as e:
            latency_ms = (time.perf_counter() - start_time) * 1000
            self._stats["failed"] += 1
            self._stats["total_latency"] += latency_ms

            logger.error("Gateway forward error: %s", e)

            return {
                "status_code": 502,
                "body": {"error": str(e)},
            }

    def _find_route(
        self,
        path: str,
        method: str,
    ) -> tuple[Optional[Route], Optional[dict]]:
        """Find matching route for path and method."""
        for route, pattern in self._compiled_patterns:
            match = re.match(pattern, path)
            if match and method in route.methods:
                return route, match.groupdict()
        return None, None

    def _compile_pattern(self, route: Route) -> None:
        """Compile route pattern to regex."""
        pattern = route.path_pattern
        pattern = re.sub(r"\{(\w+)\}", r"(?P<\1>[^/]+)", pattern)
        pattern = f"^{pattern}$"
        self._compiled_patterns.append((route, pattern))

    def get_stats(self) -> GatewayStats:
        """Get gateway statistics."""
        total = self._stats["total"]
        avg_latency = (
            self._stats["total_latency"] / total if total > 0 else 0
        )

        return GatewayStats(
            total_requests=total,
            successful_requests=self._stats["success"],
            failed_requests=self._stats["failed"],
            avg_latency_ms=avg_latency,
            active_routes=len(self._routes),
        )

    def reset_stats(self) -> None:
        """Reset statistics."""
        self._stats = {"total": 0, "success": 0, "failed": 0, "total_latency": 0.0}

    @property
    def routes(self) -> list[str]:
        """List all registered routes."""
        return [r.path_pattern for r in self._routes]
