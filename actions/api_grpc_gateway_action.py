"""
API gRPC Gateway Action Module.

Provides gRPC to REST/HTTP gateway functionality with
protocol translation and service discovery.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class ProtocolType(Enum):
    """Protocol types for gateway."""

    GRPC = "grpc"
    REST = "rest"
    WEBSOCKET = "websocket"


@dataclass
class ServiceMethod:
    """Represents a gRPC service method."""

    name: str
    full_name: str
    input_type: str
    output_type: str
    is_client_streaming: bool = False
    is_server_streaming: bool = False


@dataclass
class GatewayRoute:
    """Route configuration for gateway."""

    http_method: str
    http_path: str
    grpc_method: str
    protocol: ProtocolType = ProtocolType.GRPC


class APIGrpcGatewayAction:
    """
    gRPC gateway for translating REST/HTTP requests to gRPC.

    Features:
    - Automatic protocol translation
    - Service method registration
    - Request/response marshaling
    - Error handling and mapping

    Example:
        gateway = APIGrpcGatewayAction()
        gateway.register_service("UserService", user_methods)
        result = await gateway.handle_request("GET", "/users/123")
    """

    def __init__(self, grpc_host: str = "localhost", grpc_port: int = 50051) -> None:
        """
        Initialize gRPC gateway.

        Args:
            grpc_host: gRPC server host.
            grpc_port: gRPC server port.
        """
        self.grpc_host = grpc_host
        self.grpc_port = grpc_port
        self._services: dict[str, dict[str, ServiceMethod]] = {}
        self._routes: list[GatewayRoute] = []
        self._request_count = 0
        self._error_count = 0

    def register_service(
        self,
        service_name: str,
        methods: list[ServiceMethod],
    ) -> None:
        """
        Register a gRPC service with its methods.

        Args:
            service_name: Name of the gRPC service.
            methods: List of service methods.
        """
        self._services[service_name] = {m.name: m for m in methods}
        logger.info(f"Registered gRPC service: {service_name} with {len(methods)} methods")

    def add_route(
        self,
        http_method: str,
        http_path: str,
        grpc_method: str,
        protocol: ProtocolType = ProtocolType.GRPC,
    ) -> None:
        """
        Add a gateway route mapping HTTP to gRPC.

        Args:
            http_method: HTTP method (GET, POST, etc.).
            http_path: HTTP path template.
            grpc_method: Full gRPC method name.
            protocol: Protocol type.
        """
        route = GatewayRoute(
            http_method=http_method,
            http_path=http_path,
            grpc_method=grpc_method,
            protocol=protocol,
        )
        self._routes.append(route)
        logger.info(f"Added route: {http_method} {http_path} -> {grpc_method}")

    async def handle_request(
        self,
        method: str,
        path: str,
        headers: Optional[dict[str, str]] = None,
        body: Optional[Any] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """
        Handle an incoming HTTP/REST request.

        Args:
            method: HTTP method.
            path: Request path.
            headers: Request headers.
            body: Request body.
            params: Query parameters.

        Returns:
            Response data.
        """
        self._request_count += 1

        route = self._find_route(method, path)
        if not route:
            self._error_count += 1
            return {"error": "Route not found", "status": 404}

        try:
            grpc_request = self._prepare_grpc_request(route, body, params)
            grpc_response = await self._call_grpc(route.grpc_method, grpc_request)
            return self._prepare_http_response(grpc_response, route)
        except Exception as e:
            self._error_count += 1
            logger.error(f"Gateway error: {e}")
            return {"error": str(e), "status": 500}

    def _find_route(self, method: str, path: str) -> Optional[GatewayRoute]:
        """Find matching route for request."""
        for route in self._routes:
            if route.http_method.upper() == method.upper():
                if self._match_path(route.http_path, path):
                    return route
        return None

    def _match_path(self, pattern: str, path: str) -> bool:
        """Match path with parameter support."""
        pattern_parts = pattern.strip("/").split("/")
        path_parts = path.strip("/").split("/")

        if len(pattern_parts) != len(path_parts):
            return False

        for p, a in zip(pattern_parts, path_parts):
            if p.startswith("{") and p.endswith("}"):
                continue
            if p != a:
                return False

        return True

    def _extract_path_params(self, pattern: str, path: str) -> dict[str, str]:
        """Extract parameters from path."""
        pattern_parts = pattern.strip("/").split("/")
        path_parts = path.strip("/").split("/")
        params = {}

        for p, a in zip(pattern_parts, path_parts):
            if p.startswith("{") and p.endswith("}"):
                params[p[1:-1]] = a

        return params

    def _prepare_grpc_request(
        self,
        route: GatewayRoute,
        body: Optional[Any],
        params: Optional[dict[str, Any]],
    ) -> dict[str, Any]:
        """Prepare gRPC request from HTTP request."""
        path_params = self._extract_path_params(route.http_path, route.http_path)
        request = {}

        if body:
            request.update(body if isinstance(body, dict) else {})
        if params:
            request.update(params)

        return request

    async def _call_grpc(
        self,
        method: str,
        request: dict[str, Any],
    ) -> dict[str, Any]:
        """Call gRPC service method."""
        await self._simulate_grpc_call()
        return {"status": "ok", "data": request, "method": method}

    async def _simulate_grpc_call(self) -> None:
        """Simulate gRPC call latency."""
        import asyncio
        await asyncio.sleep(0.001)

    def _prepare_http_response(
        self,
        grpc_response: dict[str, Any],
        route: GatewayRoute,
    ) -> dict[str, Any]:
        """Prepare HTTP response from gRPC response."""
        return {
            "status": 200,
            "headers": {
                "Content-Type": "application/json",
                "X-Protocol": route.protocol.value,
            },
            "body": grpc_response,
        }

    def get_registered_services(self) -> list[str]:
        """Get list of registered service names."""
        return list(self._services.keys())

    def get_stats(self) -> dict[str, Any]:
        """
        Get gateway statistics.

        Returns:
            Statistics dictionary.
        """
        return {
            "total_requests": self._request_count,
            "errors": self._error_count,
            "error_rate": f"{(self._error_count / max(1, self._request_count)):.2%}",
            "registered_services": len(self._services),
            "total_routes": len(self._routes),
        }
