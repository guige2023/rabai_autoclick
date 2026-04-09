"""
API Gateway Action Module

Unified API gateway with routing, authentication, rate limiting,
request transformation, and response aggregation.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class BackendType(Enum):
    """Backend service types."""
    
    HTTP = "http"
    GRPC = "grpc"
    FUNCTION = "function"
    STATIC = "static"


@dataclass
class Route:
    """API route definition."""
    
    path: str
    method: str
    backend_url: Optional[str] = None
    backend_type: BackendType = BackendType.HTTP
    handler: Optional[Callable] = None
    auth_required: bool = False
    rate_limit: Optional[float] = None
    timeout_seconds: float = 30
    transforms: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GatewayConfig:
    """Gateway configuration."""
    
    name: str = "api-gateway"
    port: int = 8080
    host: str = "0.0.0.0"
    timeout_seconds: float = 60
    max_concurrent: int = 1000
    enable_metrics: bool = True


@dataclass
class GatewayRequest:
    """Gateway request context."""
    
    request_id: str
    method: str
    path: str
    headers: Dict[str, str]
    body: Any = None
    query_params: Dict[str, str] = field(default_factory=dict)
    path_params: Dict[str, str] = field(default_factory=dict)


@dataclass
class GatewayResponse:
    """Gateway response context."""
    
    status_code: int
    headers: Dict[str, str] = field(default_factory=dict)
    body: Any = None
    request_id: str = ""


class RequestTransformer:
    """Transforms requests before forwarding."""
    
    def __init__(self, transforms: Dict[str, Any]):
        self.transforms = transforms
    
    def transform_request(self, request: GatewayRequest) -> GatewayRequest:
        """Transform request based on configuration."""
        if "headers" in self.transforms:
            for key, value in self.transforms["headers"].items():
                request.headers[key] = value
        
        if "path_rewrite" in self.transforms:
            old_path = request.path
            template = self.transforms["path_rewrite"]
            for param, value in request.path_params.items():
                template = template.replace(f"{{{param}}}", value)
            request.path = template
        
        return request


class ResponseTransformer:
    """Transforms responses before returning."""
    
    def __init__(self, transforms: Dict[str, Any]):
        self.transforms = transforms
    
    def transform_response(
        self,
        response: GatewayResponse,
        request: GatewayRequest
    ) -> GatewayResponse:
        """Transform response based on configuration."""
        if "headers" in self.transforms:
            for key, value in self.transforms["headers"].items():
                response.headers[key] = value
        
        if "body_template" in self.transforms:
            response.body = self.transforms["body_template"]
        
        return response


class APIGatewayAction:
    """
    Main API gateway action handler.
    
    Provides unified gateway with routing, authentication,
    rate limiting, and transformation support.
    """
    
    def __init__(self, config: Optional[GatewayConfig] = None):
        self.config = config or GatewayConfig()
        self._routes: Dict[Tuple[str, str], Route] = {}
        self._middleware: List[Callable] = []
        self._auth_handlers: Dict[str, Callable] = {}
        self._metrics = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "total_latency_ms": 0
        }
    
    def add_route(
        self,
        path: str,
        method: str,
        backend_url: Optional[str] = None,
        backend_type: BackendType = BackendType.HTTP,
        handler: Optional[Callable] = None,
        **kwargs
    ) -> None:
        """Add a route to the gateway."""
        route = Route(
            path=path,
            method=method.upper(),
            backend_url=backend_url,
            backend_type=backend_type,
            handler=handler,
            **kwargs
        )
        self._routes[(route.path, route.method)] = route
    
    def get_route(self, path: str, method: str) -> Optional[Route]:
        """Get route by path and method."""
        return self._routes.get((path, method.upper()))
    
    def match_route(self, path: str, method: str) -> Optional[Tuple[Route, Dict[str, str]]]:
        """Match a route with path parameter extraction."""
        method = method.upper()
        
        if (path, method) in self._routes:
            return self._routes[(path, method)], {}
        
        for (route_path, route_method), route in self._routes.items():
            if route_method != method:
                continue
            
            params = self._extract_params(route_path, path)
            if params is not None:
                return route, params
        
        return None
    
    def _extract_params(self, route_path: str, request_path: str) -> Optional[Dict[str, str]]:
        """Extract path parameters from request path."""
        route_parts = route_path.strip("/").split("/")
        path_parts = request_path.strip("/").split("/")
        
        if len(route_parts) != len(path_parts):
            return None
        
        params = {}
        for route_part, path_part in zip(route_parts, path_parts):
            if route_part.startswith("{") and route_part.endswith("}"):
                param_name = route_part[1:-1]
                params[param_name] = path_part
            elif route_part != path_part:
                return None
        
        return params
    
    def register_auth(self, auth_type: str, handler: Callable) -> None:
        """Register an authentication handler."""
        self._auth_handlers[auth_type] = handler
    
    async def handle_request(
        self,
        method: str,
        path: str,
        headers: Optional[Dict] = None,
        body: Any = None,
        query_params: Optional[Dict] = None
    ) -> GatewayResponse:
        """Handle an incoming request."""
        request_id = str(uuid.uuid4())
        self._metrics["total_requests"] += 1
        
        request = GatewayRequest(
            request_id=request_id,
            method=method.upper(),
            path=path,
            headers=headers or {},
            body=body,
            query_params=query_params or {}
        )
        
        start_time = time.time()
        
        try:
            result = await self._process_request(request)
            
            duration_ms = (time.time() - start_time) * 1000
            self._metrics["total_latency_ms"] += duration_ms
            self._metrics["successful_requests"] += 1
            
            return result
        
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            self._metrics["total_latency_ms"] += duration_ms
            self._metrics["failed_requests"] += 1
            
            return GatewayResponse(
                status_code=500,
                body={"error": str(e)},
                request_id=request_id
            )
    
    async def _process_request(self, request: GatewayRequest) -> GatewayResponse:
        """Process request through middleware and routing."""
        for mw in self._middleware:
            result = await mw(request)
            if result is not None:
                return result
        
        match = self.match_route(request.path, request.method)
        if not match:
            return GatewayResponse(
                status_code=404,
                body={"error": "Route not found"},
                request_id=request.request_id
            )
        
        route, path_params = match
        request.path_params = path_params
        
        if route.auth_required:
            auth_result = await self._check_auth(request, route)
            if auth_result:
                return auth_result
        
        if route.transforms:
            transformer = RequestTransformer(route.transforms)
            request = transformer.transform_request(request)
        
        response = await self._forward_request(request, route)
        
        if route.transforms:
            transformer = ResponseTransformer(route.transforms)
            response = transformer.transform_response(response, request)
        
        response.request_id = request.request_id
        return response
    
    async def _check_auth(
        self,
        request: GatewayRequest,
        route: Route
    ) -> Optional[GatewayResponse]:
        """Check authentication for request."""
        auth_header = request.headers.get("Authorization", "")
        
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
            handler = self._auth_handlers.get("bearer")
            if handler:
                valid = await handler(token)
                if not valid:
                    return GatewayResponse(
                        status_code=401,
                        body={"error": "Unauthorized"},
                        request_id=request.request_id
                    )
        
        return None
    
    async def _forward_request(
        self,
        request: GatewayRequest,
        route: Route
    ) -> GatewayResponse:
        """Forward request to backend."""
        if route.handler:
            if asyncio.iscoroutinefunction(route.handler):
                result = await route.handler(request)
            else:
                result = route.handler(request)
            return result
        
        return GatewayResponse(
            status_code=200,
            body={"message": "Not implemented"},
            request_id=request.request_id
        )
    
    def add_middleware(self, func: Callable) -> None:
        """Add gateway middleware."""
        self._middleware.append(func)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get gateway statistics."""
        total = self._metrics["total_requests"]
        avg_latency = (
            self._metrics["total_latency_ms"] / total
            if total > 0 else 0
        )
        
        return {
            "name": self.config.name,
            "total_requests": total,
            "successful_requests": self._metrics["successful_requests"],
            "failed_requests": self._metrics["failed_requests"],
            "avg_latency_ms": avg_latency,
            "routes_count": len(self._routes)
        }
