# Copyright (c) 2024. coded by claude
"""API Gateway Action Module.

Implements API gateway functionality with support for request routing,
load balancing, and middleware chain execution.
"""
from typing import Optional, Dict, Any, List, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import logging

logger = logging.getLogger(__name__)


class LoadBalancingStrategy(Enum):
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    RANDOM = "random"
    WEIGHTED = "weighted"


@dataclass
class Route:
    path_pattern: str
    target_service: str
    methods: List[str]
    timeout: float = 30.0
    retry_count: int = 0


@dataclass
class BackendService:
    name: str
    url: str
    weight: int = 1
    max_connections: int = 100
    active_connections: int = 0


@dataclass
class GatewayRequest:
    method: str
    path: str
    headers: Dict[str, str]
    body: Optional[Any] = None
    query_params: Dict[str, str] = field(default_factory=dict)


@dataclass
class GatewayResponse:
    status_code: int
    headers: Dict[str, str]
    body: Any


class APIGateway:
    def __init__(self, load_balancing: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN):
        self.load_balancing = load_balancing
        self._routes: Dict[str, Route] = {}
        self._services: Dict[str, List[BackendService]] = {}
        self._middleware: List[Callable] = []
        self._current_index: Dict[str, int] = {}

    def add_route(self, route: Route) -> None:
        self._routes[route.path_pattern] = route

    def add_service(self, service: BackendService) -> None:
        if service.name not in self._services:
            self._services[service.name] = []
        self._services[service.name].append(service)

    def add_middleware(self, middleware: Callable) -> None:
        self._middleware.append(middleware)

    async def handle_request(self, request: GatewayRequest) -> GatewayResponse:
        for mw in self._middleware:
            result = mw(request)
            if asyncio.iscoroutine(result):
                result = await result
            if result is not None and isinstance(result, GatewayResponse):
                return result
        route = self._find_route(request.path)
        if not route:
            return GatewayResponse(status_code=404, headers={}, body={"error": "Route not found"})
        service = self._select_backend(route.target_service)
        if not service:
            return GatewayResponse(status_code=503, headers={}, body={"error": "Service unavailable"})
        return await self._proxy_to_backend(service, route, request)

    def _find_route(self, path: str) -> Optional[Route]:
        for pattern, route in self._routes.items():
            if path.startswith(pattern):
                return route
        return None

    def _select_backend(self, service_name: str) -> Optional[BackendService]:
        services = self._services.get(service_name, [])
        if not services:
            return None
        if self.load_balancing == LoadBalancingStrategy.ROUND_ROBIN:
            if service_name not in self._current_index:
                self._current_index[service_name] = 0
            idx = self._current_index[service_name]
            service = services[idx % len(services)]
            self._current_index[service_name] = idx + 1
            return service
        elif self.load_balancing == LoadBalancingStrategy.LEAST_CONNECTIONS:
            return min(services, key=lambda s: s.active_connections)
        elif self.load_balancing == LoadBalancingStrategy.RANDOM:
            import random
            return random.choice(services)
        return services[0]

    async def _proxy_to_backend(self, service: BackendService, route: Route, request: GatewayRequest) -> GatewayResponse:
        return GatewayResponse(
            status_code=200,
            headers={"X-Gateway": "handled"},
            body={"service": service.name, "path": request.path},
        )
