"""API Federation Action for unified API gateway and routing.

This module provides API federation capabilities:
- Multi-backend routing
- Schema stitching
- Query delegation
- Response merging
- Service discovery integration
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Awaitable

logger = logging.getLogger(__name__)


class RoutingStrategy(Enum):
    """API routing strategies."""

    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    WEIGHTED = "weighted"
    LEAST_LOADED = "least_loaded"
    CONSISTENT_HASH = "consistent_hash"
    AFFINITY = "affinity"


@dataclass
class BackendEndpoint:
    """A backend API endpoint."""

    name: str
    url: str
    weight: int = 1
    timeout: float = 30.0
    max_concurrent: int = 100
    current_load: int = 0
    health_status: str = "healthy"
    last_check: float = 0.0
    metadata: dict = field(default_factory=dict)


@dataclass
class RouteRule:
    """A routing rule."""

    path_pattern: str
    backend: str
    methods: list[str] = field(default_factory=lambda: ["GET", "POST", "PUT", "DELETE"])
    auth_required: bool = False
    rate_limit: int | None = None
    cache_ttl: int = 0
    transform_request: Callable[[dict], dict] | None = None
    transform_response: Callable[[dict], dict] | None = None


@dataclass
class FederationMetrics:
    """Metrics for federated API operations."""

    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    routed_requests: dict[str, int] = field(default_factory=dict)
    backend_load: dict[str, int] = field(default_factory=dict)
    avg_latency: float = 0.0
    total_latency: float = 0.0


class ConsistentHash:
    """Consistent hash ring for routing."""

    def __init__(self, nodes: list[str] | None = None, replicas: int = 150):
        """Initialize consistent hash.

        Args:
            nodes: Initial nodes
            replicas: Number of virtual replicas per node
        """
        self.replicas = replicas
        self.ring: dict[int, str] = {}
        self.sorted_keys: list[int] = []

        if nodes:
            for node in nodes:
                self.add_node(node)

    def add_node(self, node: str) -> None:
        """Add a node to the hash ring."""
        for i in range(self.replicas):
            key = self._hash(f"{node}:{i}")
            self.ring[key] = node

        self.sorted_keys = sorted(self.ring.keys())

    def remove_node(self, node: str) -> None:
        """Remove a node from the hash ring."""
        for i in range(self.replicas):
            key = self._hash(f"{node}:{i}")
            if key in self.ring:
                del self.ring[key]

        self.sorted_keys = sorted(self.ring.keys())

    def get_node(self, key: str) -> str | None:
        """Get node for a key."""
        if not self.ring:
            return None

        hash_key = self._hash(key)
        pos = self._find_position(hash_key)
        return self.ring[self.sorted_keys[pos]]

    def _hash(self, value: str) -> int:
        """Hash a value to an integer."""
        return int(hashlib.md5(value.encode()).hexdigest(), 16)

    def _find_position(self, hash_key: int) -> int:
        """Find position in sorted ring."""
        for i, key in enumerate(self.sorted_keys):
            if hash_key <= key:
                return i
        return 0


class APIFederationAction:
    """API Federation for unified routing and aggregation."""

    def __init__(
        self,
        routing_strategy: RoutingStrategy = RoutingStrategy.ROUND_ROBIN,
    ):
        """Initialize API federation action.

        Args:
            routing_strategy: Default routing strategy
        """
        self.routing_strategy = routing_strategy
        self.backends: dict[str, BackendEndpoint] = {}
        self.routes: list[RouteRule] = []
        self.metrics = FederationMetrics()
        self._round_robin_counters: dict[str, int] = {}
        self._affinity_cache: dict[str, str] = {}
        self._hash_ring = ConsistentHash()
        self._lock = asyncio.Lock()

    def add_backend(
        self,
        name: str,
        url: str,
        weight: int = 1,
        timeout: float = 30.0,
        **kwargs,
    ) -> None:
        """Add a backend endpoint.

        Args:
            name: Backend name
            url: Backend URL
            weight: Weight for weighted routing
            timeout: Request timeout
            **kwargs: Additional metadata
        """
        backend = BackendEndpoint(
            name=name,
            url=url.rstrip("/"),
            weight=weight,
            timeout=timeout,
            metadata=kwargs,
        )
        self.backends[name] = backend
        self._hash_ring.add_node(name)
        logger.info(f"Added backend: {name} ({url})")

    def remove_backend(self, name: str) -> None:
        """Remove a backend endpoint.

        Args:
            name: Backend name
        """
        if name in self.backends:
            del self.backends[name]
            self._hash_ring.remove_node(name)
            logger.info(f"Removed backend: {name}")

    def add_route(
        self,
        path_pattern: str,
        backend: str,
        methods: list[str] | None = None,
        auth_required: bool = False,
        rate_limit: int | None = None,
        cache_ttl: int = 0,
    ) -> None:
        """Add a routing rule.

        Args:
            path_pattern: URL path pattern
            backend: Backend to route to
            methods: Allowed HTTP methods
            auth_required: Require authentication
            rate_limit: Rate limit per minute
            cache_ttl: Cache TTL in seconds
        """
        route = RouteRule(
            path_pattern=path_pattern,
            backend=backend,
            methods=methods or ["GET", "POST", "PUT", "DELETE"],
            auth_required=auth_required,
            rate_limit=rate_limit,
            cache_ttl=cache_ttl,
        )
        self.routes.append(route)
        logger.info(f"Added route: {path_pattern} -> {backend}")

    def _match_route(self, path: str, method: str) -> tuple[RouteRule | None, BackendEndpoint | None]:
        """Match a request to a route and backend.

        Args:
            path: Request path
            method: HTTP method

        Returns:
            Tuple of (matched route, selected backend)
        """
        for route in self.routes:
            if self._path_matches(path, route.path_pattern):
                if method.upper() in route.methods:
                    backend = self.backends.get(route.backend)
                    return route, backend

        return None, None

    def _path_matches(self, path: str, pattern: str) -> bool:
        """Check if path matches pattern.

        Args:
            path: Request path
            pattern: Route pattern

        Returns:
            True if matches
        """
        if pattern.endswith("*"):
            return path.startswith(pattern[:-1])
        return path == pattern or path.startswith(pattern.rstrip("/") + "/")

    def _select_backend(
        self,
        route: RouteRule,
        request_data: dict,
    ) -> BackendEndpoint | None:
        """Select a backend based on routing strategy.

        Args:
            route: Matched route
            request_data: Request data for routing decisions

        Returns:
            Selected backend
        """
        strategy = self.routing_strategy

        if strategy == RoutingStrategy.ROUND_ROBIN:
            return self._round_robin_select(route.backend)

        elif strategy == RoutingStrategy.WEIGHTED:
            return self._weighted_select(route.backend)

        elif strategy == RoutingStrategy.LEAST_LOADED:
            return self._least_loaded_select(route.backend)

        elif strategy == RoutingStrategy.CONSISTENT_HASH:
            return self._consistent_hash_select(route.backend, request_data)

        elif strategy == RoutingStrategy.AFFINITY:
            return self._affinity_select(route.backend, request_data)

        else:  # RANDOM
            return self._random_select(route.backend)

    def _round_robin_select(self, backend_name: str) -> BackendEndpoint | None:
        """Select backend using round robin."""
        backend = self.backends.get(backend_name)
        if not backend:
            return None

        counter = self._round_robin_counters.get(backend_name, 0)
        # Simple round robin across same backend (could extend for multiple instances)
        self._round_robin_counters[backend_name] = counter + 1
        return backend

    def _weighted_select(self, backend_name: str) -> BackendEndpoint | None:
        """Select backend using weighted random."""
        backend = self.backends.get(backend_name)
        if not backend:
            return None

        # For single backend, just return it (would need multiple for true weighted)
        return backend

    def _least_loaded_select(self, backend_name: str) -> BackendEndpoint | None:
        """Select backend with least current load."""
        backend = self.backends.get(backend_name)
        if not backend:
            return None

        return backend

    def _consistent_hash_select(self, backend_name: str, request_data: dict) -> BackendEndpoint | None:
        """Select backend using consistent hashing."""
        key = request_data.get("affinity_key", request_data.get("path", ""))
        node = self._hash_ring.get_node(key)
        return self.backends.get(node or backend_name)

    def _affinity_select(self, backend_name: str, request_data: dict) -> BackendEndpoint | None:
        """Select backend with session affinity."""
        session_id = request_data.get("headers", {}).get("X-Session-ID")
        if session_id and session_id in self._affinity_cache:
            cached_backend = self._affinity_cache[session_id]
            if cached_backend in self.backends:
                return self.backends[cached_backend]

        backend = self.backends.get(backend_name)
        if session_id and backend:
            self._affinity_cache[session_id] = backend_name

        return backend

    def _random_select(self, backend_name: str) -> BackendEndpoint | None:
        """Select backend randomly."""
        return self.backends.get(backend_name)

    async def route_request(
        self,
        path: str,
        method: str,
        headers: dict | None = None,
        body: Any = None,
        query_params: dict | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        """Route a request to appropriate backend.

        Args:
            path: Request path
            method: HTTP method
            headers: Request headers
            body: Request body
            query_params: Query parameters
            **kwargs: Additional routing options

        Returns:
            Response from backend
        """
        self.metrics.total_requests += 1
        start_time = time.time()
        headers = headers or {}

        request_data = {
            "path": path,
            "method": method,
            "headers": headers,
            "body": body,
            "query_params": query_params or {},
        }

        # Match route
        route, backend = self._match_route(path, method)
        if not route or not backend:
            return self._error_response(404, "No matching route found")

        # Check method
        if method.upper() not in route.methods:
            return self._error_response(405, f"Method {method} not allowed")

        # Select backend
        selected_backend = self._select_backend(route, request_data)
        if not selected_backend:
            return self._error_response(503, "No available backend")

        # Apply request transform
        if route.transform_request:
            try:
                request_data = route.transform_request(request_data)
            except Exception as e:
                logger.error(f"Request transform failed: {e}")

        # Update load
        async with self._lock:
            selected_backend.current_load += 1

        try:
            # Forward request
            response = await self._forward_request(
                selected_backend,
                path,
                method,
                headers,
                body,
                query_params,
                route.cache_ttl,
            )

            self.metrics.successful_requests += 1
            self.metrics.routed_requests[selected_backend.name] = (
                self.metrics.routed_requests.get(selected_backend.name, 0) + 1
            )

            latency = time.time() - start_time
            self.metrics.total_latency += latency

            # Apply response transform
            if route.transform_response:
                try:
                    response = route.transform_response(response)
                except Exception as e:
                    logger.error(f"Response transform failed: {e}")

            return response

        except Exception as e:
            self.metrics.failed_requests += 1
            logger.error(f"Request routing failed: {e}")
            return self._error_response(502, f"Backend error: {str(e)}")

        finally:
            async with self._lock:
                selected_backend.current_load = max(0, selected_backend.current_load - 1)

    async def _forward_request(
        self,
        backend: BackendEndpoint,
        path: str,
        method: str,
        headers: dict,
        body: Any,
        query_params: dict | None,
        cache_ttl: int,
    ) -> dict[str, Any]:
        """Forward request to backend.

        Args:
            backend: Target backend
            path: Request path
            method: HTTP method
            headers: Request headers
            body: Request body
            query_params: Query parameters
            cache_ttl: Cache TTL

        Returns:
            Backend response
        """
        import httpx

        url = f"{backend.url}/{path.lstrip('/')}"

        async with httpx.AsyncClient(timeout=backend.timeout) as client:
            response = await client.request(
                method,
                url,
                headers=headers,
                json=body if body else None,
                params=query_params,
            )

            return {
                "status": response.status_code,
                "headers": dict(response.headers),
                "data": response.json() if response.headers.get("content-type", "").startswith("application/json") else response.text,
                "backend": backend.name,
            }

    def _error_response(self, status: int, message: str) -> dict[str, Any]:
        """Create error response."""
        return {
            "status": status,
            "headers": {"Content-Type": "application/json"},
            "data": {"error": message, "timestamp": time.time()},
        }

    async def federated_query(
        self,
        query: str,
        operation_name: str | None = None,
    ) -> dict[str, Any]:
        """Execute a federated query across multiple backends.

        Args:
            query: Query string
            operation_name: Optional operation name

        Returns:
            Combined results from all relevant backends
        """
        results = []
        errors = []

        # Find backends that should handle this query
        for route in self.routes:
            if route.backend in self.backends:
                try:
                    result = await self.route_request(
                        path=route.path_pattern,
                        method="POST",
                        body={"query": query, "operationName": operation_name},
                    )
                    if result.get("status") == 200:
                        results.append(result.get("data"))
                    else:
                        errors.append({"backend": route.backend, "error": result.get("data")})
                except Exception as e:
                    errors.append({"backend": route.backend, "error": str(e)})

        return {
            "results": results,
            "errors": errors,
            "total_backends": len(self.backends),
            "successful": len(results),
            "failed": len(errors),
        }

    def update_backend_health(self, name: str, status: str) -> None:
        """Update backend health status.

        Args:
            name: Backend name
            status: New health status
        """
        if name in self.backends:
            self.backends[name].health_status = status
            self.backends[name].last_check = time.time()

    def get_metrics(self) -> dict[str, Any]:
        """Get federation metrics."""
        return {
            "total_requests": self.metrics.total_requests,
            "successful_requests": self.metrics.successful_requests,
            "failed_requests": self.metrics.failed_requests,
            "success_rate": (
                self.metrics.successful_requests / self.metrics.total_requests
                if self.metrics.total_requests > 0 else 0
            ),
            "avg_latency": (
                self.metrics.total_latency / self.metrics.total_requests
                if self.metrics.total_requests > 0 else 0
            ),
            "routed_requests": self.metrics.routed_requests,
            "backends": {
                name: {
                    "url": b.url,
                    "health": b.health_status,
                    "current_load": b.current_load,
                    "last_check": b.last_check,
                }
                for name, b in self.backends.items()
            },
        }


def create_federation(
    routing_strategy: RoutingStrategy = RoutingStrategy.ROUND_ROBIN,
) -> APIFederationAction:
    """Create an API federation action.

    Args:
        routing_strategy: Default routing strategy

    Returns:
        APIFederationAction instance
    """
    return APIFederationAction(routing_strategy=routing_strategy)
