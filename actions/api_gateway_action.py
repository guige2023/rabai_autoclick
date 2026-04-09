"""
API Gateway Action Module.

Provides API gateway functionality with routing, load balancing,
rate limiting, and request/response transformation.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple


class LoadBalancingStrategy(Enum):
    """Load balancing strategies."""
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    WEIGHTED = "weighted"
    RANDOM = "random"
    IP_HASH = "ip_hash"


@dataclass
class Backend:
    """Backend server definition."""
    url: str
    weight: int = 1
    max_connections: int = 100
    timeout: float = 30.0
    healthy: bool = True
    current_connections: int = 0


@dataclass
class Route:
    """API route definition."""
    path: str
    methods: List[str]
    backend: str
    auth_required: bool = False
    rate_limit: Optional[Tuple[int, float]] = None  # (requests, window_seconds)
    transforms: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GatewayConfig:
    """Gateway configuration."""
    host: str = "0.0.0.0"
    port: int = 8080
    backends: Dict[str, List[Backend]] = field(default_factory=dict)
    routes: List[Route] = field(default_factory=list)
    default_timeout: float = 30.0
    enable_logging: bool = True


class LoadBalancer:
    """Load balancer for backend servers."""

    def __init__(self, strategy: LoadBalancingStrategy) -> None:
        self.strategy = strategy
        self.round_robin_index: Dict[str, int] = defaultdict(int)
        self.connection_counts: Dict[str, int] = defaultdict(int)

    def select_backend(
        self,
        backends: List[Backend],
        key: Optional[str] = None,
    ) -> Optional[Backend]:
        """Select a backend based on strategy."""
        healthy = [b for b in backends if b.healthy]
        if not healthy:
            return None

        if self.strategy == LoadBalancingStrategy.ROUND_ROBIN:
            idx = self.round_robin_index[id(backends)]
            self.round_robin_index[id(backends)] = (idx + 1) % len(healthy)
            return healthy[idx % len(healthy)]

        elif self.strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
            return min(healthy, key=lambda b: b.current_connections)

        elif self.strategy == LoadBalancingStrategy.WEIGHTED:
            total_weight = sum(b.weight for b in healthy)
            r = hash(str(time.time()).encode()) % total_weight
            cumsum = 0
            for b in healthy:
                cumsum += b.weight
                if r < cumsum:
                    return b
            return healthy[-1]

        elif self.strategy == LoadBalancingStrategy.IP_HASH:
            if key:
                hash_val = int(hashlib.md5(key.encode()).hexdigest(), 16)
                return healthy[hash_val % len(healthy)]
            return healthy[0]

        else:  # RANDOM
            import random
            return healthy[int(random.random() * len(healthy))]


@dataclass
class RequestMetrics:
    """Metrics for a request."""
    path: str
    method: str
    backend: str
    status_code: int
    response_time: float
    timestamp: float


class APIGateway:
    """Main API Gateway class."""

    def __init__(self, config: GatewayConfig) -> None:
        self.config = config
        self.load_balancers: Dict[str, LoadBalancer] = {}
        self.metrics: List[RequestMetrics] = []
        self.request_counts: Dict[str, Dict[str, List[float]]] = defaultdict(
            lambda: defaultdict(list)
        )

        for backend_name in config.backends:
            self.load_balancers[backend_name] = LoadBalancer(
                LoadBalancingStrategy.ROUND_ROBIN
            )

    def add_route(self, route: Route) -> None:
        """Add a route to the gateway."""
        self.config.routes.append(route)

    def match_route(
        self,
        path: str,
        method: str,
    ) -> Optional[Route]:
        """Find matching route for path and method."""
        for route in self.config.routes:
            if self._path_matches(route.path, path) and method in route.methods:
                return route
        return None

    def _path_matches(self, pattern: str, path: str) -> bool:
        """Simple path pattern matching."""
        pattern_parts = pattern.strip("/").split("/")
        path_parts = path.strip("/").split("/")

        if len(pattern_parts) != len(path_parts):
            return False

        for p, part in zip(pattern_parts, path_parts):
            if p.startswith("{") and p.endswith("}"):
                continue
            if p != part:
                return False

        return True

    def check_rate_limit(
        self,
        client_id: str,
        limit: Tuple[int, float],
    ) -> bool:
        """Check if request is within rate limit."""
        requests, window_seconds = limit
        now = time.time()
        cutoff = now - window_seconds

        client_requests = self.request_counts[client_id]
        # Clean old entries
        for endpoint in list(client_requests):
            client_requests[endpoint] = [
                t for t in client_requests[endpoint] if t > cutoff
            ]

        total_requests = sum(len(v) for v in client_requests.values())
        return total_requests < requests

    def record_request(self, metrics: RequestMetrics) -> None:
        """Record request metrics."""
        self.metrics.append(metrics)
        if len(self.metrics) > 10000:
            self.metrics = self.metrics[-5000:]

    async def forward_request(
        self,
        route: Route,
        path: str,
        method: str,
        headers: Dict[str, str],
        body: Optional[bytes],
    ) -> Tuple[int, Dict[str, str], bytes]:
        """Forward request to backend."""
        import aiohttp

        backends = self.config.backends.get(route.backend, [])
        if not backends:
            return 502, {}, b"Bad Gateway: No backends available"

        balancer = self.load_balancers.get(route.backend)
        if not balancer:
            return 502, {}, b"Bad Gateway: No load balancer"

        backend = balancer.select_backend(backends, key=headers.get("X-Forwarded-For"))
        if not backend:
            return 503, {}, b"Service Unavailable: No healthy backends"

        backend.current_connections += 1
        start_time = time.time()

        try:
            url = f"{backend.url}{path}"
            async with aiohttp.ClientSession() as session:
                async with session.request(
                    method,
                    url,
                    headers=headers,
                    data=body,
                    timeout=aiohttp.ClientTimeout(total=backend.timeout),
                ) as response:
                    response_body = await response.read()
                    status = response.status
                    response_headers = dict(response.headers)
                    return status, response_headers, response_body
        except asyncio.TimeoutError:
            return 504, {}, b"Gateway Timeout"
        except Exception as e:
            return 502, {}, f"Bad Gateway: {str(e)}".encode()
        finally:
            backend.current_connections -= 1
            elapsed = time.time() - start_time
            self.record_request(RequestMetrics(
                path=path,
                method=method,
                backend=backend.url,
                status_code=200,
                response_time=elapsed,
                timestamp=time.time(),
            ))

    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of gateway metrics."""
        if not self.metrics:
            return {}

        total_requests = len(self.metrics)
        avg_response_time = sum(m.response_time for m in self.metrics) / total_requests
        status_counts: Dict[int, int] = defaultdict(int)
        for m in self.metrics:
            status_counts[m.status_code] += 1

        return {
            "total_requests": total_requests,
            "avg_response_time": avg_response_time,
            "status_codes": dict(status_counts),
        }
