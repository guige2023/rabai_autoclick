"""
API Gateway Action Module

Provides API gateway functionality, routing, load balancing, and request handling.
"""
from typing import Any, Optional, Callable, Literal
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from collections import defaultdict
import asyncio
import hashlib


class LoadBalancingStrategy(Enum):
    """Load balancing strategies."""
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    IP_HASH = "ip_hash"
    RANDOM = "random"
    WEIGHTED = "weighted"


@dataclass
class UpstreamServer:
    """An upstream server in the gateway."""
    server_id: str
    url: str
    weight: int = 1
    healthy: bool = True
    active_connections: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Route:
    """A gateway route definition."""
    route_id: str
    path_prefix: str
    upstream: str
    methods: list[str] = field(default_factory=lambda: ["GET"])
    timeout_seconds: float = 30.0
    retry_attempts: int = 1
    strip_path: bool = False
    plugins: list[str] = field(default_factory=list)


@dataclass
class GatewayConfig:
    """Gateway configuration."""
    name: str
    port: int = 8080
    host: str = "0.0.0.0"
    upstream_timeout: float = 60.0
    max_connections: int = 10000
    keepalive_timeout: float = 60.0


@dataclass
class GatewayRequest:
    """An incoming gateway request."""
    request_id: str
    method: str
    path: str
    headers: dict[str, str]
    query_params: dict[str, str]
    body: Optional[bytes] = None
    client_ip: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class GatewayResponse:
    """Gateway response."""
    status_code: int
    headers: dict[str, str]
    body: Any
    upstream: str
    duration_ms: float


class LoadBalancer:
    """Load balancer implementation."""
    
    def __init__(self, strategy: LoadBalancingStrategy):
        self.strategy = strategy
        self._servers: dict[str, UpstreamServer] = {}
        self._current_index: dict[str, int] = defaultdict(int)
        self._connection_counts: dict[str, int] = defaultdict(int)
    
    def add_server(self, server: UpstreamServer):
        """Add a server to the pool."""
        self._servers[server.server_id] = server
    
    def remove_server(self, server_id: str):
        """Remove a server from the pool."""
        if server_id in self._servers:
            del self._servers[server_id]
    
    def select_server(self, request: GatewayRequest) -> Optional[UpstreamServer]:
        """Select a server based on load balancing strategy."""
        healthy_servers = [s for s in self._servers.values() if s.healthy]
        
        if not healthy_servers:
            return None
        
        if self.strategy == LoadBalancingStrategy.ROUND_ROBIN:
            return self._round_robin_select(healthy_servers)
        elif self.strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
            return self._least_connections_select(healthy_servers)
        elif self.strategy == LoadBalancingStrategy.IP_HASH:
            return self._ip_hash_select(healthy_servers, request)
        elif self.strategy == LoadBalancingStrategy.WEIGHTED:
            return self._weighted_select(healthy_servers)
        else:
            return self._random_select(healthy_servers)
    
    def _round_robin_select(self, servers: list[UpstreamServer]) -> UpstreamServer:
        """Round-robin selection."""
        idx = self._current_index["round_robin"] % len(servers)
        self._current_index["round_robin"] += 1
        return servers[idx]
    
    def _least_connections_select(self, servers: list[UpstreamServer]) -> UpstreamServer:
        """Select server with least connections."""
        return min(servers, key=lambda s: s.active_connections)
    
    def _ip_hash_select(self, servers: list[UpstreamServer], request: GatewayRequest) -> UpstreamServer:
        """IP hash-based selection."""
        client_ip = request.client_ip or "unknown"
        hash_val = int(hashlib.md5(client_ip.encode()).hexdigest(), 16)
        idx = hash_val % len(servers)
        return servers[idx]
    
    def _weighted_select(self, servers: list[UpstreamServer]) -> UpstreamServer:
        """Weighted selection."""
        total_weight = sum(s.weight for s in servers)
        import random
        rand_val = random.randint(1, total_weight)
        
        cumulative = 0
        for server in servers:
            cumulative += server.weight
            if rand_val <= cumulative:
                return server
        
        return servers[-1]
    
    def _random_select(self, servers: list[UpstreamServer]) -> UpstreamServer:
        """Random selection."""
        import random
        return random.choice(servers)
    
    def increment_connections(self, server_id: str):
        """Increment active connections for a server."""
        if server_id in self._servers:
            self._servers[server_id].active_connections += 1
    
    def decrement_connections(self, server_id: str):
        """Decrement active connections for a server."""
        if server_id in self._servers:
            self._servers[server_id].active_connections = max(
                0, self._servers[server_id].active_connections - 1
            )


class ApiGatewayAction:
    """Main API gateway action handler."""
    
    def __init__(self, config: Optional[GatewayConfig] = None):
        self.config = config or GatewayConfig(name="default")
        self._routes: dict[str, Route] = {}
        self._upstreams: dict[str, list[UpstreamServer]] = defaultdict(list)
        self._load_balancers: dict[str, LoadBalancer] = {}
        self._plugins: dict[str, Callable] = {}
        self._stats: dict[str, Any] = defaultdict(int)
        self._request_handlers: dict[str, Callable] = {}
    
    def add_route(self, route: Route) -> "ApiGatewayAction":
        """Add a route to the gateway."""
        self._routes[route.route_id] = route
        
        # Initialize load balancer for upstream
        if route.upstream not in self._load_balancers:
            self._load_balancers[route.upstream] = LoadBalancer(LoadBalancingStrategy.ROUND_ROBIN)
        
        return self
    
    def add_upstream_server(
        self,
        upstream_name: str,
        server: UpstreamServer
    ) -> "ApiGatewayAction":
        """Add a server to an upstream pool."""
        self._upstreams[upstream_name].append(server)
        
        if upstream_name not in self._load_balancers:
            self._load_balancers[upstream_name] = LoadBalancer(LoadBalancingStrategy.ROUND_ROBIN)
        
        self._load_balancers[upstream_name].add_server(server)
        return self
    
    def register_plugin(
        self,
        name: str,
        plugin: Callable[[GatewayRequest, GatewayResponse], Awaitable[GatewayResponse]]
    ) -> "ApiGatewayAction":
        """Register a gateway plugin."""
        self._plugins[name] = plugin
        return self
    
    async def handle_request(
        self,
        request: GatewayRequest
    ) -> GatewayResponse:
        """
        Handle an incoming request through the gateway.
        
        Args:
            request: GatewayRequest to process
            
        Returns:
            GatewayResponse from upstream
        """
        start_time = datetime.now()
        self._stats["total_requests"] += 1
        
        # Find matching route
        route = self._match_route(request)
        
        if not route:
            self._stats["not_found"] += 1
            return GatewayResponse(
                status_code=404,
                headers={"Content-Type": "application/json"},
                body={"error": "Route not found"},
                upstream="",
                duration_ms=0
            )
        
        # Get load balancer for upstream
        lb = self._load_balancers.get(route.upstream)
        if not lb:
            self._stats["upstream_errors"] += 1
            return GatewayResponse(
                status_code=502,
                headers={"Content-Type": "application/json"},
                body={"error": "Upstream not configured"},
                upstream=route.upstream,
                duration_ms=0
            )
        
        # Select server
        server = lb.select_server(request)
        if not server:
            self._stats["no_healthy_servers"] += 1
            return GatewayResponse(
                status_code=503,
                headers={"Content-Type": "application/json"},
                body={"error": "No healthy servers available"},
                upstream=route.upstream,
                duration_ms=0
            )
        
        # Increment connection count
        lb.increment_connections(server.server_id)
        
        try:
            # Run request plugins
            for plugin_name in route.plugins:
                if plugin_name in self._plugins:
                    # Plugin processing (simplified)
                    pass
            
            # Forward request to upstream (simulated)
            response = await self._forward_request(request, server, route)
            
            self._stats["successful_requests"] += 1
            
            return response
            
        except Exception as e:
            self._stats["upstream_errors"] += 1
            return GatewayResponse(
                status_code=502,
                headers={"Content-Type": "application/json"},
                body={"error": str(e)},
                upstream=server.url,
                duration_ms=0
            )
        
        finally:
            lb.decrement_connections(server.server_id)
    
    def _match_route(self, request: GatewayRequest) -> Optional[Route]:
        """Match request to a route."""
        for route in self._routes.values():
            if request.path.startswith(route.path_prefix):
                if request.method in route.methods:
                    return route
        return None
    
    async def _forward_request(
        self,
        request: GatewayRequest,
        server: UpstreamServer,
        route: Route
    ) -> GatewayResponse:
        """Forward request to upstream server."""
        # Simulate upstream request
        await asyncio.sleep(0.01)
        
        return GatewayResponse(
            status_code=200,
            headers={
                "Content-Type": "application/json",
                "X-Upstream": server.server_id,
                "X-Request-Id": request.request_id
            },
            body={"message": "OK", "upstream": server.url},
            upstream=server.server_id,
            duration_ms=10.0
        )
    
    async def set_load_balancing_strategy(
        self,
        upstream_name: str,
        strategy: LoadBalancingStrategy
    ):
        """Change load balancing strategy for an upstream."""
        if upstream_name in self._load_balancers:
            self._load_balancers[upstream_name].strategy = strategy
    
    async def mark_server_healthy(self, server_id: str, healthy: bool):
        """Mark a server as healthy/unhealthy."""
        for servers in self._upstreams.values():
            for server in servers:
                if server.server_id == server_id:
                    server.healthy = healthy
                    self._stats["health_checks"] += 1
    
    async def get_upstream_stats(self, upstream_name: str) -> dict[str, Any]:
        """Get statistics for an upstream."""
        servers = self._upstreams.get(upstream_name, [])
        lb = self._load_balancers.get(upstream_name)
        
        return {
            "upstream": upstream_name,
            "strategy": lb.strategy.value if lb else "unknown",
            "total_servers": len(servers),
            "healthy_servers": len([s for s in servers if s.healthy]),
            "servers": [
                {
                    "id": s.server_id,
                    "url": s.url,
                    "healthy": s.healthy,
                    "active_connections": s.active_connections,
                    "weight": s.weight
                }
                for s in servers
            ]
        }
    
    def get_stats(self) -> dict[str, Any]:
        """Get gateway statistics."""
        return dict(self._stats)
    
    def list_routes(self) -> list[dict[str, Any]]:
        """List all configured routes."""
        return [
            {
                "route_id": r.route_id,
                "path_prefix": r.path_prefix,
                "methods": r.methods,
                "upstream": r.upstream,
                "plugins": r.plugins
            }
            for r in self._routes.values()
        ]
