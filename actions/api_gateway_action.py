"""API Gateway Action Module.

Provides API gateway functionality with routing, load balancing,
authentication, and request transformation.
"""

from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import hashlib
import time
from datetime import datetime


class LoadBalancingStrategy(Enum):
    """Load balancing strategies."""
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    IP_HASH = "ip_hash"
    RANDOM = "random"
    WEIGHTED = "weighted"


class AuthType(Enum):
    """Authentication types."""
    NONE = "none"
    API_KEY = "api_key"
    BEARER = "bearer"
    BASIC = "basic"
    JWT = "jwt"


@dataclass
class UpstreamServer:
    """Represents a backend server."""
    id: str
    url: str
    weight: int = 1
    max_connections: int = 100
    active_connections: int = 0
    healthy: bool = True
    last_health_check: Optional[datetime] = None


@dataclass
class RouteRule:
    """Defines routing rules."""
    path_prefix: str
    upstream: str
    methods: List[str] = field(default_factory=lambda: ["GET", "POST", "PUT", "DELETE"])
    auth_type: AuthType = AuthType.NONE
    auth_config: Dict[str, Any] = field(default_factory=dict)
    rate_limit_name: Optional[str] = None
    transform_request: Optional[Callable] = None
    transform_response: Optional[Callable] = None
    timeout: int = 30


@dataclass
class GatewayRequest:
    """Incoming gateway request."""
    method: str
    path: str
    headers: Dict[str, str]
    query_params: Dict[str, str]
    body: Optional[bytes] = None
    client_ip: str = ""
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class GatewayResponse:
    """Outgoing gateway response."""
    status_code: int
    headers: Dict[str, str]
    body: bytes
    upstream: str
    response_time_ms: float


class LoadBalancer:
    """Load balancing across upstream servers."""

    def __init__(self, strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN):
        self.strategy = strategy
        self._round_robin_index: Dict[str, int] = {}
        self._connection_counts: Dict[str, int] = {}

    def select_server(
        self,
        servers: List[UpstreamServer],
        client_ip: Optional[str] = None,
    ) -> Optional[UpstreamServer]:
        """Select a server based on load balancing strategy."""
        healthy_servers = [s for s in servers if s.healthy]

        if not healthy_servers:
            return None

        if self.strategy == LoadBalancingStrategy.ROUND_ROBIN:
            return self._round_robin(healthy_servers)
        elif self.strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
            return self._least_connections(healthy_servers)
        elif self.strategy == LoadBalancingStrategy.IP_HASH:
            return self._ip_hash(healthy_servers, client_ip or "")
        elif self.strategy == LoadBalancingStrategy.WEIGHTED:
            return self._weighted(healthy_servers)
        else:
            return self._random(healthy_servers)

    def _round_robin(self, servers: List[UpstreamServer]) -> UpstreamServer:
        """Round robin selection."""
        key = id(servers)
        if key not in self._round_robin_index:
            self._round_robin_index[key] = 0

        index = self._round_robin_index[key]
        server = servers[index % len(servers)]
        self._round_robin_index[key] = index + 1
        return server

    def _least_connections(self, servers: List[UpstreamServer]) -> UpstreamServer:
        """Select server with least connections."""
        return min(servers, key=lambda s: self._connection_counts.get(s.id, 0))

    def _ip_hash(self, servers: List[UpstreamServer], client_ip: str) -> UpstreamServer:
        """IP-based hash selection."""
        hash_val = int(hashlib.md5(client_ip.encode()).hexdigest()[:8], 16)
        return servers[hash_val % len(servers)]

    def _weighted(self, servers: List[UpstreamServer]) -> UpstreamServer:
        """Weighted selection."""
        total_weight = sum(s.weight for s in servers)
        if total_weight == 0:
            return servers[0]
        rand = hashlib.md5(str(time.time()).encode()).hexdigest()
        hash_val = int(rand[:8], 16) % total_weight

        cumulative = 0
        for server in servers:
            cumulative += server.weight
            if hash_val < cumulative:
                return server
        return servers[-1]

    def _random(self, servers: List[UpstreamServer]) -> UpstreamServer:
        """Random selection."""
        hash_val = int(hashlib.md5(str(time.time()).encode()).hexdigest()[:8], 16)
        return servers[hash_val % len(servers)]

    def increment_connections(self, server_id: str):
        """Increment connection count."""
        self._connection_counts[server_id] = self._connection_counts.get(server_id, 0) + 1

    def decrement_connections(self, server_id: str):
        """Decrement connection count."""
        self._connection_counts[server_id] = max(0, self._connection_counts.get(server_id, 0) - 1)


class Authenticator:
    """Handles request authentication."""

    def __init__(self):
        self._valid_keys: Dict[str, Dict[str, Any]] = {}
        self._jwt_secrets: Dict[str, str] = {}

    def register_api_key(self, key: str, metadata: Optional[Dict[str, Any]] = None):
        """Register an API key."""
        self._valid_keys[key] = metadata or {}

    def register_jwt_secret(self, name: str, secret: str):
        """Register a JWT secret."""
        self._jwt_secrets[name] = secret

    def authenticate(
        self,
        request: GatewayRequest,
        auth_type: AuthType,
        auth_config: Dict[str, Any],
    ) -> tuple[bool, Optional[str]]:
        """Authenticate a request."""
        if auth_type == AuthType.NONE:
            return True, None

        if auth_type == AuthType.API_KEY:
            return self._auth_api_key(request, auth_config)

        if auth_type == AuthType.BEARER:
            return self._auth_bearer(request, auth_config)

        if auth_type == AuthType.BASIC:
            return self._auth_basic(request, auth_config)

        if auth_type == AuthType.JWT:
            return self._auth_jwt(request, auth_config)

        return False, "Unknown auth type"

    def _auth_api_key(
        self,
        request: GatewayRequest,
        config: Dict[str, Any],
    ) -> tuple[bool, Optional[str]]:
        """Authenticate API key."""
        header_name = config.get("header_name", "X-API-Key")
        api_key = request.headers.get(header_name, "")

        if not api_key:
            return False, "Missing API key"

        if api_key not in self._valid_keys:
            return False, "Invalid API key"

        return True, None

    def _auth_bearer(
        self,
        request: GatewayRequest,
        config: Dict[str, Any],
    ) -> tuple[bool, Optional[str]]:
        """Authenticate bearer token."""
        auth_header = request.headers.get("Authorization", "")

        if not auth_header.startswith("Bearer "):
            return False, "Missing bearer token"

        token = auth_header[7:]
        return True, None

    def _auth_basic(
        self,
        request: GatewayRequest,
        config: Dict[str, Any],
    ) -> tuple[bool, Optional[str]]:
        """Authenticate basic auth."""
        import base64
        auth_header = request.headers.get("Authorization", "")

        if not auth_header.startswith("Basic "):
            return False, "Missing basic auth"

        try:
            decoded = base64.b64decode(auth_header[6:]).decode()
            username, password = decoded.split(":", 1)
            return True, None
        except Exception:
            return False, "Invalid basic auth"

    def _auth_jwt(
        self,
        request: GatewayRequest,
        config: Dict[str, Any],
    ) -> tuple[bool, Optional[str]]:
        """Authenticate JWT token."""
        auth_header = request.headers.get("Authorization", "")

        if not auth_header.startswith("Bearer "):
            return False, "Missing JWT token"

        token = auth_header[7:]
        return True, None


class RateLimiter:
    """Rate limiting for the gateway."""

    def __init__(self):
        self._limits: Dict[str, Dict[str, Any]] = {}
        self._requests: Dict[str, List[datetime]] = {}

    def configure_limit(
        self,
        name: str,
        requests_per_second: int,
        burst: int = 10,
    ):
        """Configure rate limit."""
        self._limits[name] = {
            "rps": requests_per_second,
            "burst": burst,
            "window_start": datetime.now(),
        }

    def check_limit(
        self,
        name: str,
        identifier: str,
    ) -> tuple[bool, int]:
        """Check if request is within rate limit."""
        if name not in self._limits:
            return True, 0

        limit = self._limits[name]
        key = f"{name}:{identifier}"

        if key not in self._requests:
            self._requests[key] = []

        now = datetime.now()
        cutoff = now - timedelta(seconds=1)
        self._requests[key] = [t for t in self._requests[key] if t > cutoff]

        rps = limit["rps"]
        burst = limit.get("burst", 10)
        max_allowed = rps + burst

        if len(self._requests[key]) >= max_allowed:
            return False, max_allowed - len(self._requests[key])

        self._requests[key].append(now)
        return True, max_allowed - len(self._requests[key])


class APIGatewayAction:
    """High-level API gateway action."""

    def __init__(
        self,
        load_balancer: Optional[LoadBalancer] = None,
        authenticator: Optional[Authenticator] = None,
        rate_limiter: Optional[RateLimiter] = None,
    ):
        self.load_balancer = load_balancer or LoadBalancer()
        self.authenticator = authenticator or Authenticator()
        self.rate_limiter = rate_limiter or RateLimiter()
        self._routes: Dict[str, RouteRule] = {}
        self._upstreams: Dict[str, List[UpstreamServer]] = {}
        self._metrics: Dict[str, int] = {
            "requests": 0,
            "auth_failures": 0,
            "rate_limited": 0,
            "errors": 0,
        }

    def add_upstream(
        self,
        name: str,
        servers: List[UpstreamServer],
    ):
        """Add an upstream with servers."""
        self._upstreams[name] = servers

    def add_route(self, rule: RouteRule):
        """Add a routing rule."""
        self._routes[rule.path_prefix] = rule

    def _find_route(self, path: str) -> Optional[RouteRule]:
        """Find matching route for path."""
        for prefix, route in self._routes.items():
            if path.startswith(prefix):
                return route
        return None

    async def handle_request(
        self,
        request: GatewayRequest,
    ) -> GatewayResponse:
        """Handle an incoming gateway request."""
        start_time = time.time()
        self._metrics["requests"] += 1

        route = self._find_route(request.path)
        if not route:
            return GatewayResponse(
                status_code=404,
                headers={"Content-Type": "application/json"},
                body=b'{"error": "Route not found"}',
                upstream="",
                response_time_ms=0,
            )

        if route.auth_type != AuthType.NONE:
            authenticated, error = self.authenticator.authenticate(
                request, route.auth_type, route.auth_config
            )
            if not authenticated:
                self._metrics["auth_failures"] += 1
                return GatewayResponse(
                    status_code=401,
                    headers={"Content-Type": "application/json"},
                    body=json.dumps({"error": error}).encode(),
                    upstream="",
                    response_time_ms=(time.time() - start_time) * 1000,
                )

        if route.rate_limit_name:
            allowed, remaining = self.rate_limiter.check_limit(
                route.rate_limit_name, request.client_ip
            )
            if not allowed:
                self._metrics["rate_limited"] += 1
                return GatewayResponse(
                    status_code=429,
                    headers={
                        "Content-Type": "application/json",
                        "X-RateLimit-Remaining": str(remaining),
                    },
                    body=b'{"error": "Rate limit exceeded"}',
                    upstream="",
                    response_time_ms=(time.time() - start_time) * 1000,
                )

        upstream_name = route.upstream
        servers = self._upstreams.get(upstream_name, [])
        server = self.load_balancer.select_server(servers, request.client_ip)

        if not server:
            self._metrics["errors"] += 1
            return GatewayResponse(
                status_code=503,
                headers={"Content-Type": "application/json"},
                body=b'{"error": "No healthy upstream"}',
                upstream="",
                response_time_ms=(time.time() - start_time) * 1000,
            )

        self.load_balancer.increment_connections(server.id)

        try:
            response = GatewayResponse(
                status_code=200,
                headers={"Content-Type": "application/json"},
                body=json.dumps({"upstream": server.id, "path": request.path}).encode(),
                upstream=server.id,
                response_time_ms=(time.time() - start_time) * 1000,
            )
        finally:
            self.load_balancer.decrement_connections(server.id)

        return response

    def get_metrics(self) -> Dict[str, Any]:
        """Get gateway metrics."""
        return {
            **self._metrics,
            "total_upstreams": len(self._upstreams),
            "total_routes": len(self._routes),
        }


from datetime import timedelta
import json

__all__ = [
    "APIGatewayAction",
    "LoadBalancer",
    "Authenticator",
    "RateLimiter",
    "UpstreamServer",
    "RouteRule",
    "GatewayRequest",
    "GatewayResponse",
    "LoadBalancingStrategy",
    "AuthType",
]
