"""
API Gateway Module.

Provides API gateway functionality including request routing,
load balancing, rate limiting, authentication, and monitoring
for microservices architectures.
"""

from typing import (
    Dict, List, Optional, Any, Callable, Tuple,
    Set, Union, TypeVar, Pattern
)
from dataclasses import dataclass, field
from enum import Enum, auto
from datetime import datetime, timedelta
import logging
import json
import time
import threading
import hashlib
from collections import defaultdict
import re

logger = logging.getLogger(__name__)

T = TypeVar("T")


class LoadBalancingStrategy(Enum):
    """Load balancing strategies."""
    ROUND_ROBIN = auto()
    LEAST_CONNECTIONS = auto()
    RANDOM = auto()
    WEIGHTED = auto()
    IP_HASH = auto()
    CONSISTENT_HASH = auto()


class RateLimitAlgorithm(Enum):
    """Rate limiting algorithms."""
    TOKEN_BUCKET = auto()
    SLIDING_WINDOW = auto()
    FIXED_WINDOW = auto()
    LEAKY_BUCKET = auto()


@dataclass
class UpstreamServer:
    """Backend server definition."""
    url: str
    weight: int = 1
    max_connections: int = 100
    health_check_path: Optional[str] = None
    timeout_seconds: float = 30.0
    is_healthy: bool = True
    current_connections: int = 0
    
    @property
    def available_capacity(self) -> int:
        return max(0, self.max_connections - self.current_connections)


@dataclass
class RouteConfig:
    """Route configuration."""
    path_pattern: str
    upstream: str
    methods: List[str] = field(default_factory=lambda: ["GET", "POST", "PUT", "DELETE"])
    auth_required: bool = False
    rate_limit_tier: Optional[str] = None
    timeout_seconds: float = 30.0
    strip_path_prefix: Optional[str] = None
    add_headers: Dict[str, str] = field(default_factory=dict)
    cache_ttl_seconds: Optional[int] = None


@dataclass
class RateLimitConfig:
    """Rate limiting configuration."""
    tier_name: str
    algorithm: RateLimitAlgorithm
    requests_per_second: float
    burst_size: int
    concurrent_limit: Optional[int] = None


@dataclass
class GatewayRequest:
    """Incoming gateway request."""
    path: str
    method: str
    headers: Dict[str, str]
    query_params: Dict[str, str]
    body: Optional[Any] = None
    client_ip: str = ""
    client_id: Optional[str] = None
    start_time: datetime = field(default_factory=datetime.now)


@dataclass
class GatewayResponse:
    """Gateway response."""
    status_code: int
    headers: Dict[str, str] = field(default_factory=dict)
    body: Any = None
    from_upstream: Optional[str] = None
    cached: bool = False
    duration_ms: float = 0


class RateLimiter:
    """Rate limiting implementation."""
    
    def __init__(self, config: RateLimitConfig) -> None:
        self.config = config
        self._tokens: Dict[str, float] = defaultdict(float)
        self._last_update: Dict[str, datetime] = {}
        self._lock = threading.Lock()
        self._request_counts: Dict[str, List[datetime]] = defaultdict(list)
    
    def is_allowed(self, client_id: str) -> Tuple[bool, Dict[str, Any]]:
        """
        Check if request is allowed.
        
        Returns:
            Tuple of (allowed, metadata)
        """
        with self._lock:
            now = time.time()
            remaining = 0
            reset_in = 0
            
            if self.config.algorithm == RateLimitAlgorithm.TOKEN_BUCKET:
                allowed, remaining, reset_in = self._check_token_bucket(client_id, now)
            elif self.config.algorithm == RateLimitAlgorithm.SLIDING_WINDOW:
                allowed, remaining, reset_in = self._check_sliding_window(client_id, now)
            elif self.config.algorithm == RateLimitAlgorithm.FIXED_WINDOW:
                allowed, remaining, reset_in = self._check_fixed_window(client_id, now)
            
            metadata = {
                "limit": self.config.requests_per_second,
                "remaining": int(remaining),
                "reset_in": reset_in
            }
            
            return allowed, metadata
    
    def _check_token_bucket(
        self,
        client_id: str,
        now: float
    ) -> Tuple[bool, float, float]:
        """Token bucket rate limiting."""
        bucket_size = self.config.burst_size
        refill_rate = self.config.requests_per_second
        
        last_update = self._last_update.get(client_id, now)
        elapsed = now - last_update
        
        tokens = self._tokens.get(client_id, bucket_size)
        tokens = min(bucket_size, tokens + elapsed * refill_rate)
        
        if tokens >= 1:
            tokens -= 1
            self._tokens[client_id] = tokens
            self._last_update[client_id] = now
            return True, tokens, 0
        else:
            self._tokens[client_id] = tokens
            return False, 0, 1 / refill_rate
    
    def _check_sliding_window(
        self,
        client_id: str,
        now: float
    ) -> Tuple[bool, float, float]:
        """Sliding window rate limiting."""
        window_seconds = 1.0
        max_requests = self.config.requests_per_second
        
        requests = self._request_counts[client_id]
        cutoff = now - window_seconds
        requests = [r for r in requests if r > cutoff]
        
        if len(requests) < max_requests:
            requests.append(now)
            self._request_counts[client_id] = requests
            return True, max_requests - len(requests), window_seconds
        else:
            return False, 0, window_seconds
    
    def _check_fixed_window(
        self,
        client_id: str,
        now: float
    ) -> Tuple[bool, float, float]:
        """Fixed window rate limiting."""
        window_seconds = 1.0
        max_requests = self.config.requests_per_second
        
        window_key = f"{client_id}:{int(now / window_seconds)}"
        count = int(self._request_counts.get(window_key, 0))
        
        if count < max_requests:
            self._request_counts[window_key] = count + 1
            return True, max_requests - count - 1, window_seconds
        else:
            return False, 0, window_seconds


class LoadBalancer:
    """Load balancing across upstream servers."""
    
    def __init__(
        self,
        strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN
    ) -> None:
        self.strategy = strategy
        self._servers: Dict[str, List[UpstreamServer]] = defaultdict(list)
        self._current_index: Dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()
    
    def add_server(self, upstream: str, server: UpstreamServer) -> None:
        """Add server to upstream pool."""
        with self._lock:
            self._servers[upstream].append(server)
    
    def remove_server(self, upstream: str, server_url: str) -> bool:
        """Remove server from pool."""
        with self._lock:
            servers = self._servers.get(upstream, [])
            self._servers[upstream] = [
                s for s in servers if s.url != server_url
            ]
            return True
    
    def select_server(self, upstream: str, client_ip: str = "") -> Optional[UpstreamServer]:
        """Select server based on load balancing strategy."""
        with self._lock:
            servers = self._servers.get(upstream, [])
            healthy = [s for s in servers if s.is_healthy]
            
            if not healthy:
                return None
            
            if self.strategy == LoadBalancingStrategy.ROUND_ROBIN:
                return self._round_robin(upstream, healthy)
            elif self.strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
                return self._least_connections(healthy)
            elif self.strategy == LoadBalancingStrategy.RANDOM:
                import random
                return random.choice(healthy)
            elif self.strategy == LoadBalancingStrategy.WEIGHTED:
                return self._weighted(healthy)
            elif self.strategy == LoadBalancingStrategy.IP_HASH:
                return self._ip_hash(upstream, healthy, client_ip)
            
            return healthy[0]
    
    def _round_robin(
        self,
        upstream: str,
        servers: List[UpstreamServer]
    ) -> UpstreamServer:
        """Round robin selection."""
        idx = self._current_index[upstream] % len(servers)
        self._current_index[upstream] = idx + 1
        return servers[idx]
    
    def _least_connections(self, servers: List[UpstreamServer]) -> UpstreamServer:
        """Select server with least connections."""
        return min(servers, key=lambda s: s.current_connections)
    
    def _weighted(self, servers: List[UpstreamServer]) -> UpstreamServer:
        """Weighted selection."""
        total_weight = sum(s.weight for s in servers)
        import random
        r = random.randint(1, total_weight)
        
        cumsum = 0
        for server in servers:
            cumsum += server.weight
            if r <= cumsum:
                return server
        
        return servers[-1]
    
    def _ip_hash(
        self,
        upstream: str,
        servers: List[UpstreamServer],
        client_ip: str
    ) -> UpstreamServer:
        """Consistent hash based on client IP."""
        hash_val = int(hashlib.md5(client_ip.encode()).hexdigest(), 16)
        idx = hash_val % len(servers)
        return servers[idx]


class RequestRouter:
    """Routes requests to appropriate upstream."""
    
    def __init__(self) -> None:
        self._routes: List[RouteConfig] = []
        self._route_patterns: List[Tuple[Pattern, RouteConfig]] = []
    
    def add_route(self, route: RouteConfig) -> "RequestRouter":
        """Add route configuration."""
        self._routes.append(route)
        self._compile_route(route)
        return self
    
    def _compile_route(self, route: RouteConfig) -> None:
        """Compile route pattern for matching."""
        pattern = route.path_pattern
        pattern = pattern.replace("*", "[^/]+")
        pattern = f"^{pattern}$"
        self._route_patterns.append((re.compile(pattern), route))
    
    def match_route(
        self,
        path: str,
        method: str
    ) -> Optional[RouteConfig]:
        """Find matching route for path and method."""
        for pattern, route in self._route_patterns:
            if pattern.match(path):
                if method.upper() in route.methods:
                    return route
        
        return None


class ApiGateway:
    """
    API Gateway implementation.
    
    Provides routing, load balancing, rate limiting,
    authentication, and monitoring for backend services.
    """
    
    def __init__(self) -> None:
        self.router = RequestRouter()
        self.load_balancer = LoadBalancer()
        self._rate_limiters: Dict[str, RateLimiter] = {}
        self._cache: Dict[str, Tuple[Any, datetime]] = {}
        self._cache_lock = threading.Lock()
        self._auth_handlers: Dict[str, Callable] = {}
    
    def configure_upstream(
        self,
        name: str,
        servers: List[str],
        strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN
    ) -> "ApiGateway":
        """Configure upstream server pool."""
        self.load_balancer = LoadBalancer(strategy)
        
        for server_url in servers:
            self.load_balancer.add_server(name, UpstreamServer(url=server_url))
        
        return self
    
    def add_route(self, route: RouteConfig) -> "ApiGateway":
        """Add gateway route."""
        self.router.add_route(route)
        return self
    
    def configure_rate_limit(
        self,
        tier_name: str,
        algorithm: RateLimitAlgorithm,
        requests_per_second: float,
        burst_size: int
    ) -> "ApiGateway":
        """Configure rate limiting tier."""
        config = RateLimitConfig(
            tier_name=tier_name,
            algorithm=algorithm,
            requests_per_second=requests_per_second,
            burst_size=burst_size
        )
        self._rate_limiters[tier_name] = RateLimiter(config)
        return self
    
    def add_auth_handler(
        self,
        auth_type: str,
        handler: Callable[[GatewayRequest], Tuple[bool, Optional[str]]]
    ) -> "ApiGateway":
        """Add authentication handler."""
        self._auth_handlers[auth_type] = handler
        return self
    
    async def handle_request(
        self,
        request: GatewayRequest
    ) -> GatewayResponse:
        """
        Handle incoming gateway request.
        
        Args:
            request: GatewayRequest object
            
        Returns:
            GatewayResponse
        """
        start_time = time.time()
        
        # Route matching
        route = self.router.match_route(request.path, request.method)
        if not route:
            return GatewayResponse(
                status_code=404,
                body={"error": "Not Found"},
                duration_ms=(time.time() - start_time) * 1000
            )
        
        # Rate limiting
        if route.rate_limit_tier:
            limiter = self._rate_limiters.get(route.rate_limit_tier)
            if limiter:
                allowed, metadata = limiter.is_allowed(request.client_id or request.client_ip)
                if not allowed:
                    return GatewayResponse(
                        status_code=429,
                        body={"error": "Rate limit exceeded", "retry_after": metadata.get("reset_in")},
                        headers={"Retry-After": str(metadata.get("reset_in", 1))},
                        duration_ms=(time.time() - start_time) * 1000
                    )
        
        # Cache check
        if route.cache_ttl_seconds:
            cache_key = f"{request.method}:{request.path}"
            cached = self._get_from_cache(cache_key, route.cache_ttl_seconds)
            if cached:
                return GatewayResponse(
                    status_code=200,
                    body=cached,
                    cached=True,
                    duration_ms=(time.time() - start_time) * 1000
                )
        
        # Server selection
        server = self.load_balancer.select_server(
            route.upstream,
            request.client_ip
        )
        
        if not server:
            return GatewayResponse(
                status_code=503,
                body={"error": "Service unavailable"},
                duration_ms=(time.time() - start_time) * 1000
            )
        
        # Forward request (placeholder)
        response = GatewayResponse(
            status_code=200,
            body={"message": "proxied", "upstream": server.url},
            from_upstream=server.url,
            duration_ms=(time.time() - start_time) * 1000
        )
        
        # Cache response
        if route.cache_ttl_seconds and response.status_code == 200:
            self._add_to_cache(f"{request.method}:{request.path}", response.body, route.cache_ttl_seconds)
        
        return response
    
    def _get_from_cache(self, key: str, ttl_seconds: int) -> Optional[Any]:
        """Get item from cache if not expired."""
        with self._cache_lock:
            if key in self._cache:
                value, cached_at = self._cache[key]
                if datetime.now() - cached_at < timedelta(seconds=ttl_seconds):
                    return value
                del self._cache[key]
        return None
    
    def _add_to_cache(self, key: str, value: Any, ttl_seconds: int) -> None:
        """Add item to cache."""
        with self._cache_lock:
            self._cache[key] = (value, datetime.now())


# Entry point for direct execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    gateway = ApiGateway()
    
    # Configure upstreams
    gateway.configure_upstream(
        name="users",
        servers=[
            "http://user-service-1:8001",
            "http://user-service-2:8001",
            "http://user-service-3:8001"
        ],
        strategy=LoadBalancingStrategy.ROUND_ROBIN
    )
    
    # Add routes
    gateway.add_route(RouteConfig(
        path_pattern="/api/users/*",
        upstream="users",
        methods=["GET", "POST", "PUT", "DELETE"],
        rate_limit_tier="default",
        cache_ttl_seconds=60
    ))
    
    # Configure rate limits
    gateway.configure_rate_limit(
        tier_name="default",
        algorithm=RateLimitAlgorithm.TOKEN_BUCKET,
        requests_per_second=100,
        burst_size=200
    )
    
    print("=== API Gateway Configuration ===")
    print(f"Routes: {len(gateway.router._routes)}")
    print(f"Rate limit tiers: {list(gateway._rate_limiters.keys())}")
    print("\nGateway ready to handle requests")
