"""
API Gateway Action Module.

Unified API gateway with request routing, load balancing,
rate limiting, authentication, and response caching.
"""

import asyncio
import hashlib
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class LoadBalanceStrategy(Enum):
    """Load balancing strategies."""
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    LEAST_CONNECTIONS = "least_connections"
    IP_HASH = "ip_hash"


@dataclass
class Upstream:
    """
    Upstream server configuration.

    Attributes:
        url: Full URL of upstream server.
        weight: Weight for weighted load balancing.
        max_fails: Max failures before marking unhealthy.
        fail_timeout: Time to wait before retrying failed upstream.
        is_healthy: Current health status.
    """
    url: str
    weight: int = 1
    max_fails: int = 3
    fail_timeout: float = 30.0
    is_healthy: bool = True
    failures: int = field(default=0, init=False)
    connections: int = field(default=0, init=False)


@dataclass
class RateLimitRule:
    """
    Rate limiting rule configuration.

    Attributes:
        path: URL path pattern to match.
        limit: Maximum requests allowed.
        window: Time window in seconds.
    """
    path: str
    limit: int
    window: float


@dataclass
class GatewayRoute:
    """
    Gateway route configuration.

    Attributes:
        path_prefix: URL path prefix to match.
        upstreams: List of upstream servers.
        auth_required: Whether authentication is required.
        rate_limit: Optional rate limit rule.
        cache_ttl: Response cache TTL in seconds.
        methods: Allowed HTTP methods.
    """
    path_prefix: str
    upstreams: list[Upstream]
    auth_required: bool = False
    rate_limit: Optional[RateLimitRule] = None
    cache_ttl: float = 0.0
    methods: list[str] = field(default_factory=lambda: ["GET", "POST", "PUT", "DELETE"])


@dataclass
class GatewayConfig:
    """Gateway configuration container."""
    host: str = "0.0.0.0"
    port: int = 8080
    load_balance: LoadBalanceStrategy = LoadBalanceStrategy.ROUND_ROBIN
    default_timeout: float = 30.0
    routes: list[GatewayRoute] = field(default_factory=list)


class APIGatewayAction:
    """
    API Gateway for routing, load balancing, and managing API requests.

    Example:
        gateway = APIGatewayAction()
        gateway.add_route("/api/users", ["http://user1:8000", "http://user2:8000"])
        await gateway.start()
    """

    def __init__(self, config: Optional[GatewayConfig] = None):
        """
        Initialize API gateway action.

        Args:
            config: Gateway configuration. Uses defaults if None.
        """
        self.config = config or GatewayConfig()
        self._route_map: dict[str, GatewayRoute] = {}
        self._upstream_index: dict[str, int] = {}
        self._request_counts: dict[str, list[tuple[float, int]]] = {}
        self._cache: dict[str, tuple[Any, float]] = {}
        self._active_connections: int = 0

        for route in self.config.routes:
            self._route_map[route.path_prefix] = route
            self._upstream_index[route.path_prefix] = 0

    def add_route(
        self,
        path_prefix: str,
        upstream_urls: list[str],
        auth_required: bool = False,
        rate_limit: Optional[RateLimitRule] = None,
        cache_ttl: float = 0.0
    ) -> GatewayRoute:
        """
        Add a route to the gateway.

        Args:
            path_prefix: URL path prefix to match.
            upstream_urls: List of upstream server URLs.
            auth_required: Whether auth is required.
            rate_limit: Optional rate limit rule.
            cache_ttl: Response cache TTL in seconds.

        Returns:
            Created GatewayRoute object.
        """
        upstreams = [Upstream(url=url) for url in upstream_urls]
        route = GatewayRoute(
            path_prefix=path_prefix,
            upstreams=upstreams,
            auth_required=auth_required,
            rate_limit=rate_limit,
            cache_ttl=cache_ttl
        )

        self._route_map[path_prefix] = route
        self._upstream_index[path_prefix] = 0
        self._request_counts[path_prefix] = []

        logger.info(f"Added route: {path_prefix} -> {upstream_urls}")
        return route

    def _select_upstream(self, route: GatewayRoute, client_ip: Optional[str] = None) -> Upstream:
        """Select an upstream based on load balancing strategy."""
        healthy = [u for u in route.upstreams if u.is_healthy]

        if not healthy:
            logger.warning("No healthy upstreams available, using all anyway")
            healthy = route.upstreams

        strategy = self.config.load_balance

        if strategy == LoadBalanceStrategy.ROUND_ROBIN:
            idx = self._upstream_index.get(route.path_prefix, 0) % len(healthy)
            self._upstream_index[route.path_prefix] = idx + 1
            return healthy[idx]

        elif strategy == LoadBalanceStrategy.RANDOM:
            return healthy[int(time.time() * 1000) % len(healthy)]

        elif strategy == LoadBalanceStrategy.LEAST_CONNECTIONS:
            return min(healthy, key=lambda u: u.connections)

        elif strategy == LoadBalanceStrategy.IP_HASH and client_ip:
            hash_val = int(hashlib.md5(client_ip.encode()).hexdigest(), 16)
            return healthy[hash_val % len(healthy)]

        return healthy[0]

    def _check_rate_limit(self, route: GatewayRoute, client_id: str) -> bool:
        """
        Check if request is within rate limit.

        Args:
            route: Gateway route with rate limit rule.
            client_id: Client identifier for tracking.

        Returns:
            True if within limit, False if exceeded.
        """
        if not route.rate_limit:
            return True

        now = time.time()
        window = route.rate_limit.window
        limit = route.rate_limit.limit
        key = f"{route.path_prefix}:{client_id}"

        if key not in self._request_counts:
            self._request_counts[key] = []

        self._request_counts[key] = [
            (t, count) for t, count in self._request_counts[key]
            if now - t < window
        ]

        total_requests = sum(count for _, count in self._request_counts[key])

        if total_requests >= limit:
            logger.warning(f"Rate limit exceeded for {client_id} on {route.path_prefix}")
            return False

        if self._request_counts[key]:
            self._request_counts[key][-1] = (self._request_counts[key][-1][0],
                                              self._request_counts[key][-1][1] + 1)
        else:
            self._request_counts[key].append((now, 1))

        return True

    def _get_cache(self, route: GatewayRoute, key: str) -> Optional[Any]:
        """Get cached response if available and fresh."""
        if route.cache_ttl <= 0:
            return None

        cache_key = f"{route.path_prefix}:{key}"
        if cache_key in self._cache:
            value, expires = self._cache[cache_key]
            if time.time() < expires:
                return value
            del self._cache[cache_key]

        return None

    def _set_cache(self, route: GatewayRoute, key: str, value: Any) -> None:
        """Cache a response."""
        if route.cache_ttl <= 0:
            return

        cache_key = f"{route.path_prefix}:{key}"
        expires = time.time() + route.cache_ttl
        self._cache[cache_key] = (value, expires)

    async def forward_request(
        self,
        path: str,
        method: str,
        headers: dict,
        body: Optional[bytes] = None,
        client_ip: Optional[str] = None
    ) -> dict:
        """
        Forward a request to appropriate upstream.

        Args:
            path: Request path.
            method: HTTP method.
            headers: Request headers.
            body: Optional request body.
            client_ip: Client IP address for rate limiting.

        Returns:
            Response dict with status, headers, body.

        Raises:
            ValueError: If no route matches or rate limit exceeded.
        """
        matched_route = None
        for prefix, route in self._route_map.items():
            if path.startswith(prefix):
                matched_route = route
                break

        if not matched_route:
            raise ValueError(f"No route found for path: {path}")

        if method not in matched_route.methods:
            raise ValueError(f"Method {method} not allowed for route {matched_route.path_prefix}")

        client_id = client_ip or headers.get("X-Client-ID", "anonymous")

        if not self._check_rate_limit(matched_route, client_id):
            raise ValueError("Rate limit exceeded")

        if matched_route.auth_required:
            if "Authorization" not in headers:
                raise ValueError("Authentication required")

        cache_key = f"{method}:{path}"
        cached = self._get_cache(matched_route, cache_key)
        if cached:
            logger.info(f"Cache hit for {path}")
            return cached

        upstream = self._select_upstream(matched_route, client_ip)
        upstream.connections += 1
        self._active_connections += 1

        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                url = f"{upstream.url}{path}"

                async with session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    data=body,
                    timeout=aiohttp.ClientTimeout(total=self.config.default_timeout)
                ) as response:
                    response_body = await response.read()
                    result = {
                        "status": response.status,
                        "headers": dict(response.headers),
                        "body": response_body
                    }

                    self._set_cache(matched_route, cache_key, result)
                    return result

        except Exception as e:
            logger.error(f"Upstream request failed: {e}")
            upstream.failures += 1

            if upstream.failures >= upstream.max_fails:
                upstream.is_healthy = False
                logger.warning(f"Upstream {upstream.url} marked unhealthy")

                await asyncio.sleep(upstream.fail_timeout)
                upstream.is_healthy = True
                upstream.failures = 0

            raise

        finally:
            upstream.connections -= 1
            self._active_connections -= 1

    def get_stats(self) -> dict:
        """Get gateway statistics."""
        return {
            "active_connections": self._active_connections,
            "routes": len(self._route_map),
            "cached_responses": len(self._cache),
            "upstreams": {
                route.path_prefix: [
                    {
                        "url": u.url,
                        "healthy": u.is_healthy,
                        "connections": u.connections
                    }
                    for u in route.upstreams
                ]
                for route in self.config.routes
            }
        }

    def health_check(self) -> dict:
        """Get overall gateway health status."""
        all_healthy = all(
            any(u.is_healthy for u in route.upstreams)
            for route in self.config.routes
        )

        return {
            "status": "healthy" if all_healthy else "degraded",
            "total_routes": len(self._route_map),
            "active_connections": self._active_connections
        }
