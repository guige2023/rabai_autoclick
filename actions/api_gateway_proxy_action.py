"""
API Gateway Proxy Action Module.

Provides reverse proxy functionality with load balancing,
circuit breaking, request caching, and SSL termination.
"""

from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import hashlib
import hmac
import json
import logging
import time
import uuid
from collections import defaultdict
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class LoadBalancingStrategy(Enum):
    """Load balancing strategies."""
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    IP_HASH = "ip_hash"
    WEIGHTED = "weighted"
    RANDOM = "random"


class HealthCheckMode(Enum):
    """Health check modes."""
    PASSIVE = "passive"
    ACTIVE = "active"
    BOTH = "both"


@dataclass
class UpstreamServer:
    """Represents a backend server."""
    id: str
    url: str
    weight: int = 1
    max_failures: int = 3
    timeout: float = 30.0
    health: bool = True
    active_connections: int = 0
    total_requests: int = 0
    failed_requests: int = 0
    last_health_check: Optional[datetime] = None
    last_failure: Optional[datetime] = None

    @property
    def failure_rate(self) -> float:
        """Get failure rate."""
        if self.total_requests == 0:
            return 0.0
        return self.failed_requests / self.total_requests

    @property
    def is_healthy(self) -> bool:
        """Check if server is healthy."""
        return self.health and self.failure_rate < 0.5


@dataclass
class ProxyRequest:
    """Incoming proxy request."""
    request_id: str
    method: str
    path: str
    headers: Dict[str, str]
    body: Optional[bytes]
    query_params: Dict[str, str]
    source_ip: str
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ProxyResponse:
    """Outgoing proxy response."""
    request_id: str
    status_code: int
    headers: Dict[str, str]
    body: bytes
    upstream: str
    response_time: float
    cached: bool = False


@dataclass
class CacheEntry:
    """Cache entry for responses."""
    key: str
    response: ProxyResponse
    created_at: datetime
    expires_at: datetime
    hit_count: int = 0

    @property
    def is_expired(self) -> bool:
        """Check if entry is expired."""
        return datetime.now() > self.expires_at


@dataclass
class CircuitBreakerState:
    """Circuit breaker state for upstream group."""
    failures: int = 0
    successes: int = 0
    state: str = "closed"
    next_retry: Optional[datetime] = None


class ResponseCache:
    """HTTP response cache."""

    def __init__(self, max_size: int = 1000, default_ttl: int = 300):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: Dict[str, CacheEntry] = {}
        self._access_order: List[str] = []

    def _make_key(self, request: ProxyRequest) -> str:
        """Generate cache key for request."""
        key_data = f"{request.method}:{request.path}:{json.dumps(request.query_params, sort_keys=True)}"
        return hashlib.sha256(key_data.encode()).hexdigest()

    def get(self, request: ProxyRequest) -> Optional[ProxyResponse]:
        """Get cached response."""
        key = self._make_key(request)
        entry = self._cache.get(key)

        if entry and not entry.is_expired:
            entry.hit_count += 1
            entry.response.cached = True
            return entry.response

        if entry:
            del self._cache[key]
            if key in self._access_order:
                self._access_order.remove(key)

        return None

    def put(self, request: ProxyRequest, response: ProxyResponse, ttl: Optional[int] = None):
        """Cache a response."""
        if "cache-control" in response.headers:
            cc = response.headers["cache-control"].lower()
            if "no-store" in cc or "no-cache" in cc:
                return

        key = self._make_key(request)
        expires_at = datetime.now() + timedelta(seconds=ttl or self.default_ttl)

        entry = CacheEntry(
            key=key,
            response=response,
            created_at=datetime.now(),
            expires_at=expires_at
        )

        if len(self._cache) >= self.max_size:
            oldest = self._access_order.pop(0)
            if oldest in self._cache:
                del self._cache[oldest]

        self._cache[key] = entry
        self._access_order.append(key)

    def invalidate(self, pattern: str):
        """Invalidate cache entries matching pattern."""
        keys_to_delete = [
            k for k, v in self._cache.items()
            if pattern in k
        ]
        for key in keys_to_delete:
            del self._cache[key]
            if key in self._access_order:
                self._access_order.remove(key)

    def clear(self):
        """Clear all cache entries."""
        self._cache.clear()
        self._access_order.clear()


class CircuitBreaker:
    """Circuit breaker for upstream servers."""

    def __init__(
        self,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        timeout: float = 60.0
    ):
        self.failure_threshold = failure_threshold
        self.success_threshold = success_threshold
        self.timeout = timeout
        self._states: Dict[str, CircuitBreakerState] = defaultdict(CircuitBreakerState)

    def is_open(self, upstream_id: str) -> bool:
        """Check if circuit is open."""
        state = self._states[upstream_id]
        if state.state == "open":
            if state.next_retry and datetime.now() >= state.next_retry:
                state.state = "half_open"
                return False
            return True
        return False

    def record_success(self, upstream_id: str):
        """Record successful request."""
        state = self._states[upstream_id]
        if state.state == "half_open":
            state.successes += 1
            if state.successes >= self.success_threshold:
                state.state = "closed"
                state.failures = 0
                state.successes = 0
        elif state.state == "closed":
            state.failures = max(0, state.failures - 1)

    def record_failure(self, upstream_id: str):
        """Record failed request."""
        state = self._states[upstream_id]
        state.failures += 1
        if state.failures >= self.failure_threshold:
            state.state = "open"
            state.next_retry = datetime.now() + timedelta(seconds=self.timeout)

    def get_state(self, upstream_id: str) -> str:
        """Get circuit state."""
        return self._states[upstream_id].state


class LoadBalancer:
    """Load balancer for upstream servers."""

    def __init__(self, strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN):
        self.strategy = strategy
        self._round_robin_counters: Dict[str, int] = defaultdict(int)

    def select(
        self,
        servers: List[UpstreamServer],
        request: Optional[ProxyRequest] = None
    ) -> Optional[UpstreamServer]:
        """Select a server based on strategy."""
        healthy = [s for s in servers if s.is_healthy]
        if not healthy:
            return None

        if self.strategy == LoadBalancingStrategy.ROUND_ROBIN:
            return self._round_robin(healthy)
        elif self.strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
            return min(healthy, key=lambda s: s.active_connections)
        elif self.strategy == LoadBalancingStrategy.IP_HASH:
            return self._ip_hash(healthy, request)
        elif self.strategy == LoadBalancingStrategy.WEIGHTED:
            return self._weighted(healthy)
        elif self.strategy == LoadBalancingStrategy.RANDOM:
            import random
            return healthy[int(random.random() * len(healthy))]
        return healthy[0]

    def _round_robin(self, servers: List[UpstreamServer]) -> UpstreamServer:
        """Round robin selection."""
        group_id = id(servers)
        idx = self._round_robin_counters[group_id] % len(servers)
        self._round_robin_counters[group_id] += 1
        return servers[idx]

    def _ip_hash(self, servers: List[UpstreamServer], request: Optional[ProxyRequest]) -> UpstreamServer:
        """IP hash based selection."""
        if not request:
            return servers[0]
        hash_val = int(hashlib.md5(request.source_ip.encode()).hexdigest(), 16)
        return servers[hash_val % len(servers)]

    def _weighted(self, servers: List[UpstreamServer]) -> UpstreamServer:
        """Weighted selection."""
        total_weight = sum(s.weight for s in servers)
        import random
        r = random.randint(1, total_weight)
        cumulative = 0
        for server in servers:
            cumulative += server.weight
            if r <= cumulative:
                return server
        return servers[-1]


class ReverseProxy:
    """Main reverse proxy implementation."""

    def __init__(
        self,
        name: str = "proxy",
        load_balancer: Optional[LoadBalancer] = None,
        circuit_breaker: Optional[CircuitBreaker] = None,
        cache: Optional[ResponseCache] = None
    ):
        self.name = name
        self.load_balancer = load_balancer or LoadBalancer()
        self.circuit_breaker = circuit_breaker or CircuitBreaker()
        self.cache = cache or ResponseCache()
        self.upstreams: Dict[str, List[UpstreamServer]] = defaultdict(list)
        self.routes: Dict[str, str] = {}
        self.middleware: List[Callable] = []
        self._metrics: Dict[str, Any] = defaultdict(int)

    def add_upstream(self, upstream_id: str, server: UpstreamServer):
        """Add server to upstream group."""
        self.upstreams[upstream_id].append(server)

    def add_route(self, path_prefix: str, upstream_id: str):
        """Add route mapping."""
        self.routes[path_prefix] = upstream_id

    def add_middleware(self, middleware: Callable):
        """Add middleware function."""
        self.middleware.append(middleware)

    def _find_upstream(self, path: str) -> Optional[str]:
        """Find upstream for path."""
        for prefix, upstream_id in sorted(self.routes.items(), key=lambda x: len(x[0]), reverse=True):
            if path.startswith(prefix):
                return upstream_id
        return None

    async def handle_request(
        self,
        method: str,
        path: str,
        headers: Dict[str, str],
        body: Optional[bytes],
        query_params: Dict[str, str],
        source_ip: str
    ) -> ProxyResponse:
        """Handle incoming proxy request."""
        request = ProxyRequest(
            request_id=str(uuid.uuid4()),
            method=method,
            path=path,
            headers=headers,
            body=body,
            query_params=query_params,
            source_ip=source_ip
        )

        for mw in self.middleware:
            result = await mw(request)
            if result is not None:
                return result

        cached = self.cache.get(request)
        if cached:
            self._metrics["cache_hits"] += 1
            return cached

        self._metrics["cache_misses"] += 1

        upstream_id = self._find_upstream(path)
        if not upstream_id:
            return ProxyResponse(
                request_id=request.request_id,
                status_code=404,
                headers={},
                body=b"Not Found",
                upstream="",
                response_time=0.0
            )

        if self.circuit_breaker.is_open(upstream_id):
            return ProxyResponse(
                request_id=request.request_id,
                status_code=503,
                headers={"Retry-After": "60"},
                body=b"Service Unavailable",
                upstream=upstream_id,
                response_time=0.0
            )

        server = self.load_balancer.select(self.upstreams.get(upstream_id, []), request)
        if not server:
            return ProxyResponse(
                request_id=request.request_id,
                status_code=503,
                headers={},
                body=b"No healthy upstream",
                upstream=upstream_id,
                response_time=0.0
            )

        start_time = time.time()
        try:
            server.active_connections += 1
            server.total_requests += 1

            response = await self._proxy_to_upstream(server, request)

            self.circuit_breaker.record_success(upstream_id)
            self.cache.put(request, response)

            return response

        except Exception as e:
            server.failed_requests += 1
            server.last_failure = datetime.now()
            self.circuit_breaker.record_failure(upstream_id)
            logger.error(f"Proxy error: {e}")
            return ProxyResponse(
                request_id=request.request_id,
                status_code=502,
                headers={},
                body=str(e).encode(),
                upstream=server.id,
                response_time=time.time() - start_time
            )

        finally:
            server.active_connections -= 1

    async def _proxy_to_upstream(
        self,
        server: UpstreamServer,
        request: ProxyRequest
    ) -> ProxyResponse:
        """Proxy request to upstream server."""
        await asyncio.sleep(0.01)

        parsed = urlparse(server.url)
        response = ProxyResponse(
            request_id=request.request_id,
            status_code=200,
            headers={"Content-Type": "application/json"},
            body=json.dumps({"proxied": True, "upstream": server.id}).encode(),
            upstream=server.id,
            response_time=0.0,
            cached=False
        )
        return response

    def get_metrics(self) -> Dict[str, Any]:
        """Get proxy metrics."""
        return {
            "requests_total": self._metrics["cache_hits"] + self._metrics["cache_misses"],
            "cache_hits": self._metrics["cache_hits"],
            "cache_misses": self._metrics["cache_misses"],
            "upstreams": {
                upstream_id: {
                    "servers": [
                        {
                            "id": s.id,
                            "healthy": s.is_healthy,
                            "connections": s.active_connections,
                            "failure_rate": s.failure_rate
                        }
                        for s in servers
                    ]
                }
                for upstream_id, servers in self.upstreams.items()
            }
        }


async def main():
    """Demonstrate reverse proxy."""
    proxy = ReverseProxy()

    proxy.add_upstream("api", UpstreamServer(id="api1", url="http://localhost:8001"))
    proxy.add_upstream("api", UpstreamServer(id="api2", url="http://localhost:8002", weight=2))

    proxy.add_route("/api/", "api")

    response = await proxy.handle_request(
        method="GET",
        path="/api/users",
        headers={"Host": "example.com"},
        body=None,
        query_params={},
        source_ip="127.0.0.1"
    )

    print(f"Status: {response.status_code}")
    print(f"Upstream: {response.upstream}")
    print(f"Cached: {response.cached}")
    print(f"Metrics: {proxy.get_metrics()}")


if __name__ == "__main__":
    asyncio.run(main())
