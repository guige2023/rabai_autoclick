"""
API gateway module for request routing, rate limiting, and authentication.

Provides request routing, load balancing, circuit breaking,
rate limiting, and authentication middleware.
"""
from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class LoadBalancingStrategy(Enum):
    """Load balancing strategies."""
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    IP_HASH = "ip_hash"
    RANDOM = "random"


class RateLimitAlgorithm(Enum):
    """Rate limiting algorithms."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"
    LEAKY_BUCKET = "leaky_bucket"


@dataclass
class Upstream:
    """An upstream service endpoint."""
    id: str
    name: str
    url: str
    weight: int = 100
    max_connections: int = 100
    timeout_seconds: int = 30
    healthy: bool = True
    latency_ms: float = 0.0
    failures: int = 0
    metadata: dict = field(default_factory=dict)


@dataclass
class Route:
    """A routing rule."""
    id: str
    path: str
    method: str
    upstream_ids: list[str]
    strip_path: bool = False
    prefix_rewrite: str = ""
    timeout_seconds: int = 30
    rate_limit_enabled: bool = True
    auth_enabled: bool = True


@dataclass
class RateLimitConfig:
    """Rate limiting configuration."""
    algorithm: RateLimitAlgorithm = RateLimitAlgorithm.TOKEN_BUCKET
    requests_per_second: int = 100
    burst_size: int = 200
    block_duration_seconds: int = 60


@dataclass
class AuthConfig:
    """Authentication configuration."""
    type: str = "none"
    api_key_header: str = "X-API-Key"
    jwt_secret: str = ""
    jwt_issuer: str = ""
    oauth2_audience: str = ""


@dataclass
class RequestLog:
    """Log of a proxied request."""
    id: str
    route_id: str
    upstream_id: str
    client_ip: str
    method: str
    path: str
    status_code: int
    latency_ms: float
    timestamp: float = field(default_factory=time.time)


@dataclass
class CircuitState:
    """Circuit breaker state."""
    state: str = "closed"
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[float] = None
    next_retry_time: Optional[float] = None


class APIGateway:
    """
    API gateway for request routing, rate limiting, and authentication.

    Provides request routing, load balancing, circuit breaking,
    rate limiting, and authentication middleware.
    """

    def __init__(
        self,
        name: str = "api-gateway",
        rate_limit: Optional[RateLimitConfig] = None,
        auth: Optional[AuthConfig] = None,
    ):
        self.name = name
        self.rate_limit = rate_limit or RateLimitConfig()
        self.auth = auth or AuthConfig()
        self._upstreams: dict[str, Upstream] = {}
        self._routes: dict[str, Route] = {}
        self._circuit_breakers: dict[str, CircuitState] = {}
        self._rate_limiters: dict[str, dict] = {}
        self._request_logs: list[RequestLog] = []
        self._round_robin_index: dict[str, int] = {}

    def add_upstream(self, upstream: Upstream) -> None:
        """Add an upstream service."""
        self._upstreams[upstream.id] = upstream
        self._circuit_breakers[upstream.id] = CircuitState()
        self._round_robin_index[upstream.id] = 0

    def get_upstream(self, upstream_id: str) -> Optional[Upstream]:
        """Get an upstream by ID."""
        return self._upstreams.get(upstream_id)

    def add_route(self, route: Route) -> None:
        """Add a routing rule."""
        self._routes[route.id] = route

    def get_route(self, route_id: str) -> Optional[Route]:
        """Get a route by ID."""
        return self._routes.get(route_id)

    def find_route(self, method: str, path: str) -> Optional[Route]:
        """Find a matching route for a request."""
        for route in self._routes.values():
            if route.method and route.method != method:
                continue

            if path.startswith(route.path):
                return route

        return None

    def select_upstream(
        self,
        route: Route,
        client_ip: str = "",
        strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN,
    ) -> Optional[Upstream]:
        """Select an upstream using load balancing strategy."""
        available = [
            u for u in self._upstreams.values()
            if u.id in route.upstream_ids and u.healthy
        ]

        if not available:
            return None

        if strategy == LoadBalancingStrategy.ROUND_ROBIN:
            upstream = available[self._round_robin_index[route.id] % len(available)]
            self._round_robin_index[route.id] += 1
            return upstream

        elif strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
            return min(available, key=lambda u: u.failures)

        elif strategy == LoadBalancingStrategy.IP_HASH:
            hash_val = int(hashlib.md5(client_ip.encode()).hexdigest(), 16)
            return available[hash_val % len(available)]

        elif strategy == LoadBalancingStrategy.RANDOM:
            import random
            return random.choice(available)

        return available[0]

    def check_rate_limit(
        self,
        key: str,
        requests_per_second: Optional[int] = None,
        burst_size: Optional[int] = None,
    ) -> tuple[bool, dict]:
        """Check if a request is within rate limits."""
        rps = requests_per_second or self.rate_limit.requests_per_second
        burst = burst_size or self.rate_limit.burst_size

        if key not in self._rate_limiters:
            self._rate_limiters[key] = {
                "tokens": float(burst),
                "last_update": time.time(),
            }

        limiter = self._rate_limiters[key]
        now = time.time()
        elapsed = now - limiter["last_update"]

        limiter["tokens"] = min(burst, limiter["tokens"] + elapsed * rps)
        limiter["last_update"] = now

        if limiter["tokens"] >= 1:
            limiter["tokens"] -= 1
            return True, {
                "allowed": True,
                "remaining": int(limiter["tokens"]),
                "reset_at": now + (burst - limiter["tokens"]) / rps,
            }

        return False, {
            "allowed": False,
            "remaining": 0,
            "reset_at": now + 1 / rps,
        }

    def check_circuit_breaker(self, upstream_id: str) -> bool:
        """Check if circuit breaker allows requests."""
        cb = self._circuit_breakers.get(upstream_id)
        if not cb:
            return True

        if cb.state == "open":
            if cb.next_retry_time and time.time() >= cb.next_retry_time:
                cb.state = "half_open"
                return True
            return False

        return True

    def record_success(self, upstream_id: str) -> None:
        """Record a successful request to an upstream."""
        upstream = self._upstreams.get(upstream_id)
        cb = self._circuit_breakers.get(upstream_id)

        if upstream:
            upstream.failures = 0

        if cb:
            cb.success_count += 1
            cb.failure_count = 0
            if cb.state == "half_open":
                cb.state = "closed"

    def record_failure(self, upstream_id: str, threshold: int = 5) -> None:
        """Record a failed request to an upstream."""
        upstream = self._upstreams.get(upstream_id)
        cb = self._circuit_breakers.get(upstream_id)

        if upstream:
            upstream.failures += 1

        if cb:
            cb.failure_count += 1
            cb.last_failure_time = time.time()

            if cb.failure_count >= threshold:
                cb.state = "open"
                cb.next_retry_time = time.time() + 30

    def authenticate_request(
        self,
        headers: dict,
        auth_type: Optional[str] = None,
    ) -> tuple[bool, Optional[str]]:
        """Authenticate a request."""
        auth_type = auth_type or self.auth.type

        if auth_type == "none":
            return True, None

        elif auth_type == "api_key":
            api_key = headers.get(self.auth.api_key_header)
            if not api_key:
                return False, "Missing API key"
            return True, None

        elif auth_type == "jwt":
            import jwt
            token = headers.get("Authorization", "").replace("Bearer ", "")
            if not token:
                return False, "Missing token"
            try:
                jwt.decode(token, self.auth.jwt_secret, algorithms=["HS256"])
                return True, None
            except jwt.InvalidTokenError as e:
                return False, str(e)

        return True, None

    def log_request(
        self,
        route_id: str,
        upstream_id: str,
        client_ip: str,
        method: str,
        path: str,
        status_code: int,
        latency_ms: float,
    ) -> None:
        """Log a proxied request."""
        log = RequestLog(
            id=str(uuid.uuid4())[:8],
            route_id=route_id,
            upstream_id=upstream_id,
            client_ip=client_ip,
            method=method,
            path=path,
            status_code=status_code,
            latency_ms=latency_ms,
        )
        self._request_logs.append(log)

        if len(self._request_logs) > 10000:
            self._request_logs = self._request_logs[-5000:]

    def get_upstream_stats(self, upstream_id: str) -> dict:
        """Get statistics for an upstream."""
        upstream = self._upstreams.get(upstream_id)
        cb = self._circuit_breakers.get(upstream_id)

        logs = [l for l in self._request_logs if l.upstream_id == upstream_id]
        total_requests = len(logs)
        failed_requests = sum(1 for l in logs if l.status_code >= 500)

        return {
            "upstream_id": upstream_id,
            "name": upstream.name if upstream else "",
            "healthy": upstream.healthy if upstream else False,
            "circuit_state": cb.state if cb else "unknown",
            "total_requests": total_requests,
            "failed_requests": failed_requests,
            "failure_rate": failed_requests / total_requests if total_requests > 0 else 0,
            "avg_latency_ms": sum(l.latency_ms for l in logs) / total_requests if logs else 0,
        }

    def list_routes(self) -> list[Route]:
        """List all routes."""
        return list(self._routes.values())

    def list_upstreams(self) -> list[Upstream]:
        """List all upstreams."""
        return list(self._upstreams.values())

    def get_request_logs(
        self,
        route_id: Optional[str] = None,
        upstream_id: Optional[str] = None,
        limit: int = 100,
    ) -> list[RequestLog]:
        """Get request logs with optional filters."""
        logs = self._request_logs

        if route_id:
            logs = [l for l in logs if l.route_id == route_id]
        if upstream_id:
            logs = [l for l in logs if l.upstream_id == upstream_id]

        return logs[-limit:]
