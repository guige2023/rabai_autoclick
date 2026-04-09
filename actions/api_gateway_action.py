"""
API Gateway Action Module.

Provides API gateway functionality including routing,
rate limiting, authentication, request/response transformation,
and upstream management for microservices architectures.
"""

import time
import hashlib
import hmac
import json
import threading
from typing import Optional, Dict, Any, List, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict


class AuthType(Enum):
    """Authentication types."""
    NONE = "none"
    API_KEY = "api_key"
    BEARER_TOKEN = "bearer_token"
    BASIC_AUTH = "basic_auth"
    HMAC_SIGNATURE = "hmac_signature"
    JWT = "jwt"


@dataclass
class Route:
    """API route definition."""
    path: str
    method: str  # GET, POST, etc.
    upstream: str  # upstream server ID
    upstream_path: Optional[str] = None  # path on upstream
    auth: AuthType = AuthType.NONE
    rate_limit: Optional[int] = None  # requests per minute
    timeout: float = 30.0
    retry_attempts: int = 0
    transforms: Dict[str, Any] = field(default_factory=dict)


@dataclass
class UpstreamServer:
    """Upstream server definition."""
    id: str
    host: str
    port: int
    weight: int = 1
    is_healthy: bool = True

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"


@dataclass
class GatewayConfig:
    """Configuration for API gateway."""
    default_timeout: float = 30.0
    default_rate_limit: int = 100  # per minute
    max_retries: int = 3
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: float = 60.0
    request_buffer_size: int = 1000
    response_buffer_size: int = 10000


@dataclass
class GatewayRequest:
    """Incoming gateway request."""
    method: str
    path: str
    headers: Dict[str, str]
    query_params: Dict[str, str]
    body: Optional[bytes] = None
    client_ip: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class GatewayResponse:
    """Outgoing gateway response."""
    status_code: int
    headers: Dict[str, str]
    body: Optional[bytes] = None
    upstream: Optional[str] = None
    duration_ms: float = 0.0
    error: Optional[str] = None


@dataclass
class GatewayStats:
    """Gateway statistics."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    rejected_requests: int = 0
    total_latency_ms: float = 0.0
    requests_by_route: Dict[str, int] = field(default_factory=dict)
    requests_by_upstream: Dict[str, int] = field(default_factory=dict)


class APIGatewayAction:
    """
    API Gateway action with routing, authentication, and rate limiting.

    Provides a complete API gateway solution for routing requests
    to upstream services with authentication, authorization, and monitoring.
    """

    def __init__(self, config: Optional[GatewayConfig] = None):
        self.config = config or GatewayConfig()
        self._routes: Dict[str, Route] = {}  # key: "method:path"
        self._upstreams: Dict[str, UpstreamServer] = {}
        self._rate_limiters: Dict[str, List[float]] = defaultdict(list)
        self._stats = GatewayStats()
        self._lock = threading.RLock()
        self._middleware: List[Callable] = []
        self._auth_handlers: Dict[AuthType, Callable] = {}
        self._circuit_breakers: Dict[str, int] = defaultdict(int)
        self._circuit_breaker_last_failure: Dict[str, float] = {}

    def add_upstream(self, upstream: UpstreamServer) -> "APIGatewayAction":
        """Add an upstream server."""
        with self._lock:
            self._upstreams[upstream.id] = upstream
        return self

    def add_route(self, route: Route) -> "APIGatewayAction":
        """Add a route."""
        key = f"{route.method}:{route.path}"
        with self._lock:
            self._routes[key] = route
        return self

    def add_middleware(self, middleware: Callable) -> "APIGatewayAction":
        """Add middleware function."""
        self._middleware.append(middleware)
        return self

    def set_auth_handler(
        self,
        auth_type: AuthType,
        handler: Callable[[GatewayRequest, Route], bool],
    ) -> "APIGatewayAction":
        """Set authentication handler for auth type."""
        self._auth_handlers[auth_type] = handler
        return self

    def _get_route_key(self, method: str, path: str) -> Optional[str]:
        """Find matching route key."""
        # Exact match
        key = f"{method}:{path}"
        if key in self._routes:
            return key

        # Pattern match
        for route_key in self._routes:
            route_method, route_path = route_key.split(":", 1)
            if route_method == method and self._match_path(route_path, path):
                return route_key

        return None

    def _match_path(self, pattern: str, path: str) -> bool:
        """Match path against pattern."""
        import re
        # Convert pattern like /api/:id to regex
        regex_pattern = re.sub(r":\w+", r"[^/]+", pattern)
        regex_pattern = f"^{regex_pattern}$"
        return bool(re.match(regex_pattern, path))

    def _check_rate_limit(
        self,
        key: str,
        limit: int,
        window_seconds: float = 60.0,
    ) -> bool:
        """Check and update rate limit."""
        now = time.time()
        window_start = now - window_seconds

        with self._lock:
            # Clean old entries
            self._rate_limiters[key] = [
                t for t in self._rate_limiters[key] if t > window_start
            ]

            if len(self._rate_limiters[key]) >= limit:
                return False

            self._rate_limiters[key].append(now)
            return True

    def _check_circuit_breaker(self, upstream_id: str) -> bool:
        """Check if circuit breaker allows requests."""
        now = time.time()
        key = upstream_id

        if key in self._circuit_breakers:
            failures = self._circuit_breakers[key]
            if failures >= self.config.circuit_breaker_threshold:
                last_failure = self._circuit_breaker_last_failure.get(key, 0)
                if now - last_failure < self.config.circuit_breaker_timeout:
                    return False
                else:
                    # Try again
                    self._circuit_breakers[key] = 0
        return True

    def _record_upstream_failure(self, upstream_id: str) -> None:
        """Record upstream failure for circuit breaker."""
        with self._lock:
            self._circuit_breakers[upstream_id] += 1
            self._circuit_breaker_last_failure[upstream_id] = time.time()

    def _record_upstream_success(self, upstream_id: str) -> None:
        """Record upstream success."""
        with self._lock:
            if upstream_id in self._circuit_breakers:
                self._circuit_breakers[upstream_id] = max(
                    0, self._circuit_breakers[upstream_id] - 1
                )

    def _authenticate(self, request: GatewayRequest, route: Route) -> bool:
        """Authenticate request."""
        if route.auth == AuthType.NONE:
            return True

        handler = self._auth_handlers.get(route.auth)
        if handler:
            return handler(request, route)

        return True

    def _transform_request(
        self,
        request: GatewayRequest,
        route: Route,
    ) -> GatewayRequest:
        """Transform request before forwarding."""
        transforms = route.transforms.get("request", {})

        if "headers" in transforms:
            for key, value in transforms["headers"].items():
                request.headers[key] = value

        if "add_headers" in transforms:
            request.headers.update(transforms["add_headers"])

        if "remove_headers" in transforms:
            for header in transforms["remove_headers"]:
                request.headers.pop(header, None)

        return request

    def _transform_response(
        self,
        response: GatewayResponse,
        route: Route,
    ) -> GatewayResponse:
        """Transform response before returning."""
        transforms = route.transforms.get("response", {})

        if "headers" in transforms:
            response.headers.update(transforms["headers"])

        if "add_headers" in transforms:
            response.headers.update(transforms["add_headers"])

        if "remove_headers" in transforms:
            for header in transforms["remove_headers"]:
                response.headers.pop(header, None)

        if "status_code" in transforms:
            response.status_code = transforms["status_code"]

        return response

    async def handle_request_async(
        self,
        request: GatewayRequest,
    ) -> GatewayResponse:
        """
        Handle incoming gateway request.

        Args:
            request: Gateway request object

        Returns:
            Gateway response object
        """
        start_time = time.time()
        route_key = self._get_route_key(request.method, request.path)

        with self._lock:
            self._stats.total_requests += 1

        # No route found
        if route_key is None:
            return GatewayResponse(
                status_code=404,
                headers={"Content-Type": "application/json"},
                body=json.dumps({"error": "Not Found"}).encode(),
                error="No route found",
                duration_ms=(time.time() - start_time) * 1000,
            )

        route = self._routes[route_key]

        # Rate limiting
        if route.rate_limit:
            rate_key = f"{route.method}:{route.path}"
            if not self._check_rate_limit(rate_key, route.rate_limit):
                with self._lock:
                    self._stats.rejected_requests += 1
                return GatewayResponse(
                    status_code=429,
                    headers={"Content-Type": "application/json"},
                    body=json.dumps({"error": "Too Many Requests"}).encode(),
                    duration_ms=(time.time() - start_time) * 1000,
                )

        # Authentication
        if not self._authenticate(request, route):
            with self._lock:
                self._stats.rejected_requests += 1
            return GatewayResponse(
                status_code=401,
                headers={"Content-Type": "application/json"},
                body=json.dumps({"error": "Unauthorized"}).encode(),
                duration_ms=(time.time() - start_time) * 1000,
            )

        # Circuit breaker check
        if not self._check_circuit_breaker(route.upstream):
            return GatewayResponse(
                status_code=503,
                headers={"Content-Type": "application/json"},
                body=json.dumps({"error": "Service Unavailable"}).encode(),
                upstream=route.upstream,
                duration_ms=(time.time() - start_time) * 1000,
            )

        # Transform request
        request = self._transform_request(request, route)

        # Get upstream
        upstream = self._upstreams.get(route.upstream)
        if not upstream:
            return GatewayResponse(
                status_code=502,
                headers={"Content-Type": "application/json"},
                body=json.dumps({"error": "Bad Gateway"}).encode(),
                error="Upstream not found",
                duration_ms=(time.time() - start_time) * 1000,
            )

        # Run middleware
        for middleware in self._middleware:
            if asyncio.iscoroutinefunction(middleware):
                request = await middleware(request)
            else:
                request = middleware(request)

        # Make upstream request (simplified - actual implementation would use httpx/aiohttp)
        try:
            # Simulated upstream call
            response = GatewayResponse(
                status_code=200,
                headers={"Content-Type": "application/json"},
                body=json.dumps({"message": "success"}).encode(),
                upstream=upstream.id,
                duration_ms=(time.time() - start_time) * 1000,
            )

            self._record_upstream_success(upstream.id)

            with self._lock:
                self._stats.successful_requests += 1
                self._stats.requests_by_route[route_key] = \
                    self._stats.requests_by_route.get(route_key, 0) + 1
                self._stats.requests_by_upstream[upstream.id] = \
                    self._stats.requests_by_upstream.get(upstream.id, 0) + 1

            # Transform response
            response = self._transform_response(response, route)
            response.duration_ms = (time.time() - start_time) * 1000

            return response

        except Exception as e:
            self._record_upstream_failure(upstream.id)

            with self._lock:
                self._stats.failed_requests += 1

            return GatewayResponse(
                status_code=502,
                headers={"Content-Type": "application/json"},
                body=json.dumps({"error": str(e)}).encode(),
                upstream=upstream.id,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )

    def handle_request(self, request: GatewayRequest) -> GatewayResponse:
        """Handle request synchronously."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                future = asyncio.run_coroutine_threadsafe(
                    self.handle_request_async(request), loop
                )
                return future.result(timeout=request.headers.get("timeout", 30))
            return asyncio.run(self.handle_request_async(request))
        except Exception as e:
            return GatewayResponse(
                status_code=500,
                headers={"Content-Type": "application/json"},
                body=json.dumps({"error": str(e)}).encode(),
                error=str(e),
            )

    def get_stats(self) -> GatewayStats:
        """Get gateway statistics."""
        return self._stats
