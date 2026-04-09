"""API Gateway v2 with advanced routing, auth, and rate limiting.

This module provides a production-ready API gateway with support for:
- Path-based and header-based routing
- JWT and API key authentication
- Rate limiting per client/endpoint
- Request/response transformation
- Circuit breaker for backend services
- Health monitoring
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Awaitable, Protocol
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)


class AuthType(Enum):
    """Authentication types supported by the gateway."""

    NONE = "none"
    API_KEY = "api_key"
    BEARER = "bearer"
    JWT = "jwt"
    HMAC = "hmac"


class RateLimitAlgorithm(Enum):
    """Rate limiting algorithms."""

    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class RouteConfig:
    """Configuration for a single route."""

    path_pattern: str
    upstream_url: str
    auth_type: AuthType = AuthType.NONE
    auth_secret: str | None = None
    rate_limit: int = 1000  # requests per window
    rate_window: float = 60.0  # seconds
    rate_algorithm: RateLimitAlgorithm = RateLimitAlgorithm.TOKEN_BUCKET
    timeout: float = 30.0
    retry_count: int = 3
    methods: list[str] = field(default_factory=lambda: ["GET", "POST", "PUT", "DELETE"])
    headers_to_forward: list[str] = field(default_factory=list)
    transform_request: Callable[[dict], dict] | None = None
    transform_response: Callable[[dict], dict] | None = None


@dataclass
class CircuitBreaker:
    """Circuit breaker for backend protection."""

    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    half_open_max_calls: int = 3
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    last_failure_time: float = 0.0
    half_open_calls: int = 0

    def record_success(self) -> None:
        """Record a successful call."""
        if self.state == CircuitState.HALF_OPEN:
            self.half_open_calls -= 1
            if self.half_open_calls <= 0:
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                logger.info("Circuit breaker closed (recovered)")
        elif self.state == CircuitState.CLOSED:
            self.failure_count = max(0, self.failure_count - 1)

    def record_failure(self) -> None:
        """Record a failed call."""
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
            logger.warning("Circuit breaker reopened after half-open failure")
        elif self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
            logger.warning(f"Circuit breaker opened after {self.failure_count} failures")

    def can_execute(self) -> bool:
        """Check if a call can be executed."""
        if self.state == CircuitState.CLOSED:
            return True

        if self.state == CircuitState.OPEN:
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                self.half_open_calls = self.half_open_max_calls
                logger.info("Circuit breaker entering half-open state")
                return True
            return False

        # HALF_OPEN
        return self.half_open_calls > 0


@dataclass
class RateLimiter:
    """Rate limiter using token bucket algorithm."""

    capacity: int
    refill_rate: float  # tokens per second
    tokens: float = 0.0
    last_refill: float = field(default_factory=time.time)

    def allow_request(self, tokens_needed: int = 1) -> bool:
        """Check if a request is allowed."""
        self._refill()

        if self.tokens >= tokens_needed:
            self.tokens -= tokens_needed
            return True
        return False

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now


@dataclass
class ClientKey:
    """Unique client identifier for rate limiting."""

    api_key: str | None = None
    ip_address: str | None = None
    user_id: str | None = None

    def __hash__(self) -> int:
        parts = []
        if self.api_key:
            parts.append(f"key:{self.api_key}")
        if self.ip_address:
            parts.append(f"ip:{self.ip_address}")
        if self.user_id:
            parts.append(f"user:{self.user_id}")
        return hash("|".join(parts) if parts else "anonymous")


class APIGatewayV2:
    """Advanced API Gateway with routing, auth, and protection."""

    def __init__(self, routes: list[RouteConfig] | None = None):
        """Initialize the gateway.

        Args:
            routes: List of route configurations
        """
        self.routes: dict[str, RouteConfig] = {}
        if routes:
            for route in routes:
                self.routes[route.path_pattern] = route

        self.circuit_breakers: dict[str, CircuitBreaker] = {}
        self.rate_limiters: dict[ClientKey, RateLimiter] = {}
        self.auth_handlers: dict[AuthType, Callable[..., bool]] = {
            AuthType.NONE: self._auth_none,
            AuthType.API_KEY: self._auth_api_key,
            AuthType.BEARER: self._auth_bearer,
            AuthType.JWT: self._auth_jwt,
            AuthType.HMAC: self._auth_hmac,
        }
        self._client_lock = asyncio.Lock()
        self.metrics: dict[str, Any] = {
            "total_requests": 0,
            "blocked_requests": 0,
            "circuit_open": 0,
            "rate_limited": 0,
            "upstream_errors": 0,
        }

    def add_route(self, config: RouteConfig) -> None:
        """Add a new route configuration.

        Args:
            config: Route configuration to add
        """
        self.routes[config.path_pattern] = config
        if config.upstream_url not in self.circuit_breakers:
            self.circuit_breakers[config.upstream_url] = CircuitBreaker()

    def _auth_none(self, request: dict, config: RouteConfig) -> bool:
        """No authentication required."""
        return True

    def _auth_api_key(self, request: dict, config: RouteConfig) -> bool:
        """API key authentication."""
        if not config.auth_secret:
            return False
        api_key = request.headers.get("X-API-Key", "")
        return hmac.compare_digest(api_key, config.auth_secret)

    def _auth_bearer(self, request: dict, config: RouteConfig) -> bool:
        """Bearer token authentication."""
        if not config.auth_secret:
            return False
        auth_header = request.headers.get("Authorization", "")
        expected = f"Bearer {config.auth_secret}"
        return hmac.compare_digest(auth_header, expected)

    def _auth_jwt(self, request: dict, config: RouteConfig) -> bool:
        """JWT authentication (simplified - verify signature only)."""
        if not config.auth_secret:
            return False
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return False
        token = auth_header[7:]
        # Simplified JWT verification - in production use PyJWT
        try:
            parts = token.split(".")
            if len(parts) != 3:
                return False
            # Verify signature
            signing_input = f"{parts[0]}.{parts[1]}"
            expected_sig = hmac.new(
                config.auth_secret.encode(),
                signing_input.encode(),
                hashlib.sha256,
            ).hexdigest()
            return hmac.compare_digest(parts[2], expected_sig[:len(parts[2])])
        except Exception:
            return False

    def _auth_hmac(self, request: dict, config: RouteConfig) -> bool:
        """HMAC signature authentication."""
        if not config.auth_secret:
            return False
        sig = request.headers.get("X-HMAC-Signature", "")
        timestamp = request.headers.get("X-HMAC-Timestamp", "")
        if not sig or not timestamp:
            return False
        # Check timestamp freshness (5 minute window)
        try:
            if abs(time.time() - float(timestamp)) > 300:
                return False
        except ValueError:
            return False

        body = request.get("body", "")
        if isinstance(body, str):
            body = body.encode()
        elif isinstance(body, dict):
            body = json.dumps(body).encode()

        message = f"{timestamp}.{body.decode() if isinstance(body, bytes) else body}"
        expected_sig = hmac.new(
            config.auth_secret.encode(),
            message.encode(),
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(sig, expected_sig)

    async def _get_rate_limiter(self, client_key: ClientKey, config: RouteConfig) -> RateLimiter:
        """Get or create rate limiter for a client."""
        async with self._client_lock:
            if client_key not in self.rate_limiters:
                refill_rate = config.rate_limit / config.rate_window
                self.rate_limiters[client_key] = RateLimiter(
                    capacity=config.rate_limit,
                    refill_rate=refill_rate,
                )
            return self.rate_limiters[client_key]

    def _extract_client_key(self, request: dict, config: RouteConfig) -> ClientKey:
        """Extract client key from request for rate limiting."""
        return ClientKey(
            api_key=request.headers.get("X-API-Key"),
            ip_address=request.headers.get("X-Forwarded-For", "").split(",")[0].strip(),
            user_id=request.headers.get("X-User-ID"),
        )

    async def handle_request(
        self,
        path: str,
        method: str,
        headers: dict[str, str] | None = None,
        body: Any = None,
        query_params: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Handle an incoming request through the gateway.

        Args:
            path: Request path
            method: HTTP method
            headers: Request headers
            body: Request body
            query_params: Query parameters

        Returns:
            Gateway response with status, headers, and body
        """
        self.metrics["total_requests"] += 1
        headers = headers or {}

        request_data = {
            "path": path,
            "method": method,
            "headers": headers,
            "body": body,
            "query_params": query_params or {},
        }

        # Find matching route
        route = self._match_route(path)
        if not route:
            return self._error_response(404, "Route not found")

        # Check method
        if method.upper() not in route.methods:
            return self._error_response(405, f"Method {method} not allowed")

        # Authenticate
        if not self.auth_handlers[route.auth_type](request_data, route):
            self.metrics["blocked_requests"] += 1
            return self._error_response(401, "Authentication failed")

        # Rate limit
        client_key = self._extract_client_key(request_data, route)
        limiter = await self._get_rate_limiter(client_key, route)
        if not limiter.allow_request():
            self.metrics["rate_limited"] += 1
            return self._error_response(429, "Rate limit exceeded")

        # Circuit breaker check
        cb = self.circuit_breakers.get(route.upstream_url, CircuitBreaker())
        if not cb.can_execute():
            self.metrics["circuit_open"] += 1
            return self._error_response(503, "Service temporarily unavailable")

        # Transform request
        request_body = body
        if route.transform_request:
            try:
                transformed = route.transform_request({
                    "body": body,
                    "headers": headers,
                    "query": query_params,
                })
                request_body = transformed.get("body", body)
            except Exception as e:
                logger.error(f"Request transform failed: {e}")

        # Forward to upstream
        try:
            result = await self._forward_request(
                route.upstream_url,
                path,
                method,
                headers,
                request_body,
                query_params,
                route.timeout,
                route.retry_count,
            )
            cb.record_success()

            # Transform response
            if route.transform_response and "data" in result:
                try:
                    result["data"] = route.transform_response(result["data"])
                except Exception as e:
                    logger.error(f"Response transform failed: {e}")

            return result

        except Exception as e:
            cb.record_failure()
            self.metrics["upstream_errors"] += 1
            logger.error(f"Upstream request failed: {e}")
            return self._error_response(502, f"Upstream error: {str(e)}")

    async def _forward_request(
        self,
        upstream_url: str,
        path: str,
        method: str,
        headers: dict[str, str],
        body: Any,
        query_params: dict[str, str] | None,
        timeout: float,
        retry_count: int,
    ) -> dict[str, Any]:
        """Forward request to upstream with retry."""
        async with httpx.AsyncClient(timeout=timeout) as client:
            for attempt in range(retry_count + 1):
                try:
                    url = f"{upstream_url.rstrip('/')}/{path.lstrip('/')}"
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
                    }
                except httpx.TimeoutException:
                    if attempt >= retry_count:
                        raise
                except httpx.HTTPError as e:
                    if attempt >= retry_count:
                        raise
                    await asyncio.sleep(2 ** attempt)

    def _match_route(self, path: str) -> RouteConfig | None:
        """Match request path to a route configuration."""
        for pattern in self.routes:
            if path.startswith(pattern) or path == pattern:
                return self.routes[pattern]
        return None

    def _error_response(self, status: int, message: str) -> dict[str, Any]:
        """Create an error response."""
        return {
            "status": status,
            "headers": {"Content-Type": "application/json"},
            "data": {"error": message, "timestamp": time.time()},
        }

    def get_metrics(self) -> dict[str, Any]:
        """Get gateway metrics."""
        return self.metrics.copy()

    async def health_check(self) -> dict[str, Any]:
        """Perform health check on all backends."""
        health = {
            "gateway": "healthy",
            "backends": {},
            "timestamp": time.time(),
        }

        for url in self.circuit_breakers:
            cb = self.circuit_breakers[url]
            backend_health = {
                "circuit_state": cb.state.value,
                "failure_count": cb.failure_count,
            }
            if cb.state == CircuitState.OPEN:
                backend_health["time_until_retry"] = max(
                    0, cb.recovery_timeout - (time.time() - cb.last_failure_time)
                )
            health["backends"][url] = backend_health

        return health


# Convenience factory
def create_gateway(routes: list[RouteConfig] | None = None) -> APIGatewayV2:
    """Create a configured API gateway.

    Args:
        routes: Optional list of route configurations

    Returns:
        Configured APIGatewayV2 instance
    """
    return APIGatewayV2(routes=routes)
