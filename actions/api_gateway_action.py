"""
API Gateway Action Module.

Provides unified API gateway with routing, authentication,
rate limiting, and request/response transformation.
"""

import asyncio
import hashlib
import hmac
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

from .api_rate_limiter_action import APIRateLimiterAction, RateLimitStrategy


class AuthType(Enum):
    """Authentication types."""
    NONE = "none"
    API_KEY = "api_key"
    Bearer = "bearer"
    HMAC = "hmac"
    BASIC = "basic"


@dataclass
class Route:
    """API route definition."""
    path: str
    method: str
    handler: Callable
    auth_type: AuthType = AuthType.NONE
    auth_config: dict = field(default_factory=dict)
    rate_limit: Optional[dict] = None
    timeout: float = 30.0
    transforms: dict = field(default_factory=dict)


@dataclass
class GatewayConfig:
    """Gateway configuration."""
    prefix: str = "/api/v1"
    default_timeout: float = 30.0
    default_rate_limit: float = 100.0
    enable_logging: bool = True
    enable_metrics: bool = True


@dataclass
class Request:
    """API request wrapper."""
    path: str
    method: str
    headers: dict
    query_params: dict
    body: Any
    ip: str = ""
    user_agent: str = ""


@dataclass
class Response:
    """API response wrapper."""
    status_code: int
    body: Any
    headers: dict = field(default_factory=dict)


@dataclass
class GatewayMetrics:
    """Gateway metrics."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_latency: float = 0.0

    @property
    def avg_latency(self) -> float:
        return self.total_latency / self.total_requests if self.total_requests else 0.0


class APIKeyAuth:
    """API key authentication handler."""

    def __init__(self, api_keys: dict[str, dict]):
        self._keys = api_keys

    def validate(self, request: Request) -> Optional[str]:
        """Validate API key and return user/client ID."""
        key = request.headers.get("X-API-Key") or request.query_params.get("api_key")
        if key in self._keys:
            return self._keys[key].get("client_id")
        return None


class HMACAuth:
    """HMAC signature authentication handler."""

    def __init__(self, secrets: dict[str, str]):
        self._secrets = secrets

    def validate(self, request: Request) -> Optional[str]:
        """Validate HMAC signature."""
        client_id = request.headers.get("X-Client-ID")
        signature = request.headers.get("X-Signature")
        timestamp = request.headers.get("X-Timestamp")

        if not all([client_id, signature, timestamp]):
            return None

        if client_id not in self._secrets:
            return None

        secret = self._secrets[client_id]
        message = f"{request.path}:{timestamp}:{request.body}"
        expected = hmac.new(
            secret.encode(),
            message.encode(),
            hashlib.sha256
        ).hexdigest()

        if hmac.compare_digest(signature, expected):
            return client_id
        return None


class BearerAuth:
    """Bearer token authentication handler."""

    def __init__(self, tokens: dict[str, dict]):
        self._tokens = tokens

    def validate(self, request: Request) -> Optional[str]:
        """Validate bearer token."""
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return None

        token = auth_header[7:]
        if token in self._tokens:
            return self._tokens[token].get("client_id")
        return None


class APIGatewayAction:
    """
    API Gateway with routing, auth, rate limiting.

    Example:
        gateway = APIGatewayAction(config=GatewayConfig(prefix="/api/v1"))

        @gateway.route("/users", method="GET")
        async def get_users(request):
            return Response(200, {"users": []})

        await gateway.serve()
    """

    def __init__(self, config: Optional[GatewayConfig] = None):
        self.config = config or GatewayConfig()
        self._routes: list[Route] = []
        self._auth_handlers: dict[AuthType, Any] = {}
        self._rate_limiters: dict[str, APIRateLimiterAction] = {}
        self.metrics = GatewayMetrics()

    def route(
        self,
        path: str,
        method: str = "GET",
        auth_type: AuthType = AuthType.NONE,
        auth_config: Optional[dict] = None,
        rate_limit: Optional[float] = None,
        timeout: Optional[float] = None
    ):
        """Decorator to register route."""
        def decorator(func: Callable) -> Callable:
            route = Route(
                path=path,
                method=method,
                handler=func,
                auth_type=auth_type,
                auth_config=auth_config or {},
                rate_limit={"requests_per_second": rate_limit} if rate_limit else None,
                timeout=timeout or self.config.default_timeout
            )
            self._routes.append(route)
            return func
        return decorator

    def register_auth(self, auth_type: AuthType, handler: Any) -> None:
        """Register authentication handler."""
        self._auth_handlers[auth_type] = handler

    def _match_route(self, path: str, method: str) -> Optional[Route]:
        """Match request to route."""
        for route in self._routes:
            if route.path == path and route.method.upper() == method.upper():
                return route
        return None

    async def _authenticate(self, route: Route, request: Request) -> bool:
        """Authenticate request."""
        if route.auth_type == AuthType.NONE:
            return True

        handler = self._auth_handlers.get(route.auth_type)
        if handler is None:
            return False

        return handler.validate(request) is not None

    async def _rate_limit(self, route: Route, client_id: str) -> bool:
        """Apply rate limiting."""
        if route.rate_limit is None:
            return True

        if client_id not in self._rate_limiters:
            self._rate_limiters[client_id] = APIRateLimiterAction(
                requests_per_second=route.rate_limit.get("requests_per_second", 100),
                burst_size=route.rate_limit.get("burst_size", 200),
                strategy=RateLimitStrategy.TOKEN_BUCKET
            )

        return await self._rate_limiters[client_id].acquire()

    async def _transform_request(self, route: Route, request: Request) -> Request:
        """Transform request."""
        return request

    async def _transform_response(self, route: Route, response: Response) -> Response:
        """Transform response."""
        return response

    async def handle(self, request: Request) -> Response:
        """Handle incoming request."""
        start = time.monotonic()
        self.metrics.total_requests += 1

        try:
            route = self._match_route(request.path, request.method)
            if route is None:
                return Response(404, {"error": "Not found"})

            if not await self._authenticate(route, request):
                return Response(401, {"error": "Unauthorized"})

            client_id = request.headers.get("X-Client-ID", "anonymous")
            if not await self._rate_limit(route, client_id):
                return Response(429, {"error": "Rate limit exceeded"})

            request = await self._transform_request(route, request)

            result = await asyncio.wait_for(
                route.handler(request),
                timeout=route.timeout
            )

            response = await self._transform_response(route, result)
            self.metrics.successful_requests += 1
            return response

        except asyncio.TimeoutError:
            self.metrics.failed_requests += 1
            return Response(504, {"error": "Gateway timeout"})
        except Exception as e:
            self.metrics.failed_requests += 1
            return Response(500, {"error": str(e)})
        finally:
            self.metrics.total_latency += time.monotonic() - start
