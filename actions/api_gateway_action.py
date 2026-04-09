"""API Gateway for routing, authentication, and rate limiting.

This module provides a lightweight API gateway that handles:
- Request routing to backend services
- API key authentication
- Rate limiting
- Request/response transformation

Example:
    >>> from actions.api_gateway_action import APIGateway
    >>> gateway = APIGateway()
    >>> gateway.add_route("/users", user_service)
"""

from __future__ import annotations

import time
import hashlib
import hmac
import logging
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class Route:
    """An API route definition."""
    path: str
    handler: Callable[..., Any]
    methods: list[str] = field(default_factory=lambda: ["GET"])
    auth_required: bool = True
    rate_limit: Optional[int] = None


@dataclass
class APIKey:
    """An API key for authentication."""
    key: str
    name: str
    secret: str
    rate_limit: int = 100
    is_active: bool = True
    created_at: float = field(default_factory=time.time)


@dataclass
class GatewayRequest:
    """A gateway request."""
    path: str
    method: str
    headers: dict[str, str]
    body: Optional[bytes] = None
    query_params: dict[str, str] = field(default_factory=dict)
    api_key: Optional[APIKey] = None


@dataclass
class GatewayResponse:
    """A gateway response."""
    status_code: int
    body: Any
    headers: dict[str, str] = field(default_factory=dict)


class RateLimiter:
    """Simple token bucket rate limiter for gateway."""

    def __init__(self, rate: int, window: float = 60.0) -> None:
        self.rate = rate
        self.window = window
        self._requests: dict[str, list[float]] = defaultdict(list)
        self._lock = threading.Lock()

    def allow(self, identifier: str) -> bool:
        """Check if request is allowed."""
        with self._lock:
            now = time.time()
            window_start = now - self.window
            self._requests[identifier] = [
                t for t in self._requests[identifier] if t > window_start
            ]
            if len(self._requests[identifier]) < self.rate:
                self._requests[identifier].append(now)
                return True
            return False


class APIGateway:
    """API Gateway for routing and authentication.

    Attributes:
        name: Gateway name for logging.
    """

    def __init__(self, name: str = "api-gateway") -> None:
        self.name = name
        self._routes: dict[str, Route] = {}
        self._api_keys: dict[str, APIKey] = {}
        self._rate_limiters: dict[str, RateLimiter] = {}
        self._lock = threading.RLock()
        logger.info(f"API Gateway '{name}' initialized")

    def add_route(
        self,
        path: str,
        handler: Callable[..., Any],
        methods: Optional[list[str]] = None,
        auth_required: bool = True,
        rate_limit: Optional[int] = None,
    ) -> None:
        """Add a route to the gateway.

        Args:
            path: URL path for the route.
            handler: Handler function.
            methods: HTTP methods allowed (default: GET).
            auth_required: Whether authentication is required.
            rate_limit: Optional requests per minute limit.
        """
        route = Route(
            path=path,
            handler=handler,
            methods=methods or ["GET"],
            auth_required=auth_required,
            rate_limit=rate_limit,
        )
        with self._lock:
            self._routes[path] = route
            if rate_limit:
                self._rate_limiters[path] = RateLimiter(rate=rate_limit)
        logger.info(f"Added route: {path}")

    def add_api_key(
        self,
        name: str,
        rate_limit: int = 100,
    ) -> APIKey:
        """Create and register a new API key.

        Args:
            name: Name/identifier for the API key.
            rate_limit: Requests per minute limit.

        Returns:
            The created APIKey object.
        """
        import secrets
        api_key = APIKey(
            key=secrets.token_urlsafe(32),
            name=name,
            secret=secrets.token_urlsafe(64),
            rate_limit=rate_limit,
        )
        with self._lock:
            self._api_keys[api_key.key] = api_key
            self._rate_limiters[api_key.key] = RateLimiter(
                rate=rate_limit, window=60.0
            )
        logger.info(f"Created API key: {name}")
        return api_key

    def revoke_api_key(self, key: str) -> bool:
        """Revoke an API key.

        Args:
            key: The API key to revoke.

        Returns:
            True if revoked, False if not found.
        """
        with self._lock:
            if key in self._api_keys:
                self._api_keys[key].is_active = False
                logger.info(f"Revoked API key: {self._api_keys[key].name}")
                return True
            return False

    def handle_request(self, request: GatewayRequest) -> GatewayResponse:
        """Handle an incoming gateway request.

        Args:
            request: The incoming GatewayRequest.

        Returns:
            GatewayResponse with the result.

        Raises:
            ValueError: If route not found or authentication fails.
        """
        route = self._routes.get(request.path)
        if not route:
            return GatewayResponse(
                status_code=404,
                body={"error": "Route not found", "path": request.path},
            )

        if request.method not in route.methods:
            return GatewayResponse(
                status_code=405,
                body={"error": "Method not allowed"},
            )

        if route.auth_required:
            auth_result = self._authenticate(request)
            if auth_result is None:
                return GatewayResponse(
                    status_code=401,
                    body={"error": "Authentication required"},
                )

        if route.rate_limit:
            limiter = self._rate_limiters.get(request.path)
            if limiter and not limiter.allow(request.path):
                return GatewayResponse(
                    status_code=429,
                    body={"error": "Rate limit exceeded"},
                )

        try:
            result = route.handler(request)
            return GatewayResponse(
                status_code=200,
                body=result,
            )
        except Exception as e:
            logger.error(f"Handler error for {request.path}: {e}")
            return GatewayResponse(
                status_code=500,
                body={"error": str(e)},
            )

    def _authenticate(self, request: GatewayRequest) -> Optional[APIKey]:
        """Authenticate a request using API key.

        Args:
            request: The request to authenticate.

        Returns:
            APIKey if authenticated, None otherwise.
        """
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return None
        key = auth_header[7:]
        with self._lock:
            api_key = self._api_keys.get(key)
            if not api_key or not api_key.is_active:
                return None
            limiter = self._rate_limiters.get(key)
            if limiter and not limiter.allow(key):
                return None
            return api_key

    def get_stats(self) -> dict[str, Any]:
        """Get gateway statistics.

        Returns:
            Dictionary containing gateway stats.
        """
        with self._lock:
            return {
                "name": self.name,
                "routes": len(self._routes),
                "api_keys": sum(1 for k in self._api_keys.values() if k.is_active),
                "total_requests": sum(
                    len(lim._requests.get(k, []))
                    for k, lim in self._rate_limiters.items()
                ),
            }
