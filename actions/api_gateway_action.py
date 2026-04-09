"""API gateway action for routing and managing API requests.

Provides request routing, authentication, rate limiting,
and response transformation for API gateways.
"""

import hashlib
import hmac
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class AuthType(Enum):
    NONE = "none"
    API_KEY = "api_key"
    BEARER = "bearer"
    HMAC = "hmac"


@dataclass
class Route:
    path: str
    method: str
    handler: Callable
    auth_type: AuthType = AuthType.NONE
    rate_limit: Optional[float] = None


@dataclass
class RequestContext:
    method: str
    path: str
    headers: dict[str, str]
    query_params: dict[str, str]
    body: Optional[dict[str, Any]] = None
    auth_token: Optional[str] = None
    client_ip: Optional[str] = None


@dataclass
class ResponseData:
    status_code: int
    headers: dict[str, str]
    body: Any


class APIGatewayAction:
    """API gateway with routing, auth, and rate limiting.

    Args:
        base_path: Base path for all routes.
        enable_cors: Enable CORS support.
        default_timeout: Default request timeout.
    """

    def __init__(
        self,
        base_path: str = "/api",
        enable_cors: bool = True,
        default_timeout: float = 30.0,
    ) -> None:
        self._routes: list[Route] = []
        self._base_path = base_path
        self._enable_cors = enable_cors
        self._default_timeout = default_timeout
        self._api_keys: dict[str, dict[str, Any]] = {}
        self._middleware: list[Callable] = []

    def register_route(
        self,
        path: str,
        method: str,
        handler: Callable,
        auth_type: AuthType = AuthType.NONE,
        rate_limit: Optional[float] = None,
    ) -> bool:
        """Register an API route.

        Args:
            path: Route path.
            method: HTTP method.
            handler: Handler function.
            auth_type: Authentication type.
            rate_limit: Optional rate limit (requests/second).

        Returns:
            True if registered successfully.
        """
        full_path = f"{self._base_path.rstrip('/')}/{path.lstrip('/')}"
        route = Route(
            path=full_path,
            method=method.upper(),
            handler=handler,
            auth_type=auth_type,
            rate_limit=rate_limit,
        )
        self._routes.append(route)
        logger.debug(f"Registered route: {method} {full_path}")
        return True

    def register_api_key(
        self,
        key: str,
        client_name: str,
        permissions: Optional[list[str]] = None,
        rate_limit: Optional[float] = None,
    ) -> None:
        """Register an API key.

        Args:
            key: API key string.
            client_name: Client name.
            permissions: List of permissions.
            rate_limit: Rate limit for this key.
        """
        self._api_keys[key] = {
            "client_name": client_name,
            "permissions": permissions or [],
            "rate_limit": rate_limit,
            "created_at": time.time(),
        }

    def authenticate_request(
        self,
        context: RequestContext,
        auth_type: AuthType,
    ) -> Optional[str]:
        """Authenticate an API request.

        Args:
            context: Request context.
            auth_type: Authentication type.

        Returns:
            Client ID if authenticated, None otherwise.
        """
        if auth_type == AuthType.NONE:
            return "anonymous"

        if auth_type == AuthType.API_KEY:
            api_key = context.headers.get("X-API-Key") or context.query_params.get("api_key")
            if api_key and api_key in self._api_keys:
                return api_key
            return None

        if auth_type == AuthType.BEARER:
            auth_header = context.headers.get("Authorization", "")
            if auth_header.startswith("Bearer "):
                token = auth_header[7:]
                for key, data in self._api_keys.items():
                    if self._validate_bearer_token(token, key):
                        return key
            return None

        if auth_type == AuthType.HMAC:
            auth_header = context.headers.get("Authorization", "")
            if auth_header.startswith("HMAC "):
                parts = auth_header[5:].split(":")
                if len(parts) == 2:
                    key, signature = parts
                    if key in self._api_keys and self._validate_hmac_signature(context, key, signature):
                        return key
            return None

        return None

    def _validate_bearer_token(self, token: str, key: str) -> bool:
        """Validate a bearer token.

        Args:
            token: Bearer token.
            key: API key.

        Returns:
            True if valid.
        """
        expected = hashlib.sha256(f"{key}:{time.time() // 3600}".encode()).hexdigest()
        return token == expected

    def _validate_hmac_signature(
        self,
        context: RequestContext,
        key: str,
        signature: str,
    ) -> bool:
        """Validate HMAC signature.

        Args:
            context: Request context.
            key: API key.
            signature: Provided signature.

        Returns:
            True if valid.
        """
        message = f"{context.method}:{context.path}:{json.dumps(context.body or {}, sort_keys=True)}"
        expected = hmac.new(key.encode(), message.encode(), hashlib.sha256).hexdigest()
        return hmac.compare_digest(signature, expected)

    def route_request(self, context: RequestContext) -> ResponseData:
        """Route and handle an API request.

        Args:
            context: Request context.

        Returns:
            Response data.
        """
        for route in self._routes:
            if route.path == context.path and route.method == context.method:
                for mw in self._middleware:
                    result = mw(context)
                    if result is not None:
                        return result

                client_id = self.authenticate_request(context, route.auth_type)
                if client_id is None:
                    return ResponseData(
                        status_code=401,
                        headers=self._get_cors_headers(),
                        body={"error": "Unauthorized"},
                    )

                try:
                    result = route.handler(context)
                    return ResponseData(
                        status_code=200,
                        headers=self._get_cors_headers(),
                        body=result,
                    )
                except Exception as e:
                    logger.error(f"Route handler error: {e}")
                    return ResponseData(
                        status_code=500,
                        headers=self._get_cors_headers(),
                        body={"error": "Internal server error"},
                    )

        return ResponseData(
            status_code=404,
            headers=self._get_cors_headers(),
            body={"error": "Not found"},
        )

    def _get_cors_headers(self) -> dict[str, str]:
        """Get CORS headers.

        Returns:
            CORS headers dictionary.
        """
        if not self._enable_cors:
            return {}
        return {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization, X-API-Key",
        }

    def use_middleware(self, middleware: Callable) -> None:
        """Register middleware.

        Args:
            middleware: Middleware function.
        """
        self._middleware.append(middleware)

    def get_stats(self) -> dict[str, Any]:
        """Get API gateway statistics.

        Returns:
            Dictionary with stats.
        """
        return {
            "total_routes": len(self._routes),
            "registered_api_keys": len(self._api_keys),
            "middleware_count": len(self._middleware),
            "cors_enabled": self._enable_cors,
        }
