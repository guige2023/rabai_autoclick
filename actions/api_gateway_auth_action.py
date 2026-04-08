"""
API Gateway Auth Middleware Action Module.

Middleware for API gateway authentication and authorization:
JWT validation, scope checking, rate limiting by client, and API key management.
"""
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class AuthContext:
    """Authentication context extracted from request."""
    client_id: Optional[str]
    scopes: list[str]
    user_id: Optional[str]
    authenticated: bool
    error: Optional[str] = None


@dataclass
class MiddlewareResult:
    """Result of middleware processing."""
    allowed: bool
    context: AuthContext
    modified_request: dict[str, Any]


class APIGatewayAuthAction(BaseAction):
    """Authentication middleware for API gateway."""

    def __init__(self) -> None:
        super().__init__("api_gateway_auth")
        self._valid_tokens: dict[str, dict[str, Any]] = {}
        self._api_keys: dict[str, dict[str, Any]] = {}
        self._scopes: dict[str, list[str]] = {}

    def execute(self, context: dict, params: dict) -> dict:
        """
        Validate authentication and authorization.

        Args:
            context: Execution context
            params: Parameters:
                - request: Incoming request dict
                - required_scopes: List of required OAuth scopes
                - validate_jwt: Validate JWT tokens (default: True)
                - validate_api_key: Validate API keys (default: True)
                - rate_limit: Enable rate limiting (default: False)
                - rate_limit_requests: Max requests per window
                - rate_limit_window: Time window in seconds

        Returns:
            MiddlewareResult with auth context and decision
        """
        import time
        import json
        import base64
        import hmac
        import hashlib

        request = params.get("request", {})
        required_scopes = params.get("required_scopes", [])
        validate_jwt = params.get("validate_jwt", True)
        validate_api_key = params.get("validate_api_key", True)
        rate_limit = params.get("rate_limit", False)
        rate_limit_requests = params.get("rate_limit_requests", 100)
        rate_limit_window = params.get("rate_limit_window", 60)

        headers = request.get("headers", {})
        auth_header = headers.get("Authorization", "")
        api_key_header = headers.get("X-API-Key", "")

        auth_context = AuthContext(
            client_id=None,
            scopes=[],
            user_id=None,
            authenticated=False
        )

        if validate_api_key and api_key_header:
            if api_key_header in self._api_keys:
                key_data = self._api_keys[api_key_header]
                if key_data.get("expires_at", float("inf")) < time.time():
                    auth_context.error = "API key expired"
                    return MiddlewareResult(False, auth_context, request)

                auth_context.client_id = key_data.get("client_id")
                auth_context.scopes = key_data.get("scopes", [])
                auth_context.authenticated = True
                if rate_limit:
                    if not self._check_rate_limit(auth_context.client_id, rate_limit_requests, rate_limit_window):
                        auth_context.error = "Rate limit exceeded"
                        return MiddlewareResult(False, auth_context, request)
                return MiddlewareResult(True, auth_context, request)
            else:
                auth_context.error = "Invalid API key"
                return MiddlewareResult(False, auth_context, request)

        if validate_jwt and auth_header.startswith("Bearer "):
            token = auth_header[7:]
            try:
                parts = token.split(".")
                if len(parts) != 3:
                    raise ValueError("Invalid JWT format")
                header_json = json.loads(base64.urlsafe_b64decode(parts[0] + "=="))
                payload_json = json.loads(base64.urlsafe_b64decode(parts[1] + "=="))

                if payload_json.get("exp", 0) < time.time():
                    auth_context.error = "Token expired"
                    return MiddlewareResult(False, auth_context, request)

                auth_context.client_id = payload_json.get("client_id")
                auth_context.user_id = payload_json.get("sub")
                auth_context.scopes = payload_json.get("scope", "").split()
                auth_context.authenticated = True

                if rate_limit:
                    if not self._check_rate_limit(auth_context.client_id, rate_limit_requests, rate_limit_window):
                        auth_context.error = "Rate limit exceeded"
                        return MiddlewareResult(False, auth_context, request)

                if required_scopes:
                    missing = [s for s in required_scopes if s not in auth_context.scopes]
                    if missing:
                        auth_context.error = f"Missing scopes: {missing}"
                        return MiddlewareResult(False, auth_context, request)

                return MiddlewareResult(True, auth_context, request)
            except Exception as e:
                auth_context.error = f"JWT validation failed: {str(e)}"
                return MiddlewareResult(False, auth_context, request)

        auth_context.error = "No valid authentication provided"
        return MiddlewareResult(False, auth_context, request)

    def _check_rate_limit(self, client_id: str, max_requests: int, window: int) -> bool:
        """Check if client is within rate limit."""
        import time
        if not hasattr(self, "_rate_limit_data"):
            self._rate_limit_data: dict[str, list[float]] = {}
        now = time.time()
        if client_id not in self._rate_limit_data:
            self._rate_limit_data[client_id] = []
        self._rate_limit_data[client_id] = [t for t in self._rate_limit_data[client_id] if now - t < window]
        if len(self._rate_limit_data[client_id]) >= max_requests:
            return False
        self._rate_limit_data[client_id].append(now)
        return True

    def register_api_key(self, key: str, client_id: str, scopes: list[str], expires_at: float = float("inf")) -> None:
        """Register an API key."""
        self._api_keys[key] = {"client_id": client_id, "scopes": scopes, "expires_at": expires_at}

    def revoke_api_key(self, key: str) -> None:
        """Revoke an API key."""
        self._api_keys.pop(key, None)
