"""
API Auth Action Module.

Handles API authentication: Bearer tokens, API keys, Basic Auth,
OAuth2 flows, JWT generation and validation.
"""
from typing import Any, Optional
from dataclasses import dataclass
from actions.base_action import BaseAction


@dataclass
class AuthResult:
    """Result of authentication."""
    success: bool
    token: Optional[str] = None
    expires_at: Optional[float] = None
    error: Optional[str] = None


@dataclass
class AuthConfig:
    """Authentication configuration."""
    type: str  # bearer, api_key, basic, oauth2, jwt
    credentials: dict[str, str]


class APIAuthAction(BaseAction):
    """Handle API authentication."""

    def __init__(self) -> None:
        super().__init__("api_auth")
        self._token_cache: dict[str, tuple[str, float]] = {}

    def execute(self, context: dict, params: dict) -> dict:
        """
        Perform authentication.

        Args:
            context: Execution context
            params: Parameters:
                - auth_type: bearer, api_key, basic, oauth2, jwt
                - credentials: Dict with required credentials
                - validate: Only validate without generating token
                - cache_token: Cache token for reuse (default: True)

        Returns:
            dict with AuthResult and headers for authenticated requests
        """
        import time
        import json
        import base64

        auth_type = params.get("auth_type", "bearer")
        credentials = params.get("credentials", {})
        validate_only = params.get("validate", False)
        cache_token = params.get("cache_token", True)
        cache_key = params.get("cache_key", "default")

        if auth_type == "bearer":
            token = credentials.get("token", "")
            if not token:
                return {"error": "Bearer token required", "success": False}
            if validate_only:
                return {"valid": True, "success": True}
            headers = {"Authorization": f"Bearer {token}"}
            return {"success": True, "headers": headers, "token": token}

        elif auth_type == "api_key":
            key = credentials.get("api_key", "")
            header_name = credentials.get("header_name", "X-API-Key")
            if not key:
                return {"error": "API key required", "success": False}
            if validate_only:
                return {"valid": True, "success": True}
            headers = {header_name: key}
            return {"success": True, "headers": headers, "token": key}

        elif auth_type == "basic":
            username = credentials.get("username", "")
            password = credentials.get("password", "")
            if not username or not password:
                return {"error": "Username and password required", "success": False}
            if validate_only:
                return {"valid": True, "success": True}
            auth_string = base64.b64encode(f"{username}:{password}".encode()).decode()
            headers = {"Authorization": f"Basic {auth_string}"}
            return {"success": True, "headers": headers, "token": auth_string}

        elif auth_type == "oauth2":
            if cache_token and cache_key in self._token_cache:
                token, expires_at = self._token_cache[cache_key]
                if expires_at > time.time():
                    return {"success": True, "headers": {"Authorization": f"Bearer {token}"}, "token": token, "cached": True}

            grant_type = credentials.get("grant_type", "client_credentials")
            client_id = credentials.get("client_id", "")
            client_secret = credentials.get("client_secret", "")
            token_url = credentials.get("token_url", "")

            if validate_only:
                return {"valid": bool(token_url), "success": True}

            if not all([token_url, client_id, client_secret]):
                return {"error": "OAuth2 credentials incomplete", "success": False}

            result = self._get_oauth2_token(token_url, client_id, client_secret, grant_type)
            if result.get("success"):
                expires_in = result.get("expires_in", 3600)
                expires_at = time.time() + expires_in
                if cache_token:
                    self._token_cache[cache_key] = (result["token"], expires_at)
            return result

        elif auth_type == "jwt":
            secret = credentials.get("secret", "")
            algorithm = credentials.get("algorithm", "HS256")
            payload = credentials.get("payload", {})
            if not secret:
                return {"error": "JWT secret required", "success": False}
            try:
                import hmac
                import hashlib
                import json
                header = base64.urlsafe_b64encode(json.dumps({"alg": algorithm, "typ": "JWT"}).encode()).decode().rstrip("=")
                payload_encoded = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
                signature = hmac.new(secret.encode(), f"{header}.{payload_encoded}".encode(), hashlib.sha256).hexdigest().rstrip("=")
                token = f"{header}.{payload_encoded}.{signature}"
                return {"success": True, "headers": {"Authorization": f"Bearer {token}"}, "token": token}
            except Exception as e:
                return {"error": str(e), "success": False}

        return {"error": f"Unknown auth type: {auth_type}", "success": False}

    def _get_oauth2_token(self, token_url: str, client_id: str, client_secret: str, grant_type: str) -> dict:
        """Get OAuth2 token from token URL."""
        import urllib.request
        import urllib.parse

        data = urllib.parse.urlencode({
            "grant_type": grant_type,
            "client_id": client_id,
            "client_secret": client_secret
        }).encode()

        try:
            req = urllib.request.Request(token_url, data=data, method="POST")
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode())
                return {
                    "success": True,
                    "token": result.get("access_token", ""),
                    "expires_in": result.get("expires_in", 3600),
                    "headers": {"Authorization": f"Bearer {result.get('access_token', '')}"}
                }
        except Exception as e:
            return {"error": str(e), "success": False}
