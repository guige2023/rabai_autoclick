"""API Authentication Action Module.

Provides comprehensive API authentication support including OAuth 2.0,
API Key, Bearer Token, Basic Auth, and JWT authentication schemes.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class AuthType(Enum):
    """Supported authentication types."""
    NONE = "none"
    API_KEY = "api_key"
    BEARER_TOKEN = "bearer_token"
    BASIC_AUTH = "basic_auth"
    OAUTH2_PASSWORD = "oauth2_password"
    OAUTH2_CLIENT_CREDENTIALS = "oauth2_client_credentials"
    OAUTH2_REFRESH_TOKEN = "oauth2_refresh_token"
    JWT = "jwt"
    HMAC_SIGNATURE = "hmac_signature"


class APIKeyLocation(Enum):
    """Where to place the API key in the request."""
    HEADER = "header"
    QUERY_PARAM = "query_param"
    BODY = "body"


@dataclass
class OAuth2Token:
    """OAuth2 token storage."""
    access_token: str
    token_type: str = "Bearer"
    expires_at: float = 0.0
    refresh_token: Optional[str] = None
    scope: Optional[str] = None

    def is_expired(self, buffer_seconds: float = 60.0) -> bool:
        """Check if token is expired or about to expire."""
        return time.time() >= (self.expires_at - buffer_seconds)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize token to dictionary."""
        return {
            "access_token": self.access_token,
            "token_type": self.token_type,
            "expires_at": self.expires_at,
            "refresh_token": self.refresh_token,
            "scope": self.scope,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> OAuth2Token:
        """Deserialize token from dictionary."""
        return cls(
            access_token=data["access_token"],
            token_type=data.get("token_type", "Bearer"),
            expires_at=data.get("expires_at", 0.0),
            refresh_token=data.get("refresh_token"),
            scope=data.get("scope"),
        )


@dataclass
class AuthCredentials:
    """Authentication credentials container."""
    auth_type: AuthType
    # API Key
    api_key: Optional[str] = None
    api_key_name: Optional[str] = None
    api_key_location: APIKeyLocation = APIKeyLocation.HEADER
    # Basic Auth
    username: Optional[str] = None
    password: Optional[str] = None
    # OAuth2
    oauth2_token_url: Optional[str] = None
    oauth2_client_id: Optional[str] = None
    oauth2_client_secret: Optional[str] = None
    oauth2_scope: Optional[str] = None
    # JWT
    jwt_secret: Optional[str] = None
    jwt_algorithm: str = "HS256"
    jwt_claims: Optional[Dict[str, Any]] = None
    # HMAC
    hmac_secret: Optional[str] = None
    hmac_algorithm: str = "sha256"
    # Token storage
    cached_token: Optional[OAuth2Token] = None
    # Custom headers
    extra_headers: Optional[Dict[str, str]] = None


class APITokenManager:
    """Manages API tokens with caching and refresh."""

    def __init__(self, credentials: AuthCredentials):
        self.credentials = credentials
        self._token_cache: Dict[str, OAuth2Token] = {}

    def get_token(self, force_refresh: bool = False) -> Optional[OAuth2Token]:
        """Get current valid token, refreshing if needed."""
        cache_key = self._get_cache_key()
        token = self._token_cache.get(cache_key)

        if token is None or token.is_expired() or force_refresh:
            token = self._refresh_oauth2_token()
            if token:
                self._token_cache[cache_key] = token

        return token

    def _get_cache_key(self) -> str:
        """Generate cache key for credentials."""
        auth = self.credentials
        key_parts = [
            str(auth.auth_type.value),
            auth.oauth2_client_id or "",
            auth.oauth2_token_url or "",
        ]
        return hashlib.sha256("|".join(key_parts).encode()).hexdigest()

    def _refresh_oauth2_token(self) -> Optional[OAuth2Token]:
        """Refresh OAuth2 token from provider."""
        import urllib.request
        import urllib.parse

        auth = self.credentials
        if not auth.oauth2_token_url:
            return None

        try:
            data: Dict[str, str] = {
                "grant_type": "client_credentials",
                "client_id": auth.oauth2_client_id or "",
                "client_secret": auth.oauth2_client_secret or "",
            }

            if auth.oauth2_scope:
                data["scope"] = auth.oauth2_scope

            body = urllib.parse.urlencode(data).encode("utf-8")
            req = urllib.request.Request(
                auth.oauth2_token_url,
                data=body,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))

            expires_in = result.get("expires_in", 3600)
            return OAuth2Token(
                access_token=result["access_token"],
                token_type=result.get("token_type", "Bearer"),
                expires_at=time.time() + expires_in,
                refresh_token=result.get("refresh_token"),
                scope=result.get("scope"),
            )
        except Exception as e:
            logger.error(f"Token refresh failed: {e}")
            return None

    def clear_cache(self) -> None:
        """Clear token cache."""
        self._token_cache.clear()


class APIAuthAction(BaseAction):
    """API Authentication Action supporting multiple auth schemes.

    Examples:
        >>> action = APIAuthAction()
        >>> creds = AuthCredentials(
        ...     auth_type=AuthType.BEARER_TOKEN,
        ...     api_key="my-secret-token"
        ... )
        >>> result = action.execute(ctx, {
        ...     "credentials": creds,
        ...     "url": "https://api.example.com/data",
        ... })
    """

    action_type = "api_auth"
    display_name = "API认证"
    description = "支持OAuth2/API Key/Bearer Token/JWT等多种认证方式"

    def __init__(self):
        super().__init__()
        self._token_managers: Dict[str, APITokenManager] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute API authentication and return auth headers.

        Args:
            context: Execution context.
            params: Dict with keys:
                - credentials: AuthCredentials instance or dict
                - url: Target URL (optional, for validation)
                - auth_type_override: Override auth type (optional)
                - include_query_params: Add auth to query params (bool)

        Returns:
            ActionResult with auth_headers and credentials info.
        """
        try:
            credentials = self._resolve_credentials(params.get("credentials"))
            if credentials is None:
                return ActionResult(
                    success=False,
                    message="Missing or invalid credentials"
                )

            auth_type = credentials.auth_type
            auth_headers, auth_params = self._build_auth(credentials, params)

            # Store token manager if OAuth2
            if auth_type in (AuthType.OAUTH2_PASSWORD,
                           AuthType.OAUTH2_CLIENT_CREDENTIALS,
                           AuthType.OAUTH2_REFRESH_TOKEN):
                cache_key = self._get_manager_cache_key(credentials)
                if cache_key not in self._token_managers:
                    self._token_managers[cache_key] = APITokenManager(credentials)

            return ActionResult(
                success=True,
                message=f"Authentication prepared: {auth_type.value}",
                data={
                    "auth_headers": auth_headers,
                    "auth_params": auth_params,
                    "auth_type": auth_type.value,
                    "has_token": bool(credentials.cached_token),
                }
            )

        except Exception as e:
            logger.exception("API auth action failed")
            return ActionResult(
                success=False,
                message=f"Authentication error: {str(e)}"
            )

    def _resolve_credentials(self, creds: Any) -> Optional[AuthCredentials]:
        """Resolve credentials from various input formats."""
        if creds is None:
            return None
        if isinstance(creds, AuthCredentials):
            return creds
        if isinstance(creds, dict):
            try:
                auth_type = AuthType(creds.get("auth_type", "none"))
                return AuthCredentials(
                    auth_type=auth_type,
                    api_key=creds.get("api_key"),
                    api_key_name=creds.get("api_key_name", "X-API-Key"),
                    api_key_location=APIKeyLocation(
                        creds.get("api_key_location", "header")
                    ),
                    username=creds.get("username"),
                    password=creds.get("password"),
                    oauth2_token_url=creds.get("oauth2_token_url"),
                    oauth2_client_id=creds.get("oauth2_client_id"),
                    oauth2_client_secret=creds.get("oauth2_client_secret"),
                    oauth2_scope=creds.get("oauth2_scope"),
                    jwt_secret=creds.get("jwt_secret"),
                    jwt_algorithm=creds.get("jwt_algorithm", "HS256"),
                    jwt_claims=creds.get("jwt_claims"),
                    hmac_secret=creds.get("hmac_secret"),
                    hmac_algorithm=creds.get("hmac_algorithm", "sha256"),
                    extra_headers=creds.get("extra_headers"),
                )
            except (ValueError, KeyError) as e:
                logger.error(f"Invalid credentials dict: {e}")
                return None
        return None

    def _build_auth(
        self, credentials: AuthCredentials, params: Dict[str, Any]
    ) -> Tuple[Dict[str, str], Dict[str, str]]:
        """Build authentication headers and query params."""
        headers: Dict[str, str] = {}
        query_params: Dict[str, str] = {}

        if credentials.extra_headers:
            headers.update(credentials.extra_headers)

        auth_type = credentials.auth_type

        if auth_type == AuthType.API_KEY:
            key_name = credentials.api_key_name or "X-API-Key"
            key_value = credentials.api_key or ""
            if credentials.api_key_location == APIKeyLocation.HEADER:
                headers[key_name] = key_value
            elif credentials.api_key_location == APIKeyLocation.QUERY_PARAM:
                query_params[key_name] = key_value
            else:
                pass  # Body handled separately

        elif auth_type == AuthType.BEARER_TOKEN:
            token = credentials.api_key or ""
            headers["Authorization"] = f"Bearer {token}"

        elif auth_type == AuthType.BASIC_AUTH:
            import base64
            credentials_b64 = base64.b64encode(
                f"{credentials.username}:{credentials.password}".encode()
            ).decode()
            headers["Authorization"] = f"Basic {credentials_b64}"

        elif auth_type in (AuthType.OAUTH2_PASSWORD, AuthType.OAUTH2_CLIENT_CREDENTIALS,
                          AuthType.OAUTH2_REFRESH_TOKEN):
            cache_key = self._get_manager_cache_key(credentials)
            manager = self._token_managers.get(cache_key)
            if manager:
                token = manager.get_token()
                if token:
                    headers["Authorization"] = f"{token.token_type} {token.access_token}"

        elif auth_type == AuthType.JWT:
            jwt_token = self._generate_jwt(credentials)
            if jwt_token:
                headers["Authorization"] = f"Bearer {jwt_token}"

        elif auth_type == AuthType.HMAC_SIGNATURE:
            url = params.get("url", "")
            signature = self._generate_hmac_signature(credentials, url)
            headers["X-HMAC-Signature"] = signature
            headers["X-Timestamp"] = str(int(time.time()))

        return headers, query_params

    def _generate_jwt(self, credentials: AuthCredentials) -> Optional[str]:
        """Generate a JWT token."""
        import jwt

        if not credentials.jwt_secret:
            return None

        payload = dict(credentials.jwt_claims) if credentials.jwt_claims else {}
        payload["exp"] = int(time.time()) + 3600
        payload["iat"] = int(time.time())

        try:
            return jwt.encode(
                payload,
                credentials.jwt_secret,
                algorithm=credentials.jwt_algorithm,
            )
        except Exception as e:
            logger.error(f"JWT generation failed: {e}")
            return None

    def _generate_hmac_signature(
        self, credentials: AuthCredentials, message: str
    ) -> Optional[str]:
        """Generate HMAC signature for request."""
        import hmac
        import hashlib

        if not credentials.hmac_secret:
            return None

        algo = getattr(hashlib, credentials.hmac_algorithm, hashlib.sha256)
        signature = hmac.new(
            credentials.hmac_secret.encode(),
            message.encode(),
            algo,
        ).hexdigest()
        return signature

    def _get_manager_cache_key(self, credentials: AuthCredentials) -> str:
        """Get cache key for token manager."""
        return hashlib.sha256(
            f"{credentials.oauth2_client_id}:{credentials.oauth2_token_url}".encode()
        ).hexdigest()

    def get_required_params(self) -> List[str]:
        return ["credentials"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "url": "",
            "auth_type_override": None,
            "include_query_params": False,
        }
