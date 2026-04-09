"""API authentication and authorization action."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Optional
import hashlib
import hmac
import secrets


class AuthType(str, Enum):
    """Authentication type."""

    NONE = "none"
    API_KEY = "api_key"
    BEARER_TOKEN = "bearer_token"
    BASIC = "basic"
    HMAC_SIGNATURE = "hmac_signature"
    OAUTH2 = "oauth2"
    JWT = "jwt"


@dataclass
class AuthConfig:
    """Authentication configuration."""

    auth_type: AuthType
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    bearer_token: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    jwt_secret: Optional[str] = None
    jwt_algorithm: str = "HS256"
    token_expiry_seconds: int = 3600
    scopes: list[str] = field(default_factory=list)


@dataclass
class AuthToken:
    """Authentication token."""

    token: str
    token_type: str
    expires_at: datetime
    scopes: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class AuthResult:
    """Result of an authentication attempt."""

    success: bool
    token: Optional[AuthToken] = None
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)


class APITokenManager:
    """Manages API tokens and authentication state."""

    def __init__(self, config: AuthConfig):
        """Initialize token manager.

        Args:
            config: Authentication configuration.
        """
        self._config = config
        self._current_token: Optional[AuthToken] = None
        self._refresh_callbacks: list[Callable[[], AuthToken]] = []

    def set_token(self, token: AuthToken) -> None:
        """Set the current authentication token."""
        self._current_token = token

    def get_token(self) -> Optional[AuthToken]:
        """Get the current token if valid."""
        if self._current_token and self._current_token.expires_at > datetime.now():
            return self._current_token
        return None

    def is_token_valid(self) -> bool:
        """Check if current token is valid."""
        return self.get_token() is not None

    def invalidate_token(self) -> None:
        """Invalidate the current token."""
        self._current_token = None


class APIAuthAction:
    """Handles API authentication workflows."""

    def __init__(self, default_config: Optional[AuthConfig] = None):
        """Initialize auth action.

        Args:
            default_config: Default auth configuration.
        """
        self._default_config = default_config
        self._token_managers: dict[str, APITokenManager] = {}
        self._api_keys: dict[str, str] = {}
        self._on_auth_success: Optional[Callable[[str, AuthToken], None]] = None
        self._on_auth_failure: Optional[Callable[[str, str], None]] = None

    def register_api_key(self, client_id: str, api_key: str) -> None:
        """Register an API key for a client."""
        self._api_keys[client_id] = api_key

    def get_token_manager(self, client_id: str, config: Optional[AuthConfig] = None) -> APITokenManager:
        """Get or create a token manager for a client."""
        if client_id not in self._token_managers:
            cfg = config or self._default_config
            if not cfg:
                raise ValueError(f"No auth config provided for client {client_id}")
            self._token_managers[client_id] = APITokenManager(cfg)
        return self._token_managers[client_id]

    async def authenticate_api_key(
        self,
        client_id: str,
        api_key: Optional[str] = None,
    ) -> AuthResult:
        """Authenticate using API key."""
        key = api_key or self._api_keys.get(client_id)
        if not key:
            return AuthResult(success=False, error="No API key provided")

        try:
            token = AuthToken(
                token=key,
                token_type="api_key",
                expires_at=datetime.now() + timedelta(days=365),
                scopes=[],
            )
            manager = self.get_token_manager(client_id)
            manager.set_token(token)

            if self._on_auth_success:
                self._on_auth_success(client_id, token)

            return AuthResult(success=True, token=token)
        except Exception as e:
            if self._on_auth_failure:
                self._on_auth_failure(client_id, str(e))
            return AuthResult(success=False, error=str(e))

    async def authenticate_bearer_token(
        self,
        client_id: str,
        token: str,
        expiry_seconds: int = 3600,
    ) -> AuthResult:
        """Authenticate using bearer token."""
        try:
            auth_token = AuthToken(
                token=token,
                token_type="bearer",
                expires_at=datetime.now() + timedelta(seconds=expiry_seconds),
                scopes=[],
            )
            manager = self.get_token_manager(client_id)
            manager.set_token(auth_token)

            if self._on_auth_success:
                self._on_auth_success(client_id, auth_token)

            return AuthResult(success=True, token=auth_token)
        except Exception as e:
            if self._on_auth_failure:
                self._on_auth_failure(client_id, str(e))
            return AuthResult(success=False, error=str(e))

    async def authenticate_jwt(
        self,
        client_id: str,
        claims: dict[str, Any],
        secret: Optional[str] = None,
        algorithm: str = "HS256",
        expiry_seconds: int = 3600,
    ) -> AuthResult:
        """Create and authenticate using JWT token."""
        import base64
        import json

        secret = secret or self._default_config.jwt_secret or "default-secret"

        header = {"alg": algorithm, "typ": "JWT"}
        payload = {
            **claims,
            "sub": client_id,
            "iat": datetime.now().timestamp(),
            "exp": (datetime.now() + timedelta(seconds=expiry_seconds)).timestamp(),
        }

        def b64_encode(data: dict) -> str:
            return base64.urlsafe_b64encode(json.dumps(data).encode()).decode().rstrip("=")

        header_b64 = b64_encode(header)
        payload_b64 = b64_encode(payload)

        signature_input = f"{header_b64}.{payload_b64}"
        signature = hmac.new(
            secret.encode(),
            signature_input.encode(),
            hashlib.sha256,
        ).hexdigest()

        token_str = f"{signature_input}.{signature}"

        try:
            token = AuthToken(
                token=token_str,
                token_type="jwt",
                expires_at=datetime.now() + timedelta(seconds=expiry_seconds),
                scopes=payload.get("scopes", []),
                metadata={"claims": claims},
            )
            manager = self.get_token_manager(client_id)
            manager.set_token(token)

            if self._on_auth_success:
                self._on_auth_success(client_id, token)

            return AuthResult(success=True, token=token)
        except Exception as e:
            if self._on_auth_failure:
                self._on_auth_failure(client_id, str(e))
            return AuthResult(success=False, error=str(e))

    def verify_hmac_signature(
        self,
        payload: bytes,
        signature: str,
        secret: Optional[str] = None,
    ) -> bool:
        """Verify HMAC signature."""
        secret = secret or self._default_config.api_secret or ""
        expected = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, signature)

    def generate_api_key(self, prefix: str = "sk") -> str:
        """Generate a new API key."""
        random_bytes = secrets.token_bytes(32)
        return f"{prefix}_{random_bytes.hex()}"

    def get_auth_headers(self, client_id: str) -> dict[str, str]:
        """Get authorization headers for a client."""
        manager = self._token_managers.get(client_id)
        token = manager.get_token() if manager else None

        if not token:
            return {}

        if token.token_type == "bearer":
            return {"Authorization": f"Bearer {token.token}"}
        elif token.token_type == "api_key":
            return {"X-API-Key": token.token}
        elif token.token_type == "jwt":
            return {"Authorization": f"Bearer {token.token}"}

        return {}
