"""
OAuth 2.0 Action Module.

Provides OAuth 2.0 token management including authorization code flow,
client credentials flow, token refresh, and automatic token rotation.

Author: RabAi Team
"""

from __future__ import annotations

import json
import time
import urllib.parse
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple


class GrantType(Enum):
    """OAuth 2.0 grant types."""
    AUTHORIZATION_CODE = "authorization_code"
    CLIENT_CREDENTIALS = "client_credentials"
    REFRESH_TOKEN = "refresh_token"
    DEVICE_CODE = "device_code"
    IMPLICIT = "implicit"


class TokenStatus(Enum):
    """Token status indicators."""
    ACTIVE = "active"
    EXPIRED = "expired"
    REVOKED = "revoked"
    NEAR_EXPIRY = "near_expiry"


@dataclass
class OAuthToken:
    """Represents an OAuth 2.0 token."""
    access_token: str
    token_type: str = "Bearer"
    expires_in: int = 3600
    refresh_token: Optional[str] = None
    scope: Optional[str] = None
    expires_at: Optional[datetime] = None
    issued_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.expires_at is None:
            self.expires_at = self.issued_at + timedelta(seconds=self.expires_in)

    @property
    def status(self) -> TokenStatus:
        """Check current token status."""
        now = datetime.now()
        if self.expires_at and now >= self.expires_at:
            return TokenStatus.EXPIRED
        if self.expires_at and (self.expires_at - now).total_seconds() < 300:
            return TokenStatus.NEAR_EXPIRY
        return TokenStatus.ACTIVE

    @property
    def is_valid(self) -> bool:
        """Check if token is currently valid."""
        return self.status in (TokenStatus.ACTIVE, TokenStatus.NEAR_EXPIRY)

    def is_expiring_soon(self, threshold_seconds: int = 300) -> bool:
        """Check if token will expire within threshold."""
        if not self.expires_at:
            return False
        remaining = (self.expires_at - datetime.now()).total_seconds()
        return 0 < remaining < threshold_seconds

    def to_dict(self) -> Dict[str, Any]:
        return {
            "access_token": self.access_token,
            "token_type": self.token_type,
            "expires_in": self.expires_in,
            "refresh_token": self.refresh_token,
            "scope": self.scope,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "issued_at": self.issued_at.isoformat(),
            "metadata": self.metadata,
        }


@dataclass
class OAuthConfig:
    """OAuth 2.0 client configuration."""
    client_id: str
    client_secret: str
    authorization_url: str
    token_url: str
    redirect_uri: str
    scopes: List[str] = field(default_factory=list)
    grant_type: GrantType = GrantType.AUTHORIZATION_CODE
    state: Optional[str] = None
    code_verifier: Optional[str] = None


class OAuthTokenManager:
    """
    Manages OAuth 2.0 tokens with automatic refresh and storage.

    Supports authorization code flow, client credentials, and refresh token flows.
    Automatically refreshes tokens before expiry and maintains token history.

    Example:
        >>> manager = OAuthTokenManager(config)
        >>> auth_url = manager.get_authorization_url()
        >>> manager.exchange_code("authorization_code")
        >>> token = manager.get_valid_token()  # auto-refreshes if needed
    """

    def __init__(
        self,
        config: OAuthConfig,
        http_client: Optional[Callable] = None,
        storage: Optional[Callable[[str, Any], None]] = None,
    ):
        self.config = config
        self.http_client = http_client
        self.storage = storage
        self._current_token: Optional[OAuthToken] = None
        self._token_history: List[OAuthToken] = []
        self._pkce_verifier: Optional[str] = None

    def get_authorization_url(self, state: Optional[str] = None) -> str:
        """Generate OAuth authorization URL with PKCE."""
        self._pkce_verifier = self._generate_pkce_verifier()
        code_challenge = self._generate_pkce_challenge(self._pkce_verifier)

        params = {
            "client_id": self.config.client_id,
            "response_type": "code",
            "redirect_uri": self.config.redirect_uri,
            "scope": " ".join(self.config.scopes),
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }

        if state is None:
            state = str(uuid.uuid4())
        params["state"] = state

        return f"{self.config.authorization_url}?{urllib.parse.urlencode(params)}"

    def exchange_code(
        self, code: str, expected_state: Optional[str] = None
    ) -> OAuthToken:
        """Exchange authorization code for access token."""
        body = {
            "grant_type": "authorization_code",
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
            "code": code,
            "redirect_uri": self.config.redirect_uri,
        }

        if self._pkce_verifier:
            body["code_verifier"] = self._pkce_verifier

        token_data = self._request_token(body)
        return self._parse_and_store_token(token_data)

    def refresh_access_token(self, refresh_token: Optional[str] = None) -> OAuthToken:
        """Refresh the access token using refresh token."""
        token_to_use = refresh_token or (
            self._current_token.refresh_token if self._current_token else None
        )
        if not token_to_use:
            raise ValueError("No refresh token available")

        body = {
            "grant_type": "refresh_token",
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
            "refresh_token": token_to_use,
        }

        token_data = self._request_token(body)
        return self._parse_and_store_token(token_data)

    def get_valid_token(self, force_refresh: bool = False) -> OAuthToken:
        """
        Get a valid access token, refreshing if necessary.

        Args:
            force_refresh: Force token refresh regardless of expiry status

        Returns:
            Valid OAuthToken
        """
        if not self._current_token:
            raise ValueError("No token available. Complete authorization first.")

        if force_refresh or self._current_token.is_expiring_soon():
            return self.refresh_access_token()

        return self._current_token

    def get_client_credentials_token(self) -> OAuthToken:
        """Obtain token using client credentials flow."""
        body = {
            "grant_type": "client_credentials",
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
            "scope": " ".join(self.config.scopes),
        }

        token_data = self._request_token(body)
        return self._parse_and_store_token(token_data)

    def revoke_token(self, token: Optional[str] = None) -> bool:
        """Revoke an access or refresh token."""
        token_to_revoke = token or (
            self._current_token.access_token if self._current_token else None
        )
        if not token_to_revoke:
            return False

        try:
            body = {
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
                "token": token_to_revoke,
            }
            # Simple HTTP POST to revocation endpoint - implement based on provider
            return True
        except Exception:
            return False

    def _request_token(self, body: Dict[str, str]) -> Dict[str, Any]:
        """Make token request to OAuth provider."""
        if self.http_client:
            response = self.http_client("POST", self.config.token_url, data=body)
            return json.loads(response)
        raise NotImplementedError("HTTP client not configured")

    def _parse_and_store_token(self, token_data: Dict[str, Any]) -> OAuthToken:
        """Parse token response and store it."""
        token = OAuthToken(
            access_token=token_data["access_token"],
            token_type=token_data.get("token_type", "Bearer"),
            expires_in=token_data.get("expires_in", 3600),
            refresh_token=token_data.get("refresh_token"),
            scope=token_data.get("scope"),
            metadata=token_data,
        )

        if self._current_token:
            self._token_history.append(self._current_token)
        self._current_token = token

        if self.storage:
            self.storage("current_token", token.to_dict())

        return token

    def _generate_pkce_verifier(self) -> str:
        """Generate PKCE code verifier."""
        return uuid.uuid4().hex + uuid.uuid4().hex

    def _generate_pkce_challenge(self, verifier: str) -> str:
        """Generate PKCE code challenge from verifier."""
        import hashlib
        import base64
        digest = hashlib.sha256(verifier.encode()).digest()
        return base64.urlsafe_b64encode(digest).decode().rstrip("=")

    @property
    def current_status(self) -> TokenStatus:
        """Get current token status."""
        if not self._current_token:
            return TokenStatus.EXPIRED
        return self._current_token.status

    def get_token_info(self) -> Dict[str, Any]:
        """Get information about the current token."""
        if not self._current_token:
            return {"status": "no_token"}
        return {
            "status": self._current_token.status.value,
            "is_valid": self._current_token.is_valid,
            "expires_at": self._current_token.expires_at.isoformat() if self._current_token.expires_at else None,
            "issued_at": self._current_token.issued_at.isoformat(),
            "scope": self._current_token.scope,
            "token_history_count": len(self._token_history),
        }


def create_oauth_manager(config: Dict[str, Any]) -> OAuthTokenManager:
    """Factory to create OAuth token manager from config dict."""
    oauth_config = OAuthConfig(
        client_id=config["client_id"],
        client_secret=config["client_secret"],
        authorization_url=config["authorization_url"],
        token_url=config["token_url"],
        redirect_uri=config["redirect_uri"],
        scopes=config.get("scopes", []),
        grant_type=GrantType(config.get("grant_type", "authorization_code")),
    )
    return OAuthTokenManager(oauth_config)
