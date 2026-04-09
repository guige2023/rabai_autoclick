"""OAuth2 authentication action module.

Provides OAuth2 client credentials and authorization code flow support
for API authentication with token refresh capabilities.
"""

from __future__ import annotations

import time
import hashlib
import hmac
import base64
import urllib.parse
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class GrantType(Enum):
    """OAuth2 grant types."""
    CLIENT_CREDENTIALS = "client_credentials"
    AUTHORIZATION_CODE = "authorization_code"
    REFRESH_TOKEN = "refresh_token"


@dataclass
class OAuth2Token:
    """Represents an OAuth2 token response."""
    access_token: str
    token_type: str = "Bearer"
    expires_in: int = 3600
    refresh_token: Optional[str] = None
    scope: Optional[str] = None
    issued_at: float = field(default_factory=time.time)

    @property
    def is_expired(self) -> bool:
        """Check if token is expired."""
        return time.time() >= (self.issued_at + self.expires_in - 60)

    def to_dict(self) -> dict[str, Any]:
        """Convert token to dictionary."""
        return {
            "access_token": self.access_token,
            "token_type": self.token_type,
            "expires_in": self.expires_in,
            "refresh_token": self.refresh_token,
            "scope": self.scope,
        }


class OAuth2Client:
    """OAuth2 client with token management."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token_url: str,
        authorize_url: Optional[str] = None,
        redirect_uri: Optional[str] = None,
    ):
        """Initialize OAuth2 client.

        Args:
            client_id: OAuth2 client ID
            client_secret: OAuth2 client secret
            token_url: Token endpoint URL
            authorize_url: Authorization endpoint URL
            redirect_uri: Redirect URI for authorization code flow
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url
        self.authorize_url = authorize_url
        self.redirect_uri = redirect_uri
        self._token: Optional[OAuth2Token] = None
        self._token_refresh_callbacks: list[Callable[[OAuth2Token], None]] = []

    def set_token(self, token: OAuth2Token) -> None:
        """Set current token."""
        self._token = token
        for callback in self._token_refresh_callbacks:
            callback(token)

    def get_token(self) -> Optional[OAuth2Token]:
        """Get current token."""
        return self._token

    def on_token_refresh(self, callback: Callable[[OAuth2Token], None]) -> None:
        """Register token refresh callback."""
        self._token_refresh_callbacks.append(callback)

    def build_authorization_url(
        self,
        scope: Optional[str] = None,
        state: Optional[str] = None,
        code_challenge: bool = True,
    ) -> tuple[str, Optional[str]]:
        """Build authorization URL for authorization code flow.

        Args:
            scope: Requested scopes
            state: State parameter for CSRF protection
            code_challenge: Whether to use PKCE code challenge

        Returns:
            Tuple of (authorization_url, code_verifier)
        """
        if not self.authorize_url:
            raise ValueError("authorize_url not configured")

        params: dict[str, str] = {
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri or "",
        }

        if scope:
            params["scope"] = scope
        if state:
            params["state"] = state

        code_verifier: Optional[str] = None
        if code_challenge:
            code_verifier = self._generate_code_verifier()
            code_challenge_digest = hashlib.sha256(code_verifier.encode()).digest()
            code_challenge_b64 = base64.urlsafe_b64encode(code_challenge_digest).rstrip(b"=").decode()
            params["code_challenge"] = code_challenge_b64
            params["code_challenge_method"] = "S256"

        auth_url = f"{self.authorize_url}?{urllib.parse.urlencode(params)}"
        return auth_url, code_verifier

    def _generate_code_verifier(self) -> str:
        """Generate PKCE code verifier."""
        random_bytes = __import__("secrets").token_bytes(32)
        return base64.urlsafe_b64encode(random_bytes).rstrip(b"=").decode()

    def exchange_code_for_token(
        self,
        code: str,
        code_verifier: Optional[str] = None,
    ) -> OAuth2Token:
        """Exchange authorization code for access token.

        Args:
            code: Authorization code
            code_verifier: PKCE code verifier

        Returns:
            OAuth2Token object
        """
        data: dict[str, str] = {
            "grant_type": GrantType.AUTHORIZATION_CODE.value,
            "code": code,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        if self.redirect_uri:
            data["redirect_uri"] = self.redirect_uri
        if code_verifier:
            data["code_verifier"] = code_verifier

        response_data = self._make_token_request(data)
        token = self._parse_token_response(response_data)
        self.set_token(token)
        return token

    def refresh_access_token(self, refresh_token: str) -> OAuth2Token:
        """Refresh access token using refresh token.

        Args:
            refresh_token: Refresh token

        Returns:
            New OAuth2Token object
        """
        data: dict[str, str] = {
            "grant_type": GrantType.REFRESH_TOKEN.value,
            "refresh_token": refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        response_data = self._make_token_request(data)
        token = self._parse_token_response(response_data)
        self.set_token(token)
        return token

    def client_credentials_grant(self, scope: Optional[str] = None) -> OAuth2Token:
        """Request token using client credentials grant.

        Args:
            scope: Requested scopes

        Returns:
            OAuth2Token object
        """
        data: dict[str, str] = {
            "grant_type": GrantType.CLIENT_CREDENTIALS.value,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        if scope:
            data["scope"] = scope

        response_data = self._make_token_request(data)
        token = self._parse_token_response(response_data)
        self.set_token(token)
        return token

    def _make_token_request(self, data: dict[str, str]) -> dict[str, Any]:
        """Make token request to authorization server.

        Args:
            data: Request body data

        Returns:
            Parsed JSON response
        """
        import json as json_module

        try:
            import urllib.request
            req = urllib.request.Request(
                self.token_url,
                data=urllib.parse.urlencode(data).encode(),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            with urllib.request.urlopen(req, timeout=30) as response:
                return json_module.loads(response.read().decode())
        except Exception as e:
            logger.error(f"OAuth2 token request failed: {e}")
            raise

    def _parse_token_response(self, data: dict[str, Any]) -> OAuth2Token:
        """Parse token response data."""
        return OAuth2Token(
            access_token=data["access_token"],
            token_type=data.get("token_type", "Bearer"),
            expires_in=data.get("expires_in", 3600),
            refresh_token=data.get("refresh_token"),
            scope=data.get("scope"),
        )

    def get_authorized_header(self) -> dict[str, str]:
        """Get authorization header with current token.

        Returns:
            Dict with Authorization header

        Raises:
            ValueError: If no token available
        """
        if not self._token:
            raise ValueError("No token available. Authenticate first.")
        if self._token.is_expired and self._token.refresh_token:
            self.refresh_access_token(self._token.refresh_token)
        return {"Authorization": f"{self._token.token_type} {self._token.access_token}"}


def create_oauth2_client(
    client_id: str,
    client_secret: str,
    token_url: str,
    authorize_url: Optional[str] = None,
    redirect_uri: Optional[str] = None,
) -> OAuth2Client:
    """Create OAuth2 client instance.

    Args:
        client_id: OAuth2 client ID
        client_secret: OAuth2 client secret
        token_url: Token endpoint URL
        authorize_url: Authorization endpoint URL
        redirect_uri: Redirect URI

    Returns:
        OAuth2Client instance
    """
    return OAuth2Client(
        client_id=client_id,
        client_secret=client_secret,
        token_url=token_url,
        authorize_url=authorize_url,
        redirect_uri=redirect_uri,
    )
