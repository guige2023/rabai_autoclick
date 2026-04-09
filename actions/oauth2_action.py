"""
OAuth 2.0 Client Action Module.

Provides OAuth 2.0 authorization code flow, client credentials,
and refresh token management for API authentication.

Author: rabai_autoclick team
"""

import time
import hashlib
import base64
import json
import logging
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from enum import Enum
from urllib.parse import urlencode, parse_qs, urlparse

logger = logging.getLogger(__name__)


class GrantType(Enum):
    """OAuth 2.0 grant types."""
    AUTHORIZATION_CODE = "authorization_code"
    CLIENT_CREDENTIALS = "client_credentials"
    REFRESH_TOKEN = "refresh_token"
    DEVICE_CODE = "device_code"


@dataclass
class TokenResponse:
    """OAuth 2.0 token response."""
    access_token: str
    token_type: str
    expires_in: int
    refresh_token: Optional[str] = None
    scope: Optional[str] = None
    id_token: Optional[str] = None
    expires_at: float = field(default=0)

    def __post_init__(self):
        if self.expires_at == 0:
            self.expires_at = time.time() + self.expires_in

    @property
    def is_expired(self) -> bool:
        """Check if token is expired."""
        return time.time() >= self.expires_at - 60

    @property
    def is_refresh_needed(self) -> bool:
        """Check if refresh is needed (within 5 min of expiry)."""
        return time.time() >= self.expires_at - 300


@dataclass
class OAuth2Config:
    """OAuth 2.0 configuration."""
    client_id: str
    client_secret: str
    authorization_url: str
    token_url: str
    redirect_uri: str
    scope: str
    state: Optional[str] = None
    timeout: int = 30


class OAuth2Action:
    """
    OAuth 2.0 Client Implementation.

    Supports multiple grant types and automatic token refresh.

    Example:
        >>> config = OAuth2Config(
        ...     client_id="app_id",
        ...     client_secret="secret",
        ...     authorization_url="https://auth.example.com/authorize",
        ...     token_url="https://auth.example.com/token",
        ...     redirect_uri="http://localhost:8080/callback",
        ...     scope="read write"
        ... )
        >>> oauth = OAuth2Action(config)
        >>> auth_url = oauth.get_authorization_url()
        >>> token = oauth.exchange_code("auth_code")
    """

    def __init__(self, config: OAuth2Config, http_client: Optional[Any] = None):
        self.config = config
        self.http_client = http_client
        self._token: Optional[TokenResponse] = None
        self._code_verifier: Optional[str] = None

    def generate_code_verifier(self, length: int = 128) -> str:
        """
        Generate PKCE code verifier.

        Args:
            length: Length of the verifier (43-128 chars)

        Returns:
            Random code verifier string
        """
        if not (43 <= length <= 128):
            raise ValueError("Code verifier length must be 43-128 characters")
        chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
        verifier = "".join(__import__("secrets").choice(chars) for _ in range(length))
        self._code_verifier = verifier
        return verifier

    def generate_code_challenge(self, verifier: str) -> str:
        """
        Generate PKCE code challenge from verifier.

        Args:
            verifier: Code verifier string

        Returns:
            S256 code challenge
        """
        digest = hashlib.sha256(verifier.encode("ascii")).digest()
        return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")

    def get_authorization_url(self, state: Optional[str] = None,
                              code_challenge: Optional[str] = None) -> str:
        """
        Get authorization URL for user to authorize.

        Args:
            state: Optional state parameter
            code_challenge: Optional PKCE code challenge

        Returns:
            Authorization URL
        """
        params = {
            "response_type": "code",
            "client_id": self.config.client_id,
            "redirect_uri": self.config.redirect_uri,
            "scope": self.config.scope,
        }

        if state:
            params["state"] = state
        elif self.config.state:
            params["state"] = self.config.state

        if code_challenge and self._code_verifier:
            params["code_challenge"] = code_challenge
            params["code_challenge_method"] = "S256"

        return f"{self.config.authorization_url}?{urlencode(params)}"

    async def exchange_code(self, code: str,
                           code_verifier: Optional[str] = None) -> TokenResponse:
        """
        Exchange authorization code for tokens.

        Args:
            code: Authorization code
            code_verifier: PKCE code verifier

        Returns:
            Token response
        """
        data = {
            "grant_type": GrantType.AUTHORIZATION_CODE.value,
            "code": code,
            "client_id": self.config.client_id,
            "redirect_uri": self.config.redirect_uri,
        }

        if code_verifier:
            data["code_verifier"] = code_verifier

        return await self._request_token(data)

    async def refresh_access_token(self, refresh_token: str) -> TokenResponse:
        """
        Refresh access token using refresh token.

        Args:
            refresh_token: Refresh token

        Returns:
            New token response
        """
        data = {
            "grant_type": GrantType.REFRESH_TOKEN.value,
            "refresh_token": refresh_token,
            "client_id": self.config.client_id,
        }

        return await self._request_token(data)

    async def client_credentials(self) -> TokenResponse:
        """
        Get access token using client credentials.

        Returns:
            Token response
        """
        data = {
            "grant_type": GrantType.CLIENT_CREDENTIALS.value,
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
            "scope": self.config.scope,
        }

        return await self._request_token(data)

    async def _request_token(self, data: Dict[str, str]) -> TokenResponse:
        """Send token request to authorization server."""
        if self.http_client is None:
            try:
                import httpx
                async with httpx.AsyncClient(timeout=self.config.timeout) as client:
                    response = await client.post(
                        self.config.token_url,
                        data=data,
                        headers={"Content-Type": "application/x-www-form-urlencoded"}
                    )
                    response.raise_for_status()
                    token_data = response.json()
            except ImportError:
                logger.warning("httpx not available, using requests")
                import requests
                resp = requests.post(
                    self.config.token_url,
                    data=data,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=self.config.timeout
                )
                resp.raise_for_status()
                token_data = resp.json()
        else:
            response = await self.http_client.post(
                self.config.token_url,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"}
            )
            token_data = response.json()

        self._token = TokenResponse(
            access_token=token_data["access_token"],
            token_type=token_data.get("token_type", "Bearer"),
            expires_in=token_data.get("expires_in", 3600),
            refresh_token=token_data.get("refresh_token"),
            scope=token_data.get("scope"),
            id_token=token_data.get("id_token"),
        )
        return self._token

    def get_valid_token(self) -> Optional[TokenResponse]:
        """Get current valid token or None."""
        return self._token if self._token and not self._token.is_expired else None

    async def ensure_valid_token(self) -> TokenResponse:
        """
        Ensure valid token is available, refreshing if needed.

        Returns:
            Valid token response
        """
        if self._token is None:
            return await self.client_credentials()

        if self._token.is_expired:
            if self._token.refresh_token:
                return await self.refresh_access_token(self._token.refresh_token)
            return await self.client_credentials()

        return self._token

    def parse_callback(self, url: str) -> Dict[str, Any]:
        """
        Parse OAuth callback URL.

        Args:
            url: Callback URL with query parameters

        Returns:
            Parsed parameters
        """
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        return {k: v[0] if len(v) == 1 else v for k, v in params.items()}

    def validate_state(self, received: str, expected: Optional[str] = None) -> bool:
        """
        Validate state parameter.

        Args:
            received: State received in callback
            expected: Expected state (uses config state if None)

        Returns:
            True if valid
        """
        expected_state = expected or self.config.state
        return received == expected_state if expected_state else True
