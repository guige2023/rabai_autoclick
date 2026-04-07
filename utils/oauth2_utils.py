"""
OAuth2 utilities for authentication and authorization flows.

Provides authorization code, client credentials, refresh token,
and JWT validation helpers.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)


class GrantType(Enum):
    AUTHORIZATION_CODE = "authorization_code"
    CLIENT_CREDENTIALS = "client_credentials"
    REFRESH_TOKEN = "refresh_token"
    PASSWORD = "password"
    DEVICE_CODE = "urn:ietf:params:oauth:grant-type:device_code"


@dataclass
class OAuth2Config:
    """OAuth2 client configuration."""
    client_id: str
    client_secret: str
    authorization_url: str
    token_url: str
    redirect_uri: str
    scope: str = ""
    state: str = ""


@dataclass
class TokenResponse:
    """OAuth2 token response."""
    access_token: str
    token_type: str = "Bearer"
    expires_in: int = 3600
    refresh_token: Optional[str] = None
    scope: str = ""
    id_token: Optional[str] = None
    issued_at: float = field(default_factory=time.time)

    @property
    def is_expired(self) -> bool:
        return time.time() >= (self.issued_at + self.expires_in)

    @property
    def expires_at(self) -> float:
        return self.issued_at + self.expires_in


@dataclass
class UserInfo:
    """OAuth2 user information."""
    sub: str
    name: Optional[str] = None
    email: Optional[str] = None
    email_verified: bool = False
    picture: Optional[str] = None
    locale: str = "en"
    claims: dict[str, Any] = field(default_factory=dict)


class OAuth2Client:
    """OAuth2 client for authorization flows."""

    def __init__(self, config: OAuth2Config) -> None:
        self.config = config
        self._token: Optional[TokenResponse] = None

    def get_authorization_url(self, state: Optional[str] = None) -> str:
        """Build the authorization URL."""
        params = {
            "client_id": self.config.client_id,
            "redirect_uri": self.config.redirect_uri,
            "response_type": "code",
            "scope": self.config.scope,
        }
        if state or self.config.state:
            params["state"] = state or self.config.state

        query = "&".join(f"{k}={v}" for k, v in params.items())
        return f"{self.config.authorization_url}?{query}"

    async def exchange_code(self, code: str) -> Optional[TokenResponse]:
        """Exchange authorization code for tokens."""
        data = {
            "grant_type": GrantType.AUTHORIZATION_CODE.value,
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
            "code": code,
            "redirect_uri": self.config.redirect_uri,
        }

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.config.token_url,
                    data=data,
                    headers={"Accept": "application/json"},
                )
                if response.status_code != 200:
                    logger.error("Token exchange failed: %s", response.text)
                    return None

                json_data = response.json()
                self._token = TokenResponse(
                    access_token=json_data["access_token"],
                    token_type=json_data.get("token_type", "Bearer"),
                    expires_in=json_data.get("expires_in", 3600),
                    refresh_token=json_data.get("refresh_token"),
                    scope=json_data.get("scope", ""),
                    id_token=json_data.get("id_token"),
                )
                return self._token
        except Exception as e:
            logger.error("Token exchange error: %s", e)
            return None

    async def refresh_access_token(self, refresh_token: str) -> Optional[TokenResponse]:
        """Refresh an access token using refresh token."""
        data = {
            "grant_type": GrantType.REFRESH_TOKEN.value,
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
            "refresh_token": refresh_token,
        }

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.config.token_url,
                    data=data,
                    headers={"Accept": "application/json"},
                )
                if response.status_code != 200:
                    logger.error("Token refresh failed: %s", response.text)
                    return None

                json_data = response.json()
                self._token = TokenResponse(
                    access_token=json_data["access_token"],
                    token_type=json_data.get("token_type", "Bearer"),
                    expires_in=json_data.get("expires_in", 3600),
                    refresh_token=json_data.get("refresh_token", refresh_token),
                    scope=json_data.get("scope", ""),
                    id_token=json_data.get("id_token"),
                )
                return self._token
        except Exception as e:
            logger.error("Token refresh error: %s", e)
            return None

    async def client_credentials_grant(self) -> Optional[TokenResponse]:
        """Perform client credentials grant flow."""
        data = {
            "grant_type": GrantType.CLIENT_CREDENTIALS.value,
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
            "scope": self.config.scope,
        }

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.config.token_url,
                    data=data,
                    headers={"Accept": "application/json"},
                )
                if response.status_code != 200:
                    logger.error("Client credentials grant failed: %s", response.text)
                    return None

                json_data = response.json()
                self._token = TokenResponse(
                    access_token=json_data["access_token"],
                    token_type=json_data.get("token_type", "Bearer"),
                    expires_in=json_data.get("expires_in", 3600),
                    scope=json_data.get("scope", ""),
                )
                return self._token
        except Exception as e:
            logger.error("Client credentials error: %s", e)
            return None

    async def get_userinfo(self, access_token: Optional[str] = None) -> Optional[UserInfo]:
        """Fetch user information from the userinfo endpoint."""
        token = access_token or (self._token.access_token if self._token else None)
        if not token:
            return None

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    self.config.authorization_url.rreplace("/authorize", "/userinfo"),
                    headers={"Authorization": f"Bearer {token}"},
                )
                if response.status_code != 200:
                    return None
                json_data = response.json()
                return UserInfo(
                    sub=json_data.get("sub", ""),
                    name=json_data.get("name"),
                    email=json_data.get("email"),
                    email_verified=json_data.get("email_verified", False),
                    picture=json_data.get("picture"),
                    locale=json_data.get("locale", "en"),
                    claims=json_data,
                )
        except Exception as e:
            logger.error("Failed to get userinfo: %s", e)
            return None

    async def revoke_token(self, token: str, token_type: str = "access_token") -> bool:
        """Revoke an OAuth2 token."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.config.token_url.rreplace('/token', '/revoke')}",
                    data={
                        "token": token,
                        "token_type_hint": token_type,
                        "client_id": self.config.client_id,
                        "client_secret": self.config.client_secret,
                    },
                )
                return response.status_code in (200, 204)
        except Exception as e:
            logger.error("Token revocation error: %s", e)
            return False

    def set_token(self, token: TokenResponse) -> None:
        """Manually set the access token."""
        self._token = token

    @property
    def token(self) -> Optional[TokenResponse]:
        return self._token


class JWTValidator:
    """Validates JWT tokens."""

    def __init__(self, jwks_url: Optional[str] = None, issuer: Optional[str] = None, audience: Optional[str] = None) -> None:
        self.jwks_url = jwks_url
        self.issuer = issuer
        self.audience = audience
        self._jwks_cache: dict[str, Any] = {}

    def decode(self, token: str, verify: bool = True) -> dict[str, Any]:
        """Decode a JWT token."""
        try:
            import jwt
            options = {"verify_signature": verify, "verify_exp": verify}
            kwargs = {"options": options}
            if self.issuer:
                kwargs["issuer"] = self.issuer
            if self.audience:
                kwargs["audience"] = self.audience
            if self.jwks_url:
                kwargs["key"] = self._get_signing_key(token)
            return jwt.decode(token, **kwargs)
        except ImportError:
            import base64, json
            parts = token.split(".")
            payload = parts[1] + "=="
            return json.loads(base64.urlsafe_b64decode(payload))

    def _get_signing_key(self, token: str) -> Any:
        """Get the signing key from JWKS."""
        import jwt
        try:
            unverified_header = jwt.get_unverified_header(token)
            kid = unverified_header.get("kid")
            if kid in self._jwks_cache:
                return self._jwks_cache[kid]
            return None
        except Exception:
            return None
