"""OAuth2 utilities: client credentials, authorization code, token refresh."""

from __future__ import annotations

import json
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "OAuth2Config",
    "OAuth2Client",
    "AuthorizationCodeFlow",
    "ClientCredentialsFlow",
]


@dataclass
class OAuth2Config:
    """OAuth2 client configuration."""

    client_id: str
    client_secret: str
    token_url: str
    authorization_url: str = ""
    redirect_uri: str = ""
    scope: str = ""


@dataclass
class Token:
    """OAuth2 access token."""

    access_token: str
    token_type: str = "Bearer"
    expires_in: int = 3600
    refresh_token: str = ""
    issued_at: float = field(default_factory=time.time)

    @property
    def is_expired(self) -> bool:
        return (time.time() - self.issued_at) >= self.expires_in

    @property
    def expires_at(self) -> float:
        return self.issued_at + self.expires_in


class OAuth2Client:
    """Base OAuth2 client."""

    def __init__(self, config: OAuth2Config) -> None:
        self.config = config
        self._token: Token | None = None

    def _request_token(self, data: dict[str, str]) -> Token:
        """Make a token request."""
        body = urllib.parse.urlencode(data).encode()
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        req = urllib.request.Request(
            self.config.token_url,
            data=body,
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                token_data = json.loads(resp.read().decode())
                return Token(
                    access_token=token_data["access_token"],
                    token_type=token_data.get("token_type", "Bearer"),
                    expires_in=token_data.get("expires_in", 3600),
                    refresh_token=token_data.get("refresh_token", ""),
                    issued_at=time.time(),
                )
        except urllib.error.HTTPError as e:
            error_data = json.loads(e.read().decode())
            raise OAuth2Error(f"Token request failed: {error_data}")


class ClientCredentialsFlow(OAuth2Client):
    """OAuth2 Client Credentials flow for machine-to-machine."""

    def get_token(self) -> Token:
        """Get an access token using client credentials."""
        if self._token and not self._token.is_expired:
            return self._token

        data = {
            "grant_type": "client_credentials",
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
            "scope": self.config.scope,
        }
        self._token = self._request_token(data)
        return self._token

    def refresh_token(self) -> Token:
        """Force refresh the access token."""
        self._token = None
        return self.get_token()


class AuthorizationCodeFlow(OAuth2Client):
    """OAuth2 Authorization Code flow for user-facing applications."""

    def __init__(self, config: OAuth2Config) -> None:
        super().__init__(config)
        self._state: str = ""

    def get_authorization_url(self) -> str:
        """Get the URL to redirect the user to for authorization."""
        import urllib.parse

        self._state = __import__("secrets").token_hex(16)
        params = {
            "client_id": self.config.client_id,
            "redirect_uri": self.config.redirect_uri,
            "response_type": "code",
            "scope": self.config.scope,
            "state": self._state,
        }
        return f"{self.config.authorization_url}?{urllib.parse.urlencode(params)}"

    def exchange_code(self, code: str, state: str = "") -> Token:
        """Exchange an authorization code for an access token."""
        if state and state != self._state:
            raise OAuth2Error("State mismatch")

        data = {
            "grant_type": "authorization_code",
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
            "code": code,
            "redirect_uri": self.config.redirect_uri,
        }
        self._token = self._request_token(data)
        return self._token

    def refresh_access_token(self) -> Token:
        """Refresh the access token using refresh_token."""
        if not self._token or not self._token.refresh_token:
            raise OAuth2Error("No refresh token available")

        data = {
            "grant_type": "refresh_token",
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
            "refresh_token": self._token.refresh_token,
        }
        self._token = self._request_token(data)
        return self._token


class OAuth2Error(Exception):
    """OAuth2 error."""
    pass
