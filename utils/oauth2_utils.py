"""OAuth2 client utilities for authorization code, client credentials, and refresh flows."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlencode

from .http_client_utils import HTTPClient

__all__ = ["OAuth2Config", "OAuth2Client", "OAuth2Token"]


@dataclass
class OAuth2Token:
    """OAuth2 access token with metadata."""
    access_token: str
    token_type: str = "Bearer"
    expires_in: float = 3600
    refresh_token: str | None = None
    scope: str | None = None
    issued_at: float = field(default_factory=time.time)
    id_token: str | None = None

    @property
    def expires_at(self) -> float:
        return self.issued_at + self.expires_in

    @property
    def is_expired(self) -> bool:
        return time.time() >= self.expires_at

    @property
    def is_near_expiry(self) -> bool:
        return time.time() >= self.expires_at - 300

    def to_dict(self) -> dict[str, Any]:
        return {
            "access_token": self.access_token,
            "token_type": self.token_type,
            "expires_in": self.expires_in,
            "refresh_token": self.refresh_token,
            "scope": self.scope,
            "id_token": self.id_token,
            "issued_at": self.issued_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> OAuth2Token:
        return cls(
            access_token=data["access_token"],
            token_type=data.get("token_type", "Bearer"),
            expires_in=float(data.get("expires_in", 3600)),
            refresh_token=data.get("refresh_token"),
            scope=data.get("scope"),
            refresh_token=data.get("id_token"),
            issued_at=data.get("issued_at", time.time()),
        )


@dataclass
class OAuth2Config:
    """OAuth2 provider configuration."""
    authorization_url: str
    token_url: str
    client_id: str
    client_secret: str
    redirect_uri: str
    scope: str = ""
    revocation_url: str | None = None


class OAuth2Client:
    """OAuth2 authorization flow client."""

    def __init__(self, config: OAuth2Config) -> None:
        self.config = config
        self._http = HTTPClient()

    def get_authorization_url(self, state: str | None = None) -> tuple[str, str]:
        """Build the authorization URL. Returns (url, state)."""
        import secrets
        state = state or secrets.token_urlsafe(16)
        params = {
            "response_type": "code",
            "client_id": self.config.client_id,
            "redirect_uri": self.config.redirect_uri,
            "scope": self.config.scope,
            "state": state,
        }
        url = f"{self.config.authorization_url}?{urlencode(params)}"
        return url, state

    def exchange_code(self, code: str) -> OAuth2Token:
        """Exchange an authorization code for tokens."""
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.config.redirect_uri,
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
        }
        response = self._http.post(
            self.config.token_url,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        payload = response.json
        return OAuth2Token(
            access_token=payload["access_token"],
            token_type=payload.get("token_type", "Bearer"),
            expires_in=float(payload.get("expires_in", 3600)),
            refresh_token=payload.get("refresh_token"),
            scope=payload.get("scope"),
            id_token=payload.get("id_token"),
        )

    def refresh(self, refresh_token: str) -> OAuth2Token:
        """Refresh an access token using a refresh token."""
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
        }
        response = self._http.post(
            self.config.token_url,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        payload = response.json
        return OAuth2Token(
            access_token=payload["access_token"],
            token_type=payload.get("token_type", "Bearer"),
            expires_in=float(payload.get("expires_in", 3600)),
            refresh_token=payload.get("refresh_token", refresh_token),
            scope=payload.get("scope"),
            id_token=payload.get("id_token"),
        )

    def revoke(self, token: str) -> bool:
        """Revoke a token."""
        if not self.config.revocation_url:
            return False
        data = {
            "token": token,
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
        }
        response = self._http.post(
            self.config.revocation_url,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        return response.status_code in (200, 204)

    def client_credentials(self) -> OAuth2Token:
        """Fetch a token using client credentials flow (machine-to-machine)."""
        data = {
            "grant_type": "client_credentials",
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
            "scope": self.config.scope,
        }
        response = self._http.post(
            self.config.token_url,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        payload = response.json
        return OAuth2Token(
            access_token=payload["access_token"],
            token_type=payload.get("token_type", "Bearer"),
            expires_in=float(payload.get("expires_in", 3600)),
            scope=payload.get("scope"),
        )
