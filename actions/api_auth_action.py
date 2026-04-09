"""API authentication utilities supporting multiple auth schemes.

Supports API keys, Bearer tokens, Basic auth, OAuth2, and AWS Signature.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable
from urllib.parse import urlencode

try:
    import secrets
except ImportError:
    import uuid as secrets


@dataclass
class AuthConfig:
    """Base authentication configuration."""

    auth_type: str = "bearer"
    header_name: str = "Authorization"


@dataclass
class APIKeyConfig(AuthConfig):
    """API key authentication configuration."""

    api_key: str = ""
    api_secret: str | None = None
    auth_type: str = "api_key"
    key_location: str = "header"
    key_prefix: str = ""


@dataclass
class BearerTokenConfig(AuthConfig):
    """Bearer token configuration."""

    token: str = ""
    auth_type: str = "bearer"
    header_name: str = "Authorization"


@dataclass
class BasicAuthConfig(AuthConfig):
    """Basic authentication configuration."""

    username: str = ""
    password: str = ""
    auth_type: str = "basic"
    header_name: str = "Authorization"


@dataclass
class OAuth2Config:
    """OAuth2 configuration."""

    client_id: str = ""
    client_secret: str = ""
    token_url: str = ""
    authorization_url: str = ""
    redirect_uri: str = ""
    scope: str = ""
    state: str | None = None


@dataclass
class OAuth2Token:
    """OAuth2 token data."""

    access_token: str
    token_type: str = "Bearer"
    expires_in: int = 0
    refresh_token: str | None = None
    scope: str = ""
    expires_at: float = field(default_factory=lambda: time.time())
    raw: dict[str, Any] = field(default_factory=dict)

    def is_expired(self, buffer_seconds: int = 60) -> bool:
        """Check if token is expired or about to expire."""
        return time.time() >= (self.expires_at - buffer_seconds)

    def to_header(self) -> dict[str, str]:
        """Convert to Authorization header."""
        return {"Authorization": f"{self.token_type} {self.access_token}"}


class OAuth2Handler:
    """OAuth2 token management and refresh."""

    def __init__(self, config: OAuth2Config, token_store: Callable[[str | None, OAuth2Token], None] | None = None) -> None:
        self.config = config
        self.token_store = token_store
        self._token: OAuth2Token | None = None

    @property
    def token(self) -> OAuth2Token | None:
        """Get current token."""
        return self._token

    @token.setter
    def token(self, value: OAuth2Token) -> None:
        """Set token and persist if store provided."""
        self._token = value
        if self.token_store:
            self.token_store(None, value)

    def get_authorization_url(self, state: str | None = None) -> str:
        """Generate OAuth2 authorization URL."""
        import secrets

        state = state or secrets.token_urlsafe(32)
        params = {
            "response_type": "code",
            "client_id": self.config.client_id,
            "redirect_uri": self.config.redirect_uri,
            "scope": self.config.scope,
            "state": state,
        }
        return f"{self.config.authorization_url}?{urlencode(params)}"

    async def exchange_code(self, code: str) -> OAuth2Token:
        """Exchange authorization code for access token."""
        import aiohttp

        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.config.redirect_uri,
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(self.config.token_url, data=data) as resp:
                resp.raise_for_status()
                token_data = await resp.json()

        token = OAuth2Token(
            access_token=token_data["access_token"],
            token_type=token_data.get("token_type", "Bearer"),
            expires_in=token_data.get("expires_in", 0),
            refresh_token=token_data.get("refresh_token"),
            scope=token_data.get("scope", ""),
            expires_at=time.time() + token_data.get("expires_in", 0),
            raw=token_data,
        )
        self.token = token
        return token

    async def refresh(self) -> OAuth2Token:
        """Refresh the access token."""
        if not self._token or not self._token.refresh_token:
            raise ValueError("No refresh token available")

        import aiohttp

        data = {
            "grant_type": "refresh_token",
            "refresh_token": self._token.refresh_token,
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(self.config.token_url, data=data) as resp:
                resp.raise_for_status()
                token_data = await resp.json()

        token = OAuth2Token(
            access_token=token_data["access_token"],
            token_type=token_data.get("token_type", "Bearer"),
            expires_in=token_data.get("expires_in", 0),
            refresh_token=token_data.get("refresh_token", self._token.refresh_token),
            scope=token_data.get("scope", self._token.scope),
            expires_at=time.time() + token_data.get("expires_in", 0),
            raw=token_data,
        )
        self.token = token
        return token

    async def get_valid_token(self) -> str:
        """Get a valid access token, refreshing if necessary."""
        if not self._token:
            raise ValueError("No token available")

        if self._token.is_expired():
            await self.refresh()

        return self._token.access_token


class AWSV4Signer:
    """AWS Signature Version 4 request signer."""

    def __init__(self, access_key: str, secret_key: str, region: str = "us-east-1", service: str = "execute-api") -> None:
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.service = service

    def sign(self, method: str, url: str, headers: dict[str, str], body: bytes | None = None) -> dict[str, str]:
        """Sign an AWS API request with AWS Signature V4."""
        from urllib.parse import urlparse

        parsed = urlparse(url)
        now = time.gmtime()
        date_stamp = time.strftime("%Y%m%d", now)
        amz_date = time.strftime("%Y%m%dT%H%M%SZ", now)

        if "x-amz-date" not in headers and "X-Amz-Date" not in headers:
            headers["X-Amz-Date"] = amz_date

        host = parsed.netloc
        if "host" not in headers and "Host" not in headers:
            headers["Host"] = host

        method = method.upper()
        canonical_uri = parsed.path or "/"
        canonical_querystring = parsed.query

        payload_hash = hashlib.sha256(body or b"").hexdigest()
        headers["X-Amz-Content-Sha256"] = payload_hash

        sorted_headers = sorted(headers.items(), key=lambda x: x[0].lower())
        canonical_headers = "".join(f"{k.lower()}:{v.strip()}\n" for k, v in sorted_headers)
        signed_headers = ";".join(k.lower() for k, v in sorted_headers)

        canonical_request = f"{method}\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        algorithm = "AWS4-HMAC-SHA256"
        credential_scope = f"{date_stamp}/{self.region}/{self.service}/aws4_request"
        string_to_sign = f"{algorithm}\n{amz_date}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode()).hexdigest()}"

        k_date = self._hmac_sha256(f"AWS4{self.secret_key}".encode(), date_stamp)
        k_region = self._hmac_sha256(k_date, self.region)
        k_service = self._hmac_sha256(k_region, self.service)
        k_signing = self._hmac_sha256(k_service, "aws4_request")
        signature = hmac.new(k_signing, string_to_sign.encode(), hashlib.sha256).hexdigest()

        authorization_header = f"{algorithm} Credential={self.access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"

        return {**headers, "Authorization": authorization_header}

    def _hmac_sha256(self, key: bytes, data: str) -> bytes:
        """Compute HMAC-SHA256."""
        return hmac.new(key, data.encode(), hashlib.sha256).digest()


class AuthBuilder:
    """Builder for creating auth configurations."""

    def __init__(self) -> None:
        self._config: AuthConfig = BearerTokenConfig()

    def api_key(self, key: str, location: str = "header", prefix: str = "") -> "AuthBuilder":
        """Configure API key auth."""
        self._config = APIKeyConfig(api_key=key, key_location=location, key_prefix=prefix)
        return self

    def bearer(self, token: str) -> "AuthBuilder":
        """Configure Bearer token auth."""
        self._config = BearerTokenConfig(token=token)
        return self

    def basic(self, username: str, password: str) -> "AuthBuilder":
        """Configure Basic auth."""
        self._config = BasicAuthConfig(username=username, password=password)
        return self

    def oauth2(self, config: OAuth2Config) -> "AuthBuilder":
        """Configure OAuth2."""
        self._config = config  # type: ignore
        return self

    def build(self) -> AuthConfig:
        """Build the auth configuration."""
        return self._config

    def apply_to_headers(self, headers: dict[str, str]) -> dict[str, str]:
        """Apply auth config to headers."""
        config = self._config

        if isinstance(config, APIKeyConfig):
            if config.key_location == "header":
                headers[config.header_name] = f"{config.key_prefix}{config.api_key}"
            return headers

        elif isinstance(config, BearerTokenConfig):
            headers[config.header_name] = f"Bearer {config.token}"
            return headers

        elif isinstance(config, BasicAuthConfig):
            credentials = base64.b64encode(f"{config.username}:{config.password}".encode()).decode()
            headers[config.header_name] = f"Basic {credentials}"
            return headers

        return headers


def apply_auth(headers: dict[str, str], config: AuthConfig | dict[str, Any]) -> dict[str, str]:
    """Apply authentication to request headers.

    Args:
        headers: Base headers dict.
        config: Auth configuration.

    Returns:
        Headers with auth applied.
    """
    if isinstance(config, dict):
        auth_type = config.get("type", "bearer")
        if auth_type == "api_key":
            key = config.get("key", "")
            prefix = config.get("prefix", "")
            location = config.get("location", "header")
            header_name = config.get("header", "X-API-Key")
            if location == "header":
                headers[header_name] = f"{prefix}{key}"
        elif auth_type == "bearer":
            token = config.get("token", "")
            headers["Authorization"] = f"Bearer {token}"
        elif auth_type == "basic":
            username = config.get("username", "")
            password = config.get("password", "")
            credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
            headers["Authorization"] = f"Basic {credentials}"
    elif isinstance(config, APIKeyConfig):
        headers[config.header_name] = f"{config.key_prefix}{config.api_key}"
    elif isinstance(config, BearerTokenConfig):
        headers[config.header_name] = f"Bearer {config.token}"
    elif isinstance(config, BasicAuthConfig):
        credentials = base64.b64encode(f"{config.username}:{config.password}".encode()).decode()
        headers[config.header_name] = f"Basic {credentials}"

    return headers
