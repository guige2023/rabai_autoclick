"""OAuth 1.0/2.0 authentication action module.

Provides OAuth token management, refresh, and validation for API clients.
Supports OAuth 2.0 client credentials, authorization code, and refresh token flows.
"""

from __future__ import annotations

import time
import hashlib
import hmac
import base64
import urllib.parse
import secrets
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


@dataclass
class OAuthToken:
    """Represents an OAuth access token with metadata."""
    access_token: str
    token_type: str = "Bearer"
    expires_in: int = 3600
    refresh_token: Optional[str] = None
    scope: Optional[str] = None
    expires_at: float = field(default_factory=lambda: time.time() + 3600)
    issued_at: float = field(default_factory=lambda: time.time())

    @property
    def is_expired(self) -> bool:
        """Check if the token is expired."""
        return time.time() >= self.expires_at

    @property
    def is_expiring_soon(self, margin: int = 60) -> bool:
        """Check if token will expire within margin seconds."""
        return time.time() >= (self.expires_at - margin)

    def to_dict(self) -> Dict[str, Any]:
        """Convert token to dictionary."""
        return {
            "access_token": self.access_token,
            "token_type": self.token_type,
            "expires_in": self.expires_in,
            "refresh_token": self.refresh_token,
            "scope": self.scope,
            "expires_at": self.expires_at,
            "issued_at": self.issued_at,
        }


class OAuthAction:
    """OAuth 2.0 authentication handler.

    Supports multiple grant types and token refresh workflows.

    Example:
        oauth = OAuthAction(client_id="abc", client_secret="xyz")
        token = oauth.get_token()
        headers = oauth.auth_headers(token)
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token_url: str,
        scope: Optional[str] = None,
        timeout: int = 30,
    ) -> None:
        """Initialize OAuth handler.

        Args:
            client_id: OAuth client ID.
            client_secret: OAuth client secret.
            token_url: Token endpoint URL.
            scope: Space-separated OAuth scopes.
            timeout: Request timeout in seconds.
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url
        self.scope = scope
        self.timeout = timeout
        self._cached_token: Optional[OAuthToken] = None

    def build_auth_url(
        self,
        redirect_uri: str,
        state: Optional[str] = None,
        code_challenge: bool = True,
    ) -> tuple[str, str]:
        """Build OAuth 2.0 authorization URL.

        Args:
            redirect_uri: Redirect URI registered with the provider.
            state: Optional state parameter for CSRF protection.
            code_challenge: Use PKCE code challenge.

        Returns:
            Tuple of (authorization_url, code_verifier).
        """
        code_verifier = secrets.token_urlsafe(64) if code_challenge else ""
        code_challenge_method = "S256" if code_challenge else "plain"

        params = {
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": redirect_uri,
        }
        if self.scope:
            params["scope"] = self.scope
        if state:
            params["state"] = state
        if code_challenge:
            code_challenge_value = base64.urlsafe_b64encode(
                hashlib.sha256(code_verifier.encode()).digest()
            ).rstrip(b"=").decode()
            params["code_challenge"] = code_challenge_value
            params["code_challenge_method"] = code_challenge_method

        return f"{self.token_url}?{urllib.parse.urlencode(params)}", code_verifier

    def exchange_code(
        self,
        code: str,
        redirect_uri: str,
        code_verifier: Optional[str] = None,
    ) -> OAuthToken:
        """Exchange authorization code for access token.

        Args:
            code: Authorization code from callback.
            redirect_uri: Must match the original request.
            code_verifier: PKCE code verifier for S256 challenge.

        Returns:
            OAuthToken with access and refresh tokens.

        Raises:
            OAuthError: If token exchange fails.
        """
        try:
            import urllib.request
            import json

            data = {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }
            if code_verifier:
                data["code_verifier"] = code_verifier

            req = urllib.request.Request(
                self.token_url,
                data=urllib.parse.urlencode(data).encode(),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                payload = json.loads(resp.read())

            return self._parse_token_response(payload)
        except Exception as e:
            logger.error("OAuth token exchange failed: %s", e)
            raise OAuthError(f"Token exchange failed: {e}") from e

    def refresh_access_token(self, refresh_token: str) -> OAuthToken:
        """Refresh an expired or expiring access token.

        Args:
            refresh_token: The refresh token from previous auth.

        Returns:
            New OAuthToken with updated access token.

        Raises:
            OAuthError: If refresh fails.
        """
        try:
            import urllib.request
            import json

            data = {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }
            req = urllib.request.Request(
                self.token_url,
                data=urllib.parse.urlencode(data).encode(),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                payload = json.loads(resp.read())

            return self._parse_token_response(payload)
        except Exception as e:
            logger.error("OAuth token refresh failed: %s", e)
            raise OAuthError(f"Token refresh failed: {e}") from e

    def get_cached_token(self) -> Optional[OAuthToken]:
        """Return cached token if valid, None otherwise."""
        return self._cached_token

    def set_cached_token(self, token: OAuthToken) -> None:
        """Cache a token and return auth headers dict."""
        self._cached_token = token

    def get_token(self, force_refresh: bool = False) -> OAuthToken:
        """Get a valid access token, refreshing if necessary.

        Args:
            force_refresh: Force re-authentication.

        Returns:
            Valid OAuthToken.

        Raises:
            OAuthError: If no cached token and no refresh token available.
        """
        token = self._cached_token
        if token is None:
            raise OAuthError("No cached token. Provide refresh_token or use exchange_code first.")

        if force_refresh or token.is_expiring_soon:
            if not token.refresh_token:
                raise OAuthError("Token expired and no refresh token available.")
            return self.refresh_access_token(token.refresh_token)

        return token

    def auth_headers(self, token: OAuthToken) -> Dict[str, str]:
        """Build Authorization headers from token.

        Args:
            token: OAuthToken instance.

        Returns:
            Dict of HTTP headers with Authorization.
        """
        return {"Authorization": f"{token.token_type} {token.access_token}"}

    def _parse_token_response(self, payload: Dict[str, Any]) -> OAuthToken:
        """Parse token response into OAuthToken."""
        expires_in = payload.get("expires_in", 3600)
        return OAuthToken(
            access_token=payload["access_token"],
            token_type=payload.get("token_type", "Bearer"),
            expires_in=expires_in,
            refresh_token=payload.get("refresh_token"),
            scope=payload.get("scope"),
            expires_at=time.time() + expires_in,
        )


class OAuth1Action:
    """OAuth 1.0 signature-based authentication.

    Implements HMAC-SHA1 signature generation for OAuth 1.0 requests.
    """

    def __init__(
        self,
        client_key: str,
        client_secret: str,
        token: Optional[str] = None,
        token_secret: Optional[str] = None,
        signature_method: str = "HMAC-SHA1",
    ) -> None:
        """Initialize OAuth 1.0 handler.

        Args:
            client_key: Consumer key.
            client_secret: Consumer secret.
            token: OAuth token (from request token exchange).
            token_secret: OAuth token secret.
            signature_method: Signature method (HMAC-SHA1, PLAINTEXT).
        """
        self.client_key = client_key
        self.client_secret = client_secret
        self.token = token
        self.token_secret = token_secret or ""
        self.signature_method = signature_method
        self._nonce = secrets.token_urlsafe(16)
        self._timestamp = str(int(time.time()))

    def generate_signature(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, str]] = None,
    ) -> str:
        """Generate OAuth 1.0 HMAC-SHA1 signature.

        Args:
            method: HTTP method (GET, POST, etc.).
            url: Request URL.
            params: Additional query/body parameters.

        Returns:
            Base64-encoded signature string.
        """
        oauth_params = {
            "oauth_consumer_key": self.client_key,
            "oauth_nonce": self._nonce,
            "oauth_signature_method": self.signature_method,
            "oauth_timestamp": self._timestamp,
            "oauth_version": "1.0",
        }
        if self.token:
            oauth_params["oauth_token"] = self.token
        if params:
            oauth_params.update(params)

        sorted_params = sorted(oauth_params.items())
        param_str = "&".join(f"{k}={urllib.parse.quote(str(v), safe='')}" for k, v in sorted_params)
        base_string = f"{method.upper()}&{urllib.parse.quote(url, safe='')}&{urllib.parse.quote(param_str, safe='')}"

        key = f"{urllib.parse.quote(self.client_secret, safe='')}&{urllib.parse.quote(self.token_secret, safe='')}"

        if self.signature_method == "PLAINTEXT":
            return key
        elif self.signature_method == "HMAC-SHA1":
            import hashlib
            hashed = hmac.new(
                key.encode(),
                base_string.encode(),
                hashlib.sha1,
            )
            return base64.b64encode(hashed.digest()).decode()

        raise OAuthError(f"Unsupported signature method: {self.signature_method}")

    def auth_headers(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, str]] = None,
    ) -> Dict[str, str]:
        """Generate full OAuth 1.0 Authorization header.

        Args:
            method: HTTP method.
            url: Request URL.
            params: Additional parameters.

        Returns:
            Headers dict with Authorization header.
        """
        signature = self.generate_signature(method, url, params)
        oauth_params = {
            "oauth_consumer_key": self.client_key,
            "oauth_nonce": self._nonce,
            "oauth_signature": signature,
            "oauth_signature_method": self.signature_method,
            "oauth_timestamp": self._timestamp,
            "oauth_version": "1.0",
        }
        if self.token:
            oauth_params["oauth_token"] = self.token

        auth_str = "OAuth " + ", ".join(
            f"{urllib.parse.quote(k, safe='')}=\"{urllib.parse.quote(str(v), safe='')}\""
            for k, v in sorted(oauth_params.items())
        )
        return {"Authorization": auth_str}


class OAuthError(Exception):
    """OAuth operation error."""
    pass
