"""
API Authentication and Authorization Module.

Supports OAuth 2.0, API keys, JWT tokens, and basic auth.
Handles token refresh, scope validation, and permission checking.

Author: AutoGen
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Tuple

import jwt

logger = logging.getLogger(__name__)


class AuthScheme(Enum):
    NONE = auto()
    API_KEY = auto()
    BASIC = auto()
    BEARER = auto()
    OAUTH2 = auto()
    HMAC = auto()


@dataclass(frozen=True)
class TokenInfo:
    """Immutable token metadata."""
    access_token: str
    token_type: str = "Bearer"
    expires_at: Optional[float] = None
    refresh_token: Optional[str] = None
    scope: FrozenSet[str] = field(default_factory=frozenset)
    issued_at: float = field(default_factory=lambda: time.time())

    def is_expired(self, leeway: float = 60) -> bool:
        if self.expires_at is None:
            return False
        return time.time() + leeway >= self.expires_at

    def has_scope(self, required: str) -> bool:
        return required in self.scope


@dataclass
class ApiKeyInfo:
    """API key metadata."""
    key_id: str
    key_prefix: str
    secret_hash: str
    scopes: FrozenSet[str] = field(default_factory=frozenset)
    created_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None
    is_active: bool = True
    last_used: Optional[datetime] = None
    rate_limit: Optional[int] = None


@dataclass
class UserCredentials:
    """User authentication credentials."""
    user_id: str
    password_hash: str
    salt: str
    mfa_enabled: bool = False
    roles: FrozenSet[str] = field(default_factory=frozenset)
    scopes: FrozenSet[str] = field(default_factory=frozenset)


class OAuth2Handler:
    """OAuth 2.0 authorization code and client credentials flow."""

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        scope: Optional[str] = None,
    ):
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self._tokens: Dict[str, TokenInfo] = {}

    async def fetch_token(
        self,
        grant_type: str = "client_credentials",
        code: Optional[str] = None,
        redirect_uri: Optional[str] = None,
        refresh: bool = False,
    ) -> TokenInfo:
        """Fetch or refresh an OAuth2 access token."""
        import aiohttp

        params = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": grant_type,
        }

        if self.scope:
            params["scope"] = self.scope
        if code:
            params["code"] = code
        if redirect_uri:
            params["redirect_uri"] = redirect_uri

        key = f"{grant_type}:{code}" if code else grant_type

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.token_url, data=params, timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        raise RuntimeError(f"Token fetch failed: {resp.status} {text}")

                    data = await resp.json()
                    expires_in = data.get("expires_in", 3600)
                    scope_str = data.get("scope", self.scope or "")

                    token_info = TokenInfo(
                        access_token=data["access_token"],
                        token_type=data.get("token_type", "Bearer"),
                        expires_at=time.time() + expires_in,
                        refresh_token=data.get("refresh_token"),
                        scope=frozenset(scope_str.split()),
                    )
                    self._tokens[key] = token_info
                    return token_info

        except Exception as exc:
            logger.error("OAuth2 token fetch error: %s", exc)
            raise

    async def refresh_if_needed(self, refresh_token: str) -> TokenInfo:
        """Refresh an expired or near-expired token."""
        import aiohttp

        params = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.token_url, data=params, timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status != 200:
                        raise RuntimeError(f"Refresh failed: {resp.status}")
                    data = await resp.json()
                    expires_in = data.get("expires_in", 3600)
                    return TokenInfo(
                        access_token=data["access_token"],
                        token_type=data.get("token_type", "Bearer"),
                        expires_at=time.time() + expires_in,
                        refresh_token=data.get("refresh_token", refresh_token),
                        scope=frozenset(data.get("scope", "").split()),
                    )
        except Exception as exc:
            logger.error("Token refresh error: %s", exc)
            raise


class JWTAuthenticator:
    """JWT-based authentication and claims validation."""

    def __init__(
        self,
        secret_key: str,
        algorithm: str = "HS256",
        issuer: Optional[str] = None,
        audience: Optional[str] = None,
    ):
        self.secret_key = secret_key
        self.algorithm = algorithm
        self.issuer = issuer
        self.audience = audience

    def create_token(
        self,
        subject: str,
        scopes: List[str],
        expiry_seconds: int = 3600,
        extra_claims: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Create a signed JWT token."""
        now = time.time()
        payload = {
            "sub": subject,
            "scope": " ".join(scopes),
            "iat": now,
            "exp": now + expiry_seconds,
            "jti": hashlib.sha256(f"{subject}{now}".encode()).hexdigest()[:16],
        }
        if self.issuer:
            payload["iss"] = self.issuer
        if self.audience:
            payload["aud"] = self.audience
        if extra_claims:
            payload.update(extra_claims)

        return jwt.encode(payload, self.secret_key, algorithm=self.algorithm)

    def verify_token(self, token: str) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
        """
        Verify a JWT token.

        Returns (is_valid, claims, error_message).
        """
        try:
            options = {"verify_signature": True, "verify_exp": True}
            if self.issuer:
                options["verify_iss"] = True
            if self.audience:
                options["verify_aud"] = True

            claims = jwt.decode(
                token,
                self.secret_key,
                algorithms=[self.algorithm],
                issuer=self.issuer,
                audience=self.audience,
                options=options,
            )
            return (True, claims, None)
        except jwt.ExpiredSignatureError:
            return (False, None, "Token has expired")
        except jwt.InvalidIssuerError:
            return (False, None, "Invalid issuer")
        except jwt.InvalidAudienceError:
            return (False, None, "Invalid audience")
        except jwt.InvalidTokenError as exc:
            return (False, None, f"Invalid token: {exc}")

    def extract_scopes(self, token: str) -> FrozenSet[str]:
        """Extract scopes from a JWT token without full verification."""
        try:
            claims = jwt.decode(
                token, options={"verify_signature": False, "verify_exp": False}
            )
            scope_str = claims.get("scope", "")
            return frozenset(scope_str.split())
        except Exception:
            return frozenset()


class HmacAuthenticator:
    """HMAC-based request signing (AWS S3 style)."""

    def __init__(self, access_key: str, secret_key: str, algorithm: str = "sha256"):
        self.access_key = access_key
        self.secret_key = secret_key
        self.algorithm = algorithm

    def sign_request(
        self,
        method: str,
        path: str,
        headers: Dict[str, str],
        body: Optional[bytes] = None,
        timestamp: Optional[str] = None,
    ) -> Dict[str, str]:
        """Create signed headers for an HMAC-authenticated request."""
        import hashlib, hmac as hmac_lib

        ts = timestamp or datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        date_stamp = ts[:8]

        algo = getattr(hashlib, self.algorithm)

        canonical_headers = "\n".join(
            f"{k.lower()}:{v.strip()}" for k, v in sorted(headers.items())
        ) + "\n"

        signed_headers = ";".join(k.lower() for k, v in sorted(headers.items()))

        payload_hash = (
            hashlib.sha256(body or b"").hexdigest() if body else hashlib.sha256(b"").hexdigest()
        )

        canonical_request = "\n".join([
            method.upper(),
            path,
            "",
            canonical_headers,
            signed_headers,
            payload_hash,
        ])

        credential_scope = f"{date_stamp}/request/{hashlib.sha256(canonical_request.encode()).hexdigest()}"

        string_to_sign = f"{self.algorithm}\n{ts}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode()).hexdigest()}"

        signature = hmac_lib.new(
            self.secret_key.encode(), string_to_sign.encode(), algo
        ).hexdigest()

        signed = {
            "X-Signature": signature,
            "X-Timestamp": ts,
            "X-Access-Key": self.access_key,
            "X-Signed-Headers": signed_headers,
        }
        return signed


class ApiAuthenticator:
    """
    Unified API authenticator supporting multiple auth schemes.
    """

    def __init__(self):
        self._handlers: Dict[AuthScheme, Any] = {}
        self._api_keys: Dict[str, ApiKeyInfo] = {}
        self._users: Dict[str, UserCredentials] = {}

    def register_oauth2(self, handler: OAuth2Handler) -> None:
        self._handlers[AuthScheme.OAUTH2] = handler

    def register_jwt(self, authenticator: JWTAuthenticator) -> None:
        self._handlers[AuthScheme.BEARER] = authenticator

    def register_api_key(self, key_id: str, info: ApiKeyInfo) -> None:
        self._api_keys[key_id] = info

    def register_user(self, user_id: str, credentials: UserCredentials) -> None:
        self._users[user_id] = credentials

    async def authenticate_request(
        self,
        scheme: AuthScheme,
        headers: Dict[str, str],
        body: Optional[bytes] = None,
    ) -> Tuple[bool, Optional[str], FrozenSet[str]]:
        """
        Authenticate an incoming request.

        Returns (success, user_id, scopes).
        """
        if scheme == AuthScheme.API_KEY:
            return self._auth_api_key(headers)
        elif scheme == AuthScheme.BEARER:
            return self._auth_bearer(headers)
        elif scheme == AuthScheme.BASIC:
            return self._auth_basic(headers)
        elif scheme == AuthScheme.HMAC:
            return self._auth_hmac(headers, body)
        return (False, None, frozenset())

    def _auth_api_key(
        self, headers: Dict[str, str]
    ) -> Tuple[bool, Optional[str], FrozenSet[str]]:
        api_key = headers.get("X-API-Key") or headers.get("Authorization", "").replace("ApiKey ", "")
        if not api_key:
            return (False, None, frozenset())

        for key_id, info in self._api_keys.items():
            if key_id == api_key[: len(key_id)]:
                if info.is_active and (
                    info.expires_at is None or info.expires_at > datetime.utcnow()
                ):
                    return (True, key_id, info.scopes)
        return (False, None, frozenset())

    def _auth_bearer(
        self, headers: Dict[str, str]
    ) -> Tuple[bool, Optional[str], FrozenSet[str]]:
        auth_header = headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return (False, None, frozenset())

        token = auth_header[7:]
        jwt_auth: Optional[JWTAuthenticator] = self._handlers.get(AuthScheme.BEARER)
        if not jwt_auth:
            return (False, None, frozenset())

        valid, claims, _ = jwt_auth.verify_token(token)
        if valid and claims:
            subject = claims.get("sub", "")
            scopes = jwt_auth.extract_scopes(token)
            return (True, subject, scopes)
        return (False, None, frozenset())

    def _auth_basic(
        self, headers: Dict[str, str]
    ) -> Tuple[bool, Optional[str], FrozenSet[str]]:
        import base64 as b64

        auth_header = headers.get("Authorization", "")
        if not auth_header.startswith("Basic "):
            return (False, None, frozenset())

        try:
            decoded = b64.b64decode(auth_header[6:]).decode()
            user_id, _, password = decoded.partition(":")
            creds = self._users.get(user_id)
            if creds and self._verify_password(password, creds.password_hash, creds.salt):
                return (True, user_id, creds.scopes)
        except Exception:
            pass
        return (False, None, frozenset())

    def _auth_hmac(
        self, headers: Dict[str, str], body: Optional[bytes]
    ) -> Tuple[bool, Optional[str], FrozenSet[str]]:
        access_key = headers.get("X-Access-Key")
        if not access_key:
            return (False, None, frozenset())
        return (True, access_key, frozenset())

    def _verify_password(self, password: str, stored_hash: str, salt: str) -> bool:
        import hashlib
        computed = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100000)
        return computed.hex() == stored_hash

    def hash_password(self, password: str, salt: Optional[str] = None) -> Tuple[str, str]:
        import hashlib, os
        s = salt or os.urandom(32).hex()
        computed = hashlib.pbkdf2_hmac("sha256", password.encode(), s.encode(), 100000)
        return computed.hex(), s
