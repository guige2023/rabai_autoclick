"""
JWT utilities for token generation and validation.

Provides JWT encoding/decoding, claims management, 
refresh token handling, and token rotation.
"""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class TokenType(Enum):
    ACCESS = auto()
    REFRESH = auto()
    ID = auto()
    API = auto()


@dataclass
class JWTConfig:
    """JWT configuration."""
    secret_key: str
    algorithm: str = "HS256"
    issuer: Optional[str] = None
    audience: Optional[str] = None
    access_token_ttl: int = 900  # 15 minutes
    refresh_token_ttl: int = 604800  # 7 days
    id_token_ttl: int = 3600  # 1 hour


@dataclass
class TokenClaims:
    """JWT token claims."""
    sub: str
    type: TokenType = TokenType.ACCESS
    iss: Optional[str] = None
    aud: Optional[str] = None
    exp: Optional[float] = None
    iat: Optional[float] = None
    nbf: Optional[float] = None
    jti: Optional[str] = None
    scope: str = ""
    roles: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert claims to dictionary."""
        result = {
            "sub": self.sub,
            "type": self.type.name,
        }
        if self.iss:
            result["iss"] = self.iss
        if self.aud:
            result["aud"] = self.aud
        if self.exp:
            result["exp"] = self.exp
        if self.iat:
            result["iat"] = self.iat
        if self.nbf:
            result["nbf"] = self.nbf
        if self.jti:
            result["jti"] = self.jti
        if self.scope:
            result["scope"] = self.scope
        if self.roles:
            result["roles"] = self.roles
        result.update(self.metadata)
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TokenClaims":
        """Create claims from dictionary."""
        return cls(
            sub=data["sub"],
            type=TokenType[data.get("type", "ACCESS")],
            iss=data.get("iss"),
            aud=data.get("aud"),
            exp=data.get("exp"),
            iat=data.get("iat"),
            nbf=data.get("nbf"),
            jti=data.get("jti"),
            scope=data.get("scope", ""),
            roles=data.get("roles", []),
            metadata={k: v for k, v in data.items() if k not in ["sub", "type", "iss", "aud", "exp", "iat", "nbf", "jti", "scope", "roles"]},
        )


class JWTManager:
    """Manages JWT token creation and validation."""

    def __init__(self, config: JWTConfig) -> None:
        self.config = config

    def create_access_token(
        self,
        subject: str,
        scope: str = "",
        roles: Optional[list[str]] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> str:
        """Create an access token."""
        now = time.time()
        claims = TokenClaims(
            sub=subject,
            type=TokenType.ACCESS,
            iss=self.config.issuer,
            aud=self.config.audience,
            iat=now,
            nbf=now,
            exp=now + self.config.access_token_ttl,
            jti=self._generate_jti(),
            scope=scope,
            roles=roles or [],
            metadata=metadata or {},
        )
        return self._encode(claims)

    def create_refresh_token(self, subject: str, metadata: Optional[dict[str, Any]] = None) -> str:
        """Create a refresh token."""
        now = time.time()
        claims = TokenClaims(
            sub=subject,
            type=TokenType.REFRESH,
            iss=self.config.issuer,
            iat=now,
            exp=now + self.config.refresh_token_ttl,
            jti=self._generate_jti(),
            metadata=metadata or {},
        )
        return self._encode(claims)

    def create_id_token(
        self,
        subject: str,
        email: Optional[str] = None,
        name: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> str:
        """Create an OpenID Connect ID token."""
        now = time.time()
        claims = TokenClaims(
            sub=subject,
            type=TokenType.ID,
            iss=self.config.issuer,
            aud=self.config.audience,
            iat=now,
            exp=now + self.config.id_token_ttl,
            jti=self._generate_jti(),
            metadata=metadata or {},
        )
        return self._encode(claims)

    def decode_token(self, token: str, verify: bool = True) -> Optional[TokenClaims]:
        """Decode and validate a JWT token."""
        try:
            import jwt
            options = {
                "verify_signature": verify,
                "verify_exp": verify,
                "verify_nbf": verify,
                "verify_iat": verify,
                "verify_iss": verify,
                "verify_aud": verify,
            }
            kwargs = {
                "options": options,
                "algorithms": [self.config.algorithm],
            }
            if not verify:
                kwargs["algorithms"] = [self.config.algorithm]

            payload = jwt.decode(token, self.config.secret_key, **kwargs)
            return TokenClaims.from_dict(payload)
        except ImportError:
            return self._decode_without_validation(token)
        except Exception as e:
            logger.error("JWT decode failed: %s", e)
            return None

    def verify_token(self, token: str) -> tuple[bool, Optional[TokenClaims], str]:
        """Verify a token and return status, claims, and error message."""
        claims = self.decode_token(token, verify=True)
        if not claims:
            return False, None, "Invalid or expired token"

        if claims.exp and time.time() > claims.exp:
            return False, None, "Token has expired"

        return True, claims, ""

    def refresh_access_token(self, refresh_token: str) -> Optional[tuple[str, str]]:
        """Create new access token from refresh token. Returns (access_token, new_refresh_token)."""
        valid, claims, _ = self.verify_token(refresh_token)
        if not valid or claims.type != TokenType.REFRESH:
            return None

        new_access = self.create_access_token(
            subject=claims.sub,
            metadata=claims.metadata,
        )
        new_refresh = self.create_refresh_token(
            subject=claims.sub,
            metadata=claims.metadata,
        )
        return new_access, new_refresh

    def _encode(self, claims: TokenClaims) -> str:
        """Encode claims to JWT."""
        try:
            import jwt
            return jwt.encode(
                claims.to_dict(),
                self.config.secret_key,
                algorithm=self.config.algorithm,
            )
        except ImportError:
            import base64, json
            header = base64.urlsafe_b64encode(json.dumps({"alg": self.config.algorithm, "typ": "JWT"}).encode()).decode().rstrip("=")
            payload = base64.urlsafe_b64encode(json.dumps(claims.to_dict()).encode()).decode().rstrip("=")
            signature = hashlib.sha256(f"{header}.{payload}".encode()).hexdigest()[:16]
            return f"{header}.{payload}.{signature}"

    def _decode_without_validation(self, token: str) -> Optional[TokenClaims]:
        """Decode without signature verification (for debugging)."""
        import base64, json
        try:
            parts = token.split(".")
            if len(parts) != 3:
                return None
            payload = parts[1] + "=="
            data = json.loads(base64.urlsafe_b64decode(payload))
            return TokenClaims.from_dict(data)
        except Exception:
            return None

    def _generate_jti(self) -> str:
        """Generate a unique JWT ID."""
        import uuid
        return hashlib.sha256(str(uuid.uuid4()).encode()).hexdigest()[:16]


class TokenBlacklist:
    """Token blacklist for revocation."""

    def __init__(self) -> None:
        self._blacklist: set[str] = set()
        self._expiry_times: dict[str, float] = {}

    def add(self, jti: str, exp: float) -> None:
        """Add a token ID to the blacklist."""
        self._blacklist.add(jti)
        self._expiry_times[jti] = exp

    def is_blacklisted(self, jti: str) -> bool:
        """Check if a token ID is blacklisted."""
        if jti not in self._blacklist:
            return False
        if time.time() > self._expiry_times.get(jti, 0):
            self._blacklist.discard(jti)
            return False
        return True

    def cleanup(self) -> int:
        """Remove expired entries from blacklist."""
        now = time.time()
        expired = [jti for jti, exp in self._expiry_times.items() if exp < now]
        for jti in expired:
            self._blacklist.discard(jti)
            del self._expiry_times[jti]
        return len(expired)
