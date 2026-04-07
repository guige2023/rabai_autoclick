"""
token_utils.py - Token generation and validation utilities.

Provides secure token generation, validation, and refresh mechanisms
for authentication, API access, and session management.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

T = TypeVar("T")


class TokenType(Enum):
    """Token type classification."""
    ACCESS = "access"           # Short-lived access token
    REFRESH = "refresh"        # Long-lived refresh token
    VERIFICATION = "verify"    # Email/phone verification token
    RESET = "reset"            # Password reset token
    INVITATION = "invite"      # Invitation token
    API_KEY = "api_key"        # API key
    SESSION = "session"        # Session token
    CSRF = "csrf"              # CSRF protection token
    OTP = "otp"                # One-time password
    MAGIC_LINK = "magic_link"  # Magic link token


@dataclass(frozen=True)
class TokenConfig:
    """
    Token configuration settings.

    Attributes:
        token_type: Type of token
        expiry_seconds: Time until token expires (0 = never)
        length: Token byte length (actual length varies by encoding)
        encoding: Token encoding (urlsafe, hex, base32, base64)
        prefix: Optional prefix for token identification
        charset: Custom character set (overrides encoding default)
        secret_key: Secret key for HMAC signing
        algorithm: HMAC algorithm to use
        leeway_seconds: Clock skew tolerance for expiry validation
    """

    token_type: TokenType = TokenType.ACCESS
    expiry_seconds: int = 3600
    length: int = 32
    encoding: str = "urlsafe"
    prefix: Optional[str] = None
    charset: Optional[str] = None
    secret_key: Optional[str] = None
    algorithm: str = "sha256"
    leeway_seconds: int = 30

    # Predefined configurations
    @classmethod
    def access_token(cls, secret: str) -> TokenConfig:
        """Access token configuration (1 hour expiry)."""
        return cls(TokenType.ACCESS, expiry_seconds=3600, length=32, secret_key=secret)

    @classmethod
    def refresh_token(cls, secret: str) -> TokenConfig:
        """Refresh token configuration (30 days expiry)."""
        return cls(TokenType.REFRESH, expiry_seconds=2592000, length=64, secret_key=secret)

    @classmethod
    def verification_token(cls, secret: str) -> TokenConfig:
        """Verification token configuration (15 minutes expiry)."""
        return cls(TokenType.VERIFICATION, expiry_seconds=900, length=16, secret_key=secret)

    @classmethod
    def api_key(cls, secret: str) -> TokenConfig:
        """API key configuration (no expiry)."""
        return cls(TokenType.API_KEY, expiry_seconds=0, length=32, encoding="base32", prefix="sk", secret_key=secret)

    @classmethod
    def otp_token(cls, secret: str) -> TokenConfig:
        """OTP token configuration (short expiry, numeric)."""
        return cls(TokenType.OTP, expiry_seconds=300, length=8, encoding="digit", secret_key=secret)


@dataclass
class TokenData:
    """
    Token data container.

    Attributes:
        token: The raw token string
        token_type: Type of token
        payload: Decoded payload dict
        issued_at: Token issuance timestamp
        expires_at: Token expiration timestamp
        metadata: Additional token metadata
        is_expired: Whether token has expired
        is_valid: Overall validity status
    """

    token: str
    token_type: TokenType
    payload: Dict[str, Any]
    issued_at: datetime
    expires_at: Optional[datetime]
    metadata: Dict[str, Any] = field(default_factory=dict)
    is_expired: bool = False
    is_valid: bool = True

    def __post_init__(self) -> None:
        if self.expires_at and datetime.now(timezone.utc) > self.expires_at:
            object.__setattr__(self, "is_expired", True)
            object.__setattr__(self, "is_valid", False)

    @property
    def subject(self) -> Optional[str]:
        """Get the subject (sub) from payload."""
        return self.payload.get("sub")

    @property
    def audience(self) -> Optional[str]:
        """Get the audience (aud) from payload."""
        return self.payload.get("aud")

    @property
    def scopes(self) -> List[str]:
        """Get scopes from payload."""
        scopes = self.payload.get("scope", "")
        if isinstance(scopes, list):
            return scopes
        return scopes.split() if scopes else []

    @property
    def ttl(self) -> Optional[int]:
        """Time to live in seconds."""
        if not self.expires_at:
            return None
        delta = self.expires_at - datetime.now(timezone.utc)
        return max(0, int(delta.total_seconds()))


@dataclass
class TokenPair:
    """Container for access/refresh token pairs."""

    access_token: TokenData
    refresh_token: Optional[TokenData] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        result = {
            "access_token": self.access_token.token,
            "token_type": "Bearer",
            "expires_in": self.access_token.ttl,
        }
        if self.refresh_token:
            result["refresh_token"] = self.refresh_token.token
        return result


class TokenGenerator:
    """
    Secure token generator with multiple encoding schemes.

    Example:
        >>> config = TokenConfig.access_token(secret="my-secret-key")
        >>> generator = TokenGenerator(config)
        >>> token = generator.generate(subject="user123")
        >>> print(f"Token: {token.token}")
    """

    # Encoding schemes
    ENCODINGS: Dict[str, Callable[[bytes], str]] = {
        "urlsafe": lambda b: base64.urlsafe_b64encode(b).decode().rstrip("="),
        "hex": lambda b: b.hex(),
        "base32": lambda b: base64.b32encode(b).decode().rstrip("="),
        "base64": lambda b: base64.b64encode(b).decode().rstrip("="),
        "numeric": lambda b: str(int.from_bytes(b, "big"))[: len(b) * 2],
        "digit": lambda b: "".join(c for c in str(int.from_bytes(b, "big")) if c.isdigit())[:16],
    }

    def __init__(self, config: Optional[TokenConfig] = None) -> None:
        """
        Initialize the token generator.

        Args:
            config: Token configuration
        """
        self._config = config or TokenConfig()

    @property
    def config(self) -> TokenConfig:
        """Current token configuration."""
        return self._config

    def _generate_random_bytes(self) -> bytes:
        """Generate cryptographically secure random bytes."""
        return secrets.token_bytes(self._config.length)

    def _encode(self, data: bytes) -> str:
        """Encode bytes to string based on configured encoding."""
        encoding = self._config.encoding
        encoder = self.ENCODINGS.get(encoding, self.ENCODINGS["urlsafe"])
        return encoder(data)

    def generate(
        self,
        subject: Optional[str] = None,
        audience: Optional[str] = None,
        scopes: Optional[List[str]] = None,
        claims: Optional[Dict[str, Any]] = None,
        expires_at: Optional[datetime] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> TokenData:
        """
        Generate a new token.

        Args:
            subject: Token subject (e.g., user ID)
            audience: Token audience
            scopes: Permission scopes
            claims: Additional JWT-style claims
            expires_at: Custom expiration time
            metadata: Additional metadata

        Returns:
            TokenData containing the generated token
        """
        now = datetime.now(timezone.utc)
        issued_at = now

        # Calculate expiry
        if expires_at:
            expires = expires_at
        elif self._config.expiry_seconds > 0:
            expires = now + timedelta(seconds=self._config.expiry_seconds)
        else:
            expires = None

        # Build payload
        payload: Dict[str, Any] = {
            "jti": str(uuid.uuid4()),
            "iat": int(issued_at.timestamp()),
            "type": self._config.token_type.value,
        }

        if subject:
            payload["sub"] = subject
        if audience:
            payload["aud"] = audience
        if scopes:
            payload["scope"] = " ".join(scopes)
        if expires:
            payload["exp"] = int(expires.timestamp())
        if claims:
            payload.update(claims)

        # Generate token
        random_bytes = self._generate_random_bytes()

        # Sign the payload if secret is configured
        if self._config.secret_key:
            payload_bytes = json.dumps(payload, sort_keys=True).encode()
            signature = hmac.new(
                self._config.secret_key.encode(),
                payload_bytes,
                self._config.algorithm.replace("-", "_"),
            ).digest()
            combined = random_bytes + signature
        else:
            combined = random_bytes

        # Encode the token
        token_string = self._encode(combined)

        # Add prefix if configured
        if self._config.prefix:
            token_string = f"{self._config.prefix}_{token_string}"

        return TokenData(
            token=token_string,
            token_type=self._config.token_type,
            payload=payload,
            issued_at=issued_at,
            expires_at=expires,
            metadata=metadata or {},
        )

    def generate_pair(
        self,
        subject: Optional[str] = None,
        audience: Optional[str] = None,
        scopes: Optional[List[str]] = None,
        access_metadata: Optional[Dict[str, Any]] = None,
        refresh_metadata: Optional[Dict[str, Any]] = None,
    ) -> TokenPair:
        """
        Generate an access/refresh token pair.

        Args:
            subject: Token subject
            audience: Token audience
            scopes: Permission scopes
            access_metadata: Metadata for access token
            refresh_metadata: Metadata for refresh token

        Returns:
            TokenPair containing both tokens
        """
        # Create access token config
        access_config = TokenConfig(
            token_type=TokenType.ACCESS,
            expiry_seconds=self._config.expiry_seconds,
            length=32,
            secret_key=self._config.secret_key,
        )

        # Create refresh token config
        refresh_config = TokenConfig(
            token_type=TokenType.REFRESH,
            expiry_seconds=2592000,  # 30 days
            length=64,
            secret_key=self._config.secret_key,
        )

        access_gen = TokenGenerator(access_config)
        refresh_gen = TokenGenerator(refresh_config)

        return TokenPair(
            access_token=access_gen.generate(
                subject=subject,
                audience=audience,
                scopes=scopes,
                metadata=access_metadata,
            ),
            refresh_token=refresh_gen.generate(
                subject=subject,
                audience=audience,
                metadata=refresh_metadata,
            ),
        )

    def generate_api_key(
        self,
        user_id: Optional[str] = None,
        permissions: Optional[List[str]] = None,
        expires_at: Optional[datetime] = None,
    ) -> TokenData:
        """Generate an API key token."""
        return self.generate(
            subject=user_id,
            scopes=permissions,
            expires_at=expires_at,
            metadata={"permissions": permissions or []},
        )


class TokenValidator:
    """
    Token validation and verification.

    Validates token signatures, expiry, and claims.
    """

    def __init__(self, config: Optional[TokenConfig] = None) -> None:
        """
        Initialize the token validator.

        Args:
            config: Token configuration (must match generator config)
        """
        self._config = config or TokenConfig()

    def validate(
        self,
        token: str,
        expected_subject: Optional[str] = None,
        expected_audience: Optional[str] = None,
        expected_type: Optional[TokenType] = None,
        required_scopes: Optional[List[str]] = None,
    ) -> TokenData:
        """
        Validate a token.

        Args:
            token: Token string to validate
            expected_subject: Expected subject claim
            expected_audience: Expected audience claim
            expected_type: Expected token type
            required_scopes: Required scopes

        Returns:
            TokenData with validation results

        Raises:
            TokenError: If validation fails
        """
        # Decode token
        token_data = self._decode_token(token)

        # Check expiry
        if token_data.expires_at:
            now = datetime.now(timezone.utc)
            leeway = timedelta(seconds=self._config.leeway_seconds)
            if now > token_data.expires_at + leeway:
                token_data.is_valid = False

        # Check type
        if expected_type and token_data.token_type != expected_type:
            token_data.is_valid = False

        # Check subject
        if expected_subject and token_data.subject != expected_subject:
            token_data.is_valid = False

        # Check audience
        if expected_audience and token_data.audience != expected_audience:
            token_data.is_valid = False

        # Check scopes
        if required_scopes:
            token_scopes = set(token_data.scopes)
            required = set(required_scopes)
            if not required.issubset(token_scopes):
                token_data.is_valid = False

        return token_data

    def _decode_token(self, token: str) -> TokenData:
        """Decode and parse a token string."""
        # Remove prefix if configured
        if self._config.prefix:
            if token.startswith(self._config.prefix + "_"):
                token = token[len(self._config.prefix) + 1 :]
            elif token.startswith(self._config.prefix):
                token = token[len(self._config.prefix) :]

        # Decode token
        try:
            encoding = self._config.encoding
            if encoding == "urlsafe":
                # Add padding back for base64
                padding = 4 - (len(token) % 4)
                if padding < 4:
                    token += "=" * padding
                data = base64.urlsafe_b64decode(token)
            elif encoding == "hex":
                data = bytes.fromhex(token)
            elif encoding == "base32":
                padding = 8 - (len(token) % 8)
                if padding < 8:
                    token += "=" * padding
                data = base64.b32decode(token)
            elif encoding == "base64":
                padding = 4 - (len(token) % 4)
                if padding < 4:
                    token += "=" * padding
                data = base64.b64decode(token)
            else:
                raise TokenError(f"Unknown encoding: {encoding}")
        except Exception as e:
            raise TokenError(f"Failed to decode token: {e}")

        # Extract signature if secret is configured
        signature_len = 32  # SHA256 produces 32 bytes
        if self._config.secret_key and len(data) >= signature_len:
            payload_bytes = data[:-signature_len]
            signature = data[-signature_len:]

            # Verify signature
            expected = hmac.new(
                self._config.secret_key.encode(),
                payload_bytes,
                self._config.algorithm.replace("-", "_"),
            ).digest()

            if not hmac.compare_digest(signature, expected):
                raise TokenError("Invalid token signature")
        else:
            payload_bytes = data

        # Parse payload from payload_bytes (empty for simple tokens, JSON for JWT-style)
        payload: Dict[str, Any] = {}
        if payload_bytes:
            try:
                payload = json.loads(payload_bytes.decode())
            except (ValueError, UnicodeDecodeError):
                pass

        # Build token data
        issued_at = datetime.fromtimestamp(payload.get("iat", 0), tz=timezone.utc)
        expires_ts = payload.get("exp")
        expires_at = datetime.fromtimestamp(expires_ts, tz=timezone.utc) if expires_ts else None

        token_type = TokenType(payload.get("type", self._config.token_type.value))

        return TokenData(
            token=token,
            token_type=token_type,
            payload=payload,
            issued_at=issued_at,
            expires_at=expires_at,
            is_expired=expires_at is not None and datetime.now(timezone.utc) > expires_at,
            is_valid=True,
        )

    def refresh_access_token(
        self,
        refresh_token: str,
        new_access_config: Optional[TokenConfig] = None,
    ) -> TokenData:
        """
        Generate a new access token from a refresh token.

        Args:
            refresh_token: Valid refresh token
            new_access_config: Config for new access token

        Returns:
            New access token
        """
        # Validate refresh token
        data = self.validate(refresh_token, expected_type=TokenType.REFRESH)

        if not data.is_valid:
            raise TokenError("Invalid refresh token")

        # Create new access token
        access_config = new_access_config or TokenConfig(
            token_type=TokenType.ACCESS,
            expiry_seconds=3600,
            length=32,
            secret_key=self._config.secret_key,
        )

        generator = TokenGenerator(access_config)
        return generator.generate(
            subject=data.subject,
            audience=data.audience,
            scopes=data.scopes,
            metadata={"refreshed_from": refresh_token[:20] + "..."},
        )


class TokenError(Exception):
    """Token-related error."""

    pass


class TokenStore:
    """
    In-memory token storage with TTL support.

    For production, replace with Redis or database backend.
    """

    def __init__(self) -> None:
        """Initialize the token store."""
        self._tokens: Dict[str, TokenData] = {}
        self._revoked: set = set()

    def store(self, token_data: TokenData) -> None:
        """Store a token."""
        self._tokens[token_data.token] = token_data

    def get(self, token: str) -> Optional[TokenData]:
        """Retrieve a token."""
        data = self._tokens.get(token)
        if data and data.is_expired:
            del self._tokens[token]
            return None
        return data

    def revoke(self, token: str) -> None:
        """Revoke a token."""
        self._revoked.add(token)
        if token in self._tokens:
            del self._tokens[token]

    def is_revoked(self, token: str) -> bool:
        """Check if token is revoked."""
        return token in self._revoked

    def cleanup_expired(self) -> int:
        """Remove expired tokens from store."""
        now = datetime.now(timezone.utc)
        expired = [
            token
            for token, data in self._tokens.items()
            if data.expires_at and data.expires_at < now
        ]
        for token in expired:
            del self._tokens[token]
        return len(expired)


# Convenience functions
def generate_access_token(
    secret: str,
    subject: str,
    scopes: Optional[List[str]] = None,
    expiry_seconds: int = 3600,
) -> str:
    """Generate a simple access token."""
    config = TokenConfig.access_token(secret)
    config = TokenConfig(
        token_type=TokenType.ACCESS,
        expiry_seconds=expiry_seconds,
        secret_key=secret,
    )
    return TokenGenerator(config).generate(subject=subject, scopes=scopes).token


def generate_api_key(secret: str, user_id: str) -> str:
    """Generate a simple API key."""
    config = TokenConfig.api_key(secret)
    return TokenGenerator(config).generate_api_key(user_id=user_id).token


def verify_token(
    token: str,
    secret: str,
    expected_type: TokenType = TokenType.ACCESS,
) -> TokenData:
    """Verify a token and return its data."""
    config = TokenConfig(
        token_type=expected_type,
        secret_key=secret,
    )
    return TokenValidator(config).validate(token, expected_type=expected_type)


def generate_otp(length: int = 6) -> str:
    """Generate a numeric OTP."""
    return "".join(str(secrets.randbelow(10)) for _ in range(length))


def generate_csrf_token() -> str:
    """Generate a CSRF protection token."""
    return secrets.token_hex(32)
