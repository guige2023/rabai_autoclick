"""JWT authentication action module.

Provides JWT token generation and validation
for stateless authentication.
"""

from __future__ import annotations

import time
import json
import hmac
import hashlib
import base64
import logging
from typing import Any, Optional
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class JWTSecret:
    """JWT secret key management."""

    @staticmethod
    def generate_secret(length: int = 32) -> str:
        """Generate random secret.

        Args:
            length: Secret length in bytes

        Returns:
            Base64 encoded secret
        """
        import secrets
        return base64.b64encode(secrets.token_bytes(length)).decode()


class JWTConfig:
    """JWT configuration."""

    def __init__(
        self,
        secret: str,
        algorithm: str = "HS256",
        expiry_seconds: int = 3600,
        issuer: Optional[str] = None,
    ):
        self.secret = secret
        self.algorithm = algorithm
        self.expiry_seconds = expiry_seconds
        self.issuer = issuer


class JWTEncoder:
    """JWT token encoder."""

    def __init__(self, config: JWTConfig):
        self.config = config

    def encode(self, payload: dict[str, Any]) -> str:
        """Encode payload to JWT token.

        Args:
            payload: Token payload

        Returns:
            JWT token string
        """
        header = {"alg": self.config.algorithm, "typ": "JWT"}

        payload["exp"] = int(time.time()) + self.config.expiry_seconds
        payload["iat"] = int(time.time())
        if self.config.issuer:
            payload["iss"] = self.config.issuer

        header_b64 = self._b64_encode(json.dumps(header))
        payload_b64 = self._b64_encode(json.dumps(payload))
        message = f"{header_b64}.{payload_b64}"

        signature = self._sign(message)
        token = f"{message}.{signature}"

        return token

    def _b64_encode(self, data: str) -> str:
        """Base64 URL-safe encode."""
        return base64.urlsafe_b64encode(data.encode()).rstrip(b"=").decode()

    def _sign(self, message: str) -> str:
        """Sign message with secret."""
        if self.config.algorithm.startswith("HS"):
            return hmac.new(
                self.config.secret.encode(),
                message.encode(),
                hashlib.sha256
            ).hexdigest()
        return ""


class JWTDecoder:
    """JWT token decoder."""

    def __init__(self, config: JWTConfig):
        self.config = config

    def decode(self, token: str) -> Optional[dict[str, Any]]:
        """Decode JWT token.

        Args:
            token: JWT token string

        Returns:
            Decoded payload or None
        """
        try:
            parts = token.split(".")
            if len(parts) != 3:
                return None

            header_b64, payload_b64, signature = parts

            message = f"{header_b64}.{payload_b64}"
            expected_sig = self._sign(message)

            if not hmac.compare_digest(signature, expected_sig):
                return None

            payload_json = base64.urlsafe_b64decode(
                payload_b64 + "=" * (4 - len(payload_b64) % 4)
            ).decode()
            payload = json.loads(payload_json)

            if "exp" in payload and payload["exp"] < int(time.time()):
                return None

            return payload

        except Exception as e:
            logger.error(f"JWT decode error: {e}")
            return None

    def _sign(self, message: str) -> str:
        """Sign message with secret."""
        if self.config.algorithm.startswith("HS"):
            return hmac.new(
                self.config.secret.encode(),
                message.encode(),
                hashlib.sha256
            ).hexdigest()
        return ""


class JWTAuth:
    """JWT authentication handler."""

    def __init__(self, config: JWTConfig):
        self.config = config
        self.encoder = JWTEncoder(config)
        self.decoder = JWTDecoder(config)

    def generate_token(self, user_id: str, **extra_claims) -> str:
        """Generate token for user.

        Args:
            user_id: User identifier
            **extra_claims: Additional claims

        Returns:
            JWT token
        """
        payload = {"sub": user_id, **extra_claims}
        return self.encoder.encode(payload)

    def verify_token(self, token: str) -> Optional[dict[str, Any]]:
        """Verify token.

        Args:
            token: JWT token

        Returns:
            Decoded payload or None
        """
        return self.decoder.decode(token)

    def get_user_id(self, token: str) -> Optional[str]:
        """Get user ID from token.

        Args:
            token: JWT token

        Returns:
            User ID or None
        """
        payload = self.verify_token(token)
        return payload.get("sub") if payload else None


def create_jwt_auth(secret: str, **kwargs) -> JWTAuth:
    """Create JWT auth instance.

    Args:
        secret: Secret key
        **kwargs: Additional config

    Returns:
        JWTAuth instance
    """
    config = JWTConfig(secret=secret, **kwargs)
    return JWTAuth(config)
