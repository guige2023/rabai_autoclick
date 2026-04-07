"""JWT utilities: encoding, decoding, validation, and claims extraction."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "JWTConfig",
    "JWTDecoder",
    "JWTEncoder",
    "decode_jwt",
    "encode_jwt",
    "validate_jwt",
]


@dataclass
class JWTConfig:
    """JWT configuration with algorithm and secret."""

    secret: str
    algorithm: str = "HS256"
    issuer: str = ""
    audience: str = ""


def _base64url_encode(data: bytes) -> str:
    """Base64URL encode bytes."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _base64url_decode(data: str) -> bytes:
    """Base64URL decode a string."""
    padding = 4 - (len(data) % 4)
    if padding != 4:
        data += "=" * padding
    return base64.urlsafe_b64decode(data)


@dataclass
class JWTDecoder:
    """Decode and validate JWT tokens."""

    def __init__(self, config: JWTConfig) -> None:
        self.config = config

    def decode(self, token: str) -> dict[str, Any]:
        """Decode a JWT token without verification (for inspection)."""
        parts = token.split(".")
        if len(parts) != 3:
            raise ValueError("Invalid JWT format")

        header = json.loads(_base64url_decode(parts[0]))
        payload = json.loads(_base64url_decode(parts[1]))
        signature = parts[2]

        return {
            "header": header,
            "payload": payload,
            "signature": signature,
        }

    def verify(self, token: str) -> dict[str, Any]:
        """Verify and decode a JWT token."""
        parts = token.split(".")
        if len(parts) != 3:
            raise ValueError("Invalid JWT format")

        header_b64, payload_b64, signature_b64 = parts[0], parts[1], parts[2]

        expected_sig = self._compute_signature(header_b64, payload_b64)
        if not hmac.compare_digest(signature_b64, expected_sig):
            raise ValueError("Invalid signature")

        payload = json.loads(_base64url_decode(payload_b64))

        if "exp" in payload and payload["exp"] < time.time():
            raise ValueError("Token has expired")

        if self.config.issuer and payload.get("iss") != self.config.issuer:
            raise ValueError("Invalid issuer")

        if self.config.audience and payload.get("aud") != self.config.audience:
            raise ValueError("Invalid audience")

        return payload

    def _compute_signature(self, header_b64: str, payload_b64: str) -> str:
        """Compute expected signature for verification."""
        if self.config.algorithm == "HS256":
            msg = f"{header_b64}.{payload_b64}".encode()
            key = self.config.secret.encode()
            sig = hmac.new(key, msg, hashlib.sha256).digest()
            return _base64url_encode(sig)
        return ""


class JWTEncoder:
    """Encode JWT tokens."""

    def __init__(self, config: JWTConfig) -> None:
        self.config = config

    def encode(self, payload: dict[str, Any]) -> str:
        """Encode a payload into a JWT token."""
        header = {"alg": self.config.algorithm, "typ": "JWT"}

        header_b64 = _base64url_encode(json.dumps(header).encode())
        payload_b64 = _base64url_encode(json.dumps(payload).encode())

        signature = self._compute_signature(header_b64, payload_b64)

        return f"{header_b64}.{payload_b64}.{signature}"

    def _compute_signature(self, header_b64: str, payload_b64: str) -> str:
        """Compute JWT signature."""
        if self.config.algorithm == "HS256":
            msg = f"{header_b64}.{payload_b64}".encode()
            key = self.config.secret.encode()
            sig = hmac.new(key, msg, hashlib.sha256).digest()
            return _base64url_encode(sig)
        return ""


def encode_jwt(payload: dict[str, Any], secret: str, **kwargs: Any) -> str:
    """Convenience function to encode a JWT."""
    config = JWTConfig(secret=secret, **kwargs)
    return JWTEncoder(config).encode(payload)


def decode_jwt(token: str, secret: str) -> dict[str, Any]:
    """Convenience function to decode a JWT."""
    config = JWTConfig(secret=secret)
    return JWTDecoder(config).verify(token)


def validate_jwt(token: str, secret: str, required_claims: list[str] | None = None) -> bool:
    """Validate a JWT token and check required claims."""
    try:
        payload = decode_jwt(token, secret)
        if required_claims:
            for claim in required_claims:
                if claim not in payload:
                    return False
        return True
    except Exception:
        return False
