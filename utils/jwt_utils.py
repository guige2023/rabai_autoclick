"""JWT utilities: token generation, validation, claims management."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from typing import Any

__all__ = [
    "JWTConfig",
    "JWTToken",
    "create_token",
    "validate_token",
    "decode_token",
]


@dataclass
class JWTConfig:
    """JWT configuration."""

    secret: str
    algorithm: str = "HS256"
    issuer: str = ""
    audience: str = ""
    expiry_seconds: int = 3600


@dataclass
class JWTToken:
    """A decoded JWT token."""

    header: dict[str, Any]
    payload: dict[str, Any]
    signature: str
    raw: str

    @property
    def subject(self) -> str | None:
        return self.payload.get("sub")

    @property
    def is_expired(self) -> bool:
        exp = self.payload.get("exp", 0)
        return time.time() > exp

    @property
    def issued_at(self) -> float:
        return self.payload.get("iat", 0)


def _b64_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _b64_decode(data: str) -> bytes:
    pad = 4 - (len(data) % 4)
    if pad != 4:
        data += "=" * pad
    return base64.urlsafe_b64decode(data)


def create_token(
    payload: dict[str, Any],
    secret: str,
    algorithm: str = "HS256",
    expiry_seconds: int = 3600,
    issuer: str = "",
    audience: str = "",
) -> str:
    """Create a new JWT token."""
    header = {"alg": algorithm, "typ": "JWT"}
    now = time.time()

    full_payload = {
        **payload,
        "iat": int(now),
        "exp": int(now + expiry_seconds),
    }
    if issuer:
        full_payload["iss"] = issuer
    if audience:
        full_payload["aud"] = audience

    header_b64 = _b64_encode(json.dumps(header).encode())
    payload_b64 = _b64_encode(json.dumps(full_payload).encode())

    msg = f"{header_b64}.{payload_b64}".encode()
    key = secret.encode()

    if algorithm == "HS256":
        sig = hmac.new(key, msg, hashlib.sha256).digest()
    elif algorithm == "HS384":
        sig = hmac.new(key, msg, hashlib.sha384).digest()
    else:
        sig = hmac.new(key, msg, hashlib.sha256).digest()

    sig_b64 = _b64_encode(sig)
    return f"{header_b64}.{payload_b64}.{sig_b64}"


def validate_token(
    token: str,
    secret: str,
    issuer: str = "",
    audience: str = "",
) -> bool:
    """Validate a JWT token."""
    try:
        decode_token(token, secret, issuer, audience)
        return True
    except Exception:
        return False


def decode_token(
    token: str,
    secret: str,
    issuer: str = "",
    audience: str = "",
) -> JWTToken:
    """Decode and verify a JWT token."""
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("Invalid token format")

    header_b64, payload_b64, sig_b64 = parts

    msg = f"{header_b64}.{payload_b64}".encode()
    key = secret.encode()
    header = json.loads(_b64_decode(header_b64))

    algorithm = header.get("alg", "HS256")
    if algorithm == "HS256":
        expected = hmac.new(key, msg, hashlib.sha256).digest()
    elif algorithm == "HS384":
        expected = hmac.new(key, msg, hashlib.sha384).digest()
    else:
        expected = hmac.new(key, msg, hashlib.sha256).digest()

    if not hmac.compare_digest(sig_b64, _b64_encode(expected)):
        raise ValueError("Invalid signature")

    payload = json.loads(_b64_decode(payload_b64))

    if "exp" in payload and payload["exp"] < time.time():
        raise ValueError("Token expired")

    if issuer and payload.get("iss") != issuer:
        raise ValueError("Invalid issuer")

    if audience and payload.get("aud") != audience:
        raise ValueError("Invalid audience")

    return JWTToken(
        header=header,
        payload=payload,
        signature=sig_b64,
        raw=token,
    )
