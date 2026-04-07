"""JWT (JSON Web Token) encode/decode utilities."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from typing import Any

__all__ = ["JWTConfig", "encode_jwt", "decode_jwt", "validate_jwt", "JWTError"]


class JWTError(Exception):
    """Base exception for JWT operations."""
    pass


@dataclass
class JWTConfig:
    """JWT configuration."""
    secret: str
    algorithm: str = "HS256"
    issuer: str | None = None
    audience: str | None = None
    expiry_seconds: float = 3600
    not_before_seconds: float = 0


def _base64url_decode(data: str | bytes) -> bytes:
    if isinstance(data, str):
        data = data.encode("ascii")
    data += b"=" * (4 - len(data) % 4)
    return base64.urlsafe_b64decode(data)


def _base64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _to_base64url(obj: dict[str, Any]) -> str:
    return _base64url_encode(json.dumps(obj, separators=(",", ":")).encode("utf-8"))


def _sign(payload_bytes: bytes, secret: str, algorithm: str) -> str:
    if algorithm.startswith("HS"):
        h = hmac.new(secret.encode("utf-8"), payload_bytes, hashlib.sha256)
        return _base64url_encode(h.digest())
    elif algorithm.startswith("RS"):
        raise NotImplementedError("RSA signatures not implemented in this lightweight module")
    raise JWTError(f"Unsupported algorithm: {algorithm}")


def _header(algorithm: str) -> dict[str, str]:
    return {"typ": "JWT", "alg": algorithm}


def encode_jwt(
    payload: dict[str, Any],
    secret: str,
    algorithm: str = "HS256",
    issuer: str | None = None,
    audience: str | None = None,
    expiry_seconds: float = 3600,
    not_before_seconds: float = 0,
    extra_claims: dict[str, Any] | None = None,
) -> str:
    """Encode a payload into a JWT string."""
    now = time.time()
    claims: dict[str, Any] = {
        "iat": int(now),
        "exp": int(now + expiry_seconds),
        "nbf": int(now + not_before_seconds),
        **payload,
    }
    if issuer:
        claims["iss"] = issuer
    if audience:
        claims["aud"] = audience
    if extra_claims:
        claims.update(extra_claims)

    header_b64 = _to_base64url(_header(algorithm))
    payload_b64 = _to_base64url(claims)
    signing_input = f"{header_b64}.{payload_b64}"
    signature = _sign(signing_input.encode("utf-8"), secret, algorithm)

    return f"{signing_input}.{signature}"


def decode_jwt(
    token: str,
    secret: str,
    algorithms: list[str] | None = None,
    issuer: str | None = None,
    audience: str | None = None,
    skip_verify: bool = False,
) -> dict[str, Any]:
    """Decode and optionally verify a JWT."""
    parts = token.split(".")
    if len(parts) != 3:
        raise JWTError("Token must have 3 parts")

    header_b64, payload_b64, signature_b64 = parts
    algorithms = algorithms or ["HS256"]

    header = json.loads(_base64url_decode(header_b64))
    payload = json.loads(_base64url_decode(payload_b64))
    alg = header.get("alg", "HS256")

    if not skip_verify:
        if alg not in algorithms:
            raise JWTError(f"Algorithm '{alg}' not in allowed list: {algorithms}")

        signing_input = f"{header_b64}.{payload_b64}"
        expected_sig = _sign(signing_input.encode("utf-8"), secret, alg)
        if not hmac.compare_digest(expected_sig, signature_b64):
            raise JWTError("Invalid signature")

        now = time.time()
        if "exp" in payload and payload["exp"] < now:
            raise JWTError("Token has expired")
        if "nbf" in payload and payload["nbf"] > now:
            raise JWTError("Token not yet valid")
        if issuer and payload.get("iss") != issuer:
            raise JWTError(f"Invalid issuer: expected {issuer}, got {payload.get('iss')}")
        if audience and payload.get("aud") != audience:
            raise JWTError(f"Invalid audience: expected {audience}")

    return payload


def validate_jwt(
    token: str,
    secret: str,
    algorithms: list[str] | None = None,
    issuer: str | None = None,
    audience: str | None = None,
) -> tuple[bool, dict[str, Any] | None, str | None]:
    """Validate a JWT and return (is_valid, payload, error_message)."""
    try:
        payload = decode_jwt(token, secret, algorithms, issuer, audience)
        return True, payload, None
    except JWTError as e:
        return False, None, str(e)
