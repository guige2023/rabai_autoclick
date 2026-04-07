"""
Cryptographic utilities for hashing, HMAC, secure random, and encryption helpers.
"""

import hashlib
import hmac
import secrets
import base64
import string
from typing import Union, Optional, Tuple


BytesOrStr = Union[bytes, str]


def _to_bytes(data: BytesOrStr, encoding: str = "utf-8") -> bytes:
    """Convert input to bytes."""
    if isinstance(data, str):
        return data.encode(encoding)
    return data


# === Hashing Functions ===

def hash_sha256(data: BytesOrStr, raw: bool = False) -> Union[bytes, str]:
    """Compute SHA-256 hash."""
    h = hashlib.sha256(_to_bytes(data)).digest()
    return h if raw else h.hex()


def hash_sha512(data: BytesOrStr, raw: bool = False) -> Union[bytes, str]:
    """Compute SHA-512 hash."""
    h = hashlib.sha512(_to_bytes(data)).digest()
    return h if raw else h.hex()


def hash_blake2b(data: BytesOrStr, digest_size: int = 32, raw: bool = False) -> Union[bytes, str]:
    """Compute BLAKE2b hash."""
    h = hashlib.blake2b(_to_bytes(data), digest_size=digest_size).digest()
    return h if raw else h.hex()


# === Password Hashing ===

def hash_password(
    password: str,
    salt: Optional[bytes] = None,
    iterations: int = 100000,
    digest_size: int = 32
) -> Tuple[str, str]:
    """
    Hash a password using PBKDF2-HMAC-SHA256.

    Returns:
        (hex_hash, hex_salt) tuple
    """
    if salt is None:
        salt = secrets.token_bytes(16)
    elif isinstance(salt, str):
        salt = salt.encode()

    key = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode(),
        salt,
        iterations,
        dklen=digest_size
    )
    return key.hex(), salt.hex()


def verify_password(
    password: str,
    hashed: str,
    salt: Union[bytes, str],
    iterations: int = 100000
) -> bool:
    """Verify a password against a stored hash."""
    if isinstance(salt, str):
        salt = bytes.fromhex(salt)
    computed, _ = hash_password(password, salt, iterations)
    return secrets.compare_digest(computed, hashed)


# === HMAC ===

def hmac_sha256(key: BytesOrStr, data: BytesOrStr, raw: bool = False) -> Union[bytes, str]:
    """Compute HMAC-SHA256."""
    h = hmac.new(_to_bytes(key), _to_bytes(data), hashlib.sha256).digest()
    return h if raw else h.hex()


def hmac_sha512(key: BytesOrStr, data: BytesOrStr, raw: bool = False) -> Union[bytes, str]:
    """Compute HMAC-SHA512."""
    h = hmac.new(_to_bytes(key), _to_bytes(data), hashlib.sha512).digest()
    return h if raw else h.hex()


def hmac_verify(
    key: BytesOrStr,
    data: BytesOrStr,
    signature: BytesOrStr
) -> bool:
    """Verify HMAC signature using constant-time comparison."""
    if isinstance(signature, str):
        signature = bytes.fromhex(signature)
    expected = hmac.new(_to_bytes(key), _to_bytes(data), hashlib.sha256).digest()
    return hmac.compare_digest(expected, signature)
