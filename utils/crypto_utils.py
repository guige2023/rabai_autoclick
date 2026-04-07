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
