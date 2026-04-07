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


# === Secure Random ===

def generate_random_bytes(n: int) -> bytes:
    """Generate n cryptographically secure random bytes."""
    return secrets.token_bytes(n)


def generate_random_hex(n: int) -> str:
    """Generate n cryptographically secure random hex digits."""
    return secrets.token_hex(n)


def generate_random_urlsafe(n: int) -> str:
    """Generate n cryptographically secure URL-safe random bytes."""
    return secrets.token_urlsafe(n)


def generate_password(
    length: int = 16,
    uppercase: bool = True,
    lowercase: bool = True,
    digits: bool = True,
    punctuation: bool = True
) -> str:
    """Generate a secure random password."""
    chars = ""
    if uppercase:
        chars += string.ascii_uppercase
    if lowercase:
        chars += string.ascii_lowercase
    if digits:
        chars += string.digits
    if punctuation:
        chars += string.punctuation
    if not chars:
        chars = string.ascii_letters + string.digits
    return "".join(secrets.choice(chars) for _ in range(length))


def generate_apikey(prefix: str = "sk", total_length: int = 32) -> str:
    """Generate an API key with a prefix."""
    random_part = secrets.token_hex(total_length // 2)
    return f"{prefix}_{random_part}"


# === Base64 ===

def base64_encode(data: BytesOrStr, url_safe: bool = False) -> str:
    """Encode data to base64 string."""
    b = _to_bytes(data)
    encoded = base64.urlsafe_b64encode(b) if url_safe else base64.b64encode(b)
    return encoded.decode().rstrip("=")


def base64_decode(data: str, url_safe: bool = False) -> bytes:
    """Decode base64 string to bytes."""
    padding = 4 - len(data) % 4
    if padding != 4:
        data += "=" * padding
    return base64.urlsafe_b64decode(data) if url_safe else base64.b64decode(data)


# === Key Derivation ===

def derive_key(
    password: BytesOrStr,
    salt: Optional[bytes] = None,
    key_length: int = 32,
    iterations: int = 100000
) -> Tuple[bytes, str]:
    """Derive a key from password using PBKDF2."""
    if salt is None:
        salt = secrets.token_bytes(16)
    elif isinstance(salt, str):
        salt = salt.encode()
    key = hashlib.pbkdf2_hmac(
        "sha256", _to_bytes(password), salt, iterations, dklen=key_length
    )
    return key, salt.hex()


def secure_compare(a: BytesOrStr, b: BytesOrStr) -> bool:
    """Constant-time string comparison to prevent timing attacks."""
    return hmac.compare_digest(_to_bytes(a), _to_bytes(b))
