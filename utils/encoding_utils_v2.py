"""
Encoding utilities v2 — advanced encoding and cipher utilities.

Companion to encoding_utils.py. Adds ASCII85, Base32, Percent-encoding variants,
and cryptographic hash encoding utilities.
"""

from __future__ import annotations

import base64
import hashlib
import secrets


def base32_encode(data: bytes | str) -> str:
    """
    Encode data as Base32.

    Args:
        data: Bytes or string to encode

    Returns:
        Base32-encoded string

    Example:
        >>> base32_encode("Hello")
        'JBSWY3DP'
    """
    if isinstance(data, str):
        data = data.encode("utf-8")
    return base64.b32encode(data).decode("ascii").rstrip("=")


def base32_decode(data: str) -> bytes:
    """
    Decode a Base32 string.

    Args:
        data: Base32-encoded string

    Returns:
        Decoded bytes
    """
    rem = len(data) % 8
    if rem:
        data += "=" * (8 - rem)
    return base64.b32decode(data)


def ascii85_encode(data: bytes | str) -> str:
    """
    Encode data using ASCII85 encoding.

    Args:
        data: Bytes or string to encode

    Returns:
        ASCII85-encoded string
    """
    if isinstance(data, str):
        data = data.encode("utf-8")
    result = []
    i = 0
    while i < len(data):
        chunk = data[i:i + 4]
        if len(chunk) < 4:
            chunk = chunk + b"\x00" * (4 - len(chunk))
            padded = True
        else:
            padded = False
        n = int.from_bytes(chunk, "big")
        if n == 0 and not padded:
            result.append("z" if not padded else "")
        else:
            chars = []
            for _ in range(5):
                chars.append(chr((n % 85) + 33))
                n //= 85
            result.append("".join(reversed(chars)))
        i += 4
    return "".join(result)


def ascii85_decode(data: str) -> bytes:
    """
    Decode an ASCII85-encoded string.

    Args:
        data: ASCII85-encoded string

    Returns:
        Decoded bytes
    """
    result = []
    i = 0
    data = data.replace("z", "!!!!!")
    while i < len(data):
        chunk = data[i:i + 5]
        n = 0
        for c in chunk:
            n = n * 85 + (ord(c) - 33)
        chunk_bytes = (n.to_bytes(4, "big"))[:len(chunk) - 4 or 4]
        if len(chunk) < 5:
            chunk_bytes = chunk_bytes[:len(chunk) - 1]
        result.append(chunk_bytes)
        i += 5
    return b"".join(result)


def percent_encode(s: str, safe: str = "") -> str:
    """
    Encode string for use in URI path components.

    Args:
        s: String to encode
        safe: Characters to keep unencoded

    Returns:
        Percent-encoded string
    """
    import urllib.parse
    return urllib.parse.quote(s, safe=safe)


def percent_decode(s: str) -> str:
    """
    Decode percent-encoded string.

    Args:
        s: Percent-encoded string

    Returns:
        Decoded string
    """
    import urllib.parse
    return urllib.parse.unquote(s)


def punycode_encode(s: str) -> str:
    """
    Encode string as Punycode (internationalized domain names).

    Args:
        s: Unicode string

    Returns:
        Punycode string
    """
    import idna
    return idna.encode(s).decode("ascii")


def punycode_decode(s: str) -> str:
    """
    Decode Punycode string.

    Args:
        s: Punycode string

    Returns:
        Unicode string
    """
    import idna
    return idna.decode(s)


def md5_hex(data: bytes | str) -> str:
    """Compute MD5 hash as hex string."""
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.md5(data).hexdigest()


def sha1_hex(data: bytes | str) -> str:
    """Compute SHA-1 hash as hex string."""
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha1(data).hexdigest()


def sha256_hex(data: bytes | str) -> str:
    """Compute SHA-256 hash as hex string."""
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def sha512_hex(data: bytes | str) -> str:
    """Compute SHA-512 hash as hex string."""
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha512(data).hexdigest()


def hmac_sha256(key: bytes | str, msg: bytes | str) -> str:
    """
    Compute HMAC-SHA256.

    Args:
        key: Secret key
        msg: Message

    Returns:
        HMAC as hex string
    """
    import hmac
    if isinstance(key, str):
        key = key.encode("utf-8")
    if isinstance(msg, str):
        msg = msg.encode("utf-8")
    return hmac.new(key, msg, hashlib.sha256).hexdigest()


def argon2_hash(password: str, salt: bytes | None = None) -> str:
    """
    Hash password using Argon2.

    Args:
        password: Password to hash
        salt: Optional salt (generated if not provided)

    Returns:
        Argon2 hash string
    """
    try:
        import argon2
        if salt is None:
            salt = secrets.token_bytes(16)
        ph = argon2.PasswordHasher()
        return ph.hash(password)
    except ImportError:
        import hashlib
        import secrets
        if salt is None:
            salt = secrets.token_bytes(16)
        return hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100000).hex()


def bcrypt_hash(password: str, rounds: int = 12) -> str:
    """
    Hash password using bcrypt.

    Args:
        password: Password to hash
        rounds: Cost factor (higher = more secure but slower)

    Returns:
        bcrypt hash string
    """
    import bcrypt
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt(rounds)).decode()


def bcrypt_verify(password: str, hashed: str) -> bool:
    """Verify bcrypt hash."""
    import bcrypt
    return bcrypt.checkpw(password.encode(), hashed.encode())


def uuid4_str() -> str:
    """Generate a random UUID4 string."""
    return str(secrets.token_uuid())


def x509_dummy(name: str) -> dict[str, str]:
    """
    Create a dummy X.509 certificate info dict.

    Args:
        name: Common name

    Returns:
        Dict with subject fields
    """
    return {
        "subject": f"CN={name}",
        "issuer": f"CN=Dummy CA",
        "serial": secrets.token_hex(8),
        "not_before": "2024-01-01T00:00:00Z",
        "not_after": "2025-01-01T00:00:00Z",
    }


def hexlify(data: bytes | str) -> str:
    """Hexlify data (alias for hex_encode)."""
    if isinstance(data, str):
        data = data.encode("utf-8")
    return data.hex()


def unhexlify(s: str) -> bytes:
    """Unhexlify string (alias for hex_decode)."""
    return bytes.fromhex(s)


def charset_encode(s: str, src_charset: str, dst_charset: str) -> bytes:
    """
    Encode string from one charset to another.

    Args:
        s: String to encode
        src_charset: Source charset name
        dst_charset: Target charset name

    Returns:
        Encoded bytes
    """
    return s.encode(src_charset).decode(dst_charset).encode(dst_charset)
