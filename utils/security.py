"""Security utilities for RabAI AutoClick.

Provides:
- Password hashing
- Encryption helpers
- Secure token generation
"""

import hashlib
import os
import secrets
from typing import Optional


def generate_token(length: int = 32) -> str:
    """Generate cryptographically secure token.

    Args:
        length: Token length in bytes.

    Returns:
        Hex-encoded token string.
    """
    return secrets.token_hex(length)


def generate_password(length: int = 16) -> str:
    """Generate random password.

    Args:
        length: Password length.

    Returns:
        Random password string.
    """
    chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*"
    return ''.join(secrets.choice(chars) for _ in range(length))


def hash_password(password: str, salt: Optional[str] = None) -> tuple:
    """Hash password using PBKDF2.

    Args:
        password: Password to hash.
        salt: Optional salt (generated if not provided).

    Returns:
        Tuple of (hash, salt).
    """
    if salt is None:
        salt = secrets.token_hex(16)

    key = hashlib.pbkdf2_hmac(
        'sha256',
        password.encode('utf-8'),
        salt.encode('utf-8'),
        100000
    )

    return key.hex(), salt


def verify_password(password: str, hashed: str, salt: str) -> bool:
    """Verify password against hash.

    Args:
        password: Password to verify.
        hashed: Expected hash.
        salt: Salt used in hashing.

    Returns:
        True if password matches.
    """
    key, _ = hash_password(password, salt)
    return secrets.compare_digest(key, hashed)


def hash_sha256(data: str) -> str:
    """Generate SHA256 hash.

    Args:
        data: Data to hash.

    Returns:
        Hex-encoded hash.
    """
    return hashlib.sha256(data.encode('utf-8')).hexdigest()


def hash_sha512(data: str) -> str:
    """Generate SHA512 hash.

    Args:
        data: Data to hash.

    Returns:
        Hex-encoded hash.
    """
    return hashlib.sha512(data.encode('utf-8')).hexdigest()


def secure_compare(a: str, b: str) -> bool:
    """Constant-time string comparison.

    Args:
        a: First string.
        b: Second string.

    Returns:
        True if equal.
    """
    return secrets.compare_digest(a.encode('utf-8'), b.encode('utf-8'))


def mask_sensitive(text: str, visible_chars: int = 4) -> str:
    """Mask sensitive information.

    Args:
        text: Text to mask.
        visible_chars: Number of visible characters at end.

    Returns:
        Masked text.
    """
    if len(text) <= visible_chars:
        return '*' * len(text)

    masked = '*' * (len(text) - visible_chars)
    return masked + text[-visible_chars:]


def generate_api_key() -> str:
    """Generate API key.

    Returns:
        Formatted API key.
    """
    prefix = "rbai"
    key = secrets.token_urlsafe(32)
    return f"{prefix}_{key}"


class RateLimiter:
    """Simple rate limiter for API operations.

    Tracks call counts per key with automatic expiration.
    """

    def __init__(self, max_calls: int = 100, window_seconds: int = 60) -> None:
        """Initialize rate limiter.

        Args:
            max_calls: Maximum calls per window.
            window_seconds: Time window in seconds.
        """
        self.max_calls = max_calls
        self.window_seconds = window_seconds
        self._calls: dict = {}

    def is_allowed(self, key: str) -> bool:
        """Check if call is allowed.

        Args:
            key: Identifier for caller.

        Returns:
            True if call allowed.
        """
        import time

        now = time.time()

        if key not in self._calls:
            self._calls[key] = []

        # Remove expired entries
        self._calls[key] = [
            t for t in self._calls[key]
            if now - t < self.window_seconds
        ]

        if len(self._calls[key]) < self.max_calls:
            self._calls[key].append(now)
            return True

        return False


def sanitize_filename(filename: str) -> str:
    """Sanitize filename for safe storage.

    Args:
        filename: Original filename.

    Returns:
        Sanitized filename.
    """
    import re
    # Remove invalid characters
    filename = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', filename)
    # Remove leading/trailing whitespace and dots
    filename = filename.strip('. ')
    # Limit length
    if len(filename) > 255:
        name, ext = os.path.splitext(filename)
        filename = name[:255 - len(ext)] + ext
    return filename