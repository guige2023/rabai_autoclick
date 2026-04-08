"""ID generation utilities.

Provides various ID generation strategies including
UUID, ULID, and short ID generation for automation workflows.
"""

import hashlib
import secrets
import time
import uuid
from typing import Optional


def generate_uuid() -> str:
    """Generate a standard UUID v4.

    Returns:
        UUID string (e.g., "550e8400-e29b-41d4-a716-446655440000").
    """
    return str(uuid.uuid4())


def generate_short_id(length: int = 8) -> str:
    """Generate a random alphanumeric ID.

    Args:
        length: ID length in characters.

    Returns:
        Random alphanumeric string.
    """
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    return "".join(secrets.choice(chars) for _ in range(length))


def generate_numeric_id(length: int = 6) -> str:
    """Generate a random numeric ID.

    Args:
        length: ID length in digits.

    Returns:
        Random numeric string.
    """
    if length <= 0:
        return ""
    max_val = 10 ** length - 1
    return str(secrets.randbelow(max_val + 1)).zfill(length)


def generate_hash_id(content: str, length: int = 12) -> str:
    """Generate deterministic hash ID from content.

    Args:
        content: Content to hash.
        length: Truncate hash to this length.

    Returns:
        Hex hash string.
    """
    return hashlib.sha256(content.encode()).hexdigest()[:length]


def generate_timestamp_id(prefix: Optional[str] = None) -> str:
    """Generate ID with embedded timestamp.

    Args:
        prefix: Optional prefix string.

    Returns:
        ID string with timestamp.
    """
    ts = int(time.time() * 1000)
    random_part = secrets.token_hex(4)
    base = f"{ts:x}-{random_part}"
    if prefix:
        return f"{prefix}_{base}"
    return base


def generate_node_id(node: str = "node") -> str:
    """Generate a unique node-scoped ID.

    Args:
        node: Node identifier.

    Returns:
        Unique node-scoped ID.
    """
    return f"{node}-{generate_timestamp_id()}"


def generate_ulid() -> str:
    """Generate a ULID-like ID.

    Returns:
        26-character ULID string.
    """
    timestamp_ms = int(time.time() * 1000)
    random_part = secrets.token_bytes(16).hex()[:16]
    return f"{_encode_timestamp(timestamp_ms)}{random_part}"


def _encode_timestamp(ms: int) -> str:
    """Encode timestamp in Crockford base32."""
    ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    result = []
    for _ in range(10):
        result.append(ALPHABET[ms & 0x1F])
        ms >>= 5
    return "".join(reversed(result))


def generate_short_hash(content: str, length: int = 8) -> str:
    """Generate short hash from content.

    Args:
        content: Content to hash.
        length: Output length.

    Returns:
        Short hash string.
    """
    return hashlib.md5(content.encode()).hexdigest()[:length]


def is_valid_uuid(s: str) -> bool:
    """Check if string is a valid UUID.

    Args:
        s: String to check.

    Returns:
        True if valid UUID.
    """
    try:
        uuid.UUID(s)
        return True
    except (ValueError, AttributeError):
        return False
