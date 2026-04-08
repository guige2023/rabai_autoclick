"""UUID utilities for RabAI AutoClick.

Provides:
- UUID generation and manipulation
- Short UUID creation
- UUID parsing and formatting
- UUID-based utilities
"""

from __future__ import annotations

import uuid
from typing import (
    List,
    Optional,
)


def generate_uuid() -> str:
    """Generate a random UUID string.

    Returns:
        UUID string (e.g., '550e8400-e29b-41d4-a716-446655440000').
    """
    return str(uuid.uuid4())


def generate_short_uuid(length: int = 12) -> str:
    """Generate a short UUID string.

    Args:
        length: Length of the UUID string.

    Returns:
        Short UUID string.
    """
    return uuid.uuid4().hex[:length]


def generate_uuid1(node: Optional[int] = None, clock_seq: Optional[int] = None) -> str:
    """Generate UUID from host ID, timestamp, and random numbers.

    Args:
        node: Optional node ID.
        clock_seq: Optional clock sequence.

    Returns:
        UUID string.
    """
    return str(uuid.uuid1(node=node, clock_seq=clock_seq))


def generate_uuid3(namespace: str, name: str) -> str:
    """Generate deterministic UUID from namespace and name using MD5.

    Args:
        namespace: Namespace string.
        name: Name string.

    Returns:
        UUID string.
    """
    return str(uuid.uuid3(uuid.NAMESPACE_DNS, f"{namespace}:{name}"))


def generate_uuid5(namespace: str, name: str) -> str:
    """Generate deterministic UUID from namespace and name using SHA-1.

    Args:
        namespace: Namespace string.
        name: Name string.

    Returns:
        UUID string.
    """
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{namespace}:{name}"))


def is_valid_uuid(value: str) -> bool:
    """Check if a string is a valid UUID.

    Args:
        value: String to check.

    Returns:
        True if valid UUID format.
    """
    try:
        uuid.UUID(value)
        return True
    except (ValueError, AttributeError):
        return False


def parse_uuid(value: str) -> Optional[uuid.UUID]:
    """Parse a string into a UUID object.

    Args:
        value: UUID string.

    Returns:
        UUID object or None if invalid.
    """
    try:
        return uuid.UUID(value)
    except ValueError:
        return None


def uuid_to_int(u: str) -> int:
    """Convert UUID string to integer.

    Args:
        u: UUID string.

    Returns:
        Integer representation.
    """
    return int(uuid.UUID(u))


def int_to_uuid(i: int) -> str:
    """Convert integer to UUID string.

    Args:
        i: Integer representation.

    Returns:
        UUID string.
    """
    return str(uuid.UUID(int=i))


__all__ = [
    "generate_uuid",
    "generate_short_uuid",
    "generate_uuid1",
    "generate_uuid3",
    "generate_uuid5",
    "is_valid_uuid",
    "parse_uuid",
    "uuid_to_int",
    "int_to_uuid",
]
