"""UUID generation and manipulation utilities.

Provides various UUID formats and utilities for
generating unique identifiers in automation workflows.
"""

import uuid
from typing import Optional


def generate_uuid4() -> str:
    """Generate standard UUID v4.

    Returns:
        UUID string like "550e8400-e29b-41d4-a716-446655440000".
    """
    return str(uuid.uuid4())


def generate_uuid4_hex() -> str:
    """Generate UUID v4 as hex string (no dashes).

    Returns:
        UUID hex string like "550e8400e29b41d4a716446655440000".
    """
    return uuid.uuid4().hex


def generate_uuid1() -> str:
    """Generate UUID v1 (time-based).

    Returns:
        UUID string.
    """
    return str(uuid.uuid1())


def generate_namespace_uuid(namespace: str, name: str) -> str:
    """Generate deterministic UUID from namespace and name.

    Args:
        namespace: Namespace string (e.g., DNS, URL, OID).
        name: Name within namespace.

    Returns:
        UUID string.
    """
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{namespace}:{name}"))


def is_valid_uuid(value: str) -> bool:
    """Check if string is a valid UUID.

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


def extract_uuid_parts(uuid_str: str) -> dict:
    """Extract parts from UUID string.

    Args:
        uuid_str: UUID string.

    Returns:
        Dict with uuid components.

    Raises:
        ValueError: If not a valid UUID.
    """
    u = uuid.UUID(uuid_str)
    return {
        "hex": u.hex,
        "int": u.int,
        "urn": str(u),
        "variant": u.variant,
        "version": u.version,
        "time": u.time,
        "time_low": u.time_low,
        "time_mid": u.time_mid,
        "time_hi_version": u.time_hi_version,
        "clock_seq": u.clock_seq,
        "node": u.node,
    }


def short_uuid(length: int = 8) -> str:
    """Generate short UUID-like string.

    Args:
        length: Length of string (max 32).

    Returns:
        Short UUID string.
    """
    return uuid.uuid4().hex[:length]


def sequential_uuid(prefix: Optional[str] = None) -> str:
    """Generate UUID with optional prefix.

    Args:
        prefix: Optional prefix string.

    Returns:
        UUID string with prefix.
    """
    uid = uuid.uuid4().hex
    if prefix:
        return f"{prefix}_{uid}"
    return uid


class UUIDGenerator:
    """Configurable UUID generator.

    Example:
        gen = UUIDGenerator(prefix="device")
        id1 = gen.generate()  # "device-xxxx-xxxx-xxxx-xxxx"
    """

    def __init__(
        self,
        prefix: Optional[str] = None,
        uppercase: bool = False,
        hyphenate: bool = True,
    ) -> None:
        self._prefix = prefix
        self._uppercase = uppercase
        self._hyphenate = hyphenate

    def generate(self) -> str:
        """Generate a UUID.

        Returns:
            UUID string.
        """
        u = uuid.uuid4()
        result = str(u) if self._hyphenate else u.hex
        if self._uppercase:
            result = result.upper()
        if self._prefix:
            result = f"{self._prefix}-{result}"
        return result

    def generate_batch(self, count: int) -> list:
        """Generate multiple UUIDs.

        Args:
            count: Number of UUIDs to generate.

        Returns:
            List of UUID strings.
        """
        return [self.generate() for _ in range(count)]
