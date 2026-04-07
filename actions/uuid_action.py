"""uuid action extensions for rabai_autoclick.

Provides utilities for generating and manipulating UUIDs,
including various UUID versions and conversion utilities.
"""

from __future__ import annotations

import uuid
from typing import Any

__all__ = [
    "uuid1",
    "uuid4",
    "uuid3",
    "uuid5",
    "generate_uuid",
    "generate_uuid1",
    "generate_uuid4",
    "parse_uuid",
    "is_valid_uuid",
    "uuid_to_bytes",
    "bytes_to_uuid",
    "uuid_to_int",
    "int_to_uuid",
    "uuid_to_str",
    "str_to_uuid",
    "uuid_to_hex",
    "hex_to_uuid",
    "get_uuid_version",
    "get_uuid_variant",
    "get_uuid_fields",
    "compact_uuid",
    "expand_uuid",
    "is_null_uuid",
    "is_max_uuid",
    "random_uuid_string",
    "ordered_uuid",
    "UUIDBuilder",
    "UUIDNamespace",
]


class UUIDNamespace:
    """Well-known UUID namespaces."""

    NIL = uuid.UUID(int=0)
    DNS = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
    URL = uuid.UUID("6ba7b811-9dad-11d1-80b4-00c04fd430c8")
    OID = uuid.UUID("6ba7b812-9dad-11d1-80b4-00c04fd430c8")
    X500 = uuid.UUID("6ba7b814-9dad-11d1-80b4-00c04fd430c8")


def generate_uuid() -> str:
    """Generate a random UUID string.

    Returns:
        UUID string.
    """
    return str(uuid.uuid4())


def generate_uuid1() -> str:
    """Generate UUID version 1 (timestamp-based).

    Returns:
        UUID string.
    """
    return str(uuid.uuid1())


def generate_uuid4() -> str:
    """Generate UUID version 4 (random).

    Returns:
        UUID string.
    """
    return str(uuid.uuid4())


def parse_uuid(uuid_str: str) -> uuid.UUID:
    """Parse UUID string to UUID object.

    Args:
        uuid_str: UUID string.

    Returns:
        UUID object.

    Raises:
        ValueError: If not valid UUID.
    """
    return uuid.UUID(uuid_str)


def is_valid_uuid(uuid_str: str) -> bool:
    """Check if string is valid UUID.

    Args:
        uuid_str: String to check.

    Returns:
        True if valid UUID.
    """
    try:
        uuid.UUID(uuid_str)
        return True
    except (ValueError, AttributeError):
        return False


def uuid_to_bytes(u: uuid.UUID | str) -> bytes:
    """Convert UUID to bytes.

    Args:
        u: UUID object or string.

    Returns:
        16-byte representation.
    """
    if isinstance(u, str):
        u = uuid.UUID(u)
    return u.bytes


def bytes_to_uuid(data: bytes) -> uuid.UUID:
    """Convert bytes to UUID.

    Args:
        data: 16-byte data.

    Returns:
        UUID object.
    """
    return uuid.UUID(bytes=data)


def uuid_to_int(u: uuid.UUID | str) -> int:
    """Convert UUID to integer.

    Args:
        u: UUID object or string.

    Returns:
        Integer representation.
    """
    if isinstance(u, str):
        u = uuid.UUID(u)
    return u.int


def int_to_uuid(value: int) -> uuid.UUID:
    """Convert integer to UUID.

    Args:
        value: Integer value.

    Returns:
        UUID object.
    """
    return uuid.UUID(int=value)


def uuid_to_str(u: uuid.UUID | str) -> str:
    """Convert UUID to string.

    Args:
        u: UUID object or string.

    Returns:
        UUID string.
    """
    if isinstance(u, str):
        return u
    return str(u)


def str_to_uuid(s: str) -> uuid.UUID:
    """Convert string to UUID.

    Args:
        s: UUID string.

    Returns:
        UUID object.
    """
    return uuid.UUID(s)


def uuid_to_hex(u: uuid.UUID | str) -> str:
    """Convert UUID to hex string without dashes.

    Args:
        u: UUID object or string.

    Returns:
        32-character hex string.
    """
    if isinstance(u, str):
        u = uuid.UUID(u)
    return u.hex


def hex_to_uuid(hex_str: str) -> uuid.UUID:
    """Convert hex string to UUID.

    Args:
        hex_str: 32-character hex string.

    Returns:
        UUID object.
    """
    return uuid.UUID(hex=hex_str)


def get_uuid_version(u: uuid.UUID | str) -> int:
    """Get UUID version.

    Args:
        u: UUID object or string.

    Returns:
        Version number (1-5).
    """
    if isinstance(u, str):
        u = uuid.UUID(u)
    return u.version


def get_uuid_variant(u: uuid.UUID | str) -> str:
    """Get UUID variant.

    Args:
        u: UUID object or string.

    Returns:
        Variant name (nil, standard, reserved, future).
    """
    if isinstance(u, str):
        u = uuid.UUID(u)
    variant = u.variant
    if variant == uuid.RFC_4122:
        return "rfc4122"
    elif variant == uuid.NAMESPACE_X500:
        return "x500"
    elif variant == uuid.RESERVED_MICROSOFT:
        return "reserved_microsoft"
    elif variant == uuid.RESERVED_NCS:
        return "reserved_ncs"
    return "unknown"


def get_uuid_fields(u: uuid.UUID | str) -> dict[str, Any]:
    """Get UUID field values.

    Args:
        u: UUID object or string.

    Returns:
        Dict with field values.
    """
    if isinstance(u, str):
        u = uuid.UUID(u)
    return {
        "time_low": u.time_low,
        "time_mid": u.time_mid,
        "time_hi_version": u.time_hi_version,
        "clock_seq_hi_variant": u.clock_seq_hi_variant,
        "clock_seq_low": u.clock_seq_low,
        "node": u.node,
        "time": u.time,
        "time_usec": u.time_usec,
        "clock_seq": u.clock_seq,
    }


def compact_uuid(u: uuid.UUID | str) -> str:
    """Convert UUID to compact form (no dashes).

    Args:
        u: UUID object or string.

    Returns:
        Compact UUID string.
    """
    if isinstance(u, str):
        u = uuid.UUID(u)
    return u.hex


def expand_uuid(compact: str) -> uuid.UUID:
    """Expand compact UUID string.

    Args:
        compact: 32-character hex string.

    Returns:
        UUID object.
    """
    return uuid.UUID(hex=compact)


def is_null_uuid(u: uuid.UUID | str) -> bool:
    """Check if UUID is null (all zeros).

    Args:
        u: UUID object or string.

    Returns:
        True if null UUID.
    """
    if isinstance(u, str):
        u = uuid.UUID(u)
    return u == uuid.UUID(int=0)


def is_max_uuid(u: uuid.UUID | str) -> bool:
    """Check if UUID is maximum value.

    Args:
        u: UUID object or string.

    Returns:
        True if max UUID.
    """
    if isinstance(u, str):
        u = uuid.UUID(u)
    return u == uuid.UUID(int=(1 << 128) - 1)


def random_uuid_string(length: int = 32) -> str:
    """Generate random string of hex characters.

    Args:
        length: Length of string.

    Returns:
        Random hex string.
    """
    return uuid.uuid4().hex[:length]


def ordered_uuid() -> str:
    """Generate UUID with timestamp-like ordering.

    Useful for database keys where time-ordered
    UUIDs perform better.

    Returns:
        Time-ordered UUID string.
    """
    import time
    nanoseconds = int(time.time() * 1e9)
    clock_seq = int(uuid.uuid4().node)
    return str(uuid.UUID(
        fields=(
            (nanoseconds >> 28) & 0xFFFFFFFF,
            (nanoseconds >> 12) & 0xFFFF,
            ((nanoseconds << 4) & 0x0FFF) | (4 << 12),
            ((clock_seq >> 10) & 0x3F) | 0x80,
            clock_seq & 0xFF,
            0,
        ),
        version=7,
    ))


class UUIDBuilder:
    """Builder for constructing UUIDs programmatically."""

    def __init__(self) -> None:
        self._fields: dict[str, int] = {}

    def with_time_low(self, value: int) -> UUIDBuilder:
        """Set time_low field."""
        self._fields["time_low"] = value & 0xFFFFFFFF
        return self

    def with_time_mid(self, value: int) -> UUIDBuilder:
        """Set time_mid field."""
        self._fields["time_mid"] = value & 0xFFFF
        return self

    def with_time_hi_version(self, value: int) -> UUIDBuilder:
        """Set time_hi field."""
        self._fields["time_hi_version"] = value & 0x0FFF
        return self

    def with_clock_seq(self, value: int) -> UUIDBuilder:
        """Set clock_seq field."""
        self._fields["clock_seq"] = value & 0x3FFF
        return self

    def with_node(self, node: int) -> UUIDBuilder:
        """Set node field."""
        self._fields["node"] = node & 0xFFFFFFFFFFFF
        return self

    def with_version(self, version: int) -> UUIDBuilder:
        """Set version (appended to time_hi)."""
        self._fields["_version"] = version & 0x0F
        return self

    def build(self) -> uuid.UUID:
        """Build UUID object.

        Returns:
            UUID object.
        """
        return uuid.UUID(
            time_low=self._fields.get("time_low", 0),
            time_mid=self._fields.get("time_mid", 0),
            time_hi_version=self._fields.get("time_hi_version", 0),
            clock_seq_hi_variant=self._fields.get("clock_seq_hi", 0),
            clock_seq_low=self._fields.get("clock_seq_low", 0),
            node=self._fields.get("node", 0),
        )


def generate_named_uuid(namespace: uuid.UUID, name: str) -> str:
    """Generate deterministic UUID from namespace and name.

    Args:
        namespace: UUID namespace.
        name: Name within namespace.

    Returns:
        UUID string.
    """
    return str(uuid.uuid5(namespace, name))


def generate_dns_uuid(name: str) -> str:
    """Generate DNS UUID (version 5).

    Args:
        name: DNS name.

    Returns:
        UUID string.
    """
    return str(uuid.uuid5(uuid.UUIDNamespace.DNS, name))


def generate_url_uuid(url: str) -> str:
    """Generate URL UUID (version 5).

    Args:
        url: URL string.

    Returns:
        UUID string.
    """
    return str(uuid.uuid5(uuid.UUIDNamespace.URL, url))


def generate_oid_uuid(oid: str) -> str:
    """Generate OID UUID (version 5).

    Args:
        oid: OID string.

    Returns:
        UUID string.
    """
    return str(uuid.uuid5(uuid.UUIDNamespace.OID, oid))


def compare_uuids(a: str | uuid.UUID, b: str | uuid.UUID) -> int:
    """Compare two UUIDs.

    Args:
        a: First UUID.
        b: Second UUID.

    Returns:
        -1 if a < b, 0 if equal, 1 if a > b.
    """
    ua = uuid.UUID(str(a)) if isinstance(a, str) else a
    ub = uuid.UUID(str(b)) if isinstance(b, str) else b
    if ua < ub:
        return -1
    elif ua > ub:
        return 1
    return 0
