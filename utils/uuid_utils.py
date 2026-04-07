"""UUID utilities for RabAI AutoClick.

Provides:
- UUID generation and manipulation
- UUID parsing and formatting
- UUID validation
"""

import uuid
from typing import List, Optional


def generate() -> str:
    """Generate a random UUID string.

    Returns:
        UUID string.
    """
    return str(uuid.uuid4())


def generate_hex() -> str:
    """Generate UUID without hyphens.

    Returns:
        UUID hex string.
    """
    return uuid.uuid4().hex


def generate_int() -> int:
    """Generate UUID as integer.

    Returns:
        UUID integer.
    """
    return uuid.uuid4().int


def generate_time() -> str:
    """Generate time-based UUID string.

    Returns:
        UUID string.
    """
    return str(uuid.uuid1())


def generate_safe() -> str:
    """Generate safe UUID string.

    Returns:
        UUID string.
    """
    return str(uuid.uuid4())


def parse(uuid_str: str) -> Optional[uuid.UUID]:
    """Parse UUID string.

    Args:
        uuid_str: UUID string to parse.

    Returns:
        UUID object or None.
    """
    try:
        return uuid.UUID(uuid_str)
    except (ValueError, AttributeError):
        return None


def is_valid(uuid_str: str) -> bool:
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


def to_hex(uuid_str: str) -> Optional[str]:
    """Convert UUID to hex string.

    Args:
        uuid_str: UUID string.

    Returns:
        Hex string or None.
    """
    parsed = parse(uuid_str)
    if parsed is None:
        return None
    return parsed.hex


def to_int(uuid_str: str) -> Optional[int]:
    """Convert UUID to integer.

    Args:
        uuid_str: UUID string.

    Returns:
        Integer or None.
    """
    parsed = parse(uuid_str)
    if parsed is None:
        return None
    return parsed.int


def to_bytes(uuid_str: str) -> Optional[bytes]:
    """Convert UUID to bytes.

    Args:
        uuid_str: UUID string.

    Returns:
        Bytes or None.
    """
    parsed = parse(uuid_str)
    if parsed is None:
        return None
    return parsed.bytes


def from_hex(hex_str: str) -> Optional[str]:
    """Create UUID from hex string.

    Args:
        hex_str: Hex string.

    Returns:
        UUID string or None.
    """
    try:
        return str(uuid.UUID(hex=hex_str))
    except (ValueError, AttributeError):
        return None


def from_int(int_val: int) -> Optional[str]:
    """Create UUID from integer.

    Args:
        int_val: Integer value.

    Returns:
        UUID string or None.
    """
    try:
        return str(uuid.UUID(int=int_val))
    except (ValueError, AttributeError):
        return None


def from_bytes(bytes_val: bytes) -> Optional[str]:
    """Create UUID from bytes.

    Args:
        bytes_val: Bytes value.

    Returns:
        UUID string or None.
    """
    try:
        return str(uuid.UUID(bytes=bytes_val))
    except (ValueError, AttributeError):
        return None


def get_version(uuid_str: str) -> Optional[int]:
    """Get UUID version.

    Args:
        uuid_str: UUID string.

    Returns:
        Version number or None.
    """
    parsed = parse(uuid_str)
    if parsed is None:
        return None
    return parsed.version


def get_variant(uuid_str: str) -> Optional[int]:
    """Get UUID variant.

    Args:
        uuid_str: UUID string.

    Returns:
        Variant number or None.
    """
    parsed = parse(uuid_str)
    if parsed is None:
        return None
    return parsed.variant


def is_time_based(uuid_str: str) -> bool:
    """Check if UUID is time-based (v1).

    Args:
        uuid_str: UUID string.

    Returns:
        True if time-based.
    """
    version = get_version(uuid_str)
    return version == 1


def is_random(uuid_str: str) -> bool:
    """Check if UUID is random (v4).

    Args:
        uuid_str: UUID string.

    Returns:
        True if random.
    """
    version = get_version(uuid_str)
    return version == 4


def is_name_based(uuid_str: str) -> bool:
    """Check if UUID is name-based (v3 or v5).

    Args:
        uuid_str: UUID string.

    Returns:
        True if name-based.
    """
    version = get_version(uuid_str)
    return version in (3, 5)


def make_time_based(node: Optional[int] = None) -> str:
    """Create time-based UUID.

    Args:
        node: Optional node ID.

    Returns:
        UUID string.
    """
    if node is not None:
        return str(uuid.uuid1(node=node))
    return str(uuid.uuid1())


def make_random() -> str:
    """Create random UUID.

    Returns:
        UUID string.
    """
    return str(uuid.uuid4())


def make_md5(namespace: str, name: str) -> str:
    """Create MD5-based UUID.

    Args:
        namespace: Namespace UUID.
        name: Name string.

    Returns:
        UUID string.
    """
    try:
        ns_uuid = uuid.UUID(namespace)
        return str(uuid.uuid3(ns_uuid, name))
    except (ValueError, AttributeError):
        return ""


def make_sha1(namespace: str, name: str) -> str:
    """Create SHA1-based UUID.

    Args:
        namespace: Namespace UUID.
        name: Name string.

    Returns:
        UUID string.
    """
    try:
        ns_uuid = uuid.UUID(namespace)
        return str(uuid.uuid5(ns_uuid, name))
    except (ValueError, AttributeError):
        return ""


def make_namespace_dns() -> str:
    """Get DNS namespace UUID.

    Returns:
        UUID string.
    """
    return str(uuid.NAMESPACE_DNS)


def make_namespace_url() -> str:
    """Get URL namespace UUID.

    Returns:
        UUID string.
    """
    return str(uuid.NAMESPACE_URL)


def make_namespace_oid() -> str:
    """Get OID namespace UUID.

    Returns:
        UUID string.
    """
    return str(uuid.NAMESPACE_OID)


def make_namespace_x500() -> str:
    """Get X500 namespace UUID.

    Returns:
        UUID string.
    """
    return str(uuid.NAMESPACE_X500)


def nil() -> str:
    """Get nil UUID.

    Returns:
        Nil UUID string.
    """
    return "00000000-0000-0000-0000-000000000000"


def is_nil(uuid_str: str) -> bool:
    """Check if UUID is nil.

    Args:
        uuid_str: UUID string.

    Returns:
        True if nil.
    """
    return uuid_str == nil()


def min_uuid() -> str:
    """Get minimum UUID (lowest value).

    Returns:
        Minimum UUID string.
    """
    return "00000000-0000-0000-0000-000000000000"


def max_uuid() -> str:
    """Get maximum UUID (highest value).

    Returns:
        Maximum UUID string.
    """
    return "ffffffff-ffff-ffff-ffff-ffffffffffff"


def compare(a: str, b: str) -> int:
    """Compare two UUIDs.

    Args:
        a: First UUID.
        b: Second UUID.

    Returns:
        -1 if a < b, 0 if a == b, 1 if a > b.
    """
    parsed_a = parse(a)
    parsed_b = parse(b)
    if parsed_a is None or parsed_b is None:
        return 0
    if parsed_a < parsed_b:
        return -1
    if parsed_a > parsed_b:
        return 1
    return 0


def sort_uuids(uuids: List[str]) -> List[str]:
    """Sort UUID strings.

    Args:
        uuids: List of UUID strings.

    Returns:
        Sorted list.
    """
    def sort_key(s):
        p = parse(s)
        return p if p else uuid.NIL_UUID
    return sorted(uuids, key=sort_key)


def deduplicate_uuids(uuids: List[str]) -> List[str]:
    """Remove duplicate UUIDs while preserving order.

    Args:
        uuids: List of UUID strings.

    Returns:
        Deduplicated list.
    """
    seen = set()
    result = []
    for u in uuids:
        if u not in seen and is_valid(u):
            seen.add(u)
            result.append(u)
    return result