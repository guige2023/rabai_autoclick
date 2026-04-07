"""ID utilities for RabAI AutoClick.

Provides:
- Various ID generation strategies
- ID parsing and validation
- Short ID generation
- Snowflake-like ID generation
"""

import hashlib
import os
import random
import string
import time
import uuid as uuid_lib
from typing import Optional


def generate_uuid() -> str:
    """Generate a UUID v4 string.

    Returns:
        UUID string.
    """
    return str(uuid_lib.uuid4())


def generate_uuid_hex() -> str:
    """Generate a UUID v4 as hex string.

    Returns:
        UUID hex string (no dashes).
    """
    return uuid_lib.uuid4().hex


def generate_short_id(
    length: int = 12,
    alphabet: Optional[str] = None,
) -> str:
    """Generate a short alphanumeric ID.

    Args:
        length: ID length.
        alphabet: Characters to use (default: alphanumeric).

    Returns:
        Short ID string.
    """
    if alphabet is None:
        alphabet = string.ascii_letters + string.digits

    return "".join(random.choices(alphabet, k=length))


def generate_numeric_id(
    length: int = 10,
) -> str:
    """Generate a numeric ID.

    Args:
        length: ID length.

    Returns:
        Numeric ID string.
    """
    return "".join(random.choices(string.digits, k=length))


def generate_alphanumeric_id(
    length: int = 12,
) -> str:
    """Generate an alphanumeric ID.

    Args:
        length: ID length.

    Returns:
        Alphanumeric ID string.
    """
    return generate_short_id(length, string.ascii_letters + string.digits)


def generate_safe_id(
    length: int = 12,
) -> str:
    """Generate a URL-safe ID.

    Args:
        length: ID length.

    Returns:
        URL-safe ID string.
    """
    return generate_short_id(length, string.ascii_letters + string.digits)


def generate_snowflake_id(
    node_id: int = 1,
    epoch: int = 1609459200000,  # 2021-01-01 in ms
) -> int:
    """Generate a snowflake-style ID.

    Args:
        node_id: Node/worker ID (0-1023).
        epoch: Custom epoch in milliseconds.

    Returns:
        Snowflake ID as integer.
    """
    timestamp = int(time.time() * 1000) - epoch
    if timestamp < 0:
        timestamp = 0

    node_id_bits = node_id & 0x3FF  # 10 bits
    sequence = random.randint(0, 4095)  # 12 bits

    snowflake_id = (timestamp << 22) | (node_id_bits << 12) | sequence
    return snowflake_id


def generate_snowflake_id_hex(
    node_id: int = 1,
    epoch: int = 1609459200000,
) -> str:
    """Generate a snowflake ID as hex string.

    Args:
        node_id: Node/worker ID.
        epoch: Custom epoch.

    Returns:
        Snowflake ID as hex string.
    """
    return format(generate_snowflake_id(node_id, epoch), "x")


def hash_to_id(
    data: str,
    prefix: str = "",
    length: int = 12,
) -> str:
    """Generate a hash-based short ID.

    Args:
        data: Data to hash.
        prefix: Optional prefix.
        length: Length of hash portion.

    Returns:
        Hash-based ID.
    """
    hash_hex = hashlib.sha256(data.encode()).hexdigest()[:length]
    if prefix:
        return f"{prefix}_{hash_hex}"
    return hash_hex


def generate_batch_ids(
    count: int,
    prefix: str = "",
    generator: callable = generate_short_id,
) -> list[str]:
    """Generate multiple IDs.

    Args:
        count: Number of IDs to generate.
        prefix: Optional prefix for each ID.
        generator: ID generator function.

    Returns:
        List of generated IDs.
    """
    ids = [generator() for _ in range(count)]
    if prefix:
        return [f"{prefix}_{id_}" for id_ in ids]
    return ids


def validate_uuid(uuid_str: str) -> bool:
    """Validate a UUID string.

    Args:
        uuid_str: UUID string to validate.

    Returns:
        True if valid UUID.
    """
    try:
        uuid_lib.UUID(uuid_str)
        return True
    except ValueError:
        return False


def is_valid_short_id(
    id_str: str,
    alphabet: Optional[str] = None,
    length: Optional[int] = None,
) -> bool:
    """Check if a string is a valid short ID.

    Args:
        id_str: ID to validate.
        alphabet: Expected alphabet.
        length: Expected length.

    Returns:
        True if valid.
    """
    if alphabet is None:
        alphabet = string.ascii_letters + string.digits

    if length is not None and len(id_str) != length:
        return False

    return all(c in alphabet for c in id_str)


def generate_node_id() -> str:
    """Generate a unique node/machine ID.

    Returns:
        Node ID string based on hostname + random.
    """
    import socket
    node_data = f"{socket.gethostname()}_{os.urandom(4).hex()}"
    return hashlib.md5(node_data.encode()).hexdigest()[:16]


def generate_timebased_id(
    prefix: str = "",
    include_random: bool = True,
) -> str:
    """Generate a time-based ID.

    Args:
        prefix: Optional prefix.
        include_random: Include random suffix.

    Returns:
        Time-based ID.
    """
    time_part = format(int(time.time() * 1000), "x")
    if include_random:
        random_part = generate_short_id(4)
        id_str = f"{time_part}_{random_part}"
    else:
        id_str = time_part

    if prefix:
        return f"{prefix}_{id_str}"
    return id_str


def generate_uid() -> str:
    """Generate a short unique ID (8 chars).

    Returns:
        8-character unique ID.
    """
    return uuid_lib.uuid4().hex[:8]


def generate_sequence_id(start: int = 1) -> callable:
    """Create a sequence ID generator.

    Args:
        start: Starting number.

    Returns:
        Generator function.
    """
    counter = [start - 1]
    lock = __import__("threading").Lock()

    def next_id() -> int:
        with lock:
            counter[0] += 1
            return counter[0]

    return next_id


class IDGenerator:
    """Configurable ID generator."""

    def __init__(
        self,
        prefix: str = "",
        length: int = 12,
        alphabet: Optional[str] = None,
    ) -> None:
        self._prefix = prefix
        self._length = length
        self._alphabet = alphabet or string.ascii_letters + string.digits

    def generate(self) -> str:
        """Generate an ID."""
        id_ = generate_short_id(self._length, self._alphabet)
        if self._prefix:
            return f"{self._prefix}_{id_}"
        return id_

    def generate_batch(self, count: int) -> list[str]:
        """Generate multiple IDs."""
        return [self.generate() for _ in range(count)]
