"""ID generation utilities for RabAI AutoClick.

Provides:
- Unique ID generation
- UUID generation
- Short ID generation
"""

import hashlib
import os
import threading
import time
import uuid
from typing import Optional


class IDGenerator:
    """Generates unique identifiers.

    Thread-safe ID generation with multiple ID types.
    """

    _counter = 0
    _lock = threading.Lock()

    @classmethod
    def generate(
        cls,
        prefix: str = "",
        suffix: str = "",
        include_timestamp: bool = False,
    ) -> str:
        """Generate unique ID.

        Args:
            prefix: Optional prefix.
            suffix: Optional suffix.
            include_timestamp: If True, include timestamp.

        Returns:
            Generated ID string.
        """
        parts = []

        if prefix:
            parts.append(prefix)

        if include_timestamp:
            parts.append(f"{int(time.time() * 1000)}")

        with cls._lock:
            cls._counter += 1
            parts.append(f"{cls._counter:06d}")

        # Add random component
        parts.append(f"{os.urandom(2).hex()}")

        if suffix:
            parts.append(suffix)

        return "-".join(parts)

    @classmethod
    def uuid4(cls) -> str:
        """Generate UUID4 string.

        Returns:
            UUID4 string without dashes.
        """
        return str(uuid.uuid4())

    @classmethod
    def uuid4_hex(cls) -> str:
        """Generate UUID4 as hex string.

        Returns:
            UUID4 hex string.
        """
        return uuid.uuid4().hex

    @classmethod
    def short_id(cls, length: int = 8) -> str:
        """Generate short ID.

        Args:
            length: ID length (max 32).

        Returns:
            Short ID string.
        """
        return uuid.uuid4().hex[:length]


def generate_id(
    prefix: str = "",
    suffix: str = "",
    include_timestamp: bool = False,
) -> str:
    """Generate unique ID.

    Convenience function using IDGenerator.

    Args:
        prefix: Optional prefix.
        suffix: Optional suffix.
        include_timestamp: If True, include timestamp.

    Returns:
        Generated ID string.
    """
    return IDGenerator.generate(prefix, suffix, include_timestamp)


def generate_uuid() -> str:
    """Generate UUID4 string.

    Returns:
        UUID4 string without dashes.
    """
    return IDGenerator.uuid4()


def generate_short_id(length: int = 8) -> str:
    """Generate short ID.

    Args:
        length: ID length (max 32).

    Returns:
        Short ID string.
    """
    return IDGenerator.short_id(length)


def hash_id(content: str, length: int = 8) -> str:
    """Generate deterministic hash-based ID.

    Args:
        content: Content to hash.
        length: ID length.

    Returns:
        Hash-based ID string.
    """
    h = hashlib.sha256(content.encode()).hexdigest()
    return h[:length]


def generate_workflow_id(workflow_name: str) -> str:
    """Generate workflow ID from name.

    Args:
        workflow_name: Name of workflow.

    Returns:
        Workflow ID.
    """
    normalized = workflow_name.lower().replace(" ", "-")
    normalized = ''.join(c if c.isalnum() or c in '-_' else '' for c in normalized)
    timestamp = int(time.time())
    return f"{normalized}-{timestamp}"


def generate_step_id(step_name: str, index: int) -> str:
    """Generate step ID.

    Args:
        step_name: Name of step.
        index: Step index.

    Returns:
        Step ID.
    """
    normalized = step_name.lower().replace(" ", "_")
    normalized = ''.join(c if c.isalnum() or c == '_' else '' for c in normalized)
    return f"step_{normalized}_{index}"


def generate_action_id(action_type: str) -> str:
    """Generate action ID.

    Args:
        action_type: Type of action.

    Returns:
        Action ID.
    """
    timestamp = int(time.time() * 1000)
    random_part = os.urandom(4).hex()
    return f"action_{action_type}_{timestamp}_{random_part}"


class IDPool:
    """Pool of reusable IDs.

    Useful for managing IDs that can be recycled.
    """

    def __init__(self, prefix: str = "id") -> None:
        """Initialize ID pool.

        Args:
            prefix: Prefix for generated IDs.
        """
        self._prefix = prefix
        self._available: list = []
        self._lock = threading.Lock()
        self._issued: set = set()

    def acquire(self) -> str:
        """Acquire an ID from the pool.

        Returns:
            ID string.
        """
        with self._lock:
            if self._available:
                id_str = self._available.pop()
            else:
                id_str = f"{self._prefix}_{len(self._issued)}_{IDGenerator.uuid4_hex()[:8]}"

            self._issued.add(id_str)
            return id_str

    def release(self, id_str: str) -> None:
        """Release an ID back to the pool.

        Args:
            id_str: ID to release.
        """
        with self._lock:
            if id_str in self._issued:
                self._issued.discard(id_str)
                self._available.append(id_str)

    def __len__(self) -> int:
        """Get number of issued IDs."""
        with self._lock:
            return len(self._issued)


def ulid() -> str:
    """Generate ULID (Universally Unique Lexicographically Sortable Identifier).

    Returns:
        ULID string.
    """
    # ULID format: 00000000000000000000000000 (10 chars time) + (16 chars random)
    timestamp = int(time.time() * 1000)
    random_bytes = os.urandom(10)

    # Encode timestamp in base32
    TIME_CHARS = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    time_encoded = []
    for _ in range(10):
        time_encoded.append(TIME_CHARS[timestamp & 0x1F])
        timestamp >>= 5

    # Encode random in base32
    RAND_CHARS = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    random_encoded = []
    for byte in random_bytes:
        random_encoded.append(RAND_CHARS[(byte >> 4) & 0x1F])
        random_encoded.append(RAND_CHARS[byte & 0x1F])

    return ''.join(reversed(time_encoded)) + ''.join(random_encoded)