"""ID generation utilities for RabAI AutoClick.

Provides:
- UUID generation
- Sequential ID generation
- Distributed ID generation
- ID formatting and parsing
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass
from typing import Optional


class SequentialIDGenerator:
    """Thread-safe sequential ID generator.

    Example:
        gen = SequentialIDGenerator(prefix="order_", width=6)
        gen.next()  # "order_000001"
        gen.next()  # "order_000002"
    """

    def __init__(
        self,
        prefix: str = "",
        suffix: str = "",
        width: int = 1,
        start: int = 1,
    ) -> None:
        self._prefix = prefix
        self._suffix = suffix
        self._width = width
        self._current = start
        self._lock = threading.Lock()

    def next(self) -> str:
        """Generate next sequential ID."""
        with self._lock:
            result = f"{self._prefix}{self._current:0{self._width}d}{self._suffix}"
            self._current += 1
            return result

    def peek(self) -> str:
        """Peek at next ID without incrementing."""
        with self._lock:
            return f"{self._prefix}{self._current:0{self._width}d}{self._suffix}"

    def reset(self, start: Optional[int] = None) -> None:
        """Reset the generator."""
        with self._lock:
            if start is not None:
                self._current = start

    def set_prefix(self, prefix: str) -> None:
        self._prefix = prefix

    def set_suffix(self, suffix: str) -> None:
        self._suffix = suffix


class UUIDGenerator:
    """UUID generator with multiple formats.

    Example:
        gen = UUIDGenerator()
        gen.v4()      # "550e8400-e29b-41d4-a716-446655440000"
        gen.v4_hex()  # "550e8400e29b41d4a716446655440000"
        gen.short()   # "550e8400"
    """

    @staticmethod
    def v4() -> str:
        """Generate UUID v4 (random)."""
        return str(uuid.uuid4())

    @staticmethod
    def v4_hex() -> str:
        """Generate UUID v4 as hex string."""
        return uuid.uuid4().hex

    @staticmethod
    def v1() -> str:
        """Generate UUID v1 (time-based)."""
        return str(uuid.uuid1())

    @staticmethod
    def short(n: int = 8) -> str:
        """Generate short UUID (first n hex chars)."""
        return uuid.uuid4().hex[:n]

    @staticmethod
    def urn() -> str:
        """Generate UUID as URN."""
        return uuid.uuid4().urn


class SnowflakeIDGenerator:
    """Snowflake-style distributed ID generator.

    Generates 64-bit IDs with timestamp, machine ID, and sequence.

    Format (from most significant to least):
    - Bits 63-22: Timestamp (milliseconds since custom epoch)
    - Bits 21-12: Machine ID (0-1023)
    - Bits 11-0: Sequence number (0-4095)
    """

    def __init__(
        self,
        machine_id: int = 0,
        epoch: int = 1609459200000,  # 2021-01-01 in ms
    ) -> None:
        if machine_id < 0 or machine_id > 1023:
            raise ValueError("Machine ID must be between 0 and 1023")

        self._machine_id = machine_id
        self._epoch = epoch
        self._sequence = 0
        self._last_timestamp = 0
        self._lock = threading.Lock()

    def next(self) -> int:
        """Generate next Snowflake ID.

        Returns:
            64-bit Snowflake ID.
        """
        with self._lock:
            now = int(time.time() * 1000)

            if now < self._last_timestamp:
                now = self._last_timestamp

            if now == self._last_timestamp:
                self._sequence = (self._sequence + 1) & 0xFFF
                if self._sequence == 0:
                    while now <= self._last_timestamp:
                        now = int(time.time() * 1000)
            else:
                self._sequence = 0

            self._last_timestamp = now

            timestamp = now - self._epoch
            return (
                (timestamp << 22)
                | (self._machine_id << 12)
                | self._sequence
            )

    def parse(self, id: int) -> dict:
        """Parse Snowflake ID into components.

        Returns:
            Dict with timestamp, machine_id, sequence.
        """
        sequence = id & 0xFFF
        machine_id = (id >> 12) & 0x3FF
        timestamp = (id >> 22) + self._epoch
        return {
            "timestamp": timestamp,
            "machine_id": machine_id,
            "sequence": sequence,
        }


class ULIDGenerator:
    """ULID (Universally Unique Lexicographically Sortable Identifier) generator.

    Similar to Snowflake but uses Crockford's base32 encoding.
    """

    @staticmethod
    def generate() -> str:
        """Generate a ULID string."""
        now = int(time.time() * 1000)
        rand = uuid.uuid4().int >> 80
        return ULIDGenerator._encode(now, rand)

    @staticmethod
    def _encode(timestamp: int, random: int) -> str:
        ENCODING = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
        result = []
        for i in range(26):
            remainder = (timestamp * 32 + random) % 32
            result.append(ENCODING[remainder])
            timestamp //= 32
            random //= 32
        return "".join(reversed(result))

    @staticmethod
    def decode(ulid: str) -> int:
        """Decode ULID to timestamp."""
        ENCODING = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
        timestamp = 0
        for char in ulid[:10]:
            timestamp = timestamp * 32 + ENCODING.index(char)
        return timestamp


def generate_batch_ids(prefix: str, count: int, width: int = 6) -> list[str]:
    """Generate a batch of sequential IDs.

    Args:
        prefix: ID prefix.
        count: Number of IDs to generate.
        width: Zero-padding width.

    Returns:
        List of IDs.
    """
    gen = SequentialIDGenerator(prefix=prefix, width=width)
    return [gen.next() for _ in range(count)]


def parse_id(id: str) -> dict:
    """Parse an ID string into components.

    Handles formats like:
    - prefix_NUMBER (order_001)
    - UUIDs
    - Snowflake IDs

    Returns:
        Dict with parsed components.
    """
    import re

    result = {"original": id, "type": "unknown"}

    uuid_pattern = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        re.IGNORECASE,
    )
    if uuid_pattern.match(id):
        result["type"] = "uuid"
        result["uuid"] = id
        return result

    prefix_pattern = re.compile(r"^([a-zA-Z_]+)_(\d+)$")
    match = prefix_pattern.match(id)
    if match:
        result["type"] = "sequential"
        result["prefix"] = match.group(1)
        result["number"] = int(match.group(2))
        return result

    if len(id) == 26:
        result["type"] = "ulid"
        result["timestamp"] = ULIDGenerator.decode(id)
        return result

    return result
