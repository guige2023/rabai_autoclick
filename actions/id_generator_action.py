"""ID Generator Action Module.

Generate unique identifiers with various formats and strategies.
"""

from __future__ import annotations

import hashlib
import secrets
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any


class IDFormat(Enum):
    """ID format types."""
    UUID4 = "uuid4"
    UUID1 = "uuid1"
    Snowflake = "snowflake"
    ULID = "ulid"
    NanoID = "nanoid"
    HashID = "hashid"
    Custom = "custom"


@dataclass
class IDGenerator:
    """ID generator with multiple format support."""

    def __init__(self, node_id: int = 1) -> None:
        self.node_id = node_id
        self._sequence = 0
        self._last_timestamp = 0

    def generate_uuid4(self) -> str:
        """Generate UUID v4 (random)."""
        return str(uuid.uuid4())

    def generate_uuid1(self) -> str:
        """Generate UUID v1 (time-based)."""
        return str(uuid.uuid1())

    def generate_snowflake(self) -> int:
        """Generate Twitter Snowflake ID."""
        timestamp = int((time.time() - 1609459200000) << 22)
        if timestamp == self._last_timestamp:
            self._sequence = (self._sequence + 1) & 0x3FF
        else:
            self._sequence = 0
        self._last_timestamp = timestamp
        node_shifted = (self.node_id & 0x3FF) << 12
        return timestamp | node_shifted | self._sequence

    def generate_ulid(self) -> str:
        """Generate ULID (Universally Unique Lexicographically Sortable Identifier)."""
        import base32
        now = int(time.time() * 1000)
        rand_bits = secrets.randbits(80)
        encoded = base32.encode(now << 80 | rand_bits)
        return encoded[:26].lower()

    def generate_nanoid(self, size: int = 21) -> str:
        """Generate NanoID."""
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        id_chars = []
        random_bits = secrets.randbits(size * 8)
        for _ in range(size):
            idx = random_bits & 0x3F
            id_chars.append(alphabet[idx])
            random_bits >>= 6
        return "".join(id_chars)

    def generate_hashid(self, data: str, length: int = 12) -> str:
        """Generate deterministic hash ID from data."""
        h = hashlib.sha256(data.encode()).hexdigest()[:length]
        return h

    def generate_custom(
        self,
        prefix: str = "",
        separator: str = "-",
        timestamp: bool = True,
        random_chars: int = 8
    ) -> str:
        """Generate custom format ID."""
        parts = []
        if prefix:
            parts.append(prefix)
        if timestamp:
            parts.append(str(int(time.time())))
        if random_chars > 0:
            parts.append(secrets.token_hex(random_chars)[:random_chars])
        return separator.join(parts)

    def generate_batched(self, count: int, format: IDFormat = IDFormat.UUID4) -> list[str]:
        """Generate multiple IDs at once."""
        ids = []
        for _ in range(count):
            if format == IDFormat.UUID4:
                ids.append(self.generate_uuid4())
            elif format == IDFormat.UUID1:
                ids.append(self.generate_uuid1())
            elif format == IDFormat.Snowflake:
                ids.append(str(self.generate_snowflake()))
            elif format == IDFormat.ULID:
                ids.append(self.generate_ulid())
            elif format == IDFormat.NanoID:
                ids.append(self.generate_nanoid())
            elif format == IDFormat.HashID:
                ids.append(self.generate_hashid(str(time.time())))
            else:
                ids.append(self.generate_custom())
        return ids


class IDEncoder:
    """Encode and decode IDs."""

    @staticmethod
    def encode_base62(num: int) -> str:
        """Encode number to base62 string."""
        alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
        if num == 0:
            return alphabet[0]
        result = []
        while num:
            num, rem = divmod(num, 62)
            result.append(alphabet[rem])
        return "".join(reversed(result))

    @staticmethod
    def decode_base62(encoded: str) -> int:
        """Decode base62 string to number."""
        alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
        num = 0
        for char in encoded:
            num = num * 62 + alphabet.index(char)
        return num

    @staticmethod
    def encode_kix(num: int) -> str:
        """Encode to KIX-like barcode format."""
        return IDEncoder.encode_base62(num)

    @staticmethod
    def generate_short_code(length: int = 8) -> str:
        """Generate URL-safe short code."""
        return secrets.token_urlsafe(length)[:length]
