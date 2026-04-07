"""ID generation utilities: UUID, Snowflake, ULID, KSUID, and compound IDs."""

from __future__ import annotations

import hashlib
import secrets
import time
import uuid
from dataclasses import dataclass
from typing import Any

__all__ = [
    "SnowflakeID",
    "ULIDGenerator",
    "KSUID",
    "generate_uuid",
    "generate_short_id",
    "generate_compound_id",
    "parse_compound_id",
]


@dataclass
class SnowflakeID:
    """Twitter Snowflake-style ID generator."""
    node_id: int = 0
    epoch: int = 1609459200000
    _last_time: int = 0
    _sequence: int = 0
    _lock: Any = None

    def __post_init__(self) -> None:
        import threading
        self._lock = threading.Lock()

    def generate(self) -> int:
        with self._lock:
            now = int(time.time() * 1000)
            if now == self._last_time:
                self._sequence = (self._sequence + 1) & 4095
            else:
                self._sequence = 0
                self._last_time = now
            timestamp = (now - self.epoch) << 22
            node_id_part = (self.node_id & 31) << 17
            seq_part = self._sequence & 4095
            return timestamp | node_id_part | seq_part

    def parse(self, snowflake_id: int) -> dict[str, int]:
        timestamp = ((snowflake_id >> 22) & 0x1FFFFF) + self.epoch
        node_id = (snowflake_id >> 17) & 31
        sequence = snowflake_id & 4095
        return {
            "timestamp_ms": timestamp,
            "datetime": timestamp / 1000,
            "node_id": node_id,
            "sequence": sequence,
        }


class ULIDGenerator:
    """Universally Unique Lexicographically Sortable Identifier."""

    ENCODING = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    ENCODING_LEN = 32
    TIME_LEN = 10
    RANDOM_LEN = 16

    @classmethod
    def generate(cls, timestamp: float | None = None) -> str:
        import time
        t = int((timestamp or time.time()) * 1000)
        rand = [secrets.randbelow(256) for _ in range(10)]
        out = [""] * 26
        enc_idx = 0

        for i in range(10):
            out[enc_idx] = cls.ENCODING[(t >> (5 * (9 - i))) & 31]
            enc_idx += 1

        for i in range(16):
            out[enc_idx] = cls.ENCODING[(rand[i >> 1] >> (4 * (i & 1))) & 31]
            enc_idx += 1

        return "".join(out)

    @classmethod
    def decode(cls, ulid: str) -> dict[str, Any]:
        if len(ulid) != 26:
            raise ValueError(f"Invalid ULID: {ulid}")
        t = 0
        for i in range(10):
            t = (t << 5) | cls.ENCODING.index(ulid[i].upper())
        return {
            "timestamp_ms": t,
            "datetime": t / 1000,
        }


@dataclass
class KSUID:
    """K-Sortable Unique Identifier (20 bytes: 4 time + 16 random)."""

    @staticmethod
    def generate(timestamp: float | None = None) -> str:
        import base64
        ts = int((timestamp or time.time()))
        time_bytes = ts.to_bytes(4, "big")
        random_bytes = secrets.token_bytes(16)
        raw = time_bytes + random_bytes
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    @staticmethod
    def parse(ksuid: str) -> dict[str, Any]:
        import base64
        raw = base64.urlsafe_b64decode(ksuid + "==")
        ts = int.from_bytes(raw[:4], "big")
        return {"timestamp": ts, "datetime": ts}


def generate_uuid(version: int = 4) -> str:
    """Generate a UUID of the specified version."""
    return str(uuid.uuid4())


def generate_short_id(length: int = 8) -> str:
    """Generate a short random ID."""
    chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    return "".join(secrets.choice(chars) for _ in range(length))


def generate_compound_id(
    prefix: str,
    *parts: str | int,
    separator: str = "_",
) -> str:
    """Generate a compound ID from a prefix and parts."""
    all_parts = [prefix] + [str(p) for p in parts]
    raw = separator.join(all_parts)
    return raw


def parse_compound_id(
    compound_id: str,
    expected_parts: int = 2,
    separator: str = "_",
) -> tuple[str, ...]:
    """Parse a compound ID back into its parts."""
    parts = compound_id.split(separator)
    if len(parts) < expected_parts:
        raise ValueError(f"Invalid compound ID: expected {expected_parts} parts, got {len(parts)}")
    return tuple(parts)
