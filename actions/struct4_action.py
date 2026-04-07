"""Struct utilities v4 - simple struct operations.

Simple struct pack/unpack utilities.
"""

from __future__ import annotations

import struct
from struct import pack, unpack, calcsize

__all__ = [
    "pack_int8",
    "unpack_int8",
    "pack_uint8",
    "unpack_uint8",
    "pack_int16",
    "unpack_int16",
    "pack_uint16",
    "unpack_uint16",
    "pack_int32",
    "unpack_int32",
    "pack_uint32",
    "unpack_uint32",
    "pack_float32",
    "unpack_float32",
    "pack_float64",
    "unpack_float64",
    "pack_string",
    "unpack_string",
]


def pack_int8(value: int) -> bytes:
    """Pack signed 8-bit int."""
    return pack("b", value)


def unpack_int8(data: bytes) -> int:
    """Unpack signed 8-bit int."""
    return unpack("b", data[:1])[0]


def pack_uint8(value: int) -> bytes:
    """Pack unsigned 8-bit int."""
    return pack("B", value)


def unpack_uint8(data: bytes) -> int:
    """Unpack unsigned 8-bit int."""
    return unpack("B", data[:1])[0]


def pack_int16(value: int) -> bytes:
    """Pack signed 16-bit int (big-endian)."""
    return pack(">h", value)


def unpack_int16(data: bytes) -> int:
    """Unpack signed 16-bit int."""
    return unpack(">h", data[:2])[0]


def pack_uint16(value: int) -> bytes:
    """Pack unsigned 16-bit int."""
    return pack(">H", value)


def unpack_uint16(data: bytes) -> int:
    """Unpack unsigned 16-bit int."""
    return unpack(">H", data[:2])[0]


def pack_int32(value: int) -> bytes:
    """Pack signed 32-bit int."""
    return pack(">i", value)


def unpack_int32(data: bytes) -> int:
    """Unpack signed 32-bit int."""
    return unpack(">i", data[:4])[0]


def pack_uint32(value: int) -> bytes:
    """Pack unsigned 32-bit int."""
    return pack(">I", value)


def unpack_uint32(data: bytes) -> int:
    """Unpack unsigned 32-bit int."""
    return unpack(">I", data[:4])[0]


def pack_float32(value: float) -> bytes:
    """Pack 32-bit float."""
    return pack(">f", value)


def unpack_float32(data: bytes) -> float:
    """Unpack 32-bit float."""
    return unpack(">f", data[:4])[0]


def pack_float64(value: float) -> bytes:
    """Pack 64-bit float."""
    return pack(">d", value)


def unpack_float64(data: bytes) -> float:
    """Unpack 64-bit float."""
    return unpack(">d", data[:8])[0]


def pack_string(s: str, length: int | None = None, encoding: str = "utf-8") -> bytes:
    """Pack string to bytes."""
    encoded = s.encode(encoding)
    if length:
        return encoded[:length].ljust(length, b"\x00")
    return encoded + b"\x00"


def unpack_string(data: bytes, encoding: str = "utf-8") -> str:
    """Unpack string from bytes."""
    return data.rstrip(b"\x00").decode(encoding)
