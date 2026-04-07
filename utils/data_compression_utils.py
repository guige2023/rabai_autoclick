"""Data compression utilities: gzip, zlib, LZ4, base85, and binary packing."""

from __future__ import annotations

import base64
import gzip
import json
import struct
import zlib
from typing import Any

__all__ = [
    "compress_gzip",
    "decompress_gzip",
    "compress_zlib",
    "decompress_zlib",
    "compress_base64",
    "decompress_base64",
    "pack_binary",
    "unpack_binary",
    "VarInt",
]


def compress_gzip(data: bytes, level: int = 6) -> bytes:
    """Compress bytes using gzip."""
    return gzip.compress(data, level)


def decompress_gzip(data: bytes) -> bytes:
    """Decompress gzip bytes."""
    return gzip.decompress(data)


def compress_zlib(data: bytes, level: int = 6) -> bytes:
    """Compress bytes using zlib."""
    return zlib.compress(data, level)


def decompress_zlib(data: bytes) -> bytes:
    """Decompress zlib bytes."""
    return zlib.decompress(data)


def compress_base64(data: bytes) -> str:
    """Compress then base64 encode."""
    compressed = zlib.compress(data)
    return base64.b64encode(compressed).decode()


def decompress_base64(encoded: str) -> bytes:
    """Base64 decode then decompress."""
    decoded = base64.b64decode(encoded)
    return zlib.decompress(decoded)


def pack_binary(format_str: str, *values: Any) -> bytes:
    """Pack values into binary using struct."""
    return struct.pack(format_str, *values)


def unpack_binary(format_str: str, data: bytes) -> tuple[Any, ...]:
    """Unpack binary data using struct."""
    return struct.unpack(format_str, data)


class VarInt:
    """Variable-length integer encoding (protobuf style)."""

    @staticmethod
    def encode(value: int) -> bytes:
        result = bytearray()
        while value > 0x7F:
            result.append((value & 0x7F) | 0x80)
            value >>= 7
        result.append(value & 0x7F)
        return bytes(result)

    @staticmethod
    def decode(data: bytes, offset: int = 0) -> tuple[int, int]:
        result = 0
        shift = 0
        pos = offset
        while True:
            if pos >= len(data):
                raise ValueError("VarInt decode: exceeded data length")
            byte = data[pos]
            result |= (byte & 0x7F) << shift
            pos += 1
            if not (byte & 0x80):
                break
            shift += 7
        return result, pos


def compress_json(obj: Any) -> str:
    """Compress a JSON-serializable object."""
    data = json.dumps(obj, separators=(",", ":")).encode()
    return compress_base64(data)


def decompress_json(encoded: str) -> Any:
    """Decompress and parse a compressed JSON object."""
    data = decompress_base64(encoded)
    return json.loads(data.decode())
