"""
Serialization utilities v2 — advanced encoding and decoding formats.

Companion to serialization_utils.py. Adds MessagePack, UBJSON, Bencode,
and custom binary serialization utilities.
"""

from __future__ import annotations

import json
import struct
from typing import Any


def msgpack_encode(obj: Any) -> bytes:
    """
    Encode Python object to MessagePack format (simplified).

    Supports: None, bool, int, float, str, bytes, list, dict.

    Args:
        obj: Python object to encode

    Returns:
        MessagePack-encoded bytes
    """
    if obj is None:
        return b"\xc0"
    if isinstance(obj, bool):
        return b"\xc3" if obj else b"\xc2"
    if isinstance(obj, int):
        if -32 <= obj < 0:
            return bytes([0xe0 + obj])
        if 0 <= obj < 128:
            return bytes([obj])
        if obj < 0:
            if -0x10000 <= obj:
                return struct.pack(">Bi", 0xd0, obj)
            if -0x100000000 <= obj:
                return struct.pack(">Bi", 0xd1, obj)
        else:
            if obj < 256:
                return struct.pack(">BB", 0xcc, obj)
            if obj < 65536:
                return struct.pack(">BH", 0xcd, obj)
            return struct.pack(">BI", 0xce, obj)
    if isinstance(obj, float):
        return struct.pack(">Bd", 0xcb, obj)
    if isinstance(obj, str):
        data = obj.encode("utf-8")
        n = len(data)
        if n < 32:
            return bytes([0xa0 + n]) + data
        if n < 256:
            return struct.pack(">BB", 0xd9, n) + data
        return struct.pack(">BH", 0xda, n) + data
    if isinstance(obj, bytes):
        n = len(obj)
        if n < 256:
            return struct.pack(">BB", 0xc4, n) + obj
        return struct.pack(">BH", 0xc5, n) + obj
    if isinstance(obj, (list, tuple)):
        n = len(obj)
        if n < 16:
            return bytes([0x90 + n]) + b"".join(msgpack_encode(v) for v in obj)
        if n < 65536:
            return struct.pack(">BH", 0xdc, n) + b"".join(msgpack_encode(v) for v in obj)
    if isinstance(obj, dict):
        n = len(obj)
        if n < 16:
            return bytes([0x80 + n]) + b"".join(msgpack_encode(k) + msgpack_encode(v) for k, v in obj.items())
        if n < 65536:
            return struct.pack(">BH", 0xde, n) + b"".join(msgpack_encode(k) + msgpack_encode(v) for k, v in obj.items())
    raise TypeError(f"Unsupported type: {type(obj)}")


def msgpack_decode(data: bytes) -> Any:
    """
    Decode MessagePack bytes to Python object.

    Args:
        data: MessagePack-encoded bytes

    Returns:
        Decoded Python object
    """
    pos = 0

    def read_byte() -> int:
        nonlocal pos
        b = data[pos]
        pos += 1
        return b

    def read_int(size: int) -> int:
        nonlocal pos
        val = int.from_bytes(data[pos:pos + size], "big", signed=True)
        pos += size
        return val

    b = read_byte()

    if 0xa0 <= b <= 0xbf:
        n = b - 0xa0
        val = data[pos:pos + n].decode("utf-8")
        pos += n
        return val
    if 0x80 <= b <= 0x8f:
        n = b - 0x80
        return {msgpack_decode(data[pos:]) for _ in range(n)}
    if 0x90 <= b <= 0x9f:
        n = b - 0x90
        return [msgpack_decode(data[pos:]) for _ in range(n)]
    if b == 0xc0:
        return None
    if b == 0xc2:
        return False
    if b == 0xc3:
        return True
    if 0xe0 <= b <= 0xff:
        return b - 0x100
    if b < 0x80:
        return b
    if 0xcc == b:
        return read_int(1)
    if 0xcd == b:
        return read_int(2)
    if 0xce == b:
        return read_int(4)
    if 0xd0 == b:
        return read_int(1)
    if 0xd1 == b:
        return read_int(2)
    if 0xd9 == b:
        n = read_int(1)
        val = data[pos:pos + n].decode("utf-8")
        pos += n
        return val
    if 0xda == b:
        n = read_int(2)
        val = data[pos:pos + n].decode("utf-8")
        pos += n
        return val
    if 0xcb == b:
        return struct.unpack(">d", data[pos:pos + 8])[0]
    if 0xc4 == b:
        n = read_int(1)
        val = data[pos:pos + n]
        pos += n
        return val
    if 0xc5 == b:
        n = read_int(2)
        val = data[pos:pos + n]
        pos += n
        return val
    if 0xdc == b:
        n = read_int(2)
        return [msgpack_decode(data[pos:]) for _ in range(n)]
    if 0xde == b:
        n = read_int(2)
        return {msgpack_decode(data[pos:]): msgpack_decode(data[pos:]) for _ in range(n)}
    raise ValueError(f"Unknown MessagePack marker: {b:#x}")


def ubjson_encode(obj: Any) -> bytes:
    """
    Encode Python object to UBJSON format.

    Args:
        obj: Python object to encode

    Returns:
        UBJSON-encoded bytes
    """
    if obj is None:
        return b"Z"
    if isinstance(obj, bool):
        return b"T" if obj else b"F"
    if isinstance(obj, int):
        if 0 <= obj <= 255:
            return b"U" + bytes([obj])
        if -128 <= obj <= 127:
            return b"i" + struct.pack("b", obj)
        if -32768 <= obj <= 32767:
            return b"I" + struct.pack(">h", obj)
        return b"l" + struct.pack(">i", obj)
    if isinstance(obj, float):
        return b"D" + struct.pack(">d", obj)
    if isinstance(obj, str):
        data = obj.encode("utf-8")
        return b"S" + struct.pack(">I", len(data)) + data
    if isinstance(obj, bytes):
        return b"[u" + bytes([ord("U")]) + str(len(obj)).encode() + b"]" + b"".join(bytes([obj[i]]) for i in range(len(obj)))
    if isinstance(obj, (list, tuple)):
        inner = b"".join(ubjson_encode(v) for v in obj)
        return b"[" + b"#" + struct.pack(">I", len(obj)) + inner + b"]"
    if isinstance(obj, dict):
        inner = b"".join(ubjson_encode(k) + ubjson_encode(v) for k, v in obj.items())
        return b"{" + b"#" + struct.pack(">I", len(obj)) + inner + b"}"
    raise TypeError(f"Unsupported type: {type(obj)}")


def bencode_encode(obj: Any) -> bytes:
    """
    Encode Python object to Bencode format (BitTorrent).

    Args:
        obj: Python object to encode

    Returns:
        Bencode-encoded bytes
    """
    if isinstance(obj, str):
        data = obj.encode("utf-8")
        return str(len(data)).encode() + b":" + data
    if isinstance(obj, bytes):
        return str(len(obj)).encode() + b":" + obj
    if isinstance(obj, int):
        return b"i" + str(obj).encode() + b"e"
    if isinstance(obj, list):
        return b"l" + b"".join(bencode_encode(v) for v in obj) + b"e"
    if isinstance(obj, dict):
        items = sorted(obj.items())
        return b"d" + b"".join(bencode_encode(k) + bencode_encode(v) for k, v in items) + b"e"
    raise TypeError(f"Unsupported type: {type(obj)}")


def bencode_decode(data: bytes) -> Any:
    """
    Decode Bencode bytes to Python object.

    Args:
        data: Bencode-encoded bytes

    Returns:
        Decoded Python object
    """
    pos = 0

    def peek() -> int:
        return data[pos]

    def read_byte() -> int:
        nonlocal pos
        b = data[pos]
        pos += 1
        return b

    def read_str() -> bytes:
        nonlocal pos
        colon = data.index(b":", pos)
        n = int(data[pos:colon])
        val = data[colon + 1:colon + 1 + n]
        pos = colon + 1 + n
        return val

    b = peek()
    if b == ord("i"):
        read_byte()
        end = data.index(b"e", pos)
        val = int(data[pos:end])
        pos = end + 1
        return val
    if ord("0") <= b <= ord("9"):
        return read_str().decode("utf-8")
    if b == ord("l"):
        read_byte()
        result = []
        while data[pos] != ord("e"):
            result.append(bencode_decode(data))
        pos += 1
        return result
    if b == ord("d"):
        read_byte()
        result = {}
        while data[pos] != ord("e"):
            k = read_str()
            v = bencode_decode(data)
            result[k.decode("utf-8")] = v
        pos += 1
        return result
    raise ValueError(f"Invalid Bencode: {b}")


def toml_encode(obj: dict) -> str:
    """
    Encode dict to TOML-like string (simplified).

    Args:
        obj: Dictionary to encode

    Returns:
        TOML string
    """
    lines = []
    for k, v in obj.items():
        if isinstance(v, dict):
            lines.append(f"[{k}]")
            for kk, vv in v.items():
                lines.append(f"{kk} = {repr(vv)}")
        elif isinstance(v, list):
            lines.append(f"{k} = [{', '.join(repr(x) for x in v)}]")
        else:
            lines.append(f"{k} = {repr(v)}")
    return "\n".join(lines)


def binary_pack(fmt: str, *values: Any) -> bytes:
    """
    Pack values into binary using struct format.

    Args:
        fmt: Struct format string
        *values: Values to pack

    Returns:
        Packed bytes

    Example:
        >>> binary_pack(">IHH", 256, 1, 2)
        b'\\x01\\x00\\x00\\x00\\x00\\x01\\x00\\x02'
    """
    return struct.pack(fmt, *values)


def binary_unpack(fmt: str, data: bytes) -> tuple:
    """
    Unpack binary data using struct format.

    Args:
        fmt: Struct format string
        data: Bytes to unpack

    Returns:
        Tuple of unpacked values
    """
    return struct.unpack(fmt, data)
