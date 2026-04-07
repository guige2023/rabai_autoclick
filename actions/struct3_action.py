"""Struct utilities v3 - specialized binary structures.

Specialized struct utilities for network protocols,
 file formats, and data serialization.
"""

from __future__ import annotations

import struct
from struct import unpack, pack, calcsize
from typing import Any

__all__ = [
    "MessageHeader",
    "PacketBuilder",
    "PacketParser",
    "BinarySerializer",
    "BinaryDeserializer",
    "ProtocolHandler",
]


class MessageHeader:
    """Fixed-size message header."""

    FORMAT = "!HHI"  # version, type, length
    SIZE = calcsize(FORMAT)

    def __init__(self, version: int = 1, msg_type: int = 0, length: int = 0) -> None:
        self.version = version
        self.msg_type = msg_type
        self.length = length

    def pack(self) -> bytes:
        """Pack header to bytes."""
        return pack(self.FORMAT, self.version, self.msg_type, self.length)

    @classmethod
    def unpack(cls, data: bytes) -> MessageHeader:
        """Unpack header from bytes."""
        version, msg_type, length = unpack(cls.FORMAT, data[:cls.SIZE])
        return cls(version, msg_type, length)


class PacketBuilder:
    """Build binary packets with headers."""

    def __init__(self, header: MessageHeader | None = None) -> None:
        self._header = header or MessageHeader()
        self._body = bytearray()

    def add_bytes(self, data: bytes) -> PacketBuilder:
        """Add raw bytes."""
        self._body.extend(data)
        return self

    def add_string(self, s: str, encoding: str = "utf-8") -> PacketBuilder:
        """Add string with length prefix."""
        encoded = s.encode(encoding)
        self._body.extend(pack("!I", len(encoded)))
        self._body.extend(encoded)
        return self

    def add_int(self, value: int, fmt: str = "!i") -> PacketBuilder:
        """Add integer."""
        self._body.extend(pack(fmt, value))
        return self

    def build(self) -> bytes:
        """Build final packet."""
        self._header.length = len(self._body)
        return self._header.pack() + bytes(self._body)


class PacketParser:
    """Parse binary packets."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._offset = 0
        self._header = MessageHeader.unpack(data)

    @property
    def header(self) -> MessageHeader:
        """Get parsed header."""
        return self._header

    def read_bytes(self, count: int) -> bytes:
        """Read raw bytes."""
        result = self._data[self._offset:self._offset + count]
        self._offset += count
        return result

    def read_string(self, encoding: str = "utf-8") -> str:
        """Read length-prefixed string."""
        len_bytes = self.read_bytes(4)
        length = unpack("!I", len_bytes)[0]
        return self.read_bytes(length).decode(encoding)

    def read_int(self, fmt: str = "!i") -> int:
        """Read integer."""
        size = calcsize(fmt)
        return unpack(fmt, self.read_bytes(size))[0]


class BinarySerializer:
    """Serialize Python objects to binary."""

    def serialize(self, obj: Any) -> bytes:
        """Serialize object."""
        import pickle
        return pickle.dumps(obj)

    def deserialize(self, data: bytes) -> Any:
        """Deserialize object."""
        import pickle
        return pickle.loads(data)


class BinaryDeserializer:
    """Deserialize binary data."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._offset = 0

    def read(self, fmt: str) -> tuple:
        """Read struct format."""
        size = calcsize(fmt)
        result = unpack(fmt, self._data[self._offset:self._offset + size])
        self._offset += size
        return result


class ProtocolHandler:
    """Handle custom binary protocols."""

    def __init__(self) -> None:
        self._handlers: dict[int, callable] = {}

    def register(self, msg_type: int, handler: callable) -> None:
        """Register message handler."""
        self._handlers[msg_type] = handler

    def handle(self, data: bytes) -> Any:
        """Handle incoming packet."""
        parser = PacketParser(data)
        handler = self._handlers.get(parser.header.msg_type)
        if handler:
            return handler(parser)
        return None
