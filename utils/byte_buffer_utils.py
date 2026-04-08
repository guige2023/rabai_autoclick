"""Byte buffer utilities for binary data manipulation.

Provides efficient byte buffer operations for network protocols,
file formats, and binary data processing in automation workflows.
"""

import struct
from typing import BinaryIO, List, Optional, Tuple, Union


class ByteBuffer:
    """Dynamic byte buffer with read/write operations.

    Example:
        buf = ByteBuffer()
        buf.write_int(42)
        buf.write_str("hello")
        buf.seek(0)
        print(buf.read_int())  # 42
    """

    def __init__(self, initial: bytes = b"") -> None:
        self._data = bytearray(initial)
        self._pos = 0

    @property
    def position(self) -> int:
        """Current buffer position."""
        return self._pos

    @property
    def size(self) -> int:
        """Buffer size."""
        return len(self._data)

    @property
    def available(self) -> int:
        """Bytes available from current position."""
        return len(self._data) - self._pos

    def seek(self, pos: int) -> None:
        """Seek to position.

        Args:
            pos: Position to seek to.
        """
        self._pos = max(0, min(pos, len(self._data)))

    def skip(self, count: int) -> None:
        """Skip bytes forward.

        Args:
            count: Number of bytes to skip.
        """
        self._pos = min(self._pos + count, len(self._data))

    def read(self, count: int = -1) -> bytes:
        """Read bytes from buffer.

        Args:
            count: Number of bytes to read. -1 for all remaining.

        Returns:
            Bytes read.
        """
        if count == -1:
            result = bytes(self._data[self._pos:])
            self._pos = len(self._data)
        else:
            result = bytes(self._data[self._pos:self._pos + count])
            self._pos += count
        return result

    def write(self, data: bytes) -> None:
        """Write bytes to buffer.

        Args:
            data: Bytes to write.
        """
        if self._pos >= len(self._data):
            self._data.extend(data)
        else:
            self._data[self._pos:self._pos + len(data)] = data
        self._pos += len(data)

    def read_int8(self) -> int:
        """Read signed 8-bit integer."""
        return struct.unpack("b", self.read(1))[0]

    def read_uint8(self) -> int:
        """Read unsigned 8-bit integer."""
        return struct.unpack("B", self.read(1))[0]

    def read_int16_le(self) -> int:
        """Read signed 16-bit integer little-endian."""
        return struct.unpack("<h", self.read(2))[0]

    def read_int16_be(self) -> int:
        """Read signed 16-bit integer big-endian."""
        return struct.unpack(">h", self.read(2))[0]

    def read_uint16_le(self) -> int:
        """Read unsigned 16-bit integer little-endian."""
        return struct.unpack("<H", self.read(2))[0]

    def read_uint16_be(self) -> int:
        """Read unsigned 16-bit integer big-endian."""
        return struct.unpack(">H", self.read(2))[0]

    def read_int32_le(self) -> int:
        """Read signed 32-bit integer little-endian."""
        return struct.unpack("<i", self.read(4))[0]

    def read_int32_be(self) -> int:
        """Read signed 32-bit integer big-endian."""
        return struct.unpack(">i", self.read(4))[0]

    def read_uint32_le(self) -> int:
        """Read unsigned 32-bit integer little-endian."""
        return struct.unpack("<I", self.read(4))[0]

    def read_uint32_be(self) -> int:
        """Read unsigned 32-bit integer big-endian."""
        return struct.unpack(">I", self.read(4))[0]

    def read_float_le(self) -> float:
        """Read 32-bit float little-endian."""
        return struct.unpack("<f", self.read(4))[0]

    def read_float_be(self) -> float:
        """Read 32-bit float big-endian."""
        return struct.unpack(">f", self.read(4))[0]

    def read_double_le(self) -> float:
        """Read 64-bit double little-endian."""
        return struct.unpack("<d", self.read(8))[0]

    def read_double_be(self) -> float:
        """Read 64-bit double big-endian."""
        return struct.unpack(">d", self.read(8))[0]

    def write_int8(self, value: int) -> None:
        self.write(struct.pack("b", value))

    def write_uint8(self, value: int) -> None:
        self.write(struct.pack("B", value))

    def write_int16_le(self, value: int) -> None:
        self.write(struct.pack("<h", value))

    def write_int16_be(self, value: int) -> None:
        self.write(struct.pack(">h", value))

    def write_uint16_le(self, value: int) -> None:
        self.write(struct.pack("<H", value))

    def write_uint16_be(self, value: int) -> None:
        self.write(struct.pack(">H", value))

    def write_int32_le(self, value: int) -> None:
        self.write(struct.pack("<i", value))

    def write_int32_be(self, value: int) -> None:
        self.write(struct.pack(">i", value))

    def write_uint32_le(self, value: int) -> None:
        self.write(struct.pack("<I", value))

    def write_uint32_be(self, value: int) -> None:
        self.write(struct.pack(">I", value))

    def write_float_le(self, value: float) -> None:
        self.write(struct.pack("<f", value))

    def write_float_be(self, value: float) -> None:
        self.write(struct.pack(">f", value))

    def write_double_le(self, value: float) -> None:
        self.write(struct.pack("<d", value))

    def write_double_be(self, value: float) -> None:
        self.write(struct.pack(">d", value))

    def read_cstring(self, encoding: str = "utf-8") -> str:
        """Read null-terminated string."""
        start = self._pos
        while self._pos < len(self._data) and self._data[self._pos] != 0:
            self._pos += 1
        result = bytes(self._data[start:self._pos]).decode(encoding)
        if self._pos < len(self._data):
            self._pos += 1
        return result

    def write_cstring(self, s: str, encoding: str = "utf-8") -> None:
        """Write null-terminated string."""
        self.write(s.encode(encoding) + b"\x00")

    def to_bytes(self) -> bytes:
        """Get buffer contents as bytes."""
        return bytes(self._data)

    def clear(self) -> None:
        """Clear buffer."""
        self._data.clear()
        self._pos = 0


def pack(fmt: str, *values) -> bytes:
    """Pack values into bytes.

    Args:
        fmt: Format string.
        *values: Values to pack.

    Returns:
        Packed bytes.
    """
    return struct.pack(fmt, *values)


def unpack(fmt: str, data: bytes) -> Tuple:
    """Unpack bytes to values.

    Args:
        fmt: Format string.
        data: Bytes to unpack.

    Returns:
        Tuple of unpacked values.
    """
    return struct.unpack(fmt, data)
