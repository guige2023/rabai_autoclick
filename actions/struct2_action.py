"""Struct utilities v2 - binary data structures.

Extended struct operations including bit fields,
 network protocols, and file formats.
"""

from __future__ import annotations

import struct
from struct import Struct, unpack, pack, calcsize, error as StructError
from typing import Any, Iterator

__all__ = [
    "StructBuilder",
    "BitField",
    "BitFieldView",
    "PacketParser",
    "PacketBuilder",
    "NetworkProtocol",
    "IPHeader",
    "TCPSegment",
    "UDPDatagram",
    "FileHeader",
    "BinaryReader",
    "BinaryWriter",
    "Endianess",
    "struct_decode",
    "struct_encode",
    "struct_format",
]


class Endianess:
    """Endianness constants."""
    NATIVE = "@"
    NATIVE_UNSIGNED = "="
    BIG = ">"
    LITTLE = "<"
    NETWORK = "!"


class StructBuilder:
    """Build structured data."""

    def __init__(self, fmt: str) -> None:
        self._fmt = fmt
        self._struct = Struct(fmt)

    @classmethod
    def create(cls, fmt: str) -> StructBuilder:
        """Create new builder.

        Args:
            fmt: Struct format string.

        Returns:
            StructBuilder instance.
        """
        return cls(fmt)

    def pack(self, *values: Any) -> bytes:
        """Pack values.

        Args:
            *values: Values to pack.

        Returns:
            Packed bytes.
        """
        return self._struct.pack(*values)

    def unpack(self, data: bytes) -> tuple:
        """Unpack data.

        Args:
            data: Bytes to unpack.

        Returns:
            Tuple of values.
        """
        return self._struct.unpack(data)

    def pack_into(self, data: bytes, offset: int, *values: Any) -> None:
        """Pack into existing buffer.

        Args:
            data: Buffer to write into.
            offset: Offset in buffer.
            *values: Values to pack.
        """
        self._struct.pack_into(data, offset, *values)

    def unpack_from(self, data: bytes, offset: int = 0) -> tuple:
        """Unpack from buffer.

        Args:
            data: Buffer to read.
            offset: Offset to start.

        Returns:
            Tuple of values.
        """
        return self._struct.unpack_from(data, offset)

    @property
    def size(self) -> int:
        """Get size of structure."""
        return self._struct.size


class BitField:
    """Manage a bit field within an integer."""

    def __init__(self, value: int = 0, width: int = 32, start_bit: int = 0, signed: bool = False) -> None:
        self._value = value
        self._width = width
        self._start = start_bit
        self._signed = signed
        self._mask = ((1 << width) - 1) << start_bit

    def get(self) -> int:
        """Get field value."""
        if self._signed:
            shift = 8 * struct.calcsize("q") - self._width - self._start
            return (self._value << shift) >> (shift + self._start)
        return (self._value & self._mask) >> self._start

    def set(self, value: int) -> BitField:
        """Set field value.

        Args:
            value: New value.

        Returns:
            Self.
        """
        self._value = (self._value & ~self._mask) | ((value << self._start) & self._mask)
        return self

    def __int__(self) -> int:
        return self._value

    def __repr__(self) -> str:
        return f"BitField(value={self._value:#x}, width={self._width}, start={self._start})"


class BitFieldView:
    """View a buffer as bit fields."""

    def __init__(self, data: bytes) -> None:
        self._data = bytearray(data)
        self._fields: list[tuple[str, int, int]] = []

    def add_field(self, name: str, start_bit: int, width: int) -> None:
        """Add a field definition.

        Args:
            name: Field name.
            start_bit: Starting bit position.
            width: Field width in bits.
        """
        self._fields.append((name, start_bit, width))

    def _get_bits(self, start: int, width: int) -> int:
        """Get bits from buffer."""
        value = 0
        for i in range(width):
            byte_idx = (start + i) // 8
            bit_idx = 7 - ((start + i) % 8)
            if byte_idx < len(self._data):
                value |= ((self._data[byte_idx] >> bit_idx) & 1) << i
        return value

    def _set_bits(self, start: int, width: int, value: int) -> None:
        """Set bits in buffer."""
        for i in range(width):
            byte_idx = (start + i) // 8
            bit_idx = 7 - ((start + i) % 8)
            if byte_idx < len(self._data):
                self._data[byte_idx] = (self._data[byte_idx] & ~(1 << bit_idx)) | (((value >> i) & 1) << bit_idx)

    def get(self, name: str) -> int | None:
        """Get field value by name."""
        for fname, start, width in self._fields:
            if fname == name:
                return self._get_bits(start, width)
        return None

    def set(self, name: str, value: int) -> bool:
        """Set field value by name."""
        for fname, start, width in self._fields:
            if fname == name:
                self._set_bits(start, width, value)
                return True
        return False

    def to_bytes(self) -> bytes:
        """Get modified buffer."""
        return bytes(self._data)


class PacketParser:
    """Parse binary packets."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._offset = 0

    def read(self, fmt: str) -> tuple:
        """Read struct from packet.

        Args:
            fmt: Struct format.

        Returns:
            Unpacked values.
        """
        size = calcsize(fmt)
        if self._offset + size > len(self._data):
            raise StructError("Not enough data")
        result = unpack(fmt, self._data[self._offset:self._offset + size])
        self._offset += size
        return result

    def read_bytes(self, count: int) -> bytes:
        """Read raw bytes.

        Args:
            count: Number of bytes.

        Returns:
            Bytes read.
        """
        if self._offset + count > len(self._data):
            raise StructError("Not enough data")
        result = self._data[self._offset:self._offset + count]
        self._offset += count
        return result

    def read_string(self, length: int, encoding: str = "utf-8") -> str:
        """Read string.

        Args:
            length: Byte length.
            encoding: Text encoding.

        Returns:
            Decoded string.
        """
        return self.read_bytes(length).rstrip(b"\x00").decode(encoding)

    def remaining(self) -> int:
        """Get remaining bytes."""
        return len(self._data) - self._offset

    @property
    def offset(self) -> int:
        """Get current offset."""
        return self._offset


class PacketBuilder:
    """Build binary packets."""

    def __init__(self) -> None:
        self._data = bytearray()

    def write(self, fmt: str, *values: Any) -> PacketBuilder:
        """Write struct to packet.

        Args:
            fmt: Struct format.
            *values: Values to pack.

        Returns:
            Self.
        """
        self._data.extend(pack(fmt, *values))
        return self

    def write_bytes(self, data: bytes) -> PacketBuilder:
        """Write raw bytes.

        Args:
            data: Bytes to write.

        Returns:
            Self.
        """
        self._data.extend(data)
        return self

    def write_string(self, s: str, length: int | None = None, encoding: str = "utf-8") -> PacketBuilder:
        """Write string.

        Args:
            s: String to write.
            length: Fixed length (pads with zeros).
            encoding: Text encoding.

        Returns:
            Self.
        """
        encoded = s.encode(encoding)
        if length:
            self._data.extend(encoded[:length].ljust(length, b"\x00"))
        else:
            self._data.extend(encoded + b"\x00")
        return self

    def to_bytes(self) -> bytes:
        """Get packet bytes."""
        return bytes(self._data)

    @property
    def size(self) -> int:
        """Get packet size."""
        return len(self._data)


class NetworkProtocol:
    """Base network protocol parser."""

    def __init__(self, data: bytes) -> None:
        self._parser = PacketParser(data)

    @classmethod
    def parse(cls, data: bytes) -> NetworkProtocol:
        """Parse packet data.

        Args:
            data: Raw packet bytes.

        Returns:
            Parsed protocol instance.
        """
        return cls(data)


class IPHeader(NetworkProtocol):
    """IPv4 header parser."""

    def __init__(self, data: bytes) -> None:
        super().__init__(data)
        self.version = 0
        self.ihl = 0
        self.tos = 0
        self.total_length = 0
        self.identification = 0
        self.flags = 0
        self.fragment_offset = 0
        self.ttl = 0
        self.protocol = 0
        self.checksum = 0
        self.src_ip = ""
        self.dst_ip = ""
        self._parse_header()

    def _parse_header(self) -> None:
        """Parse IPv4 header fields."""
        version_ihl, self.tos, self.total_length = self._parser.read("!BBH")
        self.version = version_ihl >> 4
        self.ihl = version_ihl & 0x0F
        ident, flags_frag = self._parser.read("!HH")
        self.identification = ident
        self.flags = flags_frag >> 13
        self.fragment_offset = flags_frag & 0x1FFF
        self.ttl, self.protocol, self.checksum = self._parser.read("!BBH")
        src = self._parser.read("!BBBB")
        dst = self._parser.read("!BBBB")
        self.src_ip = ".".join(str(x) for x in src)
        self.dst_ip = ".".join(str(x) for x in dst)


class BinaryReader:
    """Binary file reader."""

    def __init__(self, filepath: str | None = None, data: bytes | None = None) -> None:
        if filepath:
            with open(filepath, "rb") as f:
                self._data = f.read()
        else:
            self._data = data or b""
        self._offset = 0

    def read_int8(self) -> int:
        """Read signed 8-bit int."""
        val, = unpack("b", self._data[self._offset:self._offset + 1])
        self._offset += 1
        return val

    def read_uint8(self) -> int:
        """Read unsigned 8-bit int."""
        val, = unpack("B", self._data[self._offset:self._offset + 1])
        self._offset += 1
        return val

    def read_int16(self) -> int:
        """Read signed 16-bit int."""
        val, = unpack("!h", self._data[self._offset:self._offset + 2])
        self._offset += 2
        return val

    def read_uint16(self) -> int:
        """Read unsigned 16-bit int."""
        val, = unpack("!H", self._data[self._offset:self._offset + 2])
        self._offset += 2
        return val

    def read_int32(self) -> int:
        """Read signed 32-bit int."""
        val, = unpack("!i", self._data[self._offset:self._offset + 4])
        self._offset += 4
        return val

    def read_uint32(self) -> int:
        """Read unsigned 32-bit int."""
        val, = unpack("!I", self._data[self._offset:self._offset + 4])
        self._offset += 4
        return val

    def read_float32(self) -> float:
        """Read 32-bit float."""
        val, = unpack("!f", self._data[self._offset:self._offset + 4])
        self._offset += 4
        return val

    def read_float64(self) -> float:
        """Read 64-bit float."""
        val, = unpack("!d", self._data[self._offset:self._offset + 8])
        self._offset += 8
        return val

    def read_bytes(self, count: int) -> bytes:
        """Read raw bytes."""
        val = self._data[self._offset:self._offset + count]
        self._offset += count
        return val


class BinaryWriter:
    """Binary file writer."""

    def __init__(self) -> None:
        self._data = bytearray()

    def write_int8(self, value: int) -> BinaryWriter:
        """Write signed 8-bit int."""
        self._data.extend(pack("b", value))
        return self

    def write_uint8(self, value: int) -> BinaryWriter:
        """Write unsigned 8-bit int."""
        self._data.extend(pack("B", value))
        return self

    def write_int16(self, value: int) -> BinaryWriter:
        """Write signed 16-bit int."""
        self._data.extend(pack("!h", value))
        return self

    def write_uint16(self, value: int) -> BinaryWriter:
        """Write unsigned 16-bit int."""
        self._data.extend(pack("!H", value))
        return self

    def write_int32(self, value: int) -> BinaryWriter:
        """Write signed 32-bit int."""
        self._data.extend(pack("!i", value))
        return self

    def write_uint32(self, value: int) -> BinaryWriter:
        """Write unsigned 32-bit int."""
        self._data.extend(pack("!I", value))
        return self

    def write_float32(self, value: float) -> BinaryWriter:
        """Write 32-bit float."""
        self._data.extend(pack("!f", value))
        return self

    def write_float64(self, value: float) -> BinaryWriter:
        """Write 64-bit float."""
        self._data.extend(pack("!d", value))
        return self

    def write_bytes(self, data: bytes) -> BinaryWriter:
        """Write raw bytes."""
        self._data.extend(data)
        return self

    def to_bytes(self) -> bytes:
        """Get written bytes."""
        return bytes(self._data)


def struct_decode(data: bytes, fmt: str) -> tuple:
    """Decode struct from bytes.

    Args:
        data: Bytes to decode.
        fmt: Struct format.

    Returns:
        Unpacked tuple.
    """
    return unpack(fmt, data)


def struct_encode(fmt: str, *values: Any) -> bytes:
    """Encode values to bytes.

    Args:
        fmt: Struct format.
        *values: Values to encode.

    Returns:
        Encoded bytes.
    """
    return pack(fmt, *values)


def struct_format(tp: type) -> str | None:
    """Get struct format for type.

    Args:
        tp: Type to get format for.

    Returns:
        Format string or None.
    """
    formats = {
        int: "q",
        float: "d",
        bytes: "s",
    }
    return formats.get(tp)
