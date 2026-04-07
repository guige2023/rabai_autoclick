"""array action extensions for rabai_autoclick.

Provides array/bytearray operations, binary data manipulation,
buffer utilities, and memory-efficient data handling.
"""

from __future__ import annotations

import struct
from typing import (
    Any,
    Callable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

__all__ = [
    "array",
    "bytearray",
    "bytes",
    "memoryview",
    "to_bytes",
    "to_bytearray",
    "to_array",
    "from_bytes",
    "from_bytearray",
    "concat_bytes",
    "split_bytes",
    "join_bytes",
    "slice_bytes",
    "bytes_to_hex",
    "hex_to_bytes",
    "bytes_to_int",
    "int_to_bytes",
    "byteswap",
    "pack_bytes",
    "unpack_bytes",
    "read_bytes",
    "write_bytes",
    "find_pattern",
    "replace_pattern",
    "count_bytes",
    "count_nibbles",
    "bits_to_bytes",
    "bytes_to_bits",
    "xor_bytes",
    "and_bytes",
    "or_bytes",
    "flip_bytes",
    "reverse_bytes",
    "rotate_left",
    "rotate_right",
    "pack_le",
    "pack_be",
    "unpack_le",
    "unpack_be",
    "read_struct",
    "write_struct",
    "Buffer",
    "ByteBuffer",
    "ByteReader",
    "ByteWriter",
    "BitReader",
    "BitWriter",
    "ArraySlice",
    "ByteArrayOps",
    "safe_slice",
    "pad_bytes",
    "unpad_bytes",
    "align_to",
    "chunk_bytes",
    "interleave",
    "deinterleave",
    "crc32",
    "checksum",
    "parity",
    "has_parity",
]


T = TypeVar("T")


def to_bytes(obj: Any) -> bytes:
    """Convert object to bytes.

    Args:
        obj: Object to convert.

    Returns:
        Bytes representation.
    """
    if isinstance(obj, bytes):
        return obj
    if isinstance(obj, bytearray):
        return bytes(obj)
    if isinstance(obj, memoryview):
        return obj.tobytes()
    if isinstance(obj, str):
        return obj.encode()
    if isinstance(obj, (int, float)):
        return str(obj).encode()
    if isinstance(obj, (list, tuple)):
        return bytes(obj)
    raise TypeError(f"Cannot convert {type(obj)} to bytes")


def to_bytearray(obj: Any) -> bytearray:
    """Convert object to bytearray.

    Args:
        obj: Object to convert.

    Returns:
        Bytearray representation.
    """
    if isinstance(obj, bytearray):
        return obj
    if isinstance(obj, bytes):
        return bytearray(obj)
    if isinstance(obj, memoryview):
        return bytearray(obj.tobytes())
    if isinstance(obj, str):
        return bytearray(obj.encode())
    if isinstance(obj, (int, float)):
        return bytearray(str(obj).encode())
    if isinstance(obj, (list, tuple)):
        return bytearray(obj)
    raise TypeError(f"Cannot convert {type(obj)} to bytearray")


def to_array(typecode: str, obj: Any) -> Any:
    """Create array from object.

    Args:
        typecode: Array typecode ('b', 'B', 'h', 'H', 'i', 'I', 'l', 'L', 'f', 'd').
        obj: Object to convert.

    Returns:
        Array object.
    """
    import array
    if isinstance(obj, (bytes, bytearray, list, tuple)):
        return array.array(typecode, obj)
    return array.array(typecode, [obj])


def from_bytes(data: bytes, signed: bool = False) -> int:
    """Convert bytes to integer.

    Args:
        data: Bytes to convert.
        signed: If True, interpret as signed integer.

    Returns:
        Integer value.
    """
    return int.from_bytes(data, "big", signed=signed)


def from_bytearray(data: bytearray, signed: bool = False) -> int:
    """Convert bytearray to integer.

    Args:
        data: Bytearray to convert.
        signed: If True, interpret as signed integer.

    Returns:
        Integer value.
    """
    return int.from_bytes(data, "big", signed=signed)


def concat_bytes(*sequences: Any) -> bytes:
    """Concatenate multiple byte sequences.

    Args:
        *sequences: Byte sequences to concatenate.

    Returns:
        Concatenated bytes.
    """
    result = bytearray()
    for seq in sequences:
        result.extend(to_bytes(seq))
    return bytes(result)


def split_bytes(data: bytes, size: int) -> List[bytes]:
    """Split bytes into chunks.

    Args:
        data: Bytes to split.
        size: Chunk size.

    Returns:
        List of chunks.
    """
    return [data[i:i+size] for i in range(0, len(data), size)]


def join_bytes(sequences: List[bytes], separator: bytes = b"") -> bytes:
    """Join byte sequences with separator.

    Args:
        sequences: List of byte sequences.
        separator: Separator between sequences.

    Returns:
        Joined bytes.
    """
    return separator.join(sequences)


def slice_bytes(data: bytes, start: Optional[int] = None, end: Optional[int] = None, step: int = 1) -> bytes:
    """Slice bytes with optional step.

    Args:
        data: Bytes to slice.
        start: Start index.
        end: End index.
        step: Step size.

    Returns:
        Sliced bytes.
    """
    return bytes(data[start:end:step])


def bytes_to_hex(data: bytes, separator: str = "") -> str:
    """Convert bytes to hex string.

    Args:
        data: Bytes to convert.
        separator: Optional separator between hex bytes.

    Returns:
        Hex string.
    """
    if separator:
        return separator.join(f"{b:02x}" for b in data)
    return data.hex()


def hex_to_bytes(hex_str: str) -> bytes:
    """Convert hex string to bytes.

    Args:
        hex_str: Hex string.

    Returns:
        Bytes.
    """
    # Remove common separators
    hex_str = hex_str.replace(" ", "").replace("-", "").replace(":", "")
    return bytes.fromhex(hex_str)


def bytes_to_int(data: bytes, byteorder: str = "big", signed: bool = False) -> int:
    """Convert bytes to integer.

    Args:
        data: Bytes to convert.
        byteorder: 'big' or 'little'.
        signed: If True, signed integer.

    Returns:
        Integer value.
    """
    return int.from_bytes(data, byteorder, signed=signed)


def int_to_bytes(value: int, length: int, byteorder: str = "big", signed: bool = False) -> bytes:
    """Convert integer to bytes.

    Args:
        value: Integer to convert.
        length: Number of bytes.
        byteorder: 'big' or 'little'.
        signed: If True, signed integer.

    Returns:
        Bytes representation.
    """
    return value.to_bytes(length, byteorder, signed=signed)


def byteswap(data: bytes) -> bytes:
    """Swap byte order of data.

    Args:
        data: Bytes to swap.

    Returns:
        Swapped bytes.
    """
    return bytes(reversed(data))


def pack_bytes(fmt: str, *args: Any) -> bytes:
    """Pack values into bytes using struct format.

    Args:
        fmt: Struct format string.
        *args: Values to pack.

    Returns:
        Packed bytes.
    """
    return struct.pack(fmt, *args)


def unpack_bytes(fmt: str, data: bytes) -> Tuple[Any, ...]:
    """Unpack bytes using struct format.

    Args:
        fmt: Struct format string.
        data: Bytes to unpack.

    Returns:
        Tuple of unpacked values.
    """
    return struct.unpack(fmt, data)


def read_bytes(buffer: bytes, offset: int, length: int) -> bytes:
    """Read bytes from buffer at offset.

    Args:
        buffer: Source buffer.
        offset: Starting offset.
        length: Number of bytes to read.

    Returns:
        Read bytes.
    """
    return buffer[offset:offset+length]


def write_bytes(buffer: bytearray, offset: int, data: bytes) -> None:
    """Write bytes to buffer at offset.

    Args:
        buffer: Target buffer.
        offset: Starting offset.
        data: Bytes to write.
    """
    for i, b in enumerate(data):
        buffer[offset + i] = b


def find_pattern(data: bytes, pattern: bytes) -> List[int]:
    """Find all occurrences of pattern in data.

    Args:
        data: Data to search.
        pattern: Pattern to find.

    Returns:
        List of starting offsets.
    """
    offsets = []
    start = 0
    while True:
        pos = data.find(pattern, start)
        if pos == -1:
            break
        offsets.append(pos)
        start = pos + 1
    return offsets


def replace_pattern(data: bytes, old: bytes, new: bytes) -> bytes:
    """Replace pattern in data.

    Args:
        data: Data to modify.
        old: Pattern to replace.
        new: Replacement pattern.

    Returns:
        Modified data.
    """
    return data.replace(old, new)


def count_bytes(data: bytes, byte_val: int) -> int:
    """Count occurrences of byte value.

    Args:
        data: Data to count in.
        byte_val: Byte value to count.

    Returns:
        Count of occurrences.
    """
    return data.count(byte_val)


def count_nibbles(data: bytes) -> Tuple[int, int]:
    """Count number of 0 and 1 nibbles (4-bit groups).

    Args:
        data: Bytes to count.

    Returns:
        Tuple of (zeros, ones).
    """
    zeros = 0
    ones = 0
    for b in data:
        for _ in range(4):
            nibble = b & 0x0F
            if nibble == 0:
                zeros += 1
            else:
                ones += nibble_to_count(nibble)
            b >>= 4
    return zeros, ones


def nibble_to_count(nibble: int) -> int:
    """Count 1-bits in nibble."""
    return bin(nibble).count("1")


def bits_to_bytes(bits: List[int]) -> bytes:
    """Convert list of bits to bytes.

    Args:
        bits: List of 0/1 values.

    Returns:
        Bytes.
    """
    # Pad to byte boundary
    while len(bits) % 8 != 0:
        bits.append(0)
    result = bytearray()
    for i in range(0, len(bits), 8):
        byte = 0
        for j in range(8):
            byte = (byte << 1) | bits[i + j]
        result.append(byte)
    return bytes(result)


def bytes_to_bits(data: bytes) -> List[int]:
    """Convert bytes to list of bits.

    Args:
        data: Bytes to convert.

    Returns:
        List of 0/1 values.
    """
    bits = []
    for b in data:
        for i in range(7, -1, -1):
            bits.append((b >> i) & 1)
    return bits


def xor_bytes(data: bytes, key: bytes) -> bytes:
    """XOR data with key (repeating).

    Args:
        data: Data to XOR.
        key: XOR key.

    Returns:
        XORed bytes.
    """
    if not key:
        return data
    result = bytearray()
    for i, b in enumerate(data):
        result.append(b ^ key[i % len(key)])
    return bytes(result)


def and_bytes(data: bytes, mask: bytes) -> bytes:
    """AND data with mask.

    Args:
        data: Data to AND.
        mask: AND mask.

    Returns:
        ANDed bytes.
    """
    result = bytearray()
    for i, b in enumerate(data):
        result.append(b & mask[i % len(mask)])
    return bytes(result)


def or_bytes(data: bytes, mask: bytes) -> bytes:
    """OR data with mask.

    Args:
        data: Data to OR.
        mask: OR mask.

    Returns:
        ORed bytes.
    """
    result = bytearray()
    for i, b in enumerate(data):
        result.append(b | mask[i % len(mask)])
    return bytes(result)


def flip_bytes(data: bytes) -> bytes:
    """Flip all bits in data.

    Args:
        data: Bytes to flip.

    Returns:
        Flipped bytes.
    """
    return bytes(~b & 0xFF for b in data)


def reverse_bytes(data: bytes) -> bytes:
    """Reverse byte order.

    Args:
        data: Bytes to reverse.

    Returns:
        Reversed bytes.
    """
    return bytes(reversed(data))


def rotate_left(data: bytes, n: int) -> bytes:
    """Rotate bits left.

    Args:
        data: Bytes to rotate.
        n: Number of positions.

    Returns:
        Rotated bytes.
    """
    n = n % 8
    if n == 0:
        return data
    result = bytearray()
    for b in data:
        result.append(((b << n) | (b >> (8 - n))) & 0xFF)
    return bytes(result)


def rotate_right(data: bytes, n: int) -> bytes:
    """Rotate bits right.

    Args:
        data: Bytes to rotate.
        n: Number of positions.

    Returns:
        Rotated bytes.
    """
    n = n % 8
    if n == 0:
        return data
    result = bytearray()
    for b in data:
        result.append(((b >> n) | (b << (8 - n))) & 0xFF)
    return bytes(result)


def pack_le(fmt: str, *args: Any) -> bytes:
    """Pack with little-endian byte order.

    Args:
        fmt: Struct format (without < prefix).
        *args: Values to pack.

    Returns:
        Packed bytes.
    """
    return struct.pack("<" + fmt, *args)


def pack_be(fmt: str, *args: Any) -> bytes:
    """Pack with big-endian byte order.

    Args:
        fmt: Struct format (without > prefix).
        *args: Values to pack.

    Returns:
        Packed bytes.
    """
    return struct.pack(">" + fmt, *args)


def unpack_le(fmt: str, data: bytes) -> Tuple[Any, ...]:
    """Unpack little-endian bytes.

    Args:
        fmt: Struct format.
        data: Bytes to unpack.

    Returns:
        Tuple of values.
    """
    return struct.unpack("<" + fmt, data)


def unpack_be(fmt: str, data: bytes) -> Tuple[Any, ...]:
    """Unpack big-endian bytes.

    Args:
        fmt: Struct format.
        data: Bytes to unpack.

    Returns:
        Tuple of values.
    """
    return struct.unpack(">" + fmt, data)


def read_struct(buffer: bytes, offset: int, fmt: str) -> Tuple[Any, ...]:
    """Read struct from buffer at offset.

    Args:
        buffer: Source buffer.
        offset: Starting offset.
        fmt: Struct format.

    Returns:
        Tuple of unpacked values.
    """
    size = struct.calcsize(fmt)
    return struct.unpack(fmt, buffer[offset:offset+size])


def write_struct(buffer: bytearray, offset: int, fmt: str, *args: Any) -> None:
    """Write struct to buffer at offset.

    Args:
        buffer: Target buffer.
        offset: Starting offset.
        fmt: Struct format.
        *args: Values to pack and write.
    """
    data = struct.pack(fmt, *args)
    for i, b in enumerate(data):
        buffer[offset + i] = b


class Buffer:
    """Base buffer class."""

    def __init__(self, data: bytes = b"") -> None:
        """Initialize buffer.

        Args:
            data: Initial data.
        """
        self._data = bytearray(data)
        self._pos = 0

    def tell(self) -> int:
        """Get current position."""
        return self._pos

    def seek(self, pos: int) -> None:
        """Seek to position.

        Args:
            pos: New position.
        """
        self._pos = pos

    def tell_remaining(self) -> int:
        """Get remaining bytes."""
        return len(self._data) - self._pos


class ByteBuffer(Buffer):
    """Buffer for byte operations."""

    def read(self, n: int = -1) -> bytes:
        """Read bytes.

        Args:
            n: Number of bytes (-1 for all remaining).

        Returns:
            Bytes read.
        """
        if n == -1:
            result = bytes(self._data[self._pos:])
            self._pos = len(self._data)
        else:
            result = bytes(self._data[self._pos:self._pos+n])
            self._pos += n
        return result

    def write(self, data: bytes) -> None:
        """Write bytes.

        Args:
            data: Bytes to write.
        """
        if self._pos >= len(self._data):
            self._data.extend(data)
        else:
            self._data[self._pos:self._pos+len(data)] = data
        self._pos += len(data)

    def getvalue(self) -> bytes:
        """Get buffer contents."""
        return bytes(self._data)

    def truncate(self, size: int) -> None:
        """Truncate buffer to size.

        Args:
            size: New size.
        """
        self._data = self._data[:size]
        if self._pos > size:
            self._pos = size


class ByteReader:
    """Reader for byte sequences."""

    def __init__(self, data: bytes) -> None:
        """Initialize reader.

        Args:
            data: Data to read from.
        """
        self._data = data
        self._pos = 0

    def read(self, n: int) -> bytes:
        """Read n bytes.

        Args:
            n: Number of bytes.

        Returns:
            Bytes read.
        """
        result = self._data[self._pos:self._pos+n]
        self._pos += n
        return result

    def read_byte(self) -> int:
        """Read single byte.

        Returns:
            Byte value (0-255).
        """
        b = self._data[self._pos]
        self._pos += 1
        return b

    def read_uint8(self) -> int:
        """Read unsigned 8-bit int."""
        return self.read_byte()

    def read_uint16_le(self) -> int:
        """Read unsigned 16-bit int little-endian."""
        data = self.read(2)
        return struct.unpack("<H", data)[0]

    def read_uint16_be(self) -> int:
        """Read unsigned 16-bit int big-endian."""
        data = self.read(2)
        return struct.unpack(">H", data)[0]

    def read_uint32_le(self) -> int:
        """Read unsigned 32-bit int little-endian."""
        data = self.read(4)
        return struct.unpack("<I", data)[0]

    def read_uint32_be(self) -> int:
        """Read unsigned 32-bit int big-endian."""
        data = self.read(4)
        return struct.unpack(">I", data)[0]

    def read_int8(self) -> int:
        """Read signed 8-bit int."""
        b = self.read_byte()
        return b if b < 128 else b - 256

    def remaining(self) -> int:
        """Get remaining bytes."""
        return len(self._data) - self._pos


class ByteWriter:
    """Writer for byte sequences."""

    def __init__(self) -> None:
        """Initialize writer."""
        self._data = bytearray()

    def write(self, data: bytes) -> None:
        """Write bytes.

        Args:
            data: Bytes to write.
        """
        self._data.extend(data)

    def write_byte(self, value: int) -> None:
        """Write single byte.

        Args:
            value: Byte value (0-255).
        """
        self._data.append(value & 0xFF)

    def write_uint8(self, value: int) -> None:
        """Write unsigned 8-bit int."""
        self.write_byte(value)

    def write_uint16_le(self, value: int) -> None:
        """Write unsigned 16-bit int little-endian."""
        self._data.extend(struct.pack("<H", value))

    def write_uint16_be(self, value: int) -> None:
        """Write unsigned 16-bit int big-endian."""
        self._data.extend(struct.pack(">H", value))

    def write_uint32_le(self, value: int) -> None:
        """Write unsigned 32-bit int little-endian."""
        self._data.extend(struct.pack("<I", value))

    def write_uint32_be(self, value: int) -> None:
        """Write unsigned 32-bit int big-endian."""
        self._data.extend(struct.pack(">I", value))

    def getvalue(self) -> bytes:
        """Get written bytes."""
        return bytes(self._data)


class BitReader:
    """Reader for individual bits."""

    def __init__(self, data: bytes) -> None:
        """Initialize bit reader.

        Args:
            data: Bytes to read from.
        """
        self._data = data
        self._byte_pos = 0
        self._bit_pos = 0

    def read_bit(self) -> int:
        """Read single bit.

        Returns:
            0 or 1.
        """
        if self._byte_pos >= len(self._data):
            raise EOFError("End of data")
        byte = self._data[self._byte_pos]
        bit = (byte >> (7 - self._bit_pos)) & 1
        self._bit_pos += 1
        if self._bit_pos == 8:
            self._bit_pos = 0
            self._byte_pos += 1
        return bit

    def read_bits(self, n: int) -> int:
        """Read n bits.

        Args:
            n: Number of bits.

        Returns:
            Integer value.
        """
        result = 0
        for _ in range(n):
            result = (result << 1) | self.read_bit()
        return result


class BitWriter:
    """Writer for individual bits."""

    def __init__(self) -> None:
        """Initialize bit writer."""
        self._data = bytearray()
        self._current_byte = 0
        self._bits_in_byte = 0

    def write_bit(self, bit: int) -> None:
        """Write single bit.

        Args:
            bit: 0 or 1.
        """
        self._current_byte = (self._current_byte << 1) | (bit & 1)
        self._bits_in_byte += 1
        if self._bits_in_byte == 8:
            self._data.append(self._current_byte)
            self._current_byte = 0
            self._bits_in_byte = 0

    def write_bits(self, value: int, n: int) -> None:
        """Write n bits from value.

        Args:
            value: Integer value.
            n: Number of bits.
        """
        for i in range(n - 1, -1, -1):
            self.write_bit((value >> i) & 1)

    def getvalue(self) -> bytes:
        """Get written bytes, padding if needed."""
        if self._bits_in_byte > 0:
            # Pad remaining bits with zeros
            self._current_byte <<= (8 - self._bits_in_byte)
            self._data.append(self._current_byte)
        return bytes(self._data)


class ArraySlice:
    """Slice view into array-like object."""

    def __init__(self, data: Any, start: int = 0, stop: Optional[int] = None, step: int = 1) -> None:
        """Initialize slice.

        Args:
            data: Array-like object.
            start: Start index.
            stop: Stop index.
            step: Step size.
        """
        self._data = data
        self._start = start
        self._stop = stop if stop is not None else len(data)
        self._step = step

    def __len__(self) -> int:
        """Get slice length."""
        return len(range(self._start, self._stop, self._step))

    def __getitem__(self, index: int) -> Any:
        """Get item at index."""
        actual_index = self._start + index * self._step
        return self._data[actual_index]

    def __setitem__(self, index: int, value: Any) -> None:
        """Set item at index."""
        actual_index = self._start + index * self._step
        self._data[actual_index] = value

    def tolist(self) -> List[Any]:
        """Convert to list."""
        return list(self)


class ByteArrayOps:
    """Operations on bytearray objects."""

    @staticmethod
    def extend(arr: bytearray, data: bytes) -> None:
        """Extend bytearray.

        Args:
            arr: Bytearray to extend.
            data: Data to append.
        """
        arr.extend(data)

    @staticmethod
    def append(arr: bytearray, value: int) -> None:
        """Append byte.

        Args:
            arr: Bytearray.
            value: Byte value.
        """
        arr.append(value & 0xFF)

    @staticmethod
    def insert(arr: bytearray, index: int, value: int) -> None:
        """Insert byte at index.

        Args:
            arr: Bytearray.
            index: Insert position.
            value: Byte value.
        """
        arr.insert(index, value & 0xFF)

    @staticmethod
    def remove(arr: bytearray, value: int) -> None:
        """Remove first occurrence of byte.

        Args:
            arr: Bytearray.
            value: Byte value to remove.
        """
        arr.remove(value)


def safe_slice(data: bytes, start: int, end: int) -> bytes:
    """Safely slice bytes with bounds checking.

    Args:
        data: Data to slice.
        start: Start index.
        end: End index.

    Returns:
        Sliced data or empty bytes.
    """
    if start < 0:
        start = 0
    if end > len(data):
        end = len(data)
    if start >= end:
        return b""
    return data[start:end]


def pad_bytes(data: bytes, length: int, byte_val: int = 0) -> bytes:
    """Pad bytes to specified length.

    Args:
        data: Data to pad.
        length: Target length.
        byte_val: Padding byte value.

    Returns:
        Padded data.
    """
    if len(data) >= length:
        return data
    padding = bytes([byte_val]) * (length - len(data))
    return data + padding


def unpad_bytes(data: bytes, byte_val: int = 0) -> bytes:
    """Remove padding from bytes.

    Args:
        data: Padded data.
        byte_val: Padding byte value.

    Returns:
        Unpadded data.
    """
    return data.rstrip(bytes([byte_val]))


def align_to(data: bytes, alignment: int, byte_val: int = 0) -> bytes:
    """Align data to specified boundary.

    Args:
        data: Data to align.
        alignment: Alignment boundary.
        byte_val: Padding byte value.

    Returns:
        Aligned data.
    """
    remainder = len(data) % alignment
    if remainder == 0:
        return data
    padding = bytes([byte_val]) * (alignment - remainder)
    return data + padding


def chunk_bytes(data: bytes, size: int) -> List[bytes]:
    """Split data into chunks.

    Args:
        data: Data to chunk.
        size: Chunk size.

    Returns:
        List of chunks.
    """
    return [data[i:i+size] for i in range(0, len(data), size)]


def interleave(*sequences: bytes) -> bytes:
    """Interleave byte sequences.

    Args:
        *sequences: Byte sequences to interleave.

    Returns:
        Interleaved bytes.
    """
    result = []
    max_len = max(len(s) for s in sequences)
    for i in range(max_len):
        for s in sequences:
            if i < len(s):
                result.append(s[i])
    return bytes(result)


def deinterleave(data: bytes, n: int) -> List[bytes]:
    """Deinterleave bytes into n sequences.

    Args:
        data: Data to deinterleave.
        n: Number of sequences.

    Returns:
        List of sequences.
    """
    result = [bytearray() for _ in range(n)]
    for i, b in enumerate(data):
        result[i % n].append(b)
    return [bytes(r) for r in result]


def crc32(data: bytes) -> int:
    """Calculate CRC32 checksum.

    Args:
        data: Data to checksum.

    Returns:
        CRC32 value.
    """
    import zlib
    return zlib.crc32(data) & 0xFFFFFFFF


def checksum(data: bytes) -> int:
    """Calculate simple checksum (sum of bytes).

    Args:
        data: Data to checksum.

    Returns:
        Checksum value.
    """
    return sum(data) & 0xFF


def parity(data: bytes) -> int:
    """Calculate parity bit (XOR of all bytes).

    Args:
        data: Data.

    Returns:
        0 or 1.
    """
    result = 0
    for b in data:
        result ^= b
    return result & 1


def has_parity(byte: int, expected_parity: int = 1) -> bool:
    """Check if byte has expected parity.

    Args:
        byte: Byte value.
        expected_parity: Expected parity (0 or 1).

    Returns:
        True if parity matches.
    """
    return parity(bytes([byte])) == expected_parity
