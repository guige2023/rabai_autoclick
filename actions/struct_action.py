"""
Struct Action Module

Provides struct pack/unpack operations for binary data serialization
and deserialization with support for various format characters.

Author: AI Assistant
Version: 1.0.0
"""

from __future__ import annotations

import struct
from typing import (
    Any,
    BinaryIO,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

# Type aliases
PrimitiveType = Union[int, float, bytes, str, bool]
FormatChar = str
PackResult = bytes
UnpackResult = Tuple[Any, ...]


class StructAction:
    """
    Main struct action handler providing pack/unpack operations.
    
    This class wraps Python's struct module with additional utilities
    for common binary serialization tasks.
    
    Attributes:
        standard_size: Standard struct sizes for format characters
    """
    
    # Standard sizes for format characters
    STANDARD_SIZES: Dict[str, int] = {
        "x": 1,  # pad byte
        "c": 1,  # char
        "b": 1,  # signed byte
        "B": 1,  # unsigned byte
        "?": 1,  # bool
        "h": 2,  # short
        "H": 2,  # unsigned short
        "i": 4,  # int
        "I": 4,  # unsigned int
        "l": 4,  # long
        "L": 4,  # unsigned long
        "q": 8,  # long long
        "Q": 8,  # unsigned long long
        "n": 8,  # ssize_t
        "N": 8,  # size_t
        "e": 2,  # half float
        "f": 4,  # float
        "d": 8,  # double
        "s": 1,  # char[]
        "p": 1,  # char[]
        "P": 8,  # void*
    }
    
    @staticmethod
    def pack(
        format_str: str,
        *values: Any,
        **kwargs: Any,
    ) -> PackResult:
        """
        Pack values into a bytes object according to the format string.
        
        Args:
            format_str: Struct format string (e.g., 'iii', '3s', '>H')
            *values: Values to pack
        
        Returns:
            Packed bytes object
        
        Raises:
            struct.error: If values don't match format
            ValueError: If format string is invalid
        
        Example:
            >>> StructAction.pack('iii', 1, 2, 3)
            b'\\x01\\x00\\x00\\x00\\x02\\x00\\x00\\x00\\x03\\x00\\x00\\x00'
            >>> StructAction.pack('>H', 256)
            b'\\x01\\x00'
        """
        try:
            return struct.pack(format_str, *values)
        except struct.error as e:
            raise struct.error(f"Pack error: {e}") from e
        except TypeError as e:
            raise TypeError(f"Invalid value type for format '{format_str}': {e}") from e
    
    @staticmethod
    def pack_into(
        format_str: str,
        buffer: bytearray,
        offset: int,
        *values: Any,
    ) -> None:
        """
        Pack values into a buffer starting at the given offset.
        
        Args:
            format_str: Struct format string
            buffer: Mutable byte buffer to write into
            offset: Byte offset to start writing
            *values: Values to pack
        
        Raises:
            struct.error: If values don't match format or buffer overflow
            ValueError: If offset is negative
        
        Example:
            >>> buf = bytearray(12)
            >>> StructAction.pack_into('iii', buf, 0, 1, 2, 3)
            >>> buf
            bytearray(b'\\x01\\x00\\x00\\x00\\x02\\x00\\x00\\x00\\x03\\x00\\x00\\x00')
        """
        if offset < 0:
            raise ValueError("Offset must be non-negative")
        
        try:
            struct.pack_into(format_str, buffer, offset, *values)
        except struct.error as e:
            raise struct.error(f"Pack into error: {e}") from e
    
    @staticmethod
    def unpack(
        format_str: str,
        data: bytes,
        **kwargs: Any,
    ) -> UnpackResult:
        """
        Unpack bytes according to the format string.
        
        Args:
            format_str: Struct format string
            data: Bytes to unpack
        
        Returns:
            Tuple of unpacked values
        
        Raises:
            struct.error: If data doesn't match format
            ValueError: If format string is invalid
        
        Example:
            >>> StructAction.unpack('iii', b'\\x01\\x00\\x00\\x00\\x02\\x00\\x00\\x00\\x03\\x00\\x00\\x00')
            (1, 2, 3)
        """
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(f"Expected bytes-like object, got {type(data)}")
        
        try:
            return struct.unpack(format_str, data)
        except struct.error as e:
            raise struct.error(f"Unpack error: {e}") from e
    
    @staticmethod
    def unpack_from(
        format_str: str,
        data: bytes,
        offset: int = 0,
        **kwargs: Any,
    ) -> UnpackResult:
        """
        Unpack bytes from the given offset according to the format string.
        
        Args:
            format_str: Struct format string
            data: Bytes to unpack
            offset: Byte offset to start unpacking
        
        Returns:
            Tuple of unpacked values
        
        Raises:
            struct.error: If data doesn't match format
            ValueError: If offset is negative
        
        Example:
            >>> data = b'\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x02'
            >>> StructAction.unpack_from('ii', data, 4)
            (1, 2)
        """
        if offset < 0:
            raise ValueError("Offset must be non-negative")
        
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(f"Expected bytes-like object, got {type(data)}")
        
        try:
            return struct.unpack_from(format_str, data, offset)
        except struct.error as e:
            raise struct.error(f"Unpack from error: {e}") from e
    
    @staticmethod
    def calcsize(format_str: str) -> int:
        """
        Calculate the size of the struct corresponding to the format string.
        
        Args:
            format_str: Struct format string
        
        Returns:
            Size in bytes required for the format
        
        Raises:
            ValueError: If format string is invalid
        
        Example:
            >>> StructAction.calcsize('iii')
            12
            >>> StructAction.calcsize('>H')
            2
        """
        try:
            return struct.calcsize(format_str)
        except struct.error as e:
            raise ValueError(f"Invalid format string: {e}") from e
    
    @staticmethod
    def pack_int8(value: int) -> PackResult:
        """
        Pack a signed 8-bit integer.
        
        Args:
            value: Integer value (-128 to 127)
        
        Returns:
            Packed bytes
        
        Example:
            >>> StructAction.pack_int8(127)
            b'\\x7f'
            >>> StructAction.pack_int8(-128)
            b'\\x80'
        """
        return struct.pack("b", value)
    
    @staticmethod
    def pack_uint8(value: int) -> PackResult:
        """
        Pack an unsigned 8-bit integer.
        
        Args:
            value: Integer value (0 to 255)
        
        Returns:
            Packed bytes
        
        Example:
            >>> StructAction.pack_uint8(255)
            b'\\xff'
        """
        return struct.pack("B", value)
    
    @staticmethod
    def pack_int16(value: int, *, big_endian: bool = False) -> PackResult:
        """
        Pack a signed 16-bit integer.
        
        Args:
            value: Integer value (-32768 to 32767)
            big_endian: Use big-endian byte order if True
        
        Returns:
            Packed bytes
        
        Example:
            >>> StructAction.pack_int16(32767)
            b'\\xff\\xff'
            >>> StructAction.pack_int16(32767, big_endian=True)
            b'\\x7f\\xff'
        """
        fmt = ">h" if big_endian else "<h"
        return struct.pack(fmt, value)
    
    @staticmethod
    def pack_uint16(value: int, *, big_endian: bool = False) -> PackResult:
        """
        Pack an unsigned 16-bit integer.
        
        Args:
            value: Integer value (0 to 65535)
            big_endian: Use big-endian byte order if True
        
        Returns:
            Packed bytes
        
        Example:
            >>> StructAction.pack_uint16(65535)
            b'\\xff\\xff'
        """
        fmt = ">H" if big_endian else "<H"
        return struct.pack(fmt, value)
    
    @staticmethod
    def pack_int32(value: int, *, big_endian: bool = False) -> PackResult:
        """
        Pack a signed 32-bit integer.
        
        Args:
            value: Integer value (-2147483648 to 2147483647)
            big_endian: Use big-endian byte order if True
        
        Returns:
            Packed bytes
        
        Example:
            >>> StructAction.pack_int32(2147483647)
            b'\\xff\\xff\\xff\\xff'
        """
        fmt = ">i" if big_endian else "<i"
        return struct.pack(fmt, value)
    
    @staticmethod
    def pack_uint32(value: int, *, big_endian: bool = False) -> PackResult:
        """
        Pack an unsigned 32-bit integer.
        
        Args:
            value: Integer value (0 to 4294967295)
            big_endian: Use big-endian byte order if True
        
        Returns:
            Packed bytes
        
        Example:
            >>> StructAction.pack_uint32(4294967295)
            b'\\xff\\xff\\xff\\xff'
        """
        fmt = ">I" if big_endian else "<I"
        return struct.pack(fmt, value)
    
    @staticmethod
    def pack_int64(value: int, *, big_endian: bool = False) -> PackResult:
        """
        Pack a signed 64-bit integer.
        
        Args:
            value: Integer value (-9223372036854775808 to 9223372036854775807)
            big_endian: Use big-endian byte order if True
        
        Returns:
            Packed bytes
        
        Example:
            >>> StructAction.pack_int64(9223372036854775807)
            b'\\xff\\xff\\xff\\xff\\xff\\xff\\xff\\xff'
        """
        fmt = ">q" if big_endian else "<q"
        return struct.pack(fmt, value)
    
    @staticmethod
    def pack_uint64(value: int, *, big_endian: bool = False) -> PackResult:
        """
        Pack an unsigned 64-bit integer.
        
        Args:
            value: Integer value (0 to 18446744073709551615)
            big_endian: Use big-endian byte order if True
        
        Returns:
            Packed bytes
        
        Example:
            >>> StructAction.pack_uint64(18446744073709551615)
            b'\\xff\\xff\\xff\\xff\\xff\\xff\\xff\\xff'
        """
        fmt = ">Q" if big_endian else "<Q"
        return struct.pack(fmt, value)
    
    @staticmethod
    def pack_float(value: float, *, big_endian: bool = False) -> PackResult:
        """
        Pack a single-precision float (32-bit).
        
        Args:
            value: Float value
            big_endian: Use big-endian byte order if True
        
        Returns:
            Packed bytes
        
        Example:
            >>> StructAction.pack_float(3.14159)
            b'\\xc3\\xf5H\\t'
        """
        fmt = ">f" if big_endian else "<f"
        return struct.pack(fmt, value)
    
    @staticmethod
    def pack_double(value: float, *, big_endian: bool = False) -> PackResult:
        """
        Pack a double-precision float (64-bit).
        
        Args:
            value: Float value
            big_endian: Use big-endian byte order if True
        
        Returns:
            Packed bytes
        
        Example:
            >>> StructAction.pack_double(3.14159265358979)
            b'\\x1f\\x85\\xebQ\\xb8\\x1e\\t\\xd1'
        """
        fmt = ">d" if big_endian else "<d"
        return struct.pack(fmt, value)
    
    @staticmethod
    def pack_string(value: str, length: Optional[int] = None) -> PackResult:
        """
        Pack a string as bytes.
        
        Args:
            value: String to pack
            length: Fixed length (pads with null bytes), or None for null-terminated
        
        Returns:
            Packed bytes
        
        Example:
            >>> StructAction.pack_string("hello", 10)
            b'hello\\x00\\x00\\x00\\x00\\x00'
            >>> StructAction.pack_string("hello")
            b'hello\\x00'
        """
        encoded = value.encode("utf-8")
        if length is None:
            return encoded + b"\x00"
        elif len(encoded) < length:
            return encoded + b"\x00" * (length - len(encoded))
        else:
            return encoded[:length]
    
    @staticmethod
    def unpack_int8(data: bytes) -> int:
        """
        Unpack a signed 8-bit integer.
        
        Args:
            data: Bytes to unpack
        
        Returns:
            Unpacked integer value
        
        Example:
            >>> StructAction.unpack_int8(b'\\x7f')
            127
        """
        return struct.unpack("b", data[:1])[0]
    
    @staticmethod
    def unpack_uint8(data: bytes) -> int:
        """
        Unpack an unsigned 8-bit integer.
        
        Args:
            data: Bytes to unpack
        
        Returns:
            Unpacked integer value
        
        Example:
            >>> StructAction.unpack_uint8(b'\\xff')
            255
        """
        return struct.unpack("B", data[:1])[0]
    
    @staticmethod
    def unpack_int16(data: bytes, *, big_endian: bool = False) -> int:
        """
        Unpack a signed 16-bit integer.
        
        Args:
            data: Bytes to unpack
            big_endian: Use big-endian byte order if True
        
        Returns:
            Unpacked integer value
        
        Example:
            >>> StructAction.unpack_int16(b'\\xff\\xff')
            -1
        """
        fmt = ">h" if big_endian else "<h"
        return struct.unpack(fmt, data[:2])[0]
    
    @staticmethod
    def unpack_uint16(data: bytes, *, big_endian: bool = False) -> int:
        """
        Unpack an unsigned 16-bit integer.
        
        Args:
            data: Bytes to unpack
            big_endian: Use big-endian byte order if True
        
        Returns:
            Unpacked integer value
        
        Example:
            >>> StructAction.unpack_uint16(b'\\xff\\xff')
            65535
        """
        fmt = ">H" if big_endian else "<H"
        return struct.unpack(fmt, data[:2])[0]
    
    @staticmethod
    def unpack_int32(data: bytes, *, big_endian: bool = False) -> int:
        """
        Unpack a signed 32-bit integer.
        
        Args:
            data: Bytes to unpack
            big_endian: Use big-endian byte order if True
        
        Returns:
            Unpacked integer value
        
        Example:
            >>> StructAction.unpack_int32(b'\\xff\\xff\\xff\\xff')
            -1
        """
        fmt = ">i" if big_endian else "<i"
        return struct.unpack(fmt, data[:4])[0]
    
    @staticmethod
    def unpack_uint32(data: bytes, *, big_endian: bool = False) -> int:
        """
        Unpack an unsigned 32-bit integer.
        
        Args:
            data: Bytes to unpack
            big_endian: Use big-endian byte order if True
        
        Returns:
            Unpacked integer value
        
        Example:
            >>> StructAction.unpack_uint32(b'\\xff\\xff\\xff\\xff')
            4294967295
        """
        fmt = ">I" if big_endian else "<I"
        return struct.unpack(fmt, data[:4])[0]
    
    @staticmethod
    def unpack_int64(data: bytes, *, big_endian: bool = False) -> int:
        """
        Unpack a signed 64-bit integer.
        
        Args:
            data: Bytes to unpack
            big_endian: Use big-endian byte order if True
        
        Returns:
            Unpacked integer value
        
        Example:
            >>> StructAction.unpack_int64(b'\\xff\\xff\\xff\\xff\\xff\\xff\\xff\\xff')
            -1
        """
        fmt = ">q" if big_endian else "<q"
        return struct.unpack(fmt, data[:8])[0]
    
    @staticmethod
    def unpack_uint64(data: bytes, *, big_endian: bool = False) -> int:
        """
        Unpack an unsigned 64-bit integer.
        
        Args:
            data: Bytes to unpack
            big_endian: Use big-endian byte order if True
        
        Returns:
            Unpacked integer value
        
        Example:
            >>> StructAction.unpack_uint64(b'\\xff\\xff\\xff\\xff\\xff\\xff\\xff\\xff')
            18446744073709551615
        """
        fmt = ">Q" if big_endian else "<Q"
        return struct.unpack(fmt, data[:8])[0]
    
    @staticmethod
    def unpack_float(data: bytes, *, big_endian: bool = False) -> float:
        """
        Unpack a single-precision float.
        
        Args:
            data: Bytes to unpack
            big_endian: Use big-endian byte order if True
        
        Returns:
            Unpacked float value
        """
        fmt = ">f" if big_endian else "<f"
        return struct.unpack(fmt, data[:4])[0]
    
    @staticmethod
    def unpack_double(data: bytes, *, big_endian: bool = False) -> float:
        """
        Unpack a double-precision float.
        
        Args:
            data: Bytes to unpack
            big_endian: Use big-endian byte order if True
        
        Returns:
            Unpacked float value
        """
        fmt = ">d" if big_endian else "<d"
        return struct.unpack(fmt, data[:8])[0]
    
    @staticmethod
    def unpack_string(data: bytes, length: Optional[int] = None) -> str:
        """
        Unpack a string from bytes.
        
        Args:
            data: Bytes to unpack
            length: Fixed length, or None for null-terminated
        
        Returns:
            Unpacked string
        
        Example:
            >>> StructAction.unpack_string(b'hello\\x00\\x00\\x00', 10)
            'hello'
        """
        if length is not None:
            return data[:length].rstrip(b"\x00").decode("utf-8", errors="replace")
        else:
            return data.split(b"\x00")[0].decode("utf-8", errors="replace")
    
    @staticmethod
    def create_packer(format_str: str) -> Callable[..., bytes]:
        """
        Create a packer function for the given format.
        
        Args:
            format_str: Struct format string
        
        Returns:
            Packer function
        
        Example:
            >>> pack_ints = StructAction.create_packer('iii')
            >>> pack_ints(1, 2, 3)
            b'\\x01\\x00\\x00\\x00\\x02\\x00\\x00\\x00\\x03\\x00\\x00\\x00'
        """
        return lambda *args: struct.pack(format_str, *args)
    
    @staticmethod
    def create_unpacker(format_str: str) -> Callable[[bytes], Tuple[Any, ...]]:
        """
        Create an unpacker function for the given format.
        
        Args:
            format_str: Struct format string
        
        Returns:
            Unpacker function
        
        Example:
            >>> unpack_ints = StructAction.create_unpacker('iii')
            >>> unpack_ints(b'\\x01\\x00\\x00\\x00\\x02\\x00\\x00\\x00\\x03\\x00\\x00\\x00')
            (1, 2, 3)
        """
        return lambda data: struct.unpack(format_str, data)
    
    @staticmethod
    def pack_struct(
        data: Dict[str, Any],
        format_str: str,
        field_names: Optional[List[str]] = None,
    ) -> PackResult:
        """
        Pack a dictionary into a struct.
        
        Args:
            data: Dictionary of field names to values
            format_str: Struct format string
            field_names: Ordered list of field names (uses dict order if None)
        
        Returns:
            Packed bytes
        
        Example:
            >>> StructAction.pack_struct({'x': 1, 'y': 2}, 'hh', ['x', 'y'])
            b'\\x01\\x00\\x02\\x00'
        """
        if field_names is None:
            field_names = list(data.keys())
        
        values = [data[name] for name in field_names]
        return struct.pack(format_str, *values)
    
    @staticmethod
    def unpack_struct(
        data: bytes,
        format_str: str,
        field_names: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Unpack bytes into a dictionary.
        
        Args:
            data: Bytes to unpack
            format_str: Struct format string
            field_names: Ordered list of field names
        
        Returns:
            Dictionary of field names to values
        
        Example:
            >>> StructAction.unpack_struct(b'\\x01\\x00\\x02\\x00', 'hh', ['x', 'y'])
            {'x': 1, 'y': 2}
        """
        values = struct.unpack(format_str, data)
        if field_names is None:
            return {"field_" + str(i): v for i, v in enumerate(values)}
        return dict(zip(field_names, values))


# Convenience function aliases
def pack(format_str: str, *values: Any) -> bytes:
    """Pack values according to format string."""
    return StructAction.pack(format_str, *values)


def unpack(format_str: str, data: bytes) -> Tuple[Any, ...]:
    """Unpack bytes according to format string."""
    return StructAction.unpack(format_str, data)


def pack_int32(value: int, **kwargs: Any) -> bytes:
    """Pack a 32-bit integer."""
    return StructAction.pack_int32(value, **kwargs)


def unpack_int32(data: bytes, **kwargs: Any) -> int:
    """Unpack a 32-bit integer."""
    return StructAction.unpack_int32(data, **kwargs)


def pack_float(value: float, **kwargs: Any) -> bytes:
    """Pack a float."""
    return StructAction.pack_float(value, **kwargs)


def unpack_float(data: bytes, **kwargs: Any) -> float:
    """Unpack a float."""
    return StructAction.unpack_float(data, **kwargs)


# Module metadata
__author__ = "AI Assistant"
__version__ = "1.0.0"
__all__ = [
    "StructAction",
    "pack",
    "unpack",
    "pack_int32",
    "unpack_int32",
    "pack_float",
    "unpack_float",
]
