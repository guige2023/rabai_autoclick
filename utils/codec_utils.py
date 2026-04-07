"""
Codec and encoding utilities.

Provides data encoding/decoding, compression encoding,
checksum calculators, and binary protocol utilities.
"""

from __future__ import annotations

import base64
import struct
import zlib


def encode_varint(value: int) -> bytes:
    """
    Encode integer as variable-length bytes (Protocol Buffers style).

    Args:
        value: Non-negative integer

    Returns:
        Variable-length encoded bytes.
    """
    if value < 0:
        raise ValueError("Varint must be non-negative")
    result = bytearray()
    while value >= 0x80:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value & 0x7F)
    return bytes(result)


def decode_varint(data: bytes) -> tuple[int, int]:
    """
    Decode variable-length integer.

    Returns:
        Tuple of (value, bytes_consumed).
    """
    value = 0
    shift = 0
    pos = 0
    while pos < len(data):
        byte = data[pos]
        value |= (byte & 0x7F) << shift
        pos += 1
        if not (byte & 0x80):
            break
        shift += 7
    return value, pos


def encode_7bit_chained(data: bytes) -> bytes:
    """
    7-bit chained encoding (used in some image formats).

    Args:
        data: Input bytes

    Returns:
        Encoded bytes.
    """
    result = bytearray()
    i = 0
    while i < len(data):
        chunk = data[i:i+7]
        for j, b in enumerate(chunk):
            if j < len(chunk) - 1:
                result.append((b << 1) | ((chunk[j + 1] >> 7) & 1))
            else:
                result.append(b << 1)
        i += 7
    return bytes(result)


def crc16(data: bytes, polynomial: int = 0x1021, init: int = 0xFFFF) -> int:
    """
    CRC-16 checksum.

    Args:
        data: Input bytes
        polynomial: CRC polynomial
        init: Initial value

    Returns:
        16-bit CRC value.
    """
    crc = init
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = (crc << 1) ^ polynomial
            else:
                crc <<= 1
            crc &= 0xFFFF
    return crc


def crc32_checksum(data: bytes) -> int:
    """CRC-32 checksum (zlib)."""
    return zlib.crc32(data) & 0xFFFFFFFF


def adler32_checksum(data: bytes) -> int:
    """Adler-32 checksum."""
    return zlib.adler32(data) & 0xFFFFFFFF


def fletcher16(data: bytes) -> int:
    """Fletcher-16 checksum."""
    sum1 = 0
    sum2 = 0
    for byte in data:
        sum1 = (sum1 + byte) % 255
        sum2 = (sum2 + sum1) % 255
    return (sum2 << 8) | sum1


def pack_struct(fmt: str, *values: Any) -> bytes:
    """
    Pack values into binary using struct format.

    Args:
        fmt: Struct format string (e.g., '<III' for little-endian 3 unsigned ints)
        values: Values to pack

    Returns:
        Packed bytes.
    """
    return struct.pack(fmt, *values)


def unpack_struct(fmt: str, data: bytes) -> tuple:
    """Unpack binary data using struct format."""
    return struct.unpack(fmt, data)


def byteswap(data: bytes, swap_size: int = 2) -> bytes:
    """
    Byte-swap data.

    Args:
        data: Input bytes
        swap_size: Size of units to swap (2 for 16-bit, 4 for 32-bit)

    Returns:
        Byte-swapped data.
    """
    result = bytearray(data)
    for i in range(0, len(result) - swap_size + 1, swap_size):
        result[i:i+swap_size] = reversed(result[i:i+swap_size])
    return bytes(result)


def nibbles(data: bytes) -> list[int]:
    """Split bytes into nibbles (4-bit values)."""
    return [(b >> 4) & 0xF for b in data] + [b & 0xF for b in data]


def hex_encode(data: bytes, uppercase: bool = False) -> str:
    """Encode bytes as hex string."""
    fmt = "{:02X}" if uppercase else "{:02x}"
    return "".join(fmt.format(b) for b in data)


def hex_decode(hex_str: str) -> bytes:
    """Decode hex string to bytes."""
    if len(hex_str) % 2 != 0:
        hex_str = "0" + hex_str
    return bytes(int(hex_str[i:i+2], 16) for i in range(0, len(hex_str), 2))


def base32_encode(data: bytes) -> str:
    """Base32 encoding."""
    return base64.b32encode(data).decode()


def base32_decode(data: str) -> bytes:
    """Base32 decoding."""
    return base64.b32decode(data)


def base16_encode(data: bytes) -> str:
    """Base16 (hex) encoding."""
    return base64.b16encode(data).decode()


def base16_decode(data: str) -> bytes:
    """Base16 decoding."""
    return base64.b16decode(data)


def gzip_compress(data: bytes, level: int = 6) -> bytes:
    """
    Compress bytes using gzip.

    Args:
        data: Input data
        level: Compression level (1-9)

    Returns:
        Compressed data.
    """
    return zlib.compress(data, level)


def gzip_decompress(data: bytes) -> bytes:
    """Decompress gzip data."""
    return zlib.decompress(data)


def deflate_compress(data: bytes, level: int = 6) -> bytes:
    """Compress using zlib deflate."""
    return zlib.compress(data, level)


def deflate_decompress(data: bytes) -> bytes:
    """Decompress deflate data."""
    return zlib.decompress(data)


def zlib_encode(data: bytes, level: int = 6) -> bytes:
    """Encode using zlib wrapper."""
    return zlib.compress(data, level)


def zlib_decode(data: bytes) -> bytes:
    """Decode zlib-wrapped data."""
    return zlib.decompress(data)


def packed_bcd(value: int, digits: int) -> bytes:
    """
    Pack integer as packed BCD (Binary Coded Decimal).

    Args:
        value: Integer value
        digits: Number of decimal digits

    Returns:
        Packed BCD bytes.
    """
    result = bytearray()
    for i in range(digits - 1, -1, -1):
        digit = (value // (10 ** i)) % 10 if value > 0 else 0
        if len(result) == 0:
            result.append(digit << 4)
        else:
            result[-1] |= digit
            result.append(0)
    return bytes(result)


def unpacked_bcd(data: bytes) -> int:
    """Unpack BCD bytes to integer."""
    result = 0
    for byte in data:
        high = (byte >> 4) & 0x0F
        low = byte & 0x0F
        result = result * 10 + high
        if low != 0x0F:
            result = result * 10 + low
    return result


def encode_7bit_ascii(text: str) -> bytes:
    """Encode string as 7-bit ASCII."""
    return bytes(ord(c) & 0x7F for c in text)


def decode_7bit_ascii(data: bytes) -> str:
    """Decode 7-bit ASCII bytes."""
    return "".join(chr(b & 0x7F) for b in data if b != 0)


def lzw_compress(data: bytes, max_bits: int = 12) -> bytes:
    """
    LZW compression (GIF-style).

    Args:
        data: Input bytes
        max_bits: Maximum code size in bits

    Returns:
        Compressed data.
    """
    clear_code = 1 << (max_bits - 1)
    end_code = clear_code + 1
    max_code = clear_code << 1

    dict_table: dict[bytes, int] = {bytes([b]): b + 1 for b in range(256)}
    next_code = end_code + 1
    bit_buffer = 0
    bit_count = 0
    result = bytearray()
    output = []

    def output_code(code: int) -> None:
        nonlocal bit_buffer, bit_count
        bit_buffer |= code << bit_count
        bit_count += max_bits
        while bit_count >= 8:
            output.append(bit_buffer & 0xFF)
            bit_buffer >>= 8
            bit_count -= 8

    output_code(clear_code)
    current = b""
    for byte in data:
        current_byte = bytes([byte])
        combined = current + current_byte
        if combined in dict_table:
            current = combined
        else:
            output_code(dict_table[current])
            if next_code < (1 << max_bits):
                dict_table[combined] = next_code
                next_code += 1
            current = current_byte

    if current:
        output_code(dict_table[current])
    output_code(end_code)

    if bit_count > 0:
        output.append(bit_buffer & 0xFF)

    return bytes(output)


def lzw_decompress(data: bytes, max_bits: int = 12) -> bytes:
    """LZW decompression."""
    clear_code = 1 << (max_bits - 1)
    end_code = clear_code + 1

    dict_table: dict[int, bytes] = {i: bytes([i]) for i in range(256)}
    next_code = end_code + 1

    bit_buffer = 0
    bit_count = 0
    pos = 0

    def read_code() -> int:
        nonlocal bit_buffer, bit_count, pos
        while bit_count < max_bits and pos < len(data):
            bit_buffer |= data[pos] << bit_count
            bit_count += 8
            pos += 1
        code = bit_buffer & ((1 << max_bits) - 1)
        bit_buffer >>= max_bits
        bit_count -= max_bits
        return code

    result = bytearray()
    code = read_code()
    if code != clear_code:
        return b""

    old = bytes([read_code()])
    result.extend(old)

    while True:
        code = read_code()
        if code == end_code or code >= next_code:
            break
        if code == clear_code:
            dict_table = {i: bytes([i]) for i in range(256)}
            next_code = end_code + 1
            old = bytes([read_code()])
            result.extend(old)
            continue
        if code < next_code:
            entry = dict_table.get(code, old + old[0:1])
        else:
            entry = old + old[0:1]
        result.extend(entry)
        dict_table[next_code] = old + entry[0:1]
        next_code += 1
        old = entry

    return bytes(result)
