"""
Encoding and decoding utilities for various data formats.

Provides implementations for base64, URL encoding, HTML entities,
hex encoding, and unicode escaping.
"""

from __future__ import annotations

import base64
import html
import quopri
import urllib.parse
from typing import Callable


def base64_encode(data: bytes | str) -> str:
    """
    Encode data as base64.

    Args:
        data: Bytes or string to encode

    Returns:
        Base64-encoded string

    Example:
        >>> base64_encode("Hello")
        'SGVsbG8='
    """
    if isinstance(data, str):
        data = data.encode("utf-8")
    return base64.b64encode(data).decode("ascii")


def base64_decode(data: str) -> bytes:
    """
    Decode a base64 string.

    Args:
        data: Base64-encoded string

    Returns:
        Decoded bytes

    Example:
        >>> base64_decode("SGVsbG8=").decode()
        'Hello'
    """
    # Add padding if needed
    missing = (4 - len(data) % 4) % 4
    if missing:
        data += "=" * missing
    return base64.b64decode(data)


def base64_url_encode(data: bytes | str) -> str:
    """
    Encode data as URL-safe base64.

    Args:
        data: Bytes or string to encode

    Returns:
        URL-safe base64 string

    Example:
        >>> base64_url_encode(b"Hello!")
        'SGVsbG8h'
    """
    if isinstance(data, str):
        data = data.encode("utf-8")
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def base64_url_decode(data: str) -> bytes:
    """
    Decode a URL-safe base64 string.

    Args:
        data: URL-safe base64 string

    Returns:
        Decoded bytes
    """
    # Restore padding
    rem = len(data) % 4
    if rem:
        data += "=" * (4 - rem)
    return base64.urlsafe_b64decode(data)


def url_encode(s: str, safe: str = "/") -> str:
    """
    Percent-encode a string for URL query parameters.

    Args:
        s: String to encode
        safe: Characters to keep unencoded

    Returns:
        URL-encoded string

    Example:
        >>> url_encode("hello world")
        'hello+world'
    """
    return urllib.parse.quote(s, safe=safe)


def url_decode(s: str) -> str:
    """
    Decode a URL-encoded string.

    Args:
        s: URL-encoded string

    Returns:
        Decoded string

    Example:
        >>> url_decode("hello+world")
        'hello world'
    """
    return urllib.parse.unquote(s)


def html_encode(s: str, quotes: bool = True) -> str:
    """
    Escape HTML special characters.

    Args:
        s: String to encode
        quotes: Whether to escape quotes as well

    Returns:
        HTML-escaped string

    Example:
        >>> html_encode('<script>')
        '&lt;script&gt;'
    """
    return html.escape(s, quotes=quotes)


def html_decode(s: str) -> str:
    """
    Unescape HTML entities.

    Args:
        s: String with HTML entities

    Returns:
        Unescaped string

    Example:
        >>> html_decode('&lt;script&gt;')
        '<script>'
    """
    return html.unescape(s)


def hex_encode(data: bytes | str) -> str:
    """
    Encode data as hexadecimal.

    Args:
        data: Bytes or string to encode

    Returns:
        Hexadecimal string

    Example:
        >>> hex_encode("Hello")
        '48656c6c6f'
    """
    if isinstance(data, str):
        data = data.encode("utf-8")
    return data.hex()


def hex_decode(s: str) -> bytes:
    """
    Decode a hexadecimal string.

    Args:
        s: Hex string (with or without 0x prefix)

    Returns:
        Decoded bytes

    Example:
        >>> hex_decode("48656c6c6f").decode()
        'Hello'
    """
    if s.startswith("0x") or s.startswith("0X"):
        s = s[2:]
    return bytes.fromhex(s)


def quoted_printable_encode(s: str) -> str:
    """
    Encode string as quoted-printable.

    Args:
        s: String to encode

    Returns:
        Quoted-printable encoded string
    """
    return quopri.encodestring(s.encode("utf-8")).decode("ascii").strip()


def quoted_printable_decode(s: str) -> str:
    """
    Decode a quoted-printable string.

    Args:
        s: Quoted-printable encoded string

    Returns:
        Decoded string
    """
    return quopri.decodestring(s.encode("ascii")).decode("utf-8")


def unicode_escape(s: str) -> str:
    """
    Escape Unicode characters as \\uXXXX sequences.

    Args:
        s: String to escape

    Returns:
        Unicode-escaped string

    Example:
        >>> unicode_escape("中文")
        '\\\\u4e2d\\\\u6587'
    """
    result = []
    for c in s:
        if ord(c) > 127 or c in '\\"':
            result.append(f"\\u{ord(c):04x}")
        else:
            result.append(c)
    return "".join(result)


def unicode_unescape(s: str) -> str:
    """
    Unescape \\uXXXX sequences to Unicode characters.

    Args:
        s: Unicode-escaped string

    Returns:
        Decoded string
    """
    import re
    return re.sub(r"\\u([0-9a-fA-F]{4})", lambda m: chr(int(m.group(1), 16)), s)


def rot13(s: str) -> str:
    """
    Apply ROT13 cipher to a string.

    Args:
        s: String to encode

    Returns:
        ROT13-encoded string

    Example:
        >>> rot13("hello")
        'uryyb'
    """
    def rot_char(c: str) -> str:
        if "a" <= c <= "z":
            return chr((ord(c) - ord("a") + 13) % 26 + ord("a"))
        if "A" <= c <= "Z":
            return chr((ord(c) - ord("A") + 13) % 26 + ord("A"))
        return c
    return "".join(rot_char(c) for c in s)


def caesar_cipher(s: str, shift: int) -> str:
    """
    Apply Caesar cipher with given shift.

    Args:
        s: String to encode
        shift: Number of positions to shift (positive = right)

    Returns:
        Caesar cipher encoded string

    Example:
        >>> caesar_cipher("abc", 3)
        'def'
    """
    def shift_char(c: str, sh: int) -> str:
        if "a" <= c <= "z":
            return chr((ord(c) - ord("a") + sh) % 26 + ord("a"))
        if "A" <= c <= "Z":
            return chr((ord(c) - ord("A") + sh) % 26 + ord("A"))
        return c
    return "".join(shift_char(c, shift) for c in s)
