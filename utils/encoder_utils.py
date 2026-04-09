"""Encoder utilities for data serialization and encoding.

Provides encoding/decoding utilities for common formats
used in automation workflows: base64, hex, URL encoding,
HTML entities, and structured data formats.

Example:
    >>> from utils.encoder_utils import b64_encode, url_encode_params, escape_html
    >>> b64_encode("hello")
    'aGVsbG8='
    >>> url_encode_params({"x": 100, "y": "hello world"})
    'x=100&y=hello+world'
"""

from __future__ import annotations

import base64
import html
import json
import quopri
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple, Union


def b64_encode(data: Union[str, bytes]) -> str:
    """Encode bytes/string to base64.

    Args:
        data: String or bytes to encode.

    Returns:
        Base64 encoded string.
    """
    if isinstance(data, str):
        data = data.encode("utf-8")
    return base64.b64encode(data).decode("ascii")


def b64_decode(data: str) -> bytes:
    """Decode base64 to bytes.

    Args:
        data: Base64 string.

    Returns:
        Decoded bytes.
    """
    return base64.b64decode(data)


def b64_encode_json(obj: Any) -> str:
    """Encode an object as base64 JSON.

    Args:
        obj: Object to serialize and encode.

    Returns:
        Base64 encoded JSON string.
    """
    json_bytes = json.dumps(obj, default=str).encode("utf-8")
    return base64.b64encode(json_bytes).decode("ascii")


def b64_decode_json(data: str) -> Any:
    """Decode base64 JSON to object.

    Args:
        data: Base64 encoded JSON.

    Returns:
        Decoded Python object.
    """
    return json.loads(base64.b64decode(data))


def hex_encode(data: Union[str, bytes]) -> str:
    """Encode bytes to hex string.

    Args:
        data: String or bytes.

    Returns:
        Hex string.
    """
    if isinstance(data, str):
        data = data.encode("utf-8")
    return data.hex()


def hex_decode(data: str) -> bytes:
    """Decode hex string to bytes.

    Args:
        data: Hex string.

    Returns:
        Decoded bytes.
    """
    return bytes.fromhex(data)


def url_encode_params(params: Dict[str, Any]) -> str:
    """Encode dict as URL query parameters.

    Args:
        params: Dict of parameters.

    Returns:
        URL-encoded query string.

    Example:
        >>> url_encode_params({"x": 100, "name": "Alice"})
        'x=100&name=Alice'
    """
    clean: Dict[str, str] = {}
    for k, v in params.items():
        clean[str(k)] = str(v) if v is not None else ""
    return urllib.parse.urlencode(clean)


def url_decode_params(query: str) -> Dict[str, str]:
    """Decode URL query string to dict.

    Args:
        query: Query string (without leading '?').

    Returns:
        Dict of parameters.
    """
    return dict(urllib.parse.parse_qsl(query))


def escape_html(text: str) -> str:
    """Escape HTML special characters.

    Args:
        text: Raw text.

    Returns:
        HTML-escaped text.

    Example:
        >>> escape_html("<div>")
        '&lt;div&gt;'
    """
    return html.escape(text)


def unescape_html(text: str) -> str:
    """Unescape HTML entities.

    Args:
        text: HTML-escaped text.

    Returns:
        Raw text.
    """
    return html.unescape(text)


def escape_shell_arg(text: str) -> str:
    """Escape a string for safe shell embedding.

    Args:
        text: String to escape.

    Returns:
        Shell-safe quoted string.
    """
    return "'" + text.replace("'", "'\\''") + "'"


def escape_json_string(text: str) -> str:
    """Escape a string for JSON embedding.

    Args:
        text: Raw string.

    Returns:
        JSON-safe string.
    """
    return json.dumps(text)[1:-1]


def encode_quoted_printable(text: str) -> bytes:
    """Encode text as quoted-printable.

    Args:
        text: Text to encode.

    Returns:
        Quoted-printable encoded bytes.
    """
    return quopri.encodestring(text.encode("utf-8"))


def decode_quoted_printable(data: bytes) -> str:
    """Decode quoted-printable to text.

    Args:
        data: Quoted-printable bytes.

    Returns:
        Decoded string.
    """
    return quopri.decodestring(data).decode("utf-8")


def encode_punycode(domain: str) -> str:
    """Encode domain to punycode.

    Args:
        domain: Unicode domain name.

    Returns:
        Punycode domain string.
    """
    return domain.encode("idna").decode("ascii")


def decode_punycode(domain: str) -> str:
    """Decode punycode to unicode domain.

    Args:
        domain: Punycode domain.

    Returns:
        Unicode domain name.
    """
    return domain.encode("ascii").decode("idna")


def percent_encode(text: str) -> str:
    """Percent-encode a string (RFC 3986).

    Args:
        text: Text to encode.

    Returns:
        Percent-encoded string.
    """
    return urllib.parse.quote(text, safe="")


def percent_decode(text: str) -> str:
    """Decode percent-encoded string.

    Args:
        text: Percent-encoded string.

    Returns:
        Decoded string.
    """
    return urllib.parse.unquote(text)


def encode_varint(value: int) -> bytes:
    """Encode an integer as a variable-length integer.

    Args:
        value: Integer to encode (must be non-negative).

    Returns:
        Varint bytes.
    """
    if value < 0:
        raise ValueError("Varint cannot encode negative values")
    result = bytearray()
    while value > 0x7F:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value & 0x7F)
    return bytes(result)


def decode_varint(data: bytes) -> Tuple[int, int]:
    """Decode a variable-length integer.

    Args:
        data: Varint bytes.

    Returns:
        (value, bytes_consumed) tuple.
    """
    value = 0
    shift = 0
    pos = 0
    for i, b in enumerate(data):
        if i > 10:
            raise ValueError("Varint too long")
        value |= (b & 0x7F) << shift
        pos = i + 1
        if not (b & 0x80):
            break
        shift += 7
    return value, pos


def encode_cstruct(
    fmt: str,
    *values: Any,
) -> bytes:
    """Encode values as a C struct.

    Args:
        fmt: Struct format string (e.g., '<iih' for little-endian ints).
        *values: Values to pack.

    Returns:
        Packed bytes.
    """
    import struct
    return struct.pack(fmt, *values)


def decode_cstruct(
    fmt: str,
    data: bytes,
) -> Tuple[Any, ...]:
    """Decode a C struct.

    Args:
        fmt: Struct format string.
        data: Packed bytes.

    Returns:
        Tuple of unpacked values.
    """
    import struct
    return struct.unpack(fmt, data)
