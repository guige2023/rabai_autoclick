"""codec action extensions for rabai_autoclick.

Provides encoding/decoding utilities for various formats including
JSON, base64, hex, URL encoding, HTML entities, and more.
"""

from __future__ import annotations

import base64
import binascii
import codecs
import html
import json
import quopri
import urllib.parse
from typing import Any, Callable, Sequence

__all__ = [
    "encode_base64",
    "decode_base64",
    "encode_base32",
    "decode_base32",
    "encode_base16",
    "decode_base16",
    "encode_hex",
    "decode_hex",
    "encode_url",
    "decode_url",
    "encode_url_component",
    "decode_url_component",
    "encode_html",
    "decode_html",
    "encode_json",
    "decode_json",
    "encode_xml",
    "decode_xml",
    "encode_quoted_printable",
    "decode_quoted_printable",
    "encode_percent",
    "decode_percent",
    "encode_mime",
    "decode_mime",
    "encode_utf8",
    "decode_utf8",
    "encode_ascii",
    "decode_ascii",
    "encode_bytes",
    "decode_bytes",
    "CodecResult",
    "CodecChain",
    "EncodingPipeline",
    "detect_encoding",
    "normalize_encoding",
]


class CodecResult:
    """Result of an encoding/decoding operation."""

    def __init__(
        self,
        success: bool,
        data: bytes | str | None = None,
        error: str | None = None,
    ) -> None:
        self.success = success
        self.data = data
        self.error = error

    def __repr__(self) -> str:
        if self.success:
            return f"CodecResult(success=True, data={self.data!r})"
        return f"CodecResult(success=False, error={self.error!r})"


def encode_base64(data: bytes | str) -> str:
    """Encode data as base64.

    Args:
        data: Data to encode.

    Returns:
        Base64 encoded string.
    """
    if isinstance(data, str):
        data = data.encode("utf-8")
    return base64.b64encode(data).decode("ascii")


def decode_base64(data: str) -> bytes:
    """Decode base64 string.

    Args:
        data: Base64 string to decode.

    Returns:
        Decoded bytes.
    """
    return base64.b64decode(data)


def encode_base32(data: bytes | str) -> str:
    """Encode data as base32.

    Args:
        data: Data to encode.

    Returns:
        Base32 encoded string.
    """
    if isinstance(data, str):
        data = data.encode("utf-8")
    return base64.b32encode(data).decode("ascii")


def decode_base32(data: str) -> bytes:
    """Decode base32 string.

    Args:
        data: Base32 string to decode.

    Returns:
        Decoded bytes.
    """
    return base64.b32decode(data)


def encode_base16(data: bytes | str) -> str:
    """Encode data as base16 (hex).

    Args:
        data: Data to encode.

    Returns:
        Hex encoded string.
    """
    if isinstance(data, str):
        data = data.encode("utf-8")
    return base64.b16encode(data).decode("ascii")


def decode_base16(data: str) -> bytes:
    """Decode base16 (hex) string.

    Args:
        data: Hex string to decode.

    Returns:
        Decoded bytes.
    """
    return base64.b16decode(data)


def encode_hex(data: bytes | str, encoding: str = "utf-8") -> str:
    """Encode data as hexadecimal.

    Args:
        data: Data to encode.
        encoding: Text encoding.

    Returns:
        Hex encoded string.
    """
    if isinstance(data, str):
        data = data.encode(encoding)
    return data.hex()


def decode_hex(data: str) -> bytes:
    """Decode hexadecimal string.

    Args:
        data: Hex string to decode.

    Returns:
        Decoded bytes.
    """
    return bytes.fromhex(data)


def encode_url(data: str, encoding: str = "utf-8") -> str:
    """Encode URL (percent encoding).

    Args:
        data: String to encode.
        encoding: Text encoding.

    Returns:
        URL encoded string.
    """
    return urllib.parse.quote(data, safe="")


def decode_url(data: str, encoding: str = "utf-8") -> str:
    """Decode URL (percent decoding).

    Args:
        data: URL encoded string.
        encoding: Text encoding.

    Returns:
        Decoded string.
    """
    return urllib.parse.unquote(data, encoding=encoding)


def encode_url_component(data: str) -> str:
    """Encode URL component.

    Args:
        data: String to encode.

    Returns:
        Encoded component string.
    """
    return urllib.parse.quote_plus(data)


def decode_url_component(data: str) -> str:
    """Decode URL component.

    Args:
        data: Encoded component string.

    Returns:
        Decoded string.
    """
    return urllib.parse.unquote_plus(data)


def encode_html(data: str) -> str:
    """Encode HTML special characters.

    Args:
        data: String to encode.

    Returns:
        HTML encoded string.
    """
    return html.escape(data)


def decode_html(data: str) -> str:
    """Decode HTML entities.

    Args:
        data: HTML encoded string.

    Returns:
        Decoded string.
    """
    return html.unescape(data)


def encode_json(data: Any) -> str:
    """Encode data as JSON.

    Args:
        data: Object to encode.

    Returns:
        JSON string.
    """
    return json.dumps(data, ensure_ascii=False)


def decode_json(data: str) -> Any:
    """Decode JSON string.

    Args:
        data: JSON string to decode.

    Returns:
        Decoded object.
    """
    return json.loads(data)


def encode_xml(data: str) -> str:
    """Encode XML special characters.

    Args:
        data: String to encode.

    Returns:
        XML encoded string.
    """
    return (
        data.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def decode_xml(data: str) -> str:
    """Decode XML entities.

    Args:
        data: XML encoded string.

    Returns:
        Decoded string.
    """
    return (
        data.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", '"')
        .replace("&apos;", "'")
        .replace("&amp;", "&")
    )


def encode_quoted_printable(data: str) -> str:
    """Encode data as quoted-printable.

    Args:
        data: String to encode.

    Returns:
        Quoted-printable encoded string.
    """
    return quopri.encodestring(data.encode("utf-8")).decode("ascii")


def decode_quoted_printable(data: str) -> str:
    """Decode quoted-printable string.

    Args:
        data: Quoted-printable string.

    Returns:
        Decoded string.
    """
    return quopri.decodestring(data.encode("ascii")).decode("utf-8")


def encode_percent(data: str) -> str:
    """Encode using percent encoding (alias for URL encoding).

    Args:
        data: String to encode.

    Returns:
        Percent encoded string.
    """
    return encode_url(data)


def decode_percent(data: str) -> str:
    """Decode percent-encoded string.

    Args:
        data: Percent encoded string.

    Returns:
        Decoded string.
    """
    return decode_url(data)


def encode_mime(data: str) -> str:
    """Encode for MIME transfer.

    Args:
        data: String to encode.

    Returns:
        MIME encoded string.
    """
    encoded = data.encode("utf-8")
    return f"=?utf-8?B?{encode_base64(encoded)}?="


def decode_mime(data: str) -> str:
    """Decode MIME encoded string.

    Args:
        data: MIME encoded string.

    Returns:
        Decoded string.
    """
    if data.startswith("=?") and data.endswith("?="):
        parts = data[2:-2].split("?")
        if len(parts) >= 3 and parts[2] == "B":
            return decode_base64(parts[3]).decode("utf-8")
    return data


def encode_utf8(data: str) -> bytes:
    """Encode string as UTF-8 bytes.

    Args:
        data: String to encode.

    Returns:
        UTF-8 bytes.
    """
    return data.encode("utf-8")


def decode_utf8(data: bytes) -> str:
    """Decode UTF-8 bytes to string.

    Args:
        data: Bytes to decode.

    Returns:
        Decoded string.
    """
    return data.decode("utf-8")


def encode_ascii(data: str) -> bytes:
    """Encode string as ASCII bytes.

    Args:
        data: String to encode.

    Returns:
        ASCII bytes.

    Raises:
        UnicodeEncodeError: If string contains non-ASCII.
    """
    return data.encode("ascii")


def decode_ascii(data: bytes) -> str:
    """Decode ASCII bytes to string.

    Args:
        data: Bytes to decode.

    Returns:
        Decoded string.
    """
    return data.decode("ascii")


def encode_bytes(data: str, encoding: str = "utf-8") -> bytes:
    """Encode string to bytes.

    Args:
        data: String to encode.
        encoding: Text encoding.

    Returns:
        Encoded bytes.
    """
    return data.encode(encoding)


def decode_bytes(data: bytes, encoding: str = "utf-8") -> str:
    """Decode bytes to string.

    Args:
        data: Bytes to decode.
        encoding: Text encoding.

    Returns:
        Decoded string.
    """
    return data.decode(encoding)


def detect_encoding(data: bytes) -> str | None:
    """Detect the encoding of bytes.

    Args:
        data: Bytes to detect.

    Returns:
        Detected encoding name or None.
    """
    try:
        data.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        pass

    try:
        data.decode("ascii")
        return "ascii"
    except UnicodeDecodeError:
        pass

    return None


def normalize_encoding(encoding: str) -> str:
    """Normalize encoding name to standard form.

    Args:
        encoding: Encoding name (case-insensitive).

    Returns:
        Normalized encoding name.
    """
    import codecs
    normalized = codecs.lookup(encoding).name
    return normalized


class CodecChain:
    """Chain multiple encoding operations."""

    def __init__(self) -> None:
        self._steps: list[tuple[str, Callable]] = []

    def add(
        self,
        name: str,
        encoder: Callable[[Any], Any],
    ) -> CodecChain:
        """Add an encoding step.

        Args:
            name: Step name.
            encoder: Encoding function.

        Returns:
            Self for chaining.
        """
        self._steps.append((name, encoder))
        return self

    def encode(self, data: Any) -> Any:
        """Encode data through the chain.

        Args:
            data: Data to encode.

        Returns:
            Encoded data.
        """
        result = data
        for name, encoder in self._steps:
            result = encoder(result)
        return result

    def decode(self, data: Any) -> Any:
        """Decode data through the chain in reverse.

        Args:
            data: Data to decode.

        Returns:
            Decoded data.
        """
        result = data
        for name, encoder in reversed(self._steps):
            if hasattr(encoder, "__name__") and "decode" in encoder.__name__:
                result = encoder(result)
        return result


class EncodingPipeline:
    """Pipeline for chaining encoding/decoding operations.

    Example:
        pipeline = EncodingPipeline()
        result = pipeline.chain(
            lambda x: encode_base64(x),
            lambda x: encode_hex(x),
        ).execute("data")
    """

    def __init__(self) -> None:
        self._steps: list[Callable] = []

    def chain(self, *funcs: Callable) -> EncodingPipeline:
        """Add steps to the pipeline.

        Args:
            *funcs: Functions to chain.

        Returns:
            Self for chaining.
        """
        self._steps.extend(funcs)
        return self

    def execute(self, data: Any, reverse: bool = False) -> Any:
        """Execute the pipeline.

        Args:
            data: Input data.
            reverse: Process in reverse order.

        Returns:
            Processed data.
        """
        steps = reversed(self._steps) if reverse else self._steps
        result = data
        for step in steps:
            result = step(result)
        return result

    def encode(self, data: Any) -> Any:
        """Encode through pipeline.

        Args:
            data: Data to encode.

        Returns:
            Encoded data.
        """
        return self.execute(data, reverse=False)

    def decode(self, data: Any) -> Any:
        """Decode through pipeline.

        Args:
            data: Data to decode.

        Returns:
            Decoded data.
        """
        return self.execute(data, reverse=True)
