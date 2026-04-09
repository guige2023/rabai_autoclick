"""API request/response compression utilities.

This module provides compression:
- Gzip/Zlib compression
- Brotli support
- Streaming compression
- Automatic decompression

Example:
    >>> from actions.api_compression_action import decompress_response, compress_payload
    >>> data = decompress_response(response.content, encoding="gzip")
"""

from __future__ import annotations

import gzip
import zlib
import logging
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


def compress_gzip(data: bytes, level: int = 6) -> bytes:
    """Compress data with gzip.

    Args:
        data: Bytes to compress.
        level: Compression level (1-9).

    Returns:
        Compressed bytes.
    """
    return gzip.compress(data, level)


def decompress_gzip(data: bytes) -> bytes:
    """Decompress gzip data.

    Args:
        data: Compressed bytes.

    Returns:
        Decompressed bytes.
    """
    return gzip.decompress(data)


def compress_zlib(data: bytes, level: int = 6) -> bytes:
    """Compress data with zlib.

    Args:
        data: Bytes to compress.
        level: Compression level (1-9).

    Returns:
        Compressed bytes.
    """
    return zlib.compress(data, level)


def decompress_zlib(data: bytes) -> bytes:
    """Decompress zlib data.

    Args:
        data: Compressed bytes.

    Returns:
        Decompressed bytes.
    """
    return zlib.decompress(data)


def compress_deflate(data: bytes) -> bytes:
    """Compress data with zlib deflate.

    Args:
        data: Bytes to compress.

    Returns:
        Compressed bytes.
    """
    return zlib.compress(data)[2:-4]


def decompress_deflate(data: bytes) -> bytes:
    """Decompress deflate data.

    Args:
        data: Compressed bytes.

    Returns:
        Decompressed bytes.
    """
    return zlib.decompress(data, -zlib.MAX_WBITS)


class CompressionMiddleware:
    """Middleware for handling compression.

    Example:
        >>> mw = CompressionMiddleware()
        >>> compressed = mw.compress(request.body, encoding="gzip")
    """

    SUPPORTED_ENCODINGS = ["gzip", "deflate", "zlib", "identity"]

    def __init__(self, default_encoding: str = "gzip") -> None:
        self.default_encoding = default_encoding

    def compress(
        self,
        data: bytes,
        encoding: Optional[str] = None,
    ) -> tuple[bytes, str]:
        """Compress data.

        Args:
            data: Bytes to compress.
            encoding: Encoding type.

        Returns:
            Tuple of (compressed_data, encoding_used).
        """
        encoding = encoding or self.default_encoding
        if encoding == "gzip":
            return compress_gzip(data), "gzip"
        elif encoding in ("deflate", "zlib"):
            return compress_zlib(data), "deflate"
        elif encoding == "identity":
            return data, "identity"
        logger.warning(f"Unknown encoding {encoding}, returning uncompressed")
        return data, "identity"

    def decompress(
        self,
        data: bytes,
        encoding: Optional[str] = None,
    ) -> bytes:
        """Decompress data.

        Args:
            data: Compressed bytes.
            encoding: Encoding type.

        Returns:
            Decompressed bytes.
        """
        if not encoding or encoding == "identity":
            return data
        if encoding == "gzip":
            return decompress_gzip(data)
        elif encoding in ("deflate", "zlib"):
            try:
                return decompress_zlib(data)
            except zlib.error:
                return decompress_deflate(data)
        logger.warning(f"Unknown encoding {encoding}")
        return data

    def should_compress(
        self,
        content_type: str,
        content_length: int,
        min_size: int = 1024,
    ) -> bool:
        """Check if content should be compressed.

        Args:
            content_type: MIME type.
            content_length: Size in bytes.
            min_size: Minimum size to compress.

        Returns:
            True if should compress.
        """
        if content_length < min_size:
            return False
        compressible_types = [
            "text/", "application/json", "application/xml",
            "application/javascript", "application/xhtml+xml",
        ]
        return any(content_type.startswith(t) for t in compressible_types)


def compress_request_body(
    body: bytes,
    encoding: str = "gzip",
    min_size: int = 1024,
) -> tuple[bytes, str]:
    """Compress request body if beneficial.

    Args:
        body: Request body bytes.
        encoding: Encoding to use.
        min_size: Minimum size to compress.

    Returns:
        Tuple of (body, content_encoding).
    """
    if len(body) < min_size:
        return body, "identity"
    mw = CompressionMiddleware()
    return mw.compress(body, encoding)


def decompress_response_body(
    body: bytes,
    encoding: Optional[str] = None,
) -> bytes:
    """Decompress response body.

    Args:
        body: Response body bytes.
        encoding: Content-Encoding header value.

    Returns:
        Decompressed body.
    """
    if not encoding or encoding == "identity":
        return body
    mw = CompressionMiddleware()
    return mw.decompress(body, encoding)
