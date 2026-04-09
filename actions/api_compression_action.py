"""API Response Compressor.

This module provides response compression:
- Gzip and deflate compression
- Brotli support (if available)
- Streaming compression
- Content negotiation

Example:
    >>> from actions.api_compression_action import ResponseCompressor
    >>> compressor = ResponseCompressor()
    >>> compressed = compressor.compress(b"large response data", accept_encoding="gzip")
"""

from __future__ import annotations

import gzip
import logging
import threading
import zlib
from typing import Optional, Callable

logger = logging.getLogger(__name__)


class ResponseCompressor:
    """Compresses API responses based on client accept-encoding."""

    MIN_SIZE = 1024

    def __init__(
        self,
        min_size: int = MIN_SIZE,
        compression_level: int = 6,
        encodings: Optional[list[str]] = None,
    ) -> None:
        """Initialize the compressor.

        Args:
            min_size: Minimum response size to compress (bytes).
            compression_level: Compression level (1-9).
            encodings: List of supported encodings.
        """
        self._min_size = min_size
        self._compression_level = compression_level
        self._encodings = encodings or ["gzip", "deflate", "br"]
        self._lock = threading.Lock()
        self._stats = {"compressed": 0, "skipped_small": 0, "bytes_in": 0, "bytes_out": 0}

    def compress(
        self,
        data: bytes,
        accept_encoding: Optional[str] = None,
    ) -> tuple[bytes, Optional[str]]:
        """Compress data based on accept-encoding header.

        Args:
            data: Data to compress.
            accept_encoding: Accept-Encoding header value.

        Returns:
            Tuple of (compressed_data, content_encoding).
        """
        with self._lock:
            self._stats["bytes_in"] += len(data)

        if len(data) < self._min_size:
            with self._lock:
                self._stats["skipped_small"] += 1
            return data, None

        if not accept_encoding:
            return self._compress_gzip(data), "gzip"

        encodings = [e.strip().lower() for e in accept_encoding.split(",")]

        for encoding in encodings:
            if "gzip" in encoding:
                result = self._compress_gzip(data)
                self._record_compress(len(result))
                return result, "gzip"
            elif "deflate" in encoding:
                result = self._compress_deflate(data)
                self._record_compress(len(result))
                return result, "deflate"
            elif "br" in encoding:
                result = self._compress_brotli(data)
                if result is not None:
                    self._record_compress(len(result))
                    return result, "br"

        return data, None

    def _compress_gzip(self, data: bytes) -> bytes:
        """Compress using gzip."""
        try:
            return gzip.compress(data, compresslevel=self._compression_level)
        except Exception as e:
            logger.error("Gzip compression failed: %s", e)
            return data

    def _compress_deflate(self, data: bytes) -> bytes:
        """Compress using deflate (zlib)."""
        try:
            return zlib.compress(data, level=self._compression_level)
        except Exception as e:
            logger.error("Deflate compression failed: %s", e)
            return data

    def _compress_brotli(self, data: bytes) -> Optional[bytes]:
        """Compress using brotli if available."""
        try:
            import brotli
            return brotli.compress(data)
        except ImportError:
            logger.debug("brotli not available")
            return None
        except Exception as e:
            logger.error("Brotli compression failed: %s", e)
            return None

    def _record_compress(self, output_size: int) -> None:
        """Record compression stats."""
        with self._lock:
            self._stats["compressed"] += 1
            self._stats["bytes_out"] += output_size

    def decompress(self, data: bytes, encoding: str) -> Optional[bytes]:
        """Decompress data.

        Args:
            data: Compressed data.
            encoding: Content encoding.

        Returns:
            Decompressed data, or None if unsupported.
        """
        if encoding == "gzip":
            try:
                return gzip.decompress(data)
            except Exception as e:
                logger.error("Gzip decompression failed: %s", e)
                return None
        elif encoding == "deflate":
            try:
                return zlib.decompress(data)
            except Exception:
                try:
                    return zlib.decompress(data, -zlib.MAX_WBITS)
                except Exception as e:
                    logger.error("Deflate decompression failed: %s", e)
                    return None
        elif encoding == "br":
            try:
                import brotli
                return brotli.decompress(data)
            except Exception as e:
                logger.error("Brotli decompression failed: %s", e)
                return None

        return None

    def get_stats(self) -> dict[str, int]:
        """Get compression statistics."""
        with self._lock:
            stats = dict(self._stats)
            if stats["bytes_in"] > 0:
                stats["compression_ratio"] = round(
                    stats["bytes_out"] / stats["bytes_in"], 3
                )
            return stats

    def stream_compress(
        self,
        chunk_size: int = 8192,
    ) -> Callable[[bytes], bytes]:
        """Create a streaming compressor.

        Args:
            chunk_size: Size of output chunks.

        Returns:
            A function that compresses incrementally.
        """
        compressor = zlib.compressobj(level=self._compression_level)

        def compress_chunk(data: bytes) -> bytes:
            return compressor.compress(data)

        return compress_chunk

    def flush_compressor(self, compressor) -> bytes:
        """Flush a streaming compressor."""
        return compressor.flush()
