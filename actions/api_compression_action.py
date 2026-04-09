"""API Compression Action module.

Provides compression and decompression utilities for
API requests and responses, supporting gzip, deflate,
and brotli algorithms.
"""

from __future__ import annotations

import gzip
import io
import json
import zlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

try:
    import brotli
    HAS_BROTLI = True
except ImportError:
    HAS_BROTLI = False


class CompressionAlgorithm(Enum):
    """Supported compression algorithms."""

    GZIP = "gzip"
    DEFLATE = "deflate"
    BROTLI = "brotli"
    IDENTITY = "identity"
    AUTO = "auto"


@dataclass
class CompressionResult:
    """Result of compression operation."""

    success: bool
    original_size: int
    compressed_size: int
    algorithm: str
    compression_ratio: float = 0.0
    data: Optional[bytes] = None
    error: Optional[str] = None

    def __post_init__(self):
        if self.original_size > 0:
            self.compression_ratio = 1 - (self.compressed_size / self.original_size)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "original_size": self.original_size,
            "compressed_size": self.compressed_size,
            "algorithm": self.algorithm,
            "compression_ratio": f"{self.compression_ratio:.2%}",
            "error": self.error,
        }


def compress_gzip(
    data: bytes | str,
    compression_level: int = 6,
) -> CompressionResult:
    """Compress data using gzip.

    Args:
        data: Data to compress
        compression_level: Compression level (1-9)

    Returns:
        CompressionResult
    """
    try:
        if isinstance(data, str):
            data = data.encode("utf-8")

        original_size = len(data)
        compressed = gzip.compress(data, compresslevel=compression_level)

        return CompressionResult(
            success=True,
            original_size=original_size,
            compressed_size=len(compressed),
            algorithm="gzip",
            data=compressed,
        )
    except Exception as e:
        return CompressionResult(
            success=False,
            original_size=len(data) if isinstance(data, bytes) else len(data.encode()),
            compressed_size=0,
            algorithm="gzip",
            error=str(e),
        )


def decompress_gzip(data: bytes) -> CompressionResult:
    """Decompress gzip data.

    Args:
        data: Compressed data

    Returns:
        CompressionResult with decompressed data
    """
    try:
        decompressed = gzip.decompress(data)

        return CompressionResult(
            success=True,
            original_size=len(data),
            compressed_size=len(decompressed),
            algorithm="gzip",
            data=decompressed,
        )
    except Exception as e:
        return CompressionResult(
            success=False,
            original_size=len(data),
            compressed_size=0,
            algorithm="gzip",
            error=str(e),
        )


def compress_deflate(
    data: bytes | str,
    compression_level: int = 6,
) -> CompressionResult:
    """Compress data using deflate.

    Args:
        data: Data to compress
        compression_level: Compression level (1-9)

    Returns:
        CompressionResult
    """
    try:
        if isinstance(data, str):
            data = data.encode("utf-8")

        original_size = len(data)
        compressed = zlib.compress(data, level=compression_level)

        return CompressionResult(
            success=True,
            original_size=original_size,
            compressed_size=len(compressed),
            algorithm="deflate",
            data=compressed,
        )
    except Exception as e:
        return CompressionResult(
            success=False,
            original_size=len(data) if isinstance(data, bytes) else len(data.encode()),
            compressed_size=0,
            algorithm="deflate",
            error=str(e),
        )


def decompress_deflate(data: bytes) -> CompressionResult:
    """Decompress deflate data.

    Args:
        data: Compressed data

    Returns:
        CompressionResult with decompressed data
    """
    try:
        decompressed = zlib.decompress(data)

        return CompressionResult(
            success=True,
            original_size=len(data),
            compressed_size=len(decompressed),
            algorithm="deflate",
            data=decompressed,
        )
    except Exception as e:
        return CompressionResult(
            success=False,
            original_size=len(data),
            compressed_size=0,
            algorithm="deflate",
            error=str(e),
        )


def compress_brotli(data: bytes | str) -> CompressionResult:
    """Compress data using brotli.

    Args:
        data: Data to compress

    Returns:
        CompressionResult
    """
    if not HAS_BROTLI:
        return CompressionResult(
            success=False,
            original_size=len(data) if isinstance(data, bytes) else len(data.encode()),
            compressed_size=0,
            algorithm="brotli",
            error="brotli library not installed",
        )

    try:
        if isinstance(data, str):
            data = data.encode("utf-8")

        original_size = len(data)
        compressed = brotli.compress(data)

        return CompressionResult(
            success=True,
            original_size=original_size,
            compressed_size=len(compressed),
            algorithm="brotli",
            data=compressed,
        )
    except Exception as e:
        return CompressionResult(
            success=False,
            original_size=len(data) if isinstance(data, bytes) else len(data.encode()),
            compressed_size=0,
            algorithm="brotli",
            error=str(e),
        )


def decompress_brotli(data: bytes) -> CompressionResult:
    """Decompress brotli data.

    Args:
        data: Compressed data

    Returns:
        CompressionResult with decompressed data
    """
    if not HAS_BROTLI:
        return CompressionResult(
            success=False,
            original_size=len(data),
            compressed_size=0,
            algorithm="brotli",
            error="brotli library not installed",
        )

    try:
        decompressed = brotli.decompress(data)

        return CompressionResult(
            success=True,
            original_size=len(data),
            compressed_size=len(decompressed),
            algorithm="brotli",
            data=decompressed,
        )
    except Exception as e:
        return CompressionResult(
            success=False,
            original_size=len(data),
            compressed_size=0,
            algorithm="brotli",
            error=str(e),
        )


def compress_auto(
    data: bytes | str,
    algorithm: CompressionAlgorithm = CompressionAlgorithm.AUTO,
) -> CompressionResult:
    """Automatically choose best compression.

    Args:
        data: Data to compress
        algorithm: Preferred algorithm (auto picks best)

    Returns:
        CompressionResult
    """
    results = []

    if algorithm in (CompressionAlgorithm.AUTO, CompressionAlgorithm.GZIP):
        results.append(("gzip", compress_gzip(data)))

    if algorithm in (CompressionAlgorithm.AUTO, CompressionAlgorithm.DEFLATE):
        results.append(("deflate", compress_deflate(data)))

    if algorithm in (CompressionAlgorithm.AUTO, CompressionAlgorithm.BROTLI):
        results.append(("brotli", compress_brotli(data)))

    best = None
    best_ratio = -1

    for name, result in results:
        if result.success and result.compression_ratio > best_ratio:
            best_ratio = result.compression_ratio
            best = result

    return best or CompressionResult(
        success=False,
        original_size=len(data) if isinstance(data, bytes) else len(data.encode()),
        compressed_size=0,
        algorithm="auto",
        error="No compression succeeded",
    )


class CompressionMiddleware:
    """Middleware for automatic request/response compression."""

    def __init__(
        self,
        algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP,
        min_size_bytes: int = 1024,
    ):
        self.algorithm = algorithm
        self.min_size_bytes = min_size_bytes

    def compress_request(
        self,
        data: bytes | str,
        content_type: Optional[str] = None,
    ) -> tuple[bytes, str]:
        """Compress request data.

        Args:
            data: Request body
            content_type: Content-Type header

        Returns:
            Tuple of (compressed_data, content_encoding)
        """
        if isinstance(data, str):
            data = data.encode("utf-8")

        if len(data) < self.min_size_bytes:
            return data, "identity"

        if self.algorithm == CompressionAlgorithm.GZIP:
            result = compress_gzip(data)
        elif self.algorithm == CompressionAlgorithm.DEFLATE:
            result = compress_deflate(data)
        elif self.algorithm == CompressionAlgorithm.BROTLI:
            result = compress_brotli(data)
        else:
            result = compress_auto(data, self.algorithm)

        if result.success:
            return result.data, result.algorithm

        return data, "identity"

    def decompress_response(
        self,
        data: bytes,
        content_encoding: str,
    ) -> bytes:
        """Decompress response data.

        Args:
            data: Response body
            content_encoding: Content-Encoding header

        Returns:
            Decompressed data
        """
        if content_encoding == "gzip":
            result = decompress_gzip(data)
        elif content_encoding == "deflate":
            result = decompress_deflate(data)
        elif content_encoding == "br" and HAS_BROTLI:
            result = decompress_brotli(data)
        else:
            return data

        if result.success:
            return result.data

        return data


class StreamingCompressor:
    """Streaming compressor for large data."""

    def __init__(
        self,
        algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP,
        chunk_size: int = 8192,
    ):
        self.algorithm = algorithm
        self.chunk_size = chunk_size
        self._compressor = None
        self._initialized = False

    def __enter__(self) -> "StreamingCompressor":
        """Initialize compressor."""
        if self.algorithm == CompressionAlgorithm.GZIP:
            self._compressor = gzip.GzipFile(
                fileobj=io.BytesIO(),
                mode="wb",
            )
        elif self.algorithm == CompressionAlgorithm.DEFLATE:
            self._compressor = zlib.compressobj()
        return self

    def __exit__(self, *args: Any) -> None:
        """Finalize compression."""
        if self._compressor:
            try:
                self._compressor.close()
            except Exception:
                pass

    def write(self, data: bytes) -> bytes:
        """Write data to compressor.

        Args:
            data: Data chunk

        Returns:
            Compressed output (may be empty if buffering)
        """
        if not self._compressor:
            return b""

        try:
            if hasattr(self._compressor, "write"):
                self._compressor.write(data)
                if hasattr(self._compressor, "flush"):
                    self._compressor.flush()
            return b""
        except Exception:
            return b""

    def finalize(self) -> bytes:
        """Get final compressed data.

        Returns:
            Final compressed bytes
        """
        if not self._compressor:
            return b""

        try:
            if hasattr(self._compressor, "getvalue"):
                return self._compressor.getvalue()
            elif hasattr(self._compressor, "flush"):
                return self._compressor.flush()
        except Exception:
            pass

        return b""
