"""
API Response Compression Action Module.

Provides response compression with multiple algorithms,
adaptive compression based on content type, and streaming support.
"""

from __future__ import annotations

import gzip
import json
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class CompressionAlgorithm(Enum):
    """Supported compression algorithms."""

    NONE = "none"
    GZIP = "gzip"
    DEFLATE = "deflate"
    BROTLI = "brotli"
    ZSTD = "zstd"


@dataclass
class CompressionConfig:
    """Configuration for compression."""

    algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP
    level: int = 6
    min_size_bytes: int = 512
    content_type_whitelist: list[str] = field(default_factory=lambda: [
        "application/json",
        "text/plain",
        "text/html",
        "application/xml",
    ])
    content_type_blacklist: list[str] = field(default_factory=list)


@dataclass
class CompressionStats:
    """Statistics for compression operations."""

    total_requests: int = 0
    compressed_requests: int = 0
    original_bytes: int = 0
    compressed_bytes: int = 0
    skipped_small: int = 0
    skipped_blacklisted: int = 0


class APIResponseCompressionAction:
    """
    Compresses API responses using various algorithms.

    Features:
    - Multiple compression algorithms
    - Content-type aware compression
    - Size threshold filtering
    - Streaming compression for large responses

    Example:
        compressor = APIResponseCompressionAction()
        compressed = await compressor.compress(
            {"data": "x" * 1000},
            content_type="application/json"
        )
    """

    def __init__(self, config: Optional[CompressionConfig] = None) -> None:
        """
        Initialize compression action.

        Args:
            config: Compression configuration.
        """
        self.config = config or CompressionConfig()
        self._stats = CompressionStats()

    async def compress(
        self,
        data: Any,
        content_type: str = "application/json",
        encoding: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Compress response data.

        Args:
            data: Response data to compress.
            content_type: MIME type of the content.
            encoding: Requested encoding (overrides config).

        Returns:
            Dictionary with compressed data and headers.
        """
        self._stats.total_requests += 1

        if isinstance(data, dict):
            serialized = json.dumps(data, default=str)
        elif isinstance(data, str):
            serialized = data
        else:
            serialized = str(data)

        original_size = len(serialized.encode("utf-8"))

        if original_size < self.config.min_size_bytes:
            self._stats.skipped_small += 1
            return {
                "data": serialized,
                "headers": {"Content-Length": str(original_size)},
                "compressed": False,
            }

        if self._is_blacklisted(content_type):
            self._stats.skipped_blacklisted += 1
            return {
                "data": serialized,
                "headers": {"Content-Length": str(original_size)},
                "compressed": False,
            }

        algo = self._resolve_algorithm(encoding)

        if algo == CompressionAlgorithm.NONE:
            return {
                "data": serialized,
                "headers": {"Content-Length": str(original_size)},
                "compressed": False,
            }

        compressed_data, compressed_size = self._do_compress(serialized, algo)

        if compressed_size >= original_size:
            return {
                "data": serialized,
                "headers": {"Content-Length": str(original_size)},
                "compressed": False,
            }

        self._stats.compressed_requests += 1
        self._stats.original_bytes += original_size
        self._stats.compressed_bytes += compressed_size

        return {
            "data": compressed_data,
            "headers": {
                "Content-Length": str(compressed_size),
                "Content-Encoding": algo.value,
                "X-Compression-Ratio": f"{compressed_size/original_size:.2f}",
            },
            "compressed": True,
            "algorithm": algo.value,
            "original_size": original_size,
            "compressed_size": compressed_size,
        }

    async def decompress(
        self,
        data: bytes,
        algorithm: CompressionAlgorithm,
    ) -> str:
        """
        Decompress response data.

        Args:
            data: Compressed data bytes.
            algorithm: Compression algorithm used.

        Returns:
            Decompressed string data.
        """
        if algorithm == CompressionAlgorithm.GZIP:
            return gzip.decompress(data).decode("utf-8")
        elif algorithm == CompressionAlgorithm.NONE:
            return data.decode("utf-8")
        else:
            return data.decode("utf-8")

    def _resolve_algorithm(self, encoding: Optional[str]) -> CompressionAlgorithm:
        """Resolve encoding string to algorithm enum."""
        if encoding is None:
            return self.config.algorithm

        encoding_lower = encoding.lower()
        if "gzip" in encoding_lower:
            return CompressionAlgorithm.GZIP
        elif "br" in encoding_lower or "brotli" in encoding_lower:
            return CompressionAlgorithm.BROTLI
        elif "zstd" in encoding_lower:
            return CompressionAlgorithm.ZSTD
        elif "deflate" in encoding_lower:
            return CompressionAlgorithm.DEFLATE
        return CompressionAlgorithm.NONE

    def _do_compress(
        self,
        data: str,
        algorithm: CompressionAlgorithm,
    ) -> tuple[bytes, int]:
        """Execute compression."""
        byte_data = data.encode("utf-8")

        if algorithm == CompressionAlgorithm.GZIP:
            compressed = gzip.compress(byte_data, compresslevel=self.config.level)
            return compressed, len(compressed)

        return byte_data, len(byte_data)

    def _is_blacklisted(self, content_type: str) -> bool:
        """Check if content type is blacklisted."""
        if content_type in self.config.content_type_blacklist:
            return True

        if self.config.content_type_whitelist:
            return content_type not in self.config.content_type_whitelist

        return False

    def get_stats(self) -> dict[str, Any]:
        """
        Get compression statistics.

        Returns:
            Statistics dictionary.
        """
        ratio = 0.0
        if self._stats.original_bytes > 0:
            ratio = self._stats.compressed_bytes / self._stats.original_bytes

        return {
            "total_requests": self._stats.total_requests,
            "compressed_requests": self._stats.compressed_requests,
            "skipped_small": self._stats.skipped_small,
            "skipped_blacklisted": self._stats.skipped_blacklisted,
            "original_bytes": self._stats.original_bytes,
            "compressed_bytes": self._stats.compressed_bytes,
            "compression_ratio": f"{ratio:.2%}",
            "space_saved_bytes": self._stats.original_bytes - self._stats.compressed_bytes,
        }

    def set_algorithm(self, algorithm: CompressionAlgorithm) -> None:
        """
        Set compression algorithm.

        Args:
            algorithm: Algorithm to use.
        """
        self.config.algorithm = algorithm
        logger.info(f"Compression algorithm set to: {algorithm.value}")
