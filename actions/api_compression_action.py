"""
API Compression Action Module.

Provides gzip/zlib compression for API requests/responses
with automatic content-type detection.
"""

import asyncio
import gzip
import zlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

import gzip


class CompressionType(Enum):
    """Compression types."""
    GZIP = "gzip"
    DEFLATE = "deflate"
    ZLIB = "zlib"
    AUTO = "auto"


@dataclass
class CompressionConfig:
    """Compression configuration."""
    compression_type: CompressionType = CompressionType.GZIP
    level: int = 6
    min_size: int = 1024
    include_content_encoding: bool = True
    include_vary_header: bool = True


@dataclass
class CompressionResult:
    """Compression result."""
    success: bool
    original_size: int = 0
    compressed_size: int = 0
    compression_ratio: float = 0.0
    algorithm: str = ""


class ContentDetector:
    """Detects content type for compression."""

    TEXT_TYPES = {
        "text/plain",
        "text/html",
        "text/css",
        "text/javascript",
        "application/json",
        "application/xml",
        "application/javascript",
        "application/xhtml+xml",
    }

    BINARY_TYPES = {
        "image/",
        "audio/",
        "video/",
        "application/octet-stream",
        "application/pdf",
        "application/zip",
    }

    @staticmethod
    def should_compress(content_type: str, size: int) -> bool:
        """Determine if content should be compressed."""
        if content_type.startswith("image/"):
            return False

        if size < 512:
            return False

        for binary_type in ContentDetector.BINARY_TYPES:
            if content_type.startswith(binary_type):
                return False

        return True

    @staticmethod
    def get_compression_type(
        accept_encoding: str,
        config: CompressionConfig
    ) -> CompressionType:
        """Determine best compression type from accept-encoding."""
        if not accept_encoding:
            return config.compression_type

        if "gzip" in accept_encoding.lower():
            return CompressionType.GZIP
        elif "deflate" in accept_encoding.lower():
            return CompressionType.DEFLATE
        elif "zlib" in accept_encoding.lower():
            return CompressionType.ZLIB

        return config.compression_type


class APICompressionAction:
    """
    API request/response compression.

    Example:
        compressor = APICompressionAction(
            compression_type=CompressionType.GZIP,
            level=6
        )

        compressed = await compressor.compress(data)
        decompressed = await compressor.decompress(compressed)
    """

    def __init__(
        self,
        compression_type: CompressionType = CompressionType.GZIP,
        level: int = 6
    ):
        self.config = CompressionConfig(
            compression_type=compression_type,
            level=level
        )
        self._detector = ContentDetector()

    async def compress(
        self,
        data: Any,
        content_type: str = "application/json"
    ) -> CompressionResult:
        """Compress data."""
        import json

        try:
            if isinstance(data, str):
                data_bytes = data.encode('utf-8')
            elif isinstance(data, bytes):
                data_bytes = data
            elif isinstance(data, dict):
                data_bytes = json.dumps(data).encode('utf-8')
            else:
                data_bytes = str(data).encode('utf-8')

            original_size = len(data_bytes)

            if original_size < self.config.min_size:
                return CompressionResult(
                    success=True,
                    original_size=original_size,
                    compressed_size=original_size,
                    compression_ratio=1.0,
                    algorithm="none"
                )

            if self.config.compression_type == CompressionType.GZIP:
                compressed = gzip.compress(data_bytes, level=self.config.level)
                algorithm = "gzip"
            elif self.config.compression_type == CompressionType.DEFLATE:
                compressed = zlib.compress(data_bytes, level=self.config.level)
                algorithm = "deflate"
            elif self.config.compression_type == CompressionType.ZLIB:
                compressed = zlib.compress(data_bytes, level=self.config.level)
                algorithm = "zlib"
            else:
                compressed = gzip.compress(data_bytes, level=self.config.level)
                algorithm = "gzip"

            compressed_size = len(compressed)
            ratio = compressed_size / original_size if original_size > 0 else 1.0

            return CompressionResult(
                success=True,
                original_size=original_size,
                compressed_size=compressed_size,
                compression_ratio=ratio,
                algorithm=algorithm
            )

        except Exception as e:
            return CompressionResult(success=False)

    async def decompress(
        self,
        data: bytes,
        compression_type: Optional[CompressionType] = None
    ) -> Any:
        """Decompress data."""
        import json

        comp_type = compression_type or self.config.compression_type

        try:
            if comp_type == CompressionType.GZIP:
                decompressed = gzip.decompress(data)
            elif comp_type == CompressionType.DEFLATE:
                decompressed = zlib.decompress(data)
            elif comp_type == CompressionType.ZLIB:
                decompressed = zlib.decompress(data)
            else:
                decompressed = gzip.decompress(data)

            try:
                return json.loads(decompressed.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                return decompressed.decode('utf-8')

        except Exception as e:
            return None

    def get_compressed_data(
        self,
        data: bytes,
        content_encoding: str
    ) -> tuple[bytes, str]:
        """Get compressed data with appropriate headers."""
        comp_type = CompressionType.AUTO
        if "gzip" in content_encoding:
            comp_type = CompressionType.GZIP
        elif "deflate" in content_encoding:
            comp_type = CompressionType.DEFLATE
        elif "zlib" in content_encoding:
            comp_type = CompressionType.ZLIB

        if comp_type == CompressionType.GZIP:
            compressed = gzip.compress(data, level=self.config.level)
            return compressed, "gzip"
        elif comp_type == CompressionType.DEFLATE:
            compressed = zlib.compress(data, level=self.config.level)
            return compressed, "deflate"
        else:
            compressed = gzip.compress(data, level=self.config.level)
            return compressed, "gzip"

    def get_decompress_func(
        self,
        content_encoding: str
    ) -> Callable[[bytes], bytes]:
        """Get decompression function for content encoding."""
        if "gzip" in content_encoding:
            return gzip.decompress
        elif "deflate" in content_encoding:
            return zlib.decompress
        elif "zlib" in content_encoding:
            return zlib.decompress
        else:
            return gzip.decompress
