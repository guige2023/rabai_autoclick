"""
Data Compressor Action Module.

Compression utilities for API data transmission.
"""

from __future__ import annotations

import gzip
import json
import zlib
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional, Union


class CompressionType(Enum):
    """Supported compression types."""
    NONE = "none"
    GZIP = "gzip"
    DEFLATE = "deflate"
    ZLIB = "zlib"


@dataclass
class CompressionResult:
    """Result of a compression operation."""
    compressed: bytes
    original_size: int
    compressed_size: int
    compression_ratio: float
    algorithm: CompressionType


@dataclass
class CompressionConfig:
    """Configuration for compression."""
    algorithm: CompressionType = CompressionType.GZIP
    level: int = 6
    min_size_bytes: int = 1024
    content_type: str = "application/json"


class DataCompressorAction:
    """
    Data compression for API requests and responses.

    Supports gzip, deflate, and zlib compression.
    """

    def __init__(
        self,
        config: Optional[CompressionConfig] = None,
    ) -> None:
        self.config = config or CompressionConfig()

    def compress(
        self,
        data: Union[str, bytes, Dict[str, Any]],
        algorithm: Optional[CompressionType] = None,
        level: Optional[int] = None,
    ) -> CompressionResult:
        """
        Compress data.

        Args:
            data: Data to compress
            algorithm: Override compression algorithm
            level: Override compression level (1-9)

        Returns:
            CompressionResult with compressed data
        """
        algo = algorithm or self.config.algorithm
        comp_level = level or self.config.level

        if isinstance(data, dict):
            data = json.dumps(data).encode("utf-8")
        elif isinstance(data, str):
            data = data.encode("utf-8")

        original_size = len(data)

        if original_size < self.config.min_size_bytes:
            return CompressionResult(
                compressed=data,
                original_size=original_size,
                compressed_size=original_size,
                compression_ratio=1.0,
                algorithm=CompressionType.NONE,
            )

        if algo == CompressionType.GZIP:
            compressed = gzip.compress(data, compresslevel=comp_level)
        elif algo == CompressionType.DEFLATE:
            compressed = zlib.compress(data, level=comp_level)
        elif algo == CompressionType.ZLIB:
            compressed = zlib.compress(data, level=comp_level)
        else:
            compressed = data

        compressed_size = len(compressed)
        ratio = compressed_size / original_size if original_size > 0 else 1.0

        return CompressionResult(
            compressed=compressed,
            original_size=original_size,
            compressed_size=compressed_size,
            compression_ratio=ratio,
            algorithm=algo,
        )

    def decompress(
        self,
        data: bytes,
        algorithm: CompressionType,
        encoding: str = "utf-8",
    ) -> Union[str, bytes]:
        """
        Decompress data.

        Args:
            data: Compressed data
            algorithm: Compression algorithm used
            encoding: Text encoding for string output

        Returns:
            Decompressed data
        """
        if algorithm == CompressionType.GZIP:
            decompressed = gzip.decompress(data)
        elif algorithm in (CompressionType.DEFLATE, CompressionType.ZLIB):
            decompressed = zlib.decompress(data)
        else:
            decompressed = data

        try:
            return decompressed.decode(encoding)
        except UnicodeDecodeError:
            return decompressed

    def compress_json(
        self,
        data: Dict[str, Any],
        algorithm: Optional[CompressionType] = None,
    ) -> tuple[bytes, CompressionType]:
        """
        Compress JSON data.

        Returns:
            Tuple of (compressed_bytes, algorithm_used)
        """
        algo = algorithm or self.config.algorithm
        json_bytes = json.dumps(data).encode("utf-8")

        result = self.compress(json_bytes, algorithm=algo)
        return result.compressed, result.algorithm

    def decompress_json(
        self,
        data: bytes,
        algorithm: CompressionType,
    ) -> Dict[str, Any]:
        """
        Decompress JSON data.

        Args:
            data: Compressed bytes
            algorithm: Algorithm used

        Returns:
            Parsed JSON as dict
        """
        decompressed = self.decompress(data, algorithm)
        if isinstance(decompressed, bytes):
            decompressed = decompressed.decode("utf-8")
        return json.loads(decompressed)

    def should_compress(
        self,
        data_size: int,
        content_type: Optional[str] = None,
    ) -> bool:
        """Determine if data should be compressed."""
        if data_size < self.config.min_size_bytes:
            return False

        if content_type is None:
            content_type = self.config.content_type

        compressible_types = {
            "application/json",
            "text/plain",
            "text/html",
            "text/xml",
            "application/javascript",
        }

        return any(
            ct in content_type.lower()
            for ct in compressible_types
        )

    def get_encoding_header(
        self,
        algorithm: CompressionType,
    ) -> str:
        """Get HTTP content encoding header value."""
        mapping = {
            CompressionType.GZIP: "gzip",
            CompressionType.DEFLATE: "deflate",
            CompressionType.ZLIB: "deflate",
            CompressionType.NONE: "identity",
        }
        return mapping.get(algorithm, "identity")
