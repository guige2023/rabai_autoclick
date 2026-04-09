"""API Compression Action Module.

Request/response compression utilities.
"""

from __future__ import annotations

import asyncio
import gzip
import io
import zlib
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class CompressionType(Enum):
    """Compression types."""
    GZIP = "gzip"
    DEFLATE = "deflate"
    BROTLI = "brotli"
    IDENTITY = "identity"


@dataclass
class CompressionConfig:
    """Compression configuration."""
    algorithm: CompressionType = CompressionType.GZIP
    level: int = 6
    min_size_bytes: int = 1024
    encodings: list[str] = ["gzip", "deflate", "br"]


class CompressionUtil:
    """Compression utilities for API data."""

    def __init__(self, config: CompressionConfig | None = None) -> None:
        self.config = config or CompressionConfig()

    def compress(
        self,
        data: bytes,
        algorithm: CompressionType | None = None
    ) -> bytes:
        """Compress data."""
        algorithm = algorithm or self.config.algorithm
        if len(data) < self.config.min_size_bytes:
            return data
        if algorithm == CompressionType.GZIP:
            return self._gzip_compress(data)
        elif algorithm == CompressionType.DEFLATE:
            return self._deflate_compress(data)
        return data

    def decompress(
        self,
        data: bytes,
        algorithm: CompressionType | None = None
    ) -> bytes:
        """Decompress data."""
        algorithm = algorithm or self.config.algorithm
        if algorithm == CompressionType.GZIP:
            return self._gzip_decompress(data)
        elif algorithm == CompressionType.DEFLATE:
            return self._deflate_decompress(data)
        return data

    def _gzip_compress(self, data: bytes) -> bytes:
        """Gzip compress."""
        buf = io.BytesIO()
        with gzip.GzipFile(file=buf, mode='wb', compresslevel=self.config.level) as f:
            f.write(data)
        return buf.getvalue()

    def _gzip_decompress(self, data: bytes) -> bytes:
        """Gzip decompress."""
        return gzip.decompress(data)

    def _deflate_compress(self, data: bytes) -> bytes:
        """Deflate compress."""
        return zlib.compress(data, level=self.config.level)

    def _deflate_decompress(self, data: bytes) -> bytes:
        """Deflate decompress."""
        return zlib.decompress(data)

    def get_best_encoding(self, accept_encoding: str | None) -> CompressionType | None:
        """Get best encoding from Accept-Encoding header."""
        if not accept_encoding:
            return None
        encodings = [e.strip().split(';')[0].lower() for e in accept_encoding.split(',')]
        if 'br' in encodings and self.config.algorithm == CompressionType.BROTLI:
            return CompressionType.BROTLI
        if 'gzip' in encodings:
            return CompressionType.GZIP
        if 'deflate' in encodings:
            return CompressionType.DEFLATE
        return None
