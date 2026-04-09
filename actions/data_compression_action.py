"""Data compression action module.

Provides data compression and decompression functionality
supporting multiple compression algorithms.
"""

from __future__ import annotations

import io
import gzip
import zlib
import bz2
import lzma
import base64
import logging
from typing import Any, Optional, Union, Literal
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class CompressionAlgorithm(Enum):
    """Supported compression algorithms."""
    GZIP = "gzip"
    ZLIB = "zlib"
    BZ2 = "bz2"
    LZMA = "lzma"
    DEFLATE = "deflate"


@dataclass
class CompressionResult:
    """Result of compression operation."""
    data: bytes
    original_size: int
    compressed_size: int
    algorithm: str

    @property
    def compression_ratio(self) -> float:
        """Calculate compression ratio."""
        if self.original_size == 0:
            return 0.0
        return 1.0 - (self.compressed_size / self.original_size)


class CompressionUtils:
    """Compression utility functions."""

    @staticmethod
    def gzip_compress(data: bytes, level: int = 6) -> bytes:
        """Compress data using GZIP.

        Args:
            data: Data to compress
            level: Compression level (1-9)

        Returns:
            Compressed data
        """
        return gzip.compress(data, level)

    @staticmethod
    def gzip_decompress(data: bytes) -> bytes:
        """Decompress GZIP data.

        Args:
            data: Compressed data

        Returns:
            Decompressed data
        """
        return gzip.decompress(data)

    @staticmethod
    def zlib_compress(data: bytes, level: int = 6) -> bytes:
        """Compress data using ZLIB.

        Args:
            data: Data to compress
            level: Compression level (1-9)

        Returns:
            Compressed data
        """
        return zlib.compress(data, level)

    @staticmethod
    def zlib_decompress(data: bytes) -> bytes:
        """Decompress ZLIB data.

        Args:
            data: Compressed data

        Returns:
            Decompressed data
        """
        return zlib.decompress(data)

    @staticmethod
    def bz2_compress(data: bytes, level: int = 9) -> bytes:
        """Compress data using BZ2.

        Args:
            data: Data to compress
            level: Compression level (1-9)

        Returns:
            Compressed data
        """
        return bz2.compress(data, level)

    @staticmethod
    def bz2_decompress(data: bytes) -> bytes:
        """Decompress BZ2 data.

        Args:
            data: Compressed data

        Returns:
            Decompressed data
        """
        return bz2.decompress(data)

    @staticmethod
    def lzma_compress(data: bytes, preset: int = 6) -> bytes:
        """Compress data using LZMA.

        Args:
            data: Data to compress
            preset: Compression preset (0-9)

        Returns:
            Compressed data
        """
        return lzma.compress(data, preset=preset)

    @staticmethod
    def lzma_decompress(data: bytes) -> bytes:
        """Decompress LZMA data.

        Args:
            data: Compressed data

        Returns:
            Decompressed data
        """
        return lzma.decompress(data)

    @staticmethod
    def deflate_compress(data: bytes, level: int = 6) -> bytes:
        """Compress data using raw DEFLATE.

        Args:
            data: Data to compress
            level: Compression level (1-9)

        Returns:
            Compressed data
        """
        return zlib.compress(data, level)[2:-4]

    @staticmethod
    def deflate_decompress(data: bytes) -> bytes:
        """Decompress raw DEFLATE data.

        Args:
            data: Compressed data

        Returns:
            Decompressed data
        """
        return zlib.decompress(data, -zlib.MAX_WBITS)


class CompressionCodec:
    """Compression codec for encoding/decoding data."""

    def __init__(self, algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP):
        """Initialize compression codec.

        Args:
            algorithm: Compression algorithm to use
        """
        self.algorithm = algorithm

    def compress(self, data: bytes, level: int = 6) -> CompressionResult:
        """Compress data.

        Args:
            data: Data to compress
            level: Compression level

        Returns:
            CompressionResult
        """
        original_size = len(data)

        if self.algorithm == CompressionAlgorithm.GZIP:
            compressed = CompressionUtils.gzip_compress(data, level)
        elif self.algorithm == CompressionAlgorithm.ZLIB:
            compressed = CompressionUtils.zlib_compress(data, level)
        elif self.algorithm == CompressionAlgorithm.BZ2:
            compressed = CompressionUtils.bz2_compress(data, level)
        elif self.algorithm == CompressionAlgorithm.LZMA:
            compressed = CompressionUtils.lzma_compress(data, level)
        elif self.algorithm == CompressionAlgorithm.DEFLATE:
            compressed = CompressionUtils.deflate_compress(data, level)
        else:
            raise ValueError(f"Unknown algorithm: {self.algorithm}")

        return CompressionResult(
            data=compressed,
            original_size=original_size,
            compressed_size=len(compressed),
            algorithm=self.algorithm.value,
        )

    def decompress(self, data: bytes) -> bytes:
        """Decompress data.

        Args:
            data: Data to decompress

        Returns:
            Decompressed data
        """
        if self.algorithm == CompressionAlgorithm.GZIP:
            return CompressionUtils.gzip_decompress(data)
        elif self.algorithm == CompressionAlgorithm.ZLIB:
            return CompressionUtils.zlib_decompress(data)
        elif self.algorithm == CompressionAlgorithm.BZ2:
            return CompressionUtils.bz2_decompress(data)
        elif self.algorithm == CompressionAlgorithm.LZMA:
            return CompressionUtils.lzma_decompress(data)
        elif self.algorithm == CompressionAlgorithm.DEFLATE:
            return CompressionUtils.deflate_decompress(data)
        else:
            raise ValueError(f"Unknown algorithm: {self.algorithm}")

    def compress_to_base64(self, data: bytes, level: int = 6) -> str:
        """Compress and encode as base64.

        Args:
            data: Data to compress
            level: Compression level

        Returns:
            Base64 encoded compressed data
        """
        result = self.compress(data, level)
        return base64.b64encode(result.data).decode("ascii")

    def decompress_from_base64(self, encoded: str) -> bytes:
        """Decompress from base64 encoded data.

        Args:
            encoded: Base64 encoded compressed data

        Returns:
            Decompressed data
        """
        data = base64.b64decode(encoded.encode("ascii"))
        return self.decompress(data)


class StreamCompressor:
    """Streaming compressor for large data."""

    def __init__(self, algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP):
        """Initialize stream compressor.

        Args:
            algorithm: Compression algorithm
        """
        self.algorithm = algorithm
        self._gzip_stream: Optional[gzip.GzipFile] = None

    def compress_stream(self, input_stream: io.BytesIO, output_stream: io.BytesIO, chunk_size: int = 8192) -> int:
        """Compress stream data.

        Args:
            input_stream: Input data stream
            output_stream: Output compressed stream
            chunk_size: Chunk size for streaming

        Returns:
            Total bytes written
        """
        total_written = 0

        if self.algorithm == CompressionAlgorithm.GZIP:
            with gzip.GzipFile(fileobj=output_stream, mode="wb") as f:
                while True:
                    chunk = input_stream.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)
                    total_written += len(chunk)

        return total_written

    def decompress_stream(self, input_stream: io.BytesIO, output_stream: io.BytesIO, chunk_size: int = 8192) -> int:
        """Decompress stream data.

        Args:
            input_stream: Input compressed stream
            output_stream: Output data stream
            chunk_size: Chunk size for streaming

        Returns:
            Total bytes written
        """
        total_written = 0

        if self.algorithm == CompressionAlgorithm.GZIP:
            with gzip.GzipFile(fileobj=input_stream, mode="rb") as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    output_stream.write(chunk)
                    total_written += len(chunk)

        return total_written


def create_compression_codec(algorithm: str = "gzip") -> CompressionCodec:
    """Create compression codec.

    Args:
        algorithm: Algorithm name

    Returns:
        CompressionCodec instance
    """
    try:
        algo = CompressionAlgorithm(algorithm.lower())
    except ValueError:
        algo = CompressionAlgorithm.GZIP
    return CompressionCodec(algo)


def compress_data(data: bytes, algorithm: str = "gzip", level: int = 6) -> CompressionResult:
    """Compress data.

    Args:
        data: Data to compress
        algorithm: Algorithm name
        level: Compression level

    Returns:
        CompressionResult
    """
    codec = create_compression_codec(algorithm)
    return codec.compress(data, level)
