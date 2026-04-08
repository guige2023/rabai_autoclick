"""
Data Compression Action Module.

Provides data compression and decompression
for various formats including gzip, zlib, and lz4.
"""

from typing import Any, BinaryIO, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import gzip
import json
import logging

logger = logging.getLogger(__name__)


class CompressionType(Enum):
    """Compression types."""
    GZIP = "gzip"
    ZLIB = "zlib"
    LZ4 = "lz4"
    ZSTD = "zstd"
    BROTLI = "brotli"


@dataclass
class CompressionResult:
    """Result of compression operation."""
    success: bool
    original_size: int
    compressed_size: int
    compression_type: CompressionType
    ratio: float = 0.0
    error: Optional[str] = None

    @property
    def space_saved(self) -> int:
        """Get bytes saved."""
        return self.original_size - self.compressed_size


class DataCompressor:
    """Compresses and decompresses data."""

    def __init__(self, compression_type: CompressionType = CompressionType.GZIP):
        self.compression_type = compression_type

    def compress(self, data: bytes) -> CompressionResult:
        """Compress bytes data."""
        try:
            original_size = len(data)

            if self.compression_type == CompressionType.GZIP:
                compressed = gzip.compress(data)
            elif self.compression_type == CompressionType.ZLIB:
                import zlib
                compressed = zlib.compress(data)
            elif self.compression_type == CompressionType.LZ4:
                import lz4.frame
                compressed = lz4.frame.compress(data)
            elif self.compression_type == CompressionType.ZSTD:
                import zstandard
                cctx = zstandard.ZstdCompressor()
                compressed = cctx.compress(data)
            elif self.compression_type == CompressionType.BROTLI:
                import brotli
                compressed = brotli.compress(data)
            else:
                compressed = data

            compressed_size = len(compressed)
            ratio = compressed_size / original_size if original_size > 0 else 0

            return CompressionResult(
                success=True,
                original_size=original_size,
                compressed_size=compressed_size,
                compression_type=self.compression_type,
                ratio=ratio
            )

        except Exception as e:
            return CompressionResult(
                success=False,
                original_size=len(data),
                compressed_size=0,
                compression_type=self.compression_type,
                error=str(e)
            )

    def decompress(self, data: bytes) -> bytes:
        """Decompress bytes data."""
        if self.compression_type == CompressionType.GZIP:
            return gzip.decompress(data)
        elif self.compression_type == CompressionType.ZLIB:
            import zlib
            return zlib.decompress(data)
        elif self.compression_type == CompressionType.LZ4:
            import lz4.frame
            return lz4.frame.decompress(data)
        elif self.compression_type == CompressionType.ZSTD:
            import zstandard
            dctx = zstandard.ZstdDecompressor()
            return dctx.decompress(data)
        elif self.compression_type == CompressionType.BROTLI:
            import brotli
            return brotli.decompress(data)
        return data


class CompressionManager:
    """Manages multiple compression configurations."""

    def __init__(self):
        self.compressors: Dict[CompressionType, DataCompressor] = {}
        self._register_default_compressors()

    def _register_default_compressors(self):
        """Register default compressors."""
        for compression_type in CompressionType:
            self.compressors[compression_type] = DataCompressor(compression_type)

    def get_compressor(self, compression_type: CompressionType) -> DataCompressor:
        """Get compressor for type."""
        if compression_type not in self.compressors:
            self.compressors[compression_type] = DataCompressor(compression_type)
        return self.compressors[compression_type]

    def compress(
        self,
        data: bytes,
        compression_type: CompressionType
    ) -> CompressionResult:
        """Compress data with specified type."""
        compressor = self.get_compressor(compression_type)
        return compressor.compress(data)

    def decompress(
        self,
        data: bytes,
        compression_type: CompressionType
    ) -> bytes:
        """Decompress data with specified type."""
        compressor = self.get_compressor(compression_type)
        return compressor.decompress(data)


class StreamingCompressor:
    """Streams compression for large data."""

    def __init__(self, compression_type: CompressionType = CompressionType.GZIP):
        self.compression_type = compression_type
        self._buffer = bytearray()

    def update(self, data: bytes) -> bytes:
        """Update compression with new data."""
        self._buffer.extend(data)
        return b""

    def flush(self) -> bytes:
        """Flush remaining compressed data."""
        if not self._buffer:
            return b""

        compressor = DataCompressor(self.compression_type)
        result = compressor.compress(bytes(self._buffer))
        self._buffer.clear()

        if result.success:
            return gzip.decompress(gzip.compress(bytes(self._buffer))) if self.compression_type == CompressionType.GZIP else bytes()
        return b""


class CompressionUtils:
    """Utility functions for compression."""

    @staticmethod
    def estimate_compressed_size(
        original_size: int,
        compression_type: CompressionType
    ) -> int:
        """Estimate compressed size."""
        ratios = {
            CompressionType.GZIP: 0.4,
            CompressionType.ZLIB: 0.4,
            CompressionType.LZ4: 0.5,
            CompressionType.ZSTD: 0.35,
            CompressionType.BROTLI: 0.3,
        }
        ratio = ratios.get(compression_type, 0.5)
        return int(original_size * ratio)

    @staticmethod
    def is_compressed(data: bytes) -> bool:
        """Check if data appears to be compressed."""
        if len(data) < 2:
            return False

        magic_bytes = {
            b'\x1f\x8b': CompressionType.GZIP,
            b'\x78\x9c': CompressionType.ZLIB,
            b'\x28\xb5\x2f\xfd': CompressionType.ZSTD,
        }

        for magic, compression_type in magic_bytes.items():
            if data[:len(magic)] == magic:
                return True

        return False


def main():
    """Demonstrate data compression."""
    manager = CompressionManager()

    data = b"Hello, this is test data for compression. " * 100
    print(f"Original size: {len(data)} bytes")

    result = manager.compress(data, CompressionType.GZIP)
    print(f"Compressed size: {result.compressed_size} bytes")
    print(f"Ratio: {result.ratio:.2%}")
    print(f"Space saved: {result.space_saved} bytes")


if __name__ == "__main__":
    main()
