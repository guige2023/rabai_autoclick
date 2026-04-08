"""
Data compression module for lossless and lossy data compression.

Supports multiple compression algorithms, streaming compression,
and archive handling.
"""
from __future__ import annotations

import gzip
import io
import json
import time
import uuid
import zlib
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, BinaryIO, Callable, Optional


class CompressionAlgorithm(Enum):
    """Compression algorithms."""
    GZIP = "gzip"
    DEFLATE = "deflate"
    LZ4 = "lz4"
    ZSTD = "zstd"
    BROTLI = "brotli"
    LZMA = "lzma"


class CompressionLevel(Enum):
    """Compression levels."""
    BEST_SPEED = "best_speed"
    BEST_COMPRESSION = "best_compression"
    DEFAULT = "default"


@dataclass
class CompressionResult:
    """Result of a compression operation."""
    original_size: int
    compressed_size: int
    algorithm: CompressionAlgorithm
    compression_ratio: float
    duration_ms: float


@dataclass
class ArchiveEntry:
    """An entry in an archive."""
    id: str
    name: str
    path: str
    size_bytes: int
    compressed_size: int
    is_directory: bool = False
    is_compressed: bool = False
    modified_at: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)


class DataCompressor:
    """
    Data compression service.

    Supports multiple compression algorithms, streaming compression,
    and archive handling.
    """

    def __init__(self, default_algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP):
        self.default_algorithm = default_algorithm
        self._algorithms: dict[CompressionAlgorithm, Callable] = {
            CompressionAlgorithm.GZIP: self._compress_gzip,
            CompressionAlgorithm.DEFLATE: self._compress_deflate,
            CompressionAlgorithm.LZ4: self._compress_lz4_fallback,
            CompressionAlgorithm.ZSTD: self._compress_zstd_fallback,
            CompressionAlgorithm.BROTLI: self._compress_brotli_fallback,
            CompressionAlgorithm.LZMA: self._compress_lzma_fallback,
        }

    def compress(
        self,
        data: bytes,
        algorithm: Optional[CompressionAlgorithm] = None,
        level: CompressionLevel = CompressionLevel.DEFAULT,
    ) -> CompressionResult:
        """Compress data using the specified algorithm."""
        algorithm = algorithm or self.default_algorithm

        start_time = time.time()

        if algorithm not in self._algorithms:
            raise ValueError(f"Unsupported algorithm: {algorithm}")

        compressed = self._algorithms[algorithm](data, level)

        duration_ms = (time.time() - start_time) * 1000

        original_size = len(data)
        compressed_size = len(compressed)
        ratio = compressed_size / original_size if original_size > 0 else 0

        return CompressionResult(
            original_size=original_size,
            compressed_size=compressed_size,
            algorithm=algorithm,
            compression_ratio=ratio,
            duration_ms=duration_ms,
        )

    def decompress(
        self,
        data: bytes,
        algorithm: CompressionAlgorithm,
    ) -> bytes:
        """Decompress data."""
        if algorithm == CompressionAlgorithm.GZIP:
            return gzip.decompress(data)
        elif algorithm == CompressionAlgorithm.DEFLATE:
            return zlib.decompress(data)
        elif algorithm == CompressionAlgorithm.LZ4:
            return self._decompress_lz4_fallback(data)
        elif algorithm == CompressionAlgorithm.ZSTD:
            return self._decompress_zstd_fallback(data)
        elif algorithm == CompressionAlgorithm.BROTLI:
            return self._decompress_brotli_fallback(data)
        elif algorithm == CompressionAlgorithm.LZMA:
            return self._decompress_lzma_fallback(data)

        return data

    def _compress_gzip(self, data: bytes, level: CompressionLevel) -> bytes:
        """Compress using GZIP."""
        gzip_level = self._get_gzip_level(level)
        return gzip.compress(data, gzip_level)

    def _compress_deflate(self, data: bytes, level: CompressionLevel) -> bytes:
        """Compress using DEFLATE."""
        deflate_level = self._get_zlib_level(level)
        return zlib.compress(data, deflate_level)

    def _compress_lz4_fallback(self, data: bytes, level: CompressionLevel) -> bytes:
        """Fallback LZ4 compression using zlib."""
        return zlib.compress(data, self._get_zlib_level(level))

    def _compress_zstd_fallback(self, data: bytes, level: CompressionLevel) -> bytes:
        """Fallback ZSTD compression using zlib."""
        return zlib.compress(data, self._get_zlib_level(level))

    def _compress_brotli_fallback(self, data: bytes, level: CompressionLevel) -> bytes:
        """Fallback Brotli compression using gzip."""
        return gzip.compress(data, self._get_gzip_level(level))

    def _compress_lzma_fallback(self, data: bytes, level: CompressionLevel) -> bytes:
        """Fallback LZMA compression using gzip."""
        return gzip.compress(data, self._get_gzip_level(level))

    def _decompress_lz4_fallback(self, data: bytes) -> bytes:
        """Fallback LZ4 decompression."""
        return zlib.decompress(data)

    def _decompress_zstd_fallback(self, data: bytes) -> bytes:
        """Fallback ZSTD decompression."""
        return zlib.decompress(data)

    def _decompress_brotli_fallback(self, data: bytes) -> bytes:
        """Fallback Brotli decompression."""
        return gzip.decompress(data)

    def _decompress_lzma_fallback(self, data: bytes) -> bytes:
        """Fallback LZMA decompression."""
        return gzip.decompress(data)

    def _get_gzip_level(self, level: CompressionLevel) -> int:
        """Get gzip compression level."""
        if level == CompressionLevel.BEST_SPEED:
            return 1
        elif level == CompressionLevel.BEST_COMPRESSION:
            return 9
        return 6

    def _get_zlib_level(self, level: CompressionLevel) -> int:
        """Get zlib compression level."""
        if level == CompressionLevel.BEST_SPEED:
            return 1
        elif level == CompressionLevel.BEST_COMPRESSION:
            return 9
        return 6

    def compress_file(
        self,
        input_path: str,
        output_path: Optional[str] = None,
        algorithm: Optional[CompressionAlgorithm] = None,
        delete_original: bool = False,
    ) -> CompressionResult:
        """Compress a file."""
        with open(input_path, "rb") as f:
            data = f.read()

        result = self.compress(data, algorithm)

        if output_path:
            with open(output_path, "wb") as f:
                f.write(self._algorithms.get(algorithm or self.default_algorithm)(data, CompressionLevel.DEFAULT))

        if delete_original:
            import os
            os.remove(input_path)

        return result

    def decompress_file(
        self,
        input_path: str,
        output_path: Optional[str] = None,
        algorithm: Optional[CompressionAlgorithm] = None,
        delete_original: bool = False,
    ) -> int:
        """Decompress a file."""
        with open(input_path, "rb") as f:
            data = f.read()

        decompressed = self.decompress(data, algorithm or self.default_algorithm)

        if output_path:
            with open(output_path, "wb") as f:
                f.write(decompressed)
        else:
            base = input_path
            for ext in [".gz", ".deflate", ".lz4", ".zst", ".br", ".lzma"]:
                if base.endswith(ext):
                    base = base[:-len(ext)]
                    break
            with open(base, "wb") as f:
                f.write(decompressed)

        if delete_original:
            import os
            os.remove(input_path)

        return len(decompressed)

    def compress_stream(
        self,
        input_stream: BinaryIO,
        output_stream: BinaryIO,
        algorithm: Optional[CompressionAlgorithm] = None,
        chunk_size: int = 8192,
    ) -> CompressionResult:
        """Compress a stream."""
        algorithm = algorithm or self.default_algorithm
        start_time = time.time()

        compressor = self._get_compressor(algorithm)
        total_size = 0
        compressed_size = 0

        while True:
            chunk = input_stream.read(chunk_size)
            if not chunk:
                break

            total_size += len(chunk)
            compressed_chunk = compressor.compress(chunk)
            compressed_size += len(compressed_chunk)
            output_stream.write(compressed_chunk)

        compressed_size += len(compressor.flush())

        duration_ms = (time.time() - start_time) * 1000
        ratio = compressed_size / total_size if total_size > 0 else 0

        return CompressionResult(
            original_size=total_size,
            compressed_size=compressed_size,
            algorithm=algorithm,
            compression_ratio=ratio,
            duration_ms=duration_ms,
        )

    def _get_compressor(self, algorithm: CompressionAlgorithm):
        """Get a compressor object."""
        if algorithm == CompressionAlgorithm.GZIP:
            return gzip.GzipFile(fileobj=io.BytesIO(), mode='wb')
        elif algorithm == CompressionAlgorithm.DEFLATE:
            return zlib.compressobj()
        return zlib.compressobj()

    def get_supported_algorithms(self) -> list[CompressionAlgorithm]:
        """Get list of supported compression algorithms."""
        return list(self._algorithms.keys())

    def estimate_compressed_size(
        self,
        data: bytes,
        algorithm: CompressionAlgorithm,
    ) -> int:
        """Estimate the compressed size of data."""
        result = self.compress(data, algorithm)
        return result.compressed_size
