"""
Data Compression Action Module.

Provides data compression and decompression utilities supporting multiple
algorithms, streaming compression, and intelligent format selection.

Author: RabAi Team
"""

from __future__ import annotations

import gzip
import io
import json
import lzma
import zlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import brotli


class CompressionAlgorithm(Enum):
    """Supported compression algorithms."""
    GZIP = "gzip"
    ZLIB = "zlib"
    LZMA = "lzma"
    BROTLI = "brotli"
    ZSTD = "zstd"
    LZ4 = "lz4"


class CompressionLevel(Enum):
    """Compression level presets."""
    BEST_SPEED = 1
    BEST_COMPRESSION = 9
    DEFAULT = 6


@dataclass
class CompressionResult:
    """Result of a compression operation."""
    original_size: int
    compressed_size: int
    algorithm: CompressionAlgorithm
    compression_ratio: float
    duration_ms: float
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def space_saved(self) -> int:
        return self.original_size - self.compressed_size

    @property
    def space_saved_percent(self) -> float:
        if self.original_size == 0:
            return 0.0
        return (1 - self.compressed_size / self.original_size) * 100

    def to_dict(self) -> Dict[str, Any]:
        return {
            "original_size": self.original_size,
            "compressed_size": self.compressed_size,
            "algorithm": self.algorithm.value,
            "compression_ratio": self.compression_ratio,
            "space_saved": self.space_saved,
            "space_saved_percent": self.space_saved_percent,
            "duration_ms": self.duration_ms,
            "metadata": self.metadata,
        }


class CompressionStrategy(ABC):
    """Abstract base for compression strategies."""

    @abstractmethod
    def compress(self, data: bytes, level: int = 6) -> bytes:
        """Compress data."""
        pass

    @abstractmethod
    def decompress(self, data: bytes) -> bytes:
        """Decompress data."""
        pass


class GzipStrategy(CompressionStrategy):
    """Gzip compression implementation."""

    def compress(self, data: bytes, level: int = 6) -> bytes:
        return gzip.compress(data, compresslevel=level)

    def decompress(self, data: bytes) -> bytes:
        return gzip.decompress(data)


class ZlibStrategy(CompressionStrategy):
    """Zlib compression implementation."""

    def compress(self, data: bytes, level: int = 6) -> bytes:
        return zlib.compress(data, level=level)

    def decompress(self, data: bytes) -> bytes:
        return zlib.decompress(data)


class LzmaStrategy(CompressionStrategy):
    """LZMA compression implementation."""

    def compress(self, data: bytes, level: int = 6) -> bytes:
        return lzma.compress(data, preset=level)

    def decompress(self, data: bytes) -> bytes:
        return lzma.decompress(data)


class BrotliStrategy(CompressionStrategy):
    """Brotli compression implementation."""

    def compress(self, data: bytes, level: int = 6) -> bytes:
        return brotli.compress(data, quality=level)

    def decompress(self, data: bytes) -> bytes:
        return brotli.decompress(data)


class DataCompressor:
    """
    Data compression and decompression utility.

    Supports multiple compression algorithms with configurable levels,
    streaming operations, and automatic format detection.

    Example:
        >>> compressor = DataCompressor()
        >>> result = compressor.compress(b"large dataset...", algorithm=CompressionAlgorithm.GZIP)
        >>> decompressed = compressor.decompress(result.compressed_data)
    """

    def __init__(self):
        self._strategies: Dict[CompressionAlgorithm, CompressionStrategy] = {
            CompressionAlgorithm.GZIP: GzipStrategy(),
            CompressionAlgorithm.ZLIB: ZlibStrategy(),
            CompressionAlgorithm.LZMA: LzmaStrategy(),
            CompressionAlgorithm.BROTLI: BrotliStrategy(),
        }

    def compress(
        self,
        data: Union[bytes, str],
        algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP,
        level: int = 6,
    ) -> Tuple[bytes, CompressionResult]:
        """
        Compress data using specified algorithm.

        Returns:
            Tuple of (compressed_bytes, CompressionResult)
        """
        if isinstance(data, str):
            data = data.encode("utf-8")

        import time
        start = time.time()

        strategy = self._strategies.get(algorithm)
        if not strategy:
            raise ValueError(f"Unsupported algorithm: {algorithm}")

        compressed = strategy.compress(data, level)
        duration_ms = (time.time() - start) * 1000

        result = CompressionResult(
            original_size=len(data),
            compressed_size=len(compressed),
            algorithm=algorithm,
            compression_ratio=len(compressed) / len(data) if data else 0,
            duration_ms=duration_ms,
            metadata={"level": level},
        )

        return compressed, result

    def decompress(
        self,
        data: bytes,
        algorithm: CompressionAlgorithm,
    ) -> bytes:
        """Decompress data using specified algorithm."""
        strategy = self._strategies.get(algorithm)
        if not strategy:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
        return strategy.decompress(data)

    def decompress_auto(self, data: bytes) -> Tuple[bytes, CompressionAlgorithm]:
        """
        Auto-detect compression format and decompress.

        Tries each algorithm until one succeeds.
        """
        for algo in CompressionAlgorithm:
            strategy = self._strategies.get(algo)
            if strategy:
                try:
                    decompressed = strategy.decompress(data)
                    return decompressed, algo
                except Exception:
                    continue
        raise ValueError("Could not auto-detect compression format")

    def compress_file(
        self,
        input_path: str,
        output_path: Optional[str] = None,
        algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP,
        chunk_size: int = 8192,
    ) -> CompressionResult:
        """Compress a file."""
        import time
        start = time.time()

        if output_path is None:
            output_path = f"{input_path}.{algorithm.value}"

        strategy = self._strategies.get(algorithm)
        if not strategy:
            raise ValueError(f"Unsupported algorithm: {algorithm}")

        original_size = 0
        with open(input_path, "rb") as fin:
            with open(output_path, "wb") as fout:
                compressor = zlib.compressobj(level=6) if algorithm == CompressionAlgorithm.ZLIB else None
                while True:
                    chunk = fin.read(chunk_size)
                    if not chunk:
                        break
                    original_size += len(chunk)
                    compressed_chunk = strategy.compress(chunk) if compressor is None else compressor.compress(chunk)
                    fout.write(compressed_chunk)
                if compressor:
                    fout.write(compressor.flush())

        compressed_size = 0
        with open(output_path, "rb") as f:
            compressed_size = f.seek(0, 2)

        duration_ms = (time.time() - start) * 1000

        return CompressionResult(
            original_size=original_size,
            compressed_size=compressed_size,
            algorithm=algorithm,
            compression_ratio=compressed_size / original_size if original_size else 0,
            duration_ms=duration_ms,
            metadata={"input_path": input_path, "output_path": output_path},
        )

    def get_recommended_algorithm(
        self,
        data: bytes,
        sample_size: int = 10240,
    ) -> CompressionAlgorithm:
        """
        Recommend best algorithm for data based on sampling.

        Tests a sample of data with each algorithm to find best compression.
        """
        sample = data[:sample_size] if len(data) > sample_size else data
        best_algo = CompressionAlgorithm.GZIP
        best_ratio = float("inf")

        for algo, strategy in self._strategies.items():
            try:
                compressed = strategy.compress(sample)
                ratio = len(compressed) / len(sample) if sample else 0
                if ratio < best_ratio:
                    best_ratio = ratio
                    best_algo = algo
            except Exception:
                continue

        return best_algo

    def stream_compress(
        self,
        input_stream,
        output_stream,
        algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP,
        chunk_size: int = 8192,
    ) -> CompressionResult:
        """Stream compress data from input to output."""
        import time
        start = time.time()

        strategy = self._strategies.get(algorithm)
        if not strategy:
            raise ValueError(f"Unsupported algorithm: {algorithm}")

        original_size = 0
        compressed_size = 0

        while True:
            chunk = input_stream.read(chunk_size)
            if not chunk:
                break
            original_size += len(chunk)
            compressed = strategy.compress(chunk)
            compressed_size += len(compressed)
            output_stream.write(compressed)

        duration_ms = (time.time() - start) * 1000

        return CompressionResult(
            original_size=original_size,
            compressed_size=compressed_size,
            algorithm=algorithm,
            compression_ratio=compressed_size / original_size if original_size else 0,
            duration_ms=duration_ms,
        )


def create_compressor() -> DataCompressor:
    """Factory to create a data compressor."""
    return DataCompressor()
