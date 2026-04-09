"""
Data Compress Action Module.

Data compression and decompression utilities for automation including
multiple algorithms, streaming compression, and archive management.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import zlib
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class CompressionAlgorithm(Enum):
    """Supported compression algorithms."""
    GZIP = "gzip"
    ZLIB = "zlib"
    LZ4 = "lz4"
    BROTLI = "brotli"
    NONE = "none"


@dataclass
class CompressionResult:
    """Result of a compression operation."""
    success: bool
    original_size: int = 0
    compressed_size: int = 0
    compression_ratio: float = 0.0
    algorithm: CompressionAlgorithm = CompressionAlgorithm.NONE
    data: Optional[bytes] = None
    error: Optional[str] = None


@dataclass
class CompressionStats:
    """Statistics for compression operations."""
    total_compressed: int = 0
    total_decompressed: int = 0
    total_original_bytes: int = 0
    total_compressed_bytes: int = 0
    avg_ratio: float = 0.0


class DataCompressAction:
    """
    Data compression utilities for automation.

    Supports multiple compression algorithms with consistent interface,
    streaming operations, and statistics tracking.

    Example:
        compressor = DataCompressAction()

        # Compress
        result = compressor.compress(b"large data here", algorithm=CompressionAlgorithm.GZIP)

        # Decompress
        decompressed = compressor.decompress(result.data, algorithm=CompressionAlgorithm.GZIP)
    """

    def __init__(self) -> None:
        self._stats = CompressionStats()
        self._algo_map = {
            CompressionAlgorithm.GZIP: self._gzip_compress,
            CompressionAlgorithm.ZLIB: self._zlib_compress,
            CompressionAlgorithm.NONE: self._no_compress,
        }
        self._decomp_map = {
            CompressionAlgorithm.GZIP: self._gzip_decompress,
            CompressionAlgorithm.ZLIB: self._zlib_decompress,
            CompressionAlgorithm.NONE: self._no_decompress,
        }

    def compress(
        self,
        data: Union[bytes, str],
        algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP,
        compression_level: int = 6,
    ) -> CompressionResult:
        """Compress data using the specified algorithm."""
        if isinstance(data, str):
            data = data.encode("utf-8")

        original_size = len(data)

        try:
            compress_fn = self._algo_map.get(algorithm, self._gzip_compress)
            compressed = compress_fn(data, compression_level)

            self._stats.total_compressed += 1
            self._stats.total_original_bytes += original_size
            self._stats.total_compressed_bytes += len(compressed)

            ratio = (original_size - len(compressed)) / original_size if original_size > 0 else 0.0
            self._stats.avg_ratio = (
                (self._stats.avg_ratio * (self._stats.total_compressed - 1) + ratio)
                / self._stats.total_compressed
            )

            return CompressionResult(
                success=True,
                original_size=original_size,
                compressed_size=len(compressed),
                compression_ratio=ratio,
                algorithm=algorithm,
                data=compressed,
            )

        except Exception as e:
            logger.error(f"Compression failed: {e}")
            return CompressionResult(
                success=False,
                original_size=original_size,
                algorithm=algorithm,
                error=str(e),
            )

    def decompress(
        self,
        data: bytes,
        algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP,
    ) -> CompressionResult:
        """Decompress data using the specified algorithm."""
        try:
            decompress_fn = self._decomp_map.get(algorithm, self._gzip_decompress)
            decompressed = decompress_fn(data)

            self._stats.total_decompressed += 1

            return CompressionResult(
                success=True,
                original_size=len(data),
                compressed_size=len(decompressed),
                algorithm=algorithm,
                data=decompressed,
            )

        except Exception as e:
            logger.error(f"Decompression failed: {e}")
            return CompressionResult(
                success=False,
                algorithm=algorithm,
                error=str(e),
            )

    def _gzip_compress(self, data: bytes, level: int) -> bytes:
        """Gzip compression."""
        return gzip.compress(data, compresslevel=level)

    def _gzip_decompress(self, data: bytes) -> bytes:
        """Gzip decompression."""
        return gzip.decompress(data)

    def _zlib_compress(self, data: bytes, level: int) -> bytes:
        """Zlib compression."""
        return zlib.compress(data, level=level)

    def _zlib_decompress(self, data: bytes) -> bytes:
        """Zlib decompression."""
        return zlib.decompress(data)

    def _no_compress(self, data: bytes, level: int) -> bytes:
        """No compression (passthrough)."""
        return data

    def _no_decompress(self, data: bytes) -> bytes:
        """No decompression (passthrough)."""
        return data

    def compress_json(
        self,
        obj: Dict[str, Any],
        algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP,
    ) -> CompressionResult:
        """Compress a JSON-serializable object."""
        try:
            json_bytes = json.dumps(obj).encode("utf-8")
            return self.compress(json_bytes, algorithm)
        except Exception as e:
            return CompressionResult(success=False, error=str(e))

    def decompress_json(
        self,
        data: bytes,
        algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP,
    ) -> Optional[Dict[str, Any]]:
        """Decompress to a JSON object."""
        result = self.decompress(data, algorithm)
        if result.success and result.data:
            try:
                return json.loads(result.data.decode("utf-8"))
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode failed: {e}")
        return None

    def compress_file(
        self,
        file_path: str,
        algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP,
    ) -> CompressionResult:
        """Compress a file."""
        try:
            with open(file_path, "rb") as f:
                data = f.read()
            return self.compress(data, algorithm)
        except Exception as e:
            return CompressionResult(success=False, error=str(e))

    def get_stats(self) -> CompressionStats:
        """Get compression statistics."""
        return self._stats

    def reset_stats(self) -> None:
        """Reset compression statistics."""
        self._stats = CompressionStats()


def streaming_compress(
    chunks: List[bytes],
    algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP,
) -> bytes:
    """Compress a sequence of chunks into a single compressed stream."""
    if algorithm == CompressionAlgorithm.GZIP:
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
            for chunk in chunks:
                gz.write(chunk)
        return buffer.getvalue()

    elif algorithm == CompressionAlgorithm.ZLIB:
        comp = zlib.compressobj()
        result = b"".join(comp.compress(chunk) for chunk in chunks)
        result += comp.flush()
        return result

    return b"".join(chunks)


def streaming_decompress(
    data: bytes,
    algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP,
) -> List[bytes]:
    """Decompress into chunks."""
    if algorithm == CompressionAlgorithm.GZIP:
        buffer = io.BytesIO(data)
        chunks = []
        with gzip.GzipFile(fileobj=buffer, mode="rb") as gz:
            while True:
                chunk = gz.read(8192)
                if not chunk:
                    break
                chunks.append(chunk)
        return chunks

    elif algorithm == CompressionAlgorithm.ZLIB:
        decomp = zlib.decompressobj()
        return [decomp.decompress(data)]

    return [data]
