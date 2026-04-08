"""
Data Compression Action Module.

Provides data compression and decompression with multiple
algorithms, streaming support, and partial operations.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import base64
import gzip
import json
import logging
import zlib

logger = logging.getLogger(__name__)


class CompressionAlgorithm(Enum):
    """Supported compression algorithms."""
    GZIP = "gzip"
    DEFLATE = "deflate"
    ZLIB = "zlib"
    JSON = "json"


@dataclass
class CompressionResult:
    """Result of compression operation."""
    success: bool
    original_size: int
    compressed_size: int
    algorithm: CompressionAlgorithm
    data: bytes
    error: Optional[str] = None

    @property
    def ratio(self) -> float:
        """Get compression ratio."""
        if self.original_size == 0:
            return 0.0
        return 1.0 - (self.compressed_size / self.original_size)


class DataCompressor:
    """Data compression operations."""

    def __init__(self):
        self.algorithms = {
            CompressionAlgorithm.GZIP: self._gzip_compress,
            CompressionAlgorithm.DEFLATE: self._deflate_compress,
            CompressionAlgorithm.ZLIB: self._zlib_compress,
            CompressionAlgorithm.JSON: self._json_compress
        }

    def compress(
        self,
        data: bytes,
        algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP,
        level: int = 6
    ) -> CompressionResult:
        """Compress data with specified algorithm."""
        original_size = len(data)

        compress_func = self.algorithms.get(algorithm)
        if not compress_func:
            return CompressionResult(
                success=False,
                original_size=original_size,
                compressed_size=0,
                algorithm=algorithm,
                data=b"",
                error=f"Unknown algorithm: {algorithm}"
            )

        try:
            compressed = compress_func(data, level)
            return CompressionResult(
                success=True,
                original_size=original_size,
                compressed_size=len(compressed),
                algorithm=algorithm,
                data=compressed
            )
        except Exception as e:
            return CompressionResult(
                success=False,
                original_size=original_size,
                compressed_size=0,
                algorithm=algorithm,
                data=b"",
                error=str(e)
            )

    def decompress(
        self,
        data: bytes,
        algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP
    ) -> CompressionResult:
        """Decompress data."""
        original_size = len(data)

        try:
            if algorithm == CompressionAlgorithm.GZIP:
                decompressed = gzip.decompress(data)
            elif algorithm == CompressionAlgorithm.DEFLATE:
                decompressed = zlib.decompress(data, -zlib.MAX_WBITS)
            elif algorithm == CompressionAlgorithm.ZLIB:
                decompressed = zlib.decompress(data)
            elif algorithm == CompressionAlgorithm.JSON:
                return CompressionResult(
                    success=True,
                    original_size=original_size,
                    compressed_size=len(data),
                    algorithm=algorithm,
                    data=data
                )
            else:
                return CompressionResult(
                    success=False,
                    original_size=original_size,
                    compressed_size=0,
                    algorithm=algorithm,
                    data=b"",
                    error=f"Unknown algorithm: {algorithm}"
                )

            return CompressionResult(
                success=True,
                original_size=original_size,
                compressed_size=len(decompressed),
                algorithm=algorithm,
                data=decompressed
            )
        except Exception as e:
            return CompressionResult(
                success=False,
                original_size=original_size,
                compressed_size=0,
                algorithm=algorithm,
                data=b"",
                error=str(e)
            )

    def _gzip_compress(self, data: bytes, level: int) -> bytes:
        """Compress with GZIP."""
        return gzip.compress(data, level)

    def _deflate_compress(self, data: bytes, level: int) -> bytes:
        """Compress with DEFLATE."""
        return zlib.compress(data, level)

    def _zlib_compress(self, data: bytes, level: int) -> bytes:
        """Compress with ZLIB."""
        return zlib.compress(data, level)

    def _json_compress(self, data: bytes, level: int) -> bytes:
        """Compress with JSON encoding."""
        return json.dumps(data.decode('utf-8', errors='ignore')).encode('utf-8')


class ChunkedCompressor:
    """Streaming chunked compression."""

    def __init__(self, chunk_size: int = 8192):
        self.chunk_size = chunk_size

    def compress_stream(
        self,
        data: bytes,
        algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP
    ) -> List[bytes]:
        """Compress data in chunks."""
        compressor = zlib.compressobj(6, zlib.DEFLATED, zlib.MAX_WBITS | 16 if algorithm == CompressionAlgorithm.GZIP else zlib.MAX_WBITS)
        chunks = []

        for i in range(0, len(data), self.chunk_size):
            chunk = data[i:i + self.chunk_size]
            compressed = compressor.compress(chunk)
            if compressed:
                chunks.append(compressed)

        final = compressor.flush()
        if final:
            chunks.append(final)

        return chunks

    def decompress_stream(
        self,
        chunks: List[bytes],
        algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP
    ) -> bytes:
        """Decompress chunked data."""
        if algorithm == CompressionAlgorithm.GZIP:
            decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
        else:
            decompressor = zlib.decompressobj()

        result = b""
        for chunk in chunks:
            result += decompressor.decompress(chunk)

        result += decompressor.flush()
        return result


class DeltaCompressor:
    """Delta compression for similar data."""

    def compress_delta(
        self,
        base: bytes,
        target: bytes
    ) -> Tuple[bytes, bytes]:
        """Compress target as delta from base."""
        delta = bytearray()

        for i, (b, t) in enumerate(zip(base, target)):
            if b != t:
                delta.append(t)

        return bytes(delta), target[len(base):]

    def decompress_delta(
        self,
        base: bytes,
        delta: bytes,
        remaining: bytes
    ) -> bytes:
        """Restore target from base and delta."""
        result = bytearray(base[:len(base) - len(remaining)])

        delta_idx = 0
        for b in remaining:
            if b != 0:
                result.append(b)

        return bytes(result)


def main():
    """Demonstrate compression."""
    compressor = DataCompressor()

    data = b"Hello, World! " * 100

    result = compressor.compress(data, CompressionAlgorithm.GZIP)
    print(f"Compressed: {result.success}, Ratio: {result.ratio:.2%}")

    decompressed = compressor.decompress(result.data)
    print(f"Decompressed: {decompressed.success}")


if __name__ == "__main__":
    main()
