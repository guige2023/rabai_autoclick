"""Data Compressor Action Module.

Provides data compression and decompression using various algorithms
including gzip, zlib, lz4, zstandard, and brotli.
"""

from __future__ import annotations

import asyncio
import gzip
import io
import logging
import lz4.frame
import zlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

try:
    import zstandard as zstd
    ZSTD_AVAILABLE = True
except ImportError:
    ZSTD_AVAILABLE = False

try:
    import brotli
    BROTLI_AVAILABLE = True
except ImportError:
    BROTLI_AVAILABLE = False


class CompressionAlgorithm(Enum):
    """Supported compression algorithms."""
    GZIP = "gzip"
    ZLIB = "zlib"
    LZ4 = "lz4"
    ZSTD = "zstd"
    BROTLI = "brotli"


@dataclass
class CompressionResult:
    """Result of a compression operation."""
    algorithm: CompressionAlgorithm
    original_size: int
    compressed_size: int
    compression_ratio: float
    duration_ms: float
    success: bool
    error: Optional[str] = None

    def space_saved(self) -> int:
        """Return bytes saved by compression."""
        return self.original_size - self.compressed_size


@dataclass
class CompressionStats:
    """Statistics for compression operations."""
    total_original_bytes: int = 0
    total_compressed_bytes: int = 0
    total_operations: int = 0
    total_time_ms: float = 0.0

    def average_ratio(self) -> float:
        """Return average compression ratio."""
        if self.total_original_bytes == 0:
            return 0.0
        return self.total_original_bytes / max(self.total_compressed_bytes, 1)

    def throughput_mbps(self) -> float:
        """Return throughput in MB/s."""
        if self.total_time_ms == 0:
            return 0.0
        return (self.total_original_bytes / (1024 * 1024)) / (self.total_time_ms / 1000)


class GzipCompressor:
    """Gzip compression implementation."""

    @staticmethod
    def compress(data: bytes, level: int = 6) -> bytes:
        """Compress data using gzip."""
        return gzip.compress(data, compresslevel=level)

    @staticmethod
    def decompress(data: bytes) -> bytes:
        """Decompress gzip data."""
        return gzip.decompress(data)


class ZlibCompressor:
    """Zlib compression implementation."""

    @staticmethod
    def compress(data: bytes, level: int = 6) -> bytes:
        """Compress data using zlib."""
        return zlib.compress(data, level=level)

    @staticmethod
    def decompress(data: bytes) -> bytes:
        """Decompress zlib data."""
        return zlib.decompress(data)


class LZ4Compressor:
    """LZ4 compression implementation."""

    @staticmethod
    def compress(data: bytes, level: int = 1) -> bytes:
        """Compress data using LZ4."""
        compression_level = lz4.frame.COMPRESSIONLEVEL_MINHC
        return lz4.frame.compress(data, compression_level=compression_level)

    @staticmethod
    def decompress(data: bytes) -> bytes:
        """Decompress LZ4 data."""
        return lz4.frame.decompress(data)


class ZstdCompressor:
    """Zstandard compression implementation."""

    @staticmethod
    def compress(data: bytes, level: int = 3) -> bytes:
        """Compress data using zstandard."""
        if not ZSTD_AVAILABLE:
            raise ImportError("zstandard not available")
        ctx = zstd.ZstdCompressor(level=level)
        return ctx.compress(data)

    @staticmethod
    def decompress(data: bytes) -> bytes:
        """Decompress zstandard data."""
        if not ZSTD_AVAILABLE:
            raise ImportError("zstandard not available")
        ctx = zstd.ZstdDecompressor()
        return ctx.decompress(data)


class BrotliCompressor:
    """Brotli compression implementation."""

    @staticmethod
    def compress(data: bytes, level: int = 6) -> bytes:
        """Compress data using brotli."""
        if not BROTLI_AVAILABLE:
            raise ImportError("brotli not available")
        return brotli.compress(data, quality=level)

    @staticmethod
    def decompress(data: bytes) -> bytes:
        """Decompress brotli data."""
        if not BROTLI_AVAILABLE:
            raise ImportError("brotli not available")
        return brotli.decompress(data)


class StreamingCompressor:
    """Streaming compression for large data."""

    def __init__(self, algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP):
        self._algorithm = algorithm
        self._compressors = {
            CompressionAlgorithm.GZIP: GzipCompressor,
            CompressionAlgorithm.ZLIB: ZlibCompressor,
            CompressionAlgorithm.LZ4: LZ4Compressor,
            CompressionAlgorithm.ZSTD: ZstdCompressor,
            CompressionAlgorithm.BROTLI: BrotliCompressor,
        }

    def compress_stream(
        self,
        input_stream: io.BytesIO,
        output_stream: io.BytesIO,
        chunk_size: int = 65536
    ) -> int:
        """Compress data from input stream to output stream."""
        compressor_class = self._compressors.get(self._algorithm)
        if not compressor_class:
            raise ValueError(f"Unsupported algorithm: {self._algorithm}")

        if self._algorithm == CompressionAlgorithm.GZIP:
            with gzip.GzipFile(fileobj=output_stream, mode="wb") as f:
                while True:
                    chunk = input_stream.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)
            return output_stream.tell()

        elif self._algorithm == CompressionAlgorithm.ZLIB:
            compressor = zlib.compressobj()
            total = 0
            while True:
                chunk = input_stream.read(chunk_size)
                if not chunk:
                    break
                compressed = compressor.compress(chunk)
                if compressed:
                    output_stream.write(compressed)
                    total += len(compressed)
            final = compressor.flush()
            if final:
                output_stream.write(final)
                total += len(final)
            return total

        else:
            # For LZ4, ZSTD, BROTLI - read all and compress
            data = input_stream.read()
            compressed = compressor_class.compress(data)
            output_stream.write(compressed)
            return len(compressed)

    def decompress_stream(
        self,
        input_stream: io.BytesIO,
        output_stream: io.BytesIO,
        chunk_size: int = 65536
    ) -> int:
        """Decompress data from input stream to output stream."""
        compressor_class = self._compressors.get(self._algorithm)
        if not compressor_class:
            raise ValueError(f"Unsupported algorithm: {self._algorithm}")

        if self._algorithm == CompressionAlgorithm.GZIP:
            with gzip.GzipFile(fileobj=input_stream, mode="rb") as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    output_stream.write(chunk)
            return output_stream.tell()

        elif self._algorithm == CompressionAlgorithm.ZLIB:
            decompressor = zlib.decompressobj()
            total = 0
            while True:
                chunk = input_stream.read(chunk_size)
                if not chunk:
                    break
                decompressed = decompressor.decompress(chunk)
                if decompressed:
                    output_stream.write(decompressed)
                    total += len(decompressed)
            return total

        else:
            # For LZ4, ZSTD, BROTLI - read all and decompress
            data = input_stream.read()
            decompressed = compressor_class.decompress(data)
            output_stream.write(decompressed)
            return len(decompressed)


class DataCompressorAction:
    """Main action class for data compression."""

    def __init__(self):
        self._compressors = {
            CompressionAlgorithm.GZIP: GzipCompressor,
            CompressionAlgorithm.ZLIB: ZlibCompressor,
            CompressionAlgorithm.LZ4: LZ4Compressor,
            CompressionAlgorithm.ZSTD: ZstdCompressor,
            CompressionAlgorithm.BROTLI: BrotliCompressor,
        }
        self._stats = CompressionStats()
        self._default_algorithm = CompressionAlgorithm.GZIP

    def set_default_algorithm(self, algorithm: CompressionAlgorithm) -> None:
        """Set the default compression algorithm."""
        self._default_algorithm = algorithm

    def compress(
        self,
        data: bytes,
        algorithm: Optional[CompressionAlgorithm] = None,
        level: int = 6
    ) -> CompressionResult:
        """Compress data using specified algorithm."""
        algo = algorithm or self._default_algorithm
        compressor_class = self._compressors.get(algo)

        if not compressor_class:
            return CompressionResult(
                algorithm=algo,
                original_size=len(data),
                compressed_size=0,
                compression_ratio=0.0,
                duration_ms=0.0,
                success=False,
                error=f"Unsupported algorithm: {algo.value}"
            )

        import time
        start = time.time()

        try:
            compressed = compressor_class.compress(data, level=level)
            duration_ms = (time.time() - start) * 1000

            result = CompressionResult(
                algorithm=algo,
                original_size=len(data),
                compressed_size=len(compressed),
                compression_ratio=len(compressed) / len(data) if data else 0.0,
                duration_ms=duration_ms,
                success=True
            )

            # Update stats
            self._stats.total_original_bytes += result.original_size
            self._stats.total_compressed_bytes += result.compressed_size
            self._stats.total_operations += 1
            self._stats.total_time_ms += duration_ms

            return result

        except Exception as e:
            logger.exception(f"Compression failed: {e}")
            return CompressionResult(
                algorithm=algo,
                original_size=len(data),
                compressed_size=0,
                compression_ratio=0.0,
                duration_ms=(time.time() - start) * 1000,
                success=False,
                error=str(e)
            )

    def decompress(
        self,
        data: bytes,
        algorithm: Optional[CompressionAlgorithm] = None
    ) -> Tuple[bool, bytes, Optional[str]]:
        """Decompress data using specified algorithm."""
        algo = algorithm or self._default_algorithm
        compressor_class = self._compressors.get(algo)

        if not compressor_class:
            return False, b"", f"Unsupported algorithm: {algo.value}"

        try:
            decompressed = compressor_class.decompress(data)
            return True, decompressed, None
        except Exception as e:
            logger.exception(f"Decompression failed: {e}")
            return False, b"", str(e)

    def get_stats(self) -> CompressionStats:
        """Return compression statistics."""
        return self._stats

    def get_supported_algorithms(self) -> List[str]:
        """Return list of supported algorithms."""
        supported = [algo.value for algo in self._compressors.keys()]

        if not ZSTD_AVAILABLE:
            supported = [s for s in supported if s != "zstd"]
        if not BROTLI_AVAILABLE:
            supported = [s for s in supported if s != "brotli"]

        return supported

    async def execute(
        self,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the data compressor action.

        Args:
            context: Dictionary containing:
                - operation: Operation to perform (compress, decompress, stats)
                - data: Data to compress/decompress (base64 encoded if provided as string)
                - algorithm: Compression algorithm to use
                - level: Compression level (1-9)

        Returns:
            Dictionary with compression results.
        """
        operation = context.get("operation", "compress")

        if operation == "compress":
            import base64

            data_input = context.get("data", "")
            if isinstance(data_input, str):
                # Assume base64 encoded
                try:
                    data = base64.b64decode(data_input)
                except Exception:
                    data = data_input.encode("utf-8")
            else:
                data = data_input

            algo_str = context.get("algorithm", "gzip")
            try:
                algo = CompressionAlgorithm(algo_str)
            except ValueError:
                algo = self._default_algorithm

            level = context.get("level", 6)

            result = self.compress(data, algo, level)

            if result.success:
                return {
                    "success": True,
                    "result": {
                        "algorithm": result.algorithm.value,
                        "original_size": result.original_size,
                        "compressed_size": result.compressed_size,
                        "compression_ratio": round(result.compression_ratio, 4),
                        "space_saved": result.space_saved(),
                        "duration_ms": round(result.duration_ms, 2),
                        "compressed_data": base64.b64encode(result.original_size > 0 and data[:0] or b"").decode() if False else base64.b64encode(self._compressors[algo].compress(data, level)).decode()
                    }
                }
            else:
                return {"success": False, "error": result.error}

        elif operation == "decompress":
            import base64

            data_input = context.get("data", "")
            if isinstance(data_input, str):
                try:
                    data = base64.b64decode(data_input)
                except Exception:
                    data = data_input.encode("utf-8")
            else:
                data = data_input

            algo_str = context.get("algorithm", "gzip")
            try:
                algo = CompressionAlgorithm(algo_str)
            except ValueError:
                algo = self._default_algorithm

            success, decompressed, error = self.decompress(data, algo)

            if success:
                return {
                    "success": True,
                    "original_size": len(decompressed),
                    "decompressed_data": decompressed.decode("utf-8", errors="replace")
                }
            else:
                return {"success": False, "error": error}

        elif operation == "stats":
            return {
                "success": True,
                "stats": {
                    "total_operations": self._stats.total_operations,
                    "total_original_bytes": self._stats.total_original_bytes,
                    "total_compressed_bytes": self._stats.total_compressed_bytes,
                    "average_ratio": round(self._stats.average_ratio(), 4),
                    "throughput_mbps": round(self._stats.throughput_mbps(), 2)
                },
                "supported_algorithms": self.get_supported_algorithms()
            }

        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
