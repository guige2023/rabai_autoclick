"""Data compressor action module for RabAI AutoClick.

Provides data compression operations:
- DataCompressor: Compress/decompress data
- ChunkCompressor: Chunk-based compression
- StreamCompressor: Stream compression
- CompressionRatioAnalyzer: Analyze compression ratios
- AutoCompressor: Auto-select best compression
"""

import gzip
import zlib
import bz2
import lzma
import base64
import io
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CompressionType(Enum):
    """Compression types."""
    GZIP = "gzip"
    ZLIB = "zlib"
    BZ2 = "bz2"
    LZMA = "lzma"
    LZ4 = "lz4"
    ZSTD = "zstd"


@dataclass
class CompressionResult:
    """Result of compression."""
    original_size: int
    compressed_size: int
    ratio: float
    algorithm: str
    duration: float


class DataCompressor:
    """General data compressor."""

    def __init__(self):
        self._compressors: Dict[CompressionType, Callable] = {
            CompressionType.GZIP: self._gzip_compress,
            CompressionType.ZLIB: self._zlib_compress,
            CompressionType.BZ2: self._bz2_compress,
            CompressionType.LZMA: self._lzma_compress,
        }
        self._decompressors: Dict[CompressionType, Callable] = {
            CompressionType.GZIP: self._gzip_decompress,
            CompressionType.ZLIB: self._zlib_decompress,
            CompressionType.BZ2: self._bz2_decompress,
            CompressionType.LZMA: self._lzma_decompress,
        }

    def compress(
        self,
        data: bytes,
        algorithm: CompressionType = CompressionType.GZIP,
        level: int = 6,
    ) -> Tuple[bytes, CompressionResult]:
        """Compress data."""
        import time
        start = time.time()

        compressor = self._compressors.get(algorithm)
        if not compressor:
            raise ValueError(f"Unknown algorithm: {algorithm}")

        compressed = compressor(data, level)
        duration = time.time() - start

        result = CompressionResult(
            original_size=len(data),
            compressed_size=len(compressed),
            ratio=len(compressed) / len(data) if len(data) > 0 else 0,
            algorithm=algorithm.value,
            duration=duration,
        )

        return compressed, result

    def decompress(
        self,
        data: bytes,
        algorithm: CompressionType = CompressionType.GZIP,
    ) -> bytes:
        """Decompress data."""
        decompressor = self._decompressors.get(algorithm)
        if not decompressor:
            raise ValueError(f"Unknown algorithm: {algorithm}")

        return decompressor(data)

    def _gzip_compress(self, data: bytes, level: int) -> bytes:
        """Gzip compress."""
        return gzip.compress(data, level)

    def _gzip_decompress(self, data: bytes) -> bytes:
        """Gzip decompress."""
        return gzip.decompress(data)

    def _zlib_compress(self, data: bytes, level: int) -> bytes:
        """Zlib compress."""
        return zlib.compress(data, level)

    def _zlib_decompress(self, data: bytes) -> bytes:
        """Zlib decompress."""
        return zlib.decompress(data)

    def _bz2_compress(self, data: bytes, level: int) -> bytes:
        """BZ2 compress."""
        return bz2.compress(data, level)

    def _bz2_decompress(self, data: bytes) -> bytes:
        """BZ2 decompress."""
        return bz2.decompress(data)

    def _lzma_compress(self, data: bytes, level: int) -> bytes:
        """LZMA compress."""
        return lzma.compress(data)

    def _lzma_decompress(self, data: bytes) -> bytes:
        """LZMA decompress."""
        return lzma.decompress(data)


class ChunkCompressor:
    """Chunk-based compression for large data."""

    def __init__(self, chunk_size: int = 1024 * 1024):
        self.chunk_size = chunk_size
        self.compressor = DataCompressor()

    def compress_stream(
        self,
        input_stream,
        output_stream,
        algorithm: CompressionType = CompressionType.GZIP,
    ) -> CompressionResult:
        """Compress stream in chunks."""
        import time
        start = time.time()

        original_size = 0
        compressed_size = 0

        while True:
            chunk = input_stream.read(self.chunk_size)
            if not chunk:
                break

            original_size += len(chunk)
            compressed_chunk, _ = self.compressor.compress(chunk, algorithm)
            compressed_size += len(compressed_chunk)
            output_stream.write(compressed_chunk)

        duration = time.time() - start

        return CompressionResult(
            original_size=original_size,
            compressed_size=compressed_size,
            ratio=compressed_size / original_size if original_size > 0 else 0,
            algorithm=algorithm.value,
            duration=duration,
        )


class AutoCompressor:
    """Auto-select best compression algorithm."""

    def __init__(self):
        self.compressor = DataCompressor()
        self._benchmark_results: Dict[CompressionType, float] = {}

    def compress_auto(
        self,
        data: bytes,
        algorithms: Optional[List[CompressionType]] = None,
        min_ratio: float = 0.5,
    ) -> Tuple[bytes, CompressionResult]:
        """Auto-select best compression."""
        if algorithms is None:
            algorithms = list(CompressionType)

        best_result = None
        best_data = None

        for algo in algorithms:
            try:
                compressed, result = self.compressor.compress(data, algo)
                if result.ratio <= min_ratio:
                    if best_result is None or result.ratio < best_result.ratio:
                        best_result = result
                        best_data = compressed
            except Exception:
                continue

        if best_data is None:
            compressed, result = self.compressor.compress(data, CompressionType.GZIP)
            return compressed, result

        return best_data, best_result


class DataCompressorAction(BaseAction):
    """Data compressor action."""
    action_type = "data_compressor"
    display_name = "数据压缩器"
    description = "数据压缩和解压缩"

    def __init__(self):
        super().__init__()
        self._compressor = DataCompressor()
        self._auto_compressor = AutoCompressor()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "compress")

            if operation == "compress":
                return self._compress(params)
            elif operation == "decompress":
                return self._decompress(params)
            elif operation == "auto":
                return self._compress_auto(params)
            elif operation == "analyze":
                return self._analyze(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Compression error: {str(e)}")

    def _compress(self, params: Dict) -> ActionResult:
        """Compress data."""
        data = params.get("data", b"")
        algo_str = params.get("algorithm", "gzip").upper()
        level = params.get("level", 6)

        if isinstance(data, str):
            data = data.encode()

        try:
            algo = CompressionType[algo_str]
        except KeyError:
            return ActionResult(success=False, message=f"Unknown algorithm: {algo_str}")

        compressed, result = self._compressor.compress(data, algo, level)

        return ActionResult(
            success=True,
            message=f"Compressed with {result.algorithm}, ratio: {result.ratio:.2%}",
            data={
                "original_size": result.original_size,
                "compressed_size": result.compressed_size,
                "ratio": result.ratio,
                "duration": result.duration,
                "data": base64.b64encode(compressed).decode(),
            },
        )

    def _decompress(self, params: Dict) -> ActionResult:
        """Decompress data."""
        data = params.get("data", b"")
        algo_str = params.get("algorithm", "gzip").upper()

        if isinstance(data, str):
            data = base64.b64decode(data)

        try:
            algo = CompressionType[algo_str]
        except KeyError:
            return ActionResult(success=False, message=f"Unknown algorithm: {algo_str}")

        try:
            decompressed = self._compressor.decompress(data, algo)
            return ActionResult(
                success=True,
                message=f"Decompressed {len(decompressed)} bytes",
                data={
                    "original_size": len(data),
                    "decompressed_size": len(decompressed),
                    "data": decompressed.decode() if isinstance(decompressed, bytes) else decompressed,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Decompression failed: {str(e)}")

    def _compress_auto(self, params: Dict) -> ActionResult:
        """Auto-compress data."""
        data = params.get("data", b"")
        min_ratio = params.get("min_ratio", 0.5)

        if isinstance(data, str):
            data = data.encode()

        compressed, result = self._auto_compressor.compress_auto(data)

        return ActionResult(
            success=True,
            message=f"Auto-compressed with {result.algorithm}, ratio: {result.ratio:.2%}",
            data={
                "algorithm": result.algorithm,
                "original_size": result.original_size,
                "compressed_size": result.compressed_size,
                "ratio": result.ratio,
                "data": base64.b64encode(compressed).decode(),
            },
        )

    def _analyze(self, params: Dict) -> ActionResult:
        """Analyze compression ratios."""
        data = params.get("data", b"")

        if isinstance(data, str):
            data = data.encode()

        results = []
        for algo in CompressionType:
            try:
                compressed, result = self._compressor.compress(data, algo)
                results.append({
                    "algorithm": algo.value,
                    "original_size": result.original_size,
                    "compressed_size": result.compressed_size,
                    "ratio": result.ratio,
                    "duration": result.duration,
                })
            except Exception:
                pass

        results.sort(key=lambda x: x["ratio"])

        return ActionResult(
            success=True,
            message=f"Analyzed {len(results)} algorithms",
            data={"results": results},
        )
