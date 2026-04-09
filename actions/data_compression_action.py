"""
Data Compression Action Module.

Data compression with multiple algorithms, streaming support,
and automatic algorithm selection based on data characteristics.
"""

import gzip
import zlib
import bz2
import lzma
import io
from dataclasses import dataclass
from typing import Any, Callable, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class CompressionAlgorithm(Enum):
    """Supported compression algorithms."""
    NONE = "none"
    ZLIB = "zlib"
    GZIP = "gzip"
    BZ2 = "bz2"
    LZMA = "lzma"
    LZ4 = "lz4"
    ZSTD = "zstd"


class CompressionLevel(Enum):
    """Compression levels."""
    BEST_SPEED = "best_speed"
    BEST_COMPRESSION = "best_compression"
    DEFAULT = "default"


@dataclass
class CompressionResult:
    """Result of compression operation."""
    success: bool
    original_size: int
    compressed_size: int
    algorithm: CompressionAlgorithm
    compression_ratio: float
    data: Any


class DataCompressionAction:
    """
    Data compression with multiple algorithm support.

    Example:
        compressor = DataCompressionAction()
        result = compressor.compress(data, algorithm=CompressionAlgorithm.ZLIB)
        decompressed = compressor.decompress(result.data, algorithm=CompressionAlgorithm.ZLIB)
    """

    ALGORITHM_MAP = {
        CompressionAlgorithm.ZLIB: ("zlib", zlib.compress, zlib.decompress),
        CompressionAlgorithm.GZIP: ("gzip", gzip.compress, gzip.decompress),
        CompressionAlgorithm.BZ2: ("bz2", bz2.compress, bz2.decompress),
        CompressionAlgorithm.LZMA: ("lzma", lzma.compress, lzma.decompress),
    }

    def __init__(self):
        """Initialize data compression action."""
        self._default_algorithm = CompressionAlgorithm.ZLIB
        self._algorithm_selector: Optional[Callable] = None

    def set_default_algorithm(self, algorithm: CompressionAlgorithm) -> None:
        """Set default compression algorithm."""
        self._default_algorithm = algorithm

    def register_algorithm_selector(
        self,
        selector: Callable[[bytes], CompressionAlgorithm]
    ) -> None:
        """
        Register custom algorithm selector.

        Args:
            selector: Function that selects algorithm based on data.
        """
        self._algorithm_selector = selector

    def compress(
        self,
        data: Any,
        algorithm: Optional[CompressionAlgorithm] = None,
        level: CompressionLevel = CompressionLevel.DEFAULT,
        encoding: str = "utf-8"
    ) -> CompressionResult:
        """
        Compress data.

        Args:
            data: Data to compress.
            algorithm: Compression algorithm (auto-selects if None).
            level: Compression level.
            encoding: Text encoding for string data.

        Returns:
            CompressionResult with compressed data.
        """
        import tempfile
        import shutil

        if isinstance(data, str):
            data_bytes = data.encode(encoding)
        elif isinstance(data, bytes):
            data_bytes = data
        else:
            data_bytes = str(data).encode(encoding)

        original_size = len(data_bytes)

        if algorithm is None:
            if self._algorithm_selector:
                algorithm = self._algorithm_selector(data_bytes)
            else:
                algorithm = self._default_algorithm

        if algorithm == CompressionAlgorithm.NONE:
            return CompressionResult(
                success=True,
                original_size=original_size,
                compressed_size=original_size,
                algorithm=algorithm,
                compression_ratio=1.0,
                data=data_bytes
            )

        compress_func, _ = self._get_algorithm_funcs(algorithm)

        if not compress_func:
            return CompressionResult(
                success=False,
                original_size=original_size,
                compressed_size=0,
                algorithm=algorithm,
                compression_ratio=0.0,
                data=None
            )

        try:
            level_value = self._get_compression_level(level, algorithm)

            if algorithm in (CompressionAlgorithm.LZ4, CompressionAlgorithm.ZSTD):
                result = self._compress_optional(data_bytes, algorithm, level)
            else:
                result = compress_func(data_bytes, level_value)

            compressed_size = len(result)
            ratio = compressed_size / original_size if original_size > 0 else 0.0

            return CompressionResult(
                success=True,
                original_size=original_size,
                compressed_size=compressed_size,
                algorithm=algorithm,
                compression_ratio=ratio,
                data=result
            )

        except Exception as e:
            logger.error(f"Compression failed: {e}")
            return CompressionResult(
                success=False,
                original_size=original_size,
                compressed_size=0,
                algorithm=algorithm,
                compression_ratio=0.0,
                data=None
            )

    def decompress(
        self,
        data: bytes,
        algorithm: CompressionAlgorithm,
        encoding: str = "utf-8"
    ) -> Any:
        """
        Decompress data.

        Args:
            data: Compressed data bytes.
            algorithm: Compression algorithm used.
            encoding: Output encoding for string data.

        Returns:
            Decompressed data (bytes or string).
        """
        if algorithm == CompressionAlgorithm.NONE:
            return data

        _, decompress_func = self._get_algorithm_funcs(algorithm)

        if not decompress_func:
            if algorithm in (CompressionAlgorithm.LZ4, CompressionAlgorithm.ZSTD):
                return self._decompress_optional(data, algorithm)
            return None

        try:
            return decompress_func(data)
        except Exception as e:
            logger.error(f"Decompression failed: {e}")
            return None

    def compress_streaming(
        self,
        input_stream: io.BytesIO,
        output_stream: io.BytesIO,
        algorithm: CompressionAlgorithm,
        chunk_size: int = 8192
    ) -> bool:
        """
        Compress data using streaming approach.

        Args:
            input_stream: Input BytesIO stream.
            output_stream: Output BytesIO stream.
            algorithm: Compression algorithm.
            chunk_size: Chunk size for streaming.

        Returns:
            True if successful.
        """
        if algorithm == CompressionAlgorithm.NONE:
            while True:
                chunk = input_stream.read(chunk_size)
                if not chunk:
                    break
                output_stream.write(chunk)
            return True

        if algorithm not in self.ALGORITHM_MAP:
            logger.error(f"Streaming not supported for algorithm: {algorithm}")
            return False

        compressor = self._create_compressor(algorithm)

        if not compressor:
            return False

        try:
            while True:
                chunk = input_stream.read(chunk_size)
                if not chunk:
                    break

                if algorithm in (CompressionAlgorithm.GZIP, CompressionAlgorithm.BZ2, CompressionAlgorithm.LZMA):
                    compressed = compressor.compress(chunk)
                else:
                    compressed = zlib.compress(chunk, level=6)

                output_stream.write(compressed)

            if hasattr(compressor, 'flush'):
                output_stream.write(compressor.flush())

            return True

        except Exception as e:
            logger.error(f"Streaming compression failed: {e}")
            return False

    def decompress_streaming(
        self,
        input_stream: io.BytesIO,
        output_stream: io.BytesIO,
        algorithm: CompressionAlgorithm,
        chunk_size: int = 8192
    ) -> bool:
        """
        Decompress data using streaming approach.

        Args:
            input_stream: Compressed input stream.
            output_stream: Decompressed output stream.
            algorithm: Compression algorithm.
            chunk_size: Chunk size for streaming.

        Returns:
            True if successful.
        """
        if algorithm == CompressionAlgorithm.NONE:
            while True:
                chunk = input_stream.read(chunk_size)
                if not chunk:
                    break
                output_stream.write(chunk)
            return True

        if algorithm not in self.ALGORITHM_MAP:
            return False

        try:
            if algorithm == CompressionAlgorithm.ZLIB:
                decompressor = zlib.decompressobj()
            elif algorithm == CompressionAlgorithm.GZIP:
                decompressor = gzip.GzipFile(fileobj=io.BytesIO())
            else:
                return False

            while True:
                chunk = input_stream.read(chunk_size)
                if not chunk:
                    break

                if hasattr(decompressor, 'decompress'):
                    output_stream.write(decompressor.decompress(chunk))
                else:
                    output_stream.write(chunk)

            return True

        except Exception as e:
            logger.error(f"Streaming decompression failed: {e}")
            return False

    def detect_algorithm(self, data: bytes) -> CompressionAlgorithm:
        """
        Detect compression algorithm from data.

        Args:
            data: Data to analyze.

        Returns:
            Detected CompressionAlgorithm.
        """
        if len(data) < 2:
            return CompressionAlgorithm.NONE

        if data[:2] == b'\x1f\x8b':
            return CompressionAlgorithm.GZIP
        elif data[:2] == b'BZ':
            return CompressionAlgorithm.BZ2
        elif data[:2] in (b'[\xfd', b'YZ'):
            return CompressionAlgorithm.LZMA
        elif data[:2] == b'\x28\xb5\x2f':
            return CompressionAlgorithm.ZSTD

        try:
            zlib.decompress(data)
            return CompressionAlgorithm.ZLIB
        except Exception:
            pass

        return CompressionAlgorithm.NONE

    def _get_algorithm_funcs(
        self,
        algorithm: CompressionAlgorithm
    ) -> tuple:
        """Get compress/decompress functions for algorithm."""
        return self.ALGORITHM_MAP.get(algorithm, (None, None))

    def _get_compression_level(
        self,
        level: CompressionLevel,
        algorithm: CompressionAlgorithm
    ) -> int:
        """Get compression level value for algorithm."""
        if algorithm == CompressionAlgorithm.ZLIB:
            if level == CompressionLevel.BEST_SPEED:
                return 1
            elif level == CompressionLevel.BEST_COMPRESSION:
                return 9
            return 6

        elif algorithm in (CompressionAlgorithm.GZIP, CompressionAlgorithm.BZ2):
            if level == CompressionLevel.BEST_SPEED:
                return 1
            elif level == CompressionLevel.BEST_COMPRESSION:
                return 9
            return 6

        elif algorithm == CompressionAlgorithm.LZMA:
            if level == CompressionLevel.BEST_SPEED:
                return 0
            elif level == CompressionLevel.BEST_COMPRESSION:
                return 9
            return 6

        return 6

    def _create_compressor(self, algorithm: CompressionAlgorithm):
        """Create compressor object for streaming."""
        if algorithm == CompressionAlgorithm.ZLIB:
            return zlib.compressobj(level=6)
        elif algorithm == CompressionAlgorithm.GZIP:
            return gzip.GzipFile(fileobj=io.BytesIO(), mode='wb')
        return None

    def _compress_optional(
        self,
        data: bytes,
        algorithm: CompressionAlgorithm,
        level: CompressionLevel
    ) -> bytes:
        """Compress using optional libraries."""
        if algorithm == CompressionAlgorithm.LZ4:
            try:
                import lz4.frame
                return lz4.frame.compress(data)
            except ImportError:
                raise ImportError("lz4 required: pip install lz4")

        elif algorithm == CompressionAlgorithm.ZSTD:
            try:
                import zstandard
                return zstandard.ZstdCompressor().compress(data)
            except ImportError:
                raise ImportError("zstandard required: pip install zstandard")

        return data

    def _decompress_optional(
        self,
        data: bytes,
        algorithm: CompressionAlgorithm
    ) -> bytes:
        """Decompress using optional libraries."""
        if algorithm == CompressionAlgorithm.LZ4:
            try:
                import lz4.frame
                return lz4.frame.decompress(data)
            except ImportError:
                raise ImportError("lz4 required")

        elif algorithm == CompressionAlgorithm.ZSTD:
            try:
                import zstandard
                return zstandard.ZstdDecompressor().decompress(data)
            except ImportError:
                raise ImportError("zstandard required")

        return data

    def estimate_compressed_size(
        self,
        data_size: int,
        algorithm: CompressionAlgorithm
    ) -> int:
        """
        Estimate compressed size.

        Args:
            data_size: Original data size.
            algorithm: Compression algorithm.

        Returns:
            Estimated compressed size in bytes.
        """
        ratios = {
            CompressionAlgorithm.NONE: 1.0,
            CompressionAlgorithm.ZLIB: 0.4,
            CompressionAlgorithm.GZIP: 0.4,
            CompressionAlgorithm.BZ2: 0.35,
            CompressionAlgorithm.LZMA: 0.3,
            CompressionAlgorithm.LZ4: 0.45,
            CompressionAlgorithm.ZSTD: 0.35
        }

        ratio = ratios.get(algorithm, 0.4)
        return int(data_size * ratio)
