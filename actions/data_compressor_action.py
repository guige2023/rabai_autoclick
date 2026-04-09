"""
Data Compression Module.

Provides data compression and decompression with multiple algorithms,
streaming support, and automatic codec detection.
"""

from __future__ import annotations

import asyncio
import gzip
import io
import zlib
from dataclasses import dataclass
from enum import Enum
from typing import Any, AsyncIterator, Callable, Optional, Union
import logging

logger = logging.getLogger(__name__)


class CompressionAlgorithm(Enum):
    """Supported compression algorithms."""
    GZIP = "gzip"
    DEFLATE = "deflate"
    ZLIB = "zlib"
    LZ4 = "lz4"
    ZSTD = "zstd"
    BROTLI = "brotli"
    IDENTITY = "identity"  # No compression


class CompressionMode(Enum):
    """Compression mode."""
    AUTO = "auto"
    COMPRESS = "compress"
    DECOMPRESS = "decompress"


@dataclass
class CompressionStats:
    """Compression statistics."""
    original_size: int = 0
    compressed_size: int = 0
    compression_ratio: float = 0.0
    processing_time: float = 0.0
    algorithm: str = "identity"
    
    @property
    def savings_percent(self) -> float:
        """Calculate space savings percentage."""
        if self.original_size == 0:
            return 0.0
        return (1 - self.compressed_size / self.original_size) * 100


@dataclass
class CompressionConfig:
    """Configuration for compression operations."""
    algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP
    level: int = 6  # 0-9 for gzip/deflate
    streaming: bool = True
    chunk_size: int = 8192
    include_checksum: bool = True
    wbits: int = 15  # Window size for zlib


class DataCompressor:
    """
    Data compression with multiple algorithm support.
    
    Example:
        compressor = DataCompressor(CompressionConfig(
            algorithm=CompressionAlgorithm.GZIP,
            level=6
        ))
        
        # Compress data
        compressed = await compressor.compress(data)
        
        # Decompress
        original = await compressor.decompress(compressed)
        
        # Streaming compress
        async for chunk in compressor.compress_stream(data_iterator):
            output.write(chunk)
    """
    
    # Algorithm-specific compression levels
    LEVEL_RANGES = {
        CompressionAlgorithm.GZIP: (0, 9),
        CompressionAlgorithm.DEFLATE: (0, 9),
        CompressionAlgorithm.ZLIB: (0, 9),
        CompressionAlgorithm.ZSTD: (-7, 22),
        CompressionAlgorithm.LZ4: (0, 12),
        CompressionAlgorithm.BROTLI: (0, 11),
    }
    
    def __init__(self, config: Optional[CompressionConfig] = None) -> None:
        """
        Initialize the compressor.
        
        Args:
            config: Compression configuration.
        """
        self.config = config or CompressionConfig()
        self._validate_config()
        
    def _validate_config(self) -> None:
        """Validate configuration parameters."""
        algo = self.config.algorithm
        level = self.config.level
        
        if algo in self.LEVEL_RANGES:
            min_level, max_level = self.LEVEL_RANGES[algo]
            if not min_level <= level <= max_level:
                raise ValueError(
                    f"Invalid level {level} for {algo.value}. "
                    f"Must be between {min_level} and {max_level}."
                )
                
    async def compress(self, data: bytes) -> bytes:
        """
        Compress data in memory.
        
        Args:
            data: Input data to compress.
            
        Returns:
            Compressed data bytes.
        """
        import time
        start_time = time.time()
        
        if self.config.algorithm == CompressionAlgorithm.GZIP:
            result = self._compress_gzip(data)
        elif self.config.algorithm == CompressionAlgorithm.DEFLATE:
            result = self._compress_deflate(data)
        elif self.config.algorithm == CompressionAlgorithm.ZLIB:
            result = self._compress_zlib(data)
        elif self.config.algorithm == CompressionAlgorithm.IDENTITY:
            result = data
        else:
            # Fallback to zlib for unsupported
            result = self._compress_zlib(data)
            
        processing_time = time.time() - start_time
        
        logger.debug(
            f"Compressed {len(data)} bytes -> {len(result)} bytes "
            f"({CompressionStats(original_size=len(data), compressed_size=len(result), processing_time=processing_time, algorithm=self.config.algorithm.value).savings_percent:.1f}% savings)"
        )
        
        return result
        
    async def decompress(self, data: bytes, config: Optional[CompressionConfig] = None) -> bytes:
        """
        Decompress data in memory.
        
        Args:
            data: Compressed data.
            config: Optional decompression config (for auto-detection).
            
        Returns:
            Decompressed data bytes.
        """
        config = config or self.config
        
        if config.algorithm == CompressionAlgorithm.AUTO:
            detected = self._detect_compression(data)
            config.algorithm = detected
            
        if config.algorithm == CompressionAlgorithm.GZIP:
            return self._decompress_gzip(data)
        elif config.algorithm == CompressionAlgorithm.DEFLATE:
            return self._decompress_deflate(data)
        elif config.algorithm == CompressionAlgorithm.ZLIB:
            return self._decompress_zlib(data)
        elif config.algorithm == CompressionAlgorithm.IDENTITY:
            return data
        else:
            return self._decompress_zlib(data)
            
    async def compress_stream(
        self,
        input_iterator: AsyncIterator[bytes],
    ) -> AsyncIterator[bytes]:
        """
        Compress data as a stream.
        
        Args:
            input_iterator: Async iterator of data chunks.
            
        Yields:
            Compressed data chunks.
        """
        if self.config.algorithm == CompressionAlgorithm.GZIP:
            yield from self._compress_gzip_stream(input_iterator)
        elif self.config.algorithm == CompressionAlgorithm.ZLIB:
            yield from self._compress_zlib_stream(input_iterator)
        else:
            # Fall back to in-memory for unsupported streaming
            data = b"".join([chunk async for chunk in input_iterator])
            compressed = await self.compress(data)
            yield compressed
            
    async def decompress_stream(
        self,
        input_iterator: AsyncIterator[bytes],
        config: Optional[CompressionConfig] = None,
    ) -> AsyncIterator[bytes]:
        """
        Decompress data as a stream.
        
        Args:
            input_iterator: Async iterator of compressed chunks.
            config: Optional decompression config.
            
        Yields:
            Decompressed data chunks.
        """
        config = config or self.config
        
        if config.algorithm == CompressionAlgorithm.GZIP:
            yield from self._decompress_gzip_stream(input_iterator)
        elif config.algorithm == CompressionAlgorithm.ZLIB:
            yield from self._decompress_zlib_stream(input_iterator)
        else:
            data = b"".join([chunk async for chunk in input_iterator])
            decompressed = await self.decompress(data, config)
            yield decompressed
            
    def _compress_gzip(self, data: bytes) -> bytes:
        """Compress with GZIP."""
        compressor = gzip.GzipFile(
            fileobj=io.BytesIO(),
            mode="wb",
            compresslevel=self.config.level
        )
        compressor.write(data)
        compressor.close()
        return compressor.fileobj.getvalue()
        
    def _decompress_gzip(self, data: bytes) -> bytes:
        """Decompress GZIP data."""
        decompressor = gzip.GzipFile(fileobj=io.BytesIO(data))
        return decompressor.read()
        
    def _compress_deflate(self, data: bytes) -> bytes:
        """Compress with raw deflate."""
        return zlib.compress(data, level=self.config.level)
        
    def _decompress_deflate(self, data: bytes) -> bytes:
        """Decompress deflate data."""
        return zlib.decompress(data)
        
    def _compress_zlib(self, data: bytes) -> bytes:
        """Compress with zlib (includes header)."""
        return zlib.compress(data, level=self.config.level)
        
    def _decompress_zlib(self, data: bytes) -> bytes:
        """Decompress zlib data."""
        return zlib.decompress(data)
        
    def _detect_compression(self, data: bytes) -> CompressionAlgorithm:
        """
        Auto-detect compression algorithm from data.
        
        Args:
            data: Compressed data.
            
        Returns:
            Detected algorithm.
        """
        if len(data) < 2:
            return CompressionAlgorithm.IDENTITY
            
        # Check for gzip magic number
        if data[:2] == b"\x1f\x8b":
            return CompressionAlgorithm.GZIP
            
        # Check for zlib header
        if data[0] == 0x78:
            return CompressionAlgorithm.ZLIB
            
        return CompressionAlgorithm.DEFLATE
        
    async def _compress_gzip_stream(
        self,
        input_iterator: AsyncIterator[bytes],
    ) -> AsyncIterator[bytes]:
        """Stream compress with GZIP."""
        compressor = gzip.GzipFile(
            fileobj=io.BytesIO(),
            mode="wb",
            compresslevel=self.config.level
        )
        
        async for chunk in input_iterator:
            compressor.write(chunk)
            # Yield compressed data if buffer has enough
            current_pos = compressor.fileobj.tell()
            if current_pos > self.config.chunk_size:
                compressed_data = compressor.fileobj.getvalue()
                # Calculate new data since last yield
                if len(compressed_data) > self.config.chunk_size:
                    yield compressed_data
                    compressor.fileobj = io.BytesIO()
                    
        compressor.close()
        remaining = compressor.fileobj.getvalue()
        if remaining:
            yield remaining
            
    async def _decompress_gzip_stream(
        self,
        input_iterator: AsyncIterator[bytes],
    ) -> AsyncIterator[bytes]:
        """Stream decompress GZIP data."""
        buffer = io.BytesIO()
        decompressor = gzip.GzipFile(fileobj=buffer, mode="rb")
        
        async for chunk in input_iterator:
            buffer.write(chunk)
            buffer.seek(0)
            try:
                while True:
                    data = decompressor.read(self.config.chunk_size)
                    if not data:
                        break
                    yield data
                # Reset buffer
                remaining = buffer.read()
                buffer = io.BytesIO(remaining)
                decompressor = gzip.GzipFile(fileobj=buffer, mode="rb")
            except EOFError:
                remaining = buffer.read()
                buffer = io.BytesIO(remaining)
                decompressor = gzip.GzipFile(fileobj=buffer, mode="rb")
                
    async def _compress_zlib_stream(
        self,
        input_iterator: AsyncIterator[bytes],
    ) -> AsyncIterator[bytes]:
        """Stream compress with zlib."""
        compressor = zlib.compressobj(level=self.config.level)
        
        async for chunk in input_iterator:
            yield compressor.compress(chunk)
            
        yield compressor.flush()
        
    async def _decompress_zlib_stream(
        self,
        input_iterator: AsyncIterator[bytes],
    ) -> AsyncIterator[bytes]:
        """Stream decompress zlib data."""
        decompressor = zlib.decompressobj(wbits=self.config.wbits)
        
        async for chunk in input_iterator:
            yield decompressor.decompress(chunk)
            
        yield decompressor.flush()
        
    def get_stats(self, original_size: int, compressed_size: int, time_taken: float) -> CompressionStats:
        """Get compression statistics."""
        return CompressionStats(
            original_size=original_size,
            compressed_size=compressed_size,
            compression_ratio=compressed_size / original_size if original_size > 0 else 0,
            processing_time=time_taken,
            algorithm=self.config.algorithm.value,
        )
        
    def set_level(self, level: int) -> None:
        """Set compression level."""
        self.config.level = level
        self._validate_config()


class CompressedFile:
    """
    Context manager for compressed file operations.
    
    Example:
        async with CompressedFile("data.gz", "rb", algorithm=CompressionAlgorithm.GZIP) as f:
            async for chunk in f.read_stream():
                process(chunk)
    """
    
    def __init__(
        self,
        path: str,
        mode: str,
        algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP,
        level: int = 6,
    ) -> None:
        """
        Initialize compressed file handler.
        
        Args:
            path: File path.
            mode: File mode (rb, wb, etc.).
            algorithm: Compression algorithm.
            level: Compression level.
        """
        self.path = path
        self.mode = mode
        self.algorithm = algorithm
        self.level = level
        self._file = None
        self._compressor: Optional[DataCompressor] = None
        
    async def __aenter__(self) -> "CompressedFile":
        """Enter context."""
        import aiofiles
        
        self._file = await aiofiles.open(self.path, self.mode)
        self._compressor = DataCompressor(CompressionConfig(
            algorithm=self.algorithm,
            level=self.level,
        ))
        return self
        
    async def __aexit__(self, *args: Any) -> None:
        """Exit context."""
        if self._file:
            await self._file.close()
            
    async def read(self) -> bytes:
        """Read entire compressed file."""
        if not self._file:
            raise RuntimeError("File not opened")
        data = await self._file.read()
        return await self._compressor.decompress(data)
        
    async def write(self, data: bytes) -> None:
        """Write and compress data to file."""
        if not self._file or not self._compressor:
            raise RuntimeError("File not opened")
        compressed = await self._compressor.compress(data)
        await self._file.write(compressed)
        
    async def read_stream(self) -> AsyncIterator[bytes]:
        """Read file as compressed stream."""
        if not self._file:
            raise RuntimeError("File not opened")
        while True:
            chunk = await self._file.read(self._compressor.config.chunk_size)
            if not chunk:
                break
            yield chunk
