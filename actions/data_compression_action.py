"""
Data Compression Action Module

Provides data compression, decompression, and format optimization.
"""
from typing import Any, Optional, Callable, TypeVar
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import gzip
import zlib
import bz2
import lzma
import base64


T = TypeVar('T')


class CompressionAlgorithm(Enum):
    """Compression algorithms."""
    GZIP = "gzip"
    ZLIB = "zlib"
    BZ2 = "bz2"
    LZMA = "lzma"
    LZ4 = "lz4"
    ZSTD = "zstd"


class CompressionLevel(Enum):
    """Compression levels."""
    BEST_SPEED = 1
    BEST_COMPRESSION = 9
    DEFAULT = 6


@dataclass
class CompressionConfig:
    """Configuration for compression."""
    algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP
    level: CompressionLevel = CompressionLevel.DEFAULT
    calculate_checksum: bool = True
    preserve_original: bool = True
    min_size_bytes: int = 1024  # Only compress if larger than this


@dataclass
class CompressionResult:
    """Result of compression operation."""
    success: bool
    original_size: int
    compressed_size: int
    compression_ratio: float
    algorithm: CompressionAlgorithm
    duration_ms: float
    checksum: Optional[str] = None
    error: Optional[str] = None


@dataclass
class DecompressionResult:
    """Result of decompression operation."""
    success: bool
    original_size: int
    decompressed_size: int
    algorithm: CompressionAlgorithm
    duration_ms: float
    checksum: Optional[str] = None
    error: Optional[str] = None


class DataCompressionAction:
    """Main data compression action handler."""
    
    def __init__(self, default_config: Optional[CompressionConfig] = None):
        self.default_config = default_config or CompressionConfig()
        self._stats: dict[str, Any] = {}
    
    async def compress(
        self,
        data: bytes,
        config: Optional[CompressionConfig] = None
    ) -> CompressionResult:
        """
        Compress data using specified algorithm.
        
        Args:
            data: Bytes to compress
            config: Compression configuration
            
        Returns:
            CompressionResult with compressed data and stats
        """
        cfg = config or self.default_config
        start_time = datetime.now()
        
        original_size = len(data)
        
        # Skip compression if data too small
        if original_size < cfg.min_size_bytes:
            return CompressionResult(
                success=True,
                original_size=original_size,
                compressed_size=original_size,
                compression_ratio=1.0,
                algorithm=cfg.algorithm,
                duration_ms=0
            )
        
        try:
            if cfg.algorithm == CompressionAlgorithm.GZIP:
                compressed = gzip.compress(
                    data,
                    compresslevel=cfg.level.value
                )
            elif cfg.algorithm == CompressionAlgorithm.ZLIB:
                compressed = zlib.compress(
                    data,
                    level=cfg.level.value * 10  # zlib uses 0-9
                )
            elif cfg.algorithm == CompressionAlgorithm.BZ2:
                compressed = bz2.compress(data, compresslevel=cfg.level.value)
            elif cfg.algorithm == CompressionAlgorithm.LZMA:
                compressed = lzma.compress(
                    data,
                    preset=cfg.level.value
                )
            elif cfg.algorithm == CompressionAlgorithm.LZ4:
                compressed = await self._compress_lz4(data, cfg.level)
            elif cfg.algorithm == CompressionAlgorithm.ZSTD:
                compressed = await self._compress_zstd(data, cfg.level)
            else:
                raise ValueError(f"Unknown algorithm: {cfg.algorithm}")
            
            compressed_size = len(compressed)
            compression_ratio = compressed_size / original_size if original_size > 0 else 1.0
            
            # Calculate checksum if requested
            checksum = None
            if cfg.calculate_checksum:
                checksum = self._calculate_checksum(compressed)
            
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            self._stats["compressions"] = self._stats.get("compressions", 0) + 1
            self._stats["bytes_saved"] = self._stats.get("bytes_saved", 0) + (original_size - compressed_size)
            
            return CompressionResult(
                success=True,
                original_size=original_size,
                compressed_size=compressed_size,
                compression_ratio=compression_ratio,
                algorithm=cfg.algorithm,
                duration_ms=duration_ms,
                checksum=checksum
            )
            
        except Exception as e:
            return CompressionResult(
                success=False,
                original_size=original_size,
                compressed_size=0,
                compression_ratio=0,
                algorithm=cfg.algorithm,
                duration_ms=0,
                error=str(e)
            )
    
    async def decompress(
        self,
        data: bytes,
        algorithm: Optional[CompressionAlgorithm] = None,
        original_checksum: Optional[str] = None
    ) -> DecompressionResult:
        """
        Decompress data.
        
        Args:
            data: Compressed bytes
            algorithm: Algorithm used (auto-detected if None)
            original_checksum: Optional checksum to verify
            
        Returns:
            DecompressionResult with decompressed data and stats
        """
        algo = algorithm or CompressionAlgorithm.GZIP
        start_time = datetime.now()
        
        compressed_size = len(data)
        
        try:
            if algo == CompressionAlgorithm.GZIP:
                decompressed = gzip.decompress(data)
            elif algo == CompressionAlgorithm.ZLIB:
                decompressed = zlib.decompress(data)
            elif algo == CompressionAlgorithm.BZ2:
                decompressed = bz2.decompress(data)
            elif algo == CompressionAlgorithm.LZMA:
                decompressed = lzma.decompress(data)
            elif algo == CompressionAlgorithm.LZ4:
                decompressed = await self._decompress_lz4(data)
            elif algo == CompressionAlgorithm.ZSTD:
                decompressed = await self._decompress_zstd(data)
            else:
                raise ValueError(f"Unknown algorithm: {algo}")
            
            decompressed_size = len(decompressed)
            
            # Verify checksum if provided
            checksum = None
            if original_checksum:
                checksum = self._calculate_checksum(decompressed)
                if checksum != original_checksum:
                    return DecompressionResult(
                        success=False,
                        original_size=compressed_size,
                        decompressed_size=0,
                        algorithm=algo,
                        duration_ms=0,
                        checksum=checksum,
                        error="Checksum mismatch"
                    )
            
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            self._stats["decompressions"] = self._stats.get("decompressions", 0) + 1
            
            return DecompressionResult(
                success=True,
                original_size=compressed_size,
                decompressed_size=decompressed_size,
                algorithm=algo,
                duration_ms=duration_ms,
                checksum=checksum
            )
            
        except Exception as e:
            return DecompressionResult(
                success=False,
                original_size=compressed_size,
                decompressed_size=0,
                algorithm=algo,
                duration_ms=0,
                error=str(e)
            )
    
    async def compress_string(
        self,
        text: str,
        encoding: str = "utf-8",
        config: Optional[CompressionConfig] = None
    ) -> tuple[str, CompressionResult]:
        """Compress a string, returning base64-encoded compressed data."""
        data = text.encode(encoding)
        result = await self.compress(data, config)
        
        if result.success:
            compressed_b64 = base64.b64encode(
                gzip.compress(data, compresslevel=result.compression_ratio)
                if not config or config.algorithm == CompressionAlgorithm.GZIP
                else data
            ).decode(encoding)
            return compressed_b64, result
        
        return "", result
    
    async def decompress_string(
        self,
        compressed_b64: str,
        encoding: str = "utf-8"
    ) -> tuple[str, DecompressionResult]:
        """Decompress a base64-encoded compressed string."""
        try:
            data = base64.b64decode(compressed_b64)
            result = await self.decompress(data)
            
            if result.success:
                text = gzip.decompress(data).decode(encoding)
                return text, result
            
            return "", result
            
        except Exception as e:
            return "", DecompressionResult(
                success=False,
                original_size=len(compressed_b64),
                decompressed_size=0,
                algorithm=CompressionAlgorithm.GZIP,
                duration_ms=0,
                error=str(e)
            )
    
    async def compress_json(
        self,
        obj: Any,
        config: Optional[CompressionConfig] = None
    ) -> tuple[str, CompressionResult]:
        """Compress a JSON-serializable object."""
        import json
        text = json.dumps(obj)
        return await self.compress_string(text, config=config)
    
    async def decompress_json(
        self,
        compressed: str
    ) -> tuple[Any, DecompressionResult]:
        """Decompress to a JSON object."""
        import json
        text, result = await self.decompress_string(compressed)
        
        if result.success:
            try:
                obj = json.loads(text)
                return obj, result
            except json.JSONDecodeError as e:
                return None, DecompressionResult(
                    success=False,
                    original_size=result.original_size,
                    decompressed_size=0,
                    algorithm=result.algorithm,
                    duration_ms=0,
                    error=f"JSON decode error: {e}"
                )
        
        return None, result
    
    async def _compress_lz4(self, data: bytes, level: CompressionLevel) -> bytes:
        """Compress using LZ4."""
        try:
            import lz4.frame
            return lz4.frame.compress(data, compression_level=level.value)
        except ImportError:
            # Fallback to gzip if lz4 not available
            return gzip.compress(data, compresslevel=level.value)
    
    async def _decompress_lz4(self, data: bytes) -> bytes:
        """Decompress LZ4 data."""
        try:
            import lz4.frame
            return lz4.frame.decompress(data)
        except ImportError:
            return gzip.decompress(data)
    
    async def _compress_zstd(self, data: bytes, level: CompressionLevel) -> bytes:
        """Compress using Zstandard."""
        try:
            import zstandard as zstd
            cctx = zstd.ZstdCompressor(level=level.value)
            return cctx.compress(data)
        except ImportError:
            return gzip.compress(data, compresslevel=level.value)
    
    async def _decompress_zstd(self, data: bytes) -> bytes:
        """Decompress Zstandard data."""
        try:
            import zstandard as zstd
            dctx = zstd.ZstdDecompressor()
            return dctx.decompress(data)
        except ImportError:
            return gzip.decompress(data)
    
    def _calculate_checksum(self, data: bytes) -> str:
        """Calculate checksum for data integrity."""
        import hashlib
        return hashlib.sha256(data).hexdigest()[:16]
    
    async def detect_algorithm(self, data: bytes) -> CompressionAlgorithm:
        """Attempt to detect compression algorithm from data."""
        # Check magic bytes
        if data[:2] == b'\x1f\x8b':  # GZIP
            return CompressionAlgorithm.GZIP
        if data[:2] == b'BZ':  # BZ2
            return CompressionAlgorithm.BZ2
        if data[:2] == b'[\xfd':  # LZMA
            return CompressionAlgorithm.LZMA
        if data[:4] == b'\x28\xb5\x2f\xfd':  # Zstandard
            return CompressionAlgorithm.ZSTD
        
        # Try gzip as default
        return CompressionAlgorithm.GZIP
    
    def get_stats(self) -> dict[str, Any]:
        """Get compression statistics."""
        total = self._stats.get("compressions", 0)
        bytes_saved = self._stats.get("bytes_saved", 0)
        
        return {
            **self._stats,
            "compression_ratio_avg": (
                1 - (bytes_saved / (total * 1024))
                if total > 0 and bytes_saved > 0
                else 1.0
            )
        }
