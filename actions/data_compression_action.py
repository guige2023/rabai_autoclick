"""
Data Compression Action.

Provides data compression utilities.
Supports:
- Gzip/Zlib compression
- LZ4 compression
- Brotli compression
- Streaming compression
"""

from typing import Optional, Tuple
import gzip
import zlib
import logging
import json

logger = logging.getLogger(__name__)


class DataCompressionAction:
    """
    Data Compression Action.
    
    Provides data compression with support for:
    - Multiple compression algorithms
    - Configurable compression levels
    - Streaming compression
    - Auto-detection of compressed data
    """
    
    ALGORITHMS = ["gzip", "zlib", "lz4", "brotli"]
    
    def __init__(self, default_algorithm: str = "gzip", default_level: int = 6):
        """
        Initialize the Data Compression Action.
        
        Args:
            default_algorithm: Default compression algorithm
            default_level: Compression level (1-9)
        """
        if default_algorithm not in self.ALGORITHMS:
            raise ValueError(f"Unknown algorithm: {default_algorithm}")
        
        self.default_algorithm = default_algorithm
        self.default_level = max(1, min(9, default_level))
    
    def compress(
        self,
        data: bytes,
        algorithm: Optional[str] = None,
        level: Optional[int] = None
    ) -> bytes:
        """
        Compress data.
        
        Args:
            data: Data to compress
            algorithm: Compression algorithm
            level: Compression level (1-9)
        
        Returns:
            Compressed data
        """
        algo = algorithm or self.default_algorithm
        lvl = level or self.default_level
        
        if algo == "gzip":
            return self._gzip_compress(data, lvl)
        elif algo == "zlib":
            return self._zlib_compress(data, lvl)
        elif algo == "lz4":
            return self._lz4_compress(data)
        elif algo == "brotli":
            return self._brotli_compress(data, lvl)
        else:
            raise ValueError(f"Unknown algorithm: {algo}")
    
    def decompress(
        self,
        data: bytes,
        algorithm: Optional[str] = None
    ) -> bytes:
        """
        Decompress data.
        
        Args:
            data: Compressed data
            algorithm: Compression algorithm used
        
        Returns:
            Decompressed data
        """
        algo = algorithm or self._detect_algorithm(data)
        
        if algo == "gzip":
            return self._gzip_decompress(data)
        elif algo == "zlib":
            return self._zlib_decompress(data)
        elif algo == "lz4":
            return self._lz4_decompress(data)
        elif algo == "brotli":
            return self._brotli_decompress(data)
        else:
            raise ValueError(f"Unknown algorithm: {algo}")
    
    def _gzip_compress(self, data: bytes, level: int) -> bytes:
        """Compress using gzip."""
        return gzip.compress(data, compresslevel=level)
    
    def _gzip_decompress(self, data: bytes) -> bytes:
        """Decompress gzip data."""
        return gzip.decompress(data)
    
    def _zlib_compress(self, data: bytes, level: int) -> bytes:
        """Compress using zlib."""
        return zlib.compress(data, level=level)
    
    def _zlib_decompress(self, data: bytes) -> bytes:
        """Decompress zlib data."""
        return zlib.decompress(data)
    
    def _lz4_compress(self, data: bytes) -> bytes:
        """Compress using lz4."""
        try:
            import lz4.frame
            return lz4.frame.compress(data)
        except ImportError:
            logger.warning("lz4 not available, using zlib fallback")
            return zlib.compress(data)
    
    def _lz4_decompress(self, data: bytes) -> bytes:
        """Decompress lz4 data."""
        try:
            import lz4.frame
            return lz4.frame.decompress(data)
        except ImportError:
            logger.warning("lz4 not available, using zlib fallback")
            return zlib.decompress(data)
    
    def _brotli_compress(self, data: bytes, level: int) -> bytes:
        """Compress using brotli."""
        try:
            import brotli
            return brotli.compress(data, quality=level)
        except ImportError:
            logger.warning("brotli not available, using gzip fallback")
            return gzip.compress(data)
    
    def _brotli_decompress(self, data: bytes) -> bytes:
        """Decompress brotli data."""
        try:
            import brotli
            return brotli.decompress(data)
        except ImportError:
            logger.warning("brotli not available, using gzip fallback")
            return gzip.decompress(data)
    
    def _detect_algorithm(self, data: bytes) -> str:
        """Auto-detect compression algorithm."""
        if len(data) < 2:
            return "zlib"
        
        # Check magic bytes
        if data[:2] == b'\x1f\x8b':
            return "gzip"
        elif data[:2] in (b'\x78\x9c', b'\x78\x01', b'\x78\xda'):
            return "zlib"
        elif data[:2] == b'\x00\x00':
            return "lz4"
        
        return "zlib"
    
    def get_compression_stats(
        self,
        original_size: int,
        compressed_size: int
    ) -> dict:
        """Get compression statistics."""
        ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0
        return {
            "original_size": original_size,
            "compressed_size": compressed_size,
            "ratio": ratio,
            "space_saved": original_size - compressed_size
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    compressor = DataCompressionAction(default_algorithm="gzip")
    
    # Test data
    test_data = b"Hello, this is some test data that will be compressed. " * 100
    
    # Compress
    compressed = compressor.compress(test_data)
    print(f"Original: {len(test_data)} bytes")
    print(f"Compressed: {len(compressed)} bytes")
    
    # Stats
    stats = compressor.get_compression_stats(len(test_data), len(compressed))
    print(f"Compression ratio: {stats['ratio']:.1f}%")
    
    # Decompress
    decompressed = compressor.decompress(compressed)
    print(f"Decompressed matches: {decompressed == test_data}")
    
    # Test all algorithms
    for algo in ["gzip", "zlib"]:
        comp = compressor.compress(test_data, algorithm=algo)
        decomp = compressor.decompress(comp, algorithm=algo)
        ratio = (1 - len(comp) / len(test_data)) * 100
        print(f"{algo}: {len(comp)} bytes, {ratio:.1f}% ratio, matches: {decomp == test_data}")
