"""
Data Compression Action - Compresses and decompresses data.

This module provides compression capabilities for
data storage and transmission optimization.
"""

from __future__ import annotations

import gzip
import zlib
from dataclasses import dataclass
from enum import Enum


class CompressionAlgorithm(Enum):
    """Compression algorithms."""
    GZIP = "gzip"
    ZLIB = "zlib"
    NONE = "none"


@dataclass
class CompressionResult:
    """Result of compression."""
    success: bool
    original_size: int
    compressed_size: int
    ratio: float


class DataCompressor:
    """Compresses and decompresses data."""
    
    def __init__(self) -> None:
        pass
    
    def compress_gzip(self, data: bytes) -> bytes:
        """Compress data using gzip."""
        return gzip.compress(data)
    
    def decompress_gzip(self, data: bytes) -> bytes:
        """Decompress gzip data."""
        return gzip.decompress(data)
    
    def compress_zlib(self, data: bytes) -> bytes:
        """Compress data using zlib."""
        return zlib.compress(data)
    
    def decompress_zlib(self, data: bytes) -> bytes:
        """Decompress zlib data."""
        return zlib.decompress(data)
    
    def compress(self, data: bytes, algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP) -> tuple[bytes, int]:
        """Compress data with specified algorithm."""
        original_size = len(data)
        
        if algorithm == CompressionAlgorithm.GZIP:
            compressed = self.compress_gzip(data)
        elif algorithm == CompressionAlgorithm.ZLIB:
            compressed = self.compress_zlib(data)
        else:
            compressed = data
        
        return compressed, original_size


class DataCompressionAction:
    """Data compression action for automation workflows."""
    
    def __init__(self, algorithm: str = "gzip") -> None:
        self.algorithm = CompressionAlgorithm(algorithm)
        self.compressor = DataCompressor()
    
    def compress(self, data: bytes) -> CompressionResult:
        """Compress data."""
        compressed, original_size = self.compressor.compress(data, self.algorithm)
        compressed_size = len(compressed)
        ratio = compressed_size / original_size if original_size > 0 else 1.0
        
        return CompressionResult(
            success=True,
            original_size=original_size,
            compressed_size=compressed_size,
            ratio=ratio,
        )
    
    def decompress(self, data: bytes) -> bytes:
        """Decompress data."""
        if self.algorithm == CompressionAlgorithm.GZIP:
            return self.compressor.decompress_gzip(data)
        elif self.algorithm == CompressionAlgorithm.ZLIB:
            return self.compressor.decompress_zlib(data)
        return data


__all__ = ["CompressionAlgorithm", "CompressionResult", "DataCompressor", "DataCompressionAction"]
