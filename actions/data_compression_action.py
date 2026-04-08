# Copyright (c) 2024. coded by claude
"""Data Compression Action Module.

Provides compression and decompression utilities for API data
with support for multiple algorithms.
"""
from typing import Optional, Dict, Any, Tuple
from dataclasses import dataclass
from enum import Enum
import zlib
import gzip
import logging

logger = logging.getLogger(__name__)


class CompressionAlgorithm(Enum):
    NONE = "none"
    DEFLATE = "deflate"
    GZIP = "gzip"
    ZLIB = "zlib"


@dataclass
class CompressionResult:
    success: bool
    original_size: int
    compressed_size: int
    algorithm: CompressionAlgorithm
    error: Optional[str] = None

    @property
    def compression_ratio(self) -> float:
        if self.original_size == 0:
            return 0.0
        return 1.0 - (self.compressed_size / self.original_size)


class DataCompressor:
    def __init__(self, algorithm: CompressionAlgorithm = CompressionAlgorithm.DEFLATE):
        self.algorithm = algorithm

    def compress(self, data: bytes) -> CompressionResult:
        try:
            original_size = len(data)
            if self.algorithm == CompressionAlgorithm.NONE:
                compressed = data
            elif self.algorithm == CompressionAlgorithm.DEFLATE:
                compressed = zlib.compress(data, level=6)
            elif self.algorithm == CompressionAlgorithm.GZIP:
                compressed = gzip.compress(data)
            elif self.algorithm == CompressionAlgorithm.ZLIB:
                compressed = zlib.compress(data)
            else:
                raise ValueError(f"Unsupported algorithm: {self.algorithm}")
            return CompressionResult(
                success=True,
                original_size=original_size,
                compressed_size=len(compressed),
                algorithm=self.algorithm,
            )
        except Exception as e:
            return CompressionResult(
                success=False,
                original_size=len(data),
                compressed_size=0,
                algorithm=self.algorithm,
                error=str(e),
            )

    def decompress(self, data: bytes) -> Tuple[bool, Optional[bytes], Optional[str]]:
        try:
            if self.algorithm == CompressionAlgorithm.NONE:
                return True, data, None
            elif self.algorithm == CompressionAlgorithm.DEFLATE:
                decompressed = zlib.decompress(data)
            elif self.algorithm == CompressionAlgorithm.GZIP:
                decompressed = gzip.decompress(data)
            elif self.algorithm == CompressionAlgorithm.ZLIB:
                decompressed = zlib.decompress(data)
            else:
                raise ValueError(f"Unsupported algorithm: {self.algorithm}")
            return True, decompressed, None
        except Exception as e:
            return False, None, str(e)
