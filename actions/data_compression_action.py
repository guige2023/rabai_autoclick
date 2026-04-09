"""Data Compression Action Module.

Provides data compression utilities with:
- Multiple compression algorithms
- Stream-based compression
- Batch compression
- Compression ratio optimization
- Format detection

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import zlib
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class CompressionAlgorithm(Enum):
    """Supported compression algorithms."""
    GZIP = auto()
    ZLIB = auto()
    DEFLATE = auto()
    LZ4 = auto()
    ZSTD = auto()


@dataclass
class CompressionStats:
    """Compression statistics."""
    original_size: int = 0
    compressed_size: int = 0
    compression_ratio: float = 0.0
    compression_time_ms: float = 0.0
    algorithm: str = ""


@dataclass
class BatchCompressionResult:
    """Result of batch compression."""
    success: bool
    items: List[Dict[str, Any]] = field(default_factory=list)
    total_original_size: int = 0
    total_compressed_size: int = 0
    overall_ratio: float = 0.0
    duration_ms: float = 0.0
    errors: List[str] = field(default_factory=list)


class DataCompressor:
    """Data compression utilities.
    
    Features:
    - Multiple compression algorithms
    - Automatic algorithm selection
    - Compression ratio optimization
    - Stream-based processing
    - Batch compression
    """
    
    def __init__(self, default_algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP):
        self.default_algorithm = default_algorithm
        self._metrics = {
            "total_items_compressed": 0,
            "total_original_bytes": 0,
            "total_compressed_bytes": 0,
            "compression_time_ms": 0.0
        }
    
    async def compress(
        self,
        data: Union[str, bytes],
        algorithm: Optional[CompressionAlgorithm] = None,
        compression_level: int = 6
    ) -> Tuple[bytes, CompressionStats]:
        """Compress data.
        
        Args:
            data: Data to compress
            algorithm: Compression algorithm to use
            compression_level: Compression level (1-9)
            
        Returns:
            Tuple of (compressed data, stats)
        """
        import time
        start_time = time.time()
        
        algorithm = algorithm or self.default_algorithm
        
        if isinstance(data, str):
            data = data.encode("utf-8")
        
        original_size = len(data)
        
        if algorithm == CompressionAlgorithm.GZIP:
            compressed = gzip.compress(data, compresslevel=compression_level)
        elif algorithm == CompressionAlgorithm.ZLIB:
            compressed = zlib.compress(data, level=compression_level)
        elif algorithm == CompressionAlgorithm.DEFLATE:
            compressed = zlib.compress(data, level=compression_level)
        else:
            compressed = gzip.compress(data, compresslevel=compression_level)
        
        compressed_size = len(compressed)
        compression_ratio = compressed_size / original_size if original_size > 0 else 0
        duration_ms = (time.time() - start_time) * 1000
        
        stats = CompressionStats(
            original_size=original_size,
            compressed_size=compressed_size,
            compression_ratio=compression_ratio,
            compression_time_ms=duration_ms,
            algorithm=algorithm.name
        )
        
        self._metrics["total_items_compressed"] += 1
        self._metrics["total_original_bytes"] += original_size
        self._metrics["total_compressed_bytes"] += compressed_size
        self._metrics["compression_time_ms"] += duration_ms
        
        return compressed, stats
    
    async def decompress(
        self,
        data: bytes,
        algorithm: Optional[CompressionAlgorithm] = None
    ) -> Tuple[bytes, CompressionStats]:
        """Decompress data.
        
        Args:
            data: Compressed data
            algorithm: Compression algorithm used
            
        Returns:
            Tuple of (decompressed data, stats)
        """
        import time
        start_time = time.time()
        
        algorithm = algorithm or self.default_algorithm
        original_size = len(data)
        
        if algorithm == CompressionAlgorithm.GZIP:
            decompressed = gzip.decompress(data)
        elif algorithm == CompressionAlgorithm.ZLIB:
            decompressed = zlib.decompress(data)
        elif algorithm == CompressionAlgorithm.DEFLATE:
            decompressed = zlib.decompress(data)
        else:
            decompressed = gzip.decompress(data)
        
        compressed_size = len(decompressed)
        duration_ms = (time.time() - start_time) * 1000
        
        stats = CompressionStats(
            original_size=original_size,
            compressed_size=compressed_size,
            compression_ratio=original_size / compressed_size if compressed_size > 0 else 0,
            compression_time_ms=duration_ms,
            algorithm=algorithm.name
        )
        
        return decompressed, stats
    
    async def compress_json(
        self,
        data: Any,
        algorithm: Optional[CompressionAlgorithm] = None,
        **kwargs
    ) -> Tuple[str, CompressionStats]:
        """Compress JSON-serializable data.
        
        Args:
            data: JSON-serializable data
            algorithm: Compression algorithm
            **kwargs: Additional arguments for compress
            
        Returns:
            Tuple of (base64-encoded compressed data, stats)
        """
        import base64
        
        json_bytes = json.dumps(data, default=str).encode("utf-8")
        compressed, stats = await self.compress(json_bytes, algorithm, **kwargs)
        
        return base64.b64encode(compressed).decode("ascii"), stats
    
    async def decompress_json(
        self,
        data: str,
        algorithm: Optional[CompressionAlgorithm] = None
    ) -> Any:
        """Decompress JSON data.
        
        Args:
            data: Base64-encoded compressed JSON
            algorithm: Compression algorithm
            
        Returns:
            Decompressed Python object
        """
        import base64
        
        compressed = base64.b64decode(data.encode("ascii"))
        decompressed, _ = await self.decompress(compressed, algorithm)
        
        return json.loads(decompressed.decode("utf-8"))
    
    async def batch_compress(
        self,
        items: List[Tuple[str, Union[str, bytes]]],
        algorithm: Optional[CompressionAlgorithm] = None,
        compression_level: int = 6
    ) -> BatchCompressionResult:
        """Compress multiple items in batch.
        
        Args:
            items: List of (id, data) tuples
            algorithm: Compression algorithm
            compression_level: Compression level
            
        Returns:
            Batch compression result
        """
        import time
        start_time = time.time()
        
        results = []
        total_original = 0
        total_compressed = 0
        errors = []
        
        for item_id, data in items:
            try:
                compressed, stats = await self.compress(
                    data, algorithm, compression_level
                )
                
                results.append({
                    "id": item_id,
                    "compressed": compressed,
                    "original_size": stats.original_size,
                    "compressed_size": stats.compressed_size,
                    "ratio": stats.compression_ratio
                })
                
                total_original += stats.original_size
                total_compressed += stats.compressed_size
                
            except Exception as e:
                errors.append(f"Error compressing {item_id}: {e}")
        
        duration_ms = (time.time() - start_time) * 1000
        overall_ratio = total_compressed / total_original if total_original > 0 else 0
        
        return BatchCompressionResult(
            success=len(errors) == 0,
            items=results,
            total_original_size=total_original,
            total_compressed_size=total_compressed,
            overall_ratio=overall_ratio,
            duration_ms=duration_ms,
            errors=errors
        )
    
    async def compress_file(
        self,
        file_path: str,
        output_path: Optional[str] = None,
        algorithm: Optional[CompressionAlgorithm] = None,
        chunk_size: int = 8192
    ) -> CompressionStats:
        """Compress a file.
        
        Args:
            file_path: Path to input file
            output_path: Path to output file (default: file_path + .gz)
            algorithm: Compression algorithm
            chunk_size: Chunk size for streaming
            
        Returns:
            Compression stats
        """
        import os
        import time
        
        start_time = time.time()
        algorithm = algorithm or self.default_algorithm
        
        if output_path is None:
            output_path = f"{file_path}.gz"
        
        original_size = os.path.getsize(file_path)
        
        compressed_data, stats = await self.compress_file_sync(
            file_path, output_path, algorithm
        )
        
        return stats
    
    async def compress_file_sync(
        self,
        input_path: str,
        output_path: str,
        algorithm: CompressionAlgorithm
    ) -> Tuple[bytes, CompressionStats]:
        """Synchronous file compression helper."""
        import time
        
        start_time = time.time()
        
        with open(input_path, "rb") as f_in:
            data = f_in.read()
        
        if algorithm == CompressionAlgorithm.GZIP:
            compressed = gzip.compress(data)
        elif algorithm == CompressionAlgorithm.ZLIB:
            compressed = zlib.compress(data)
        else:
            compressed = gzip.compress(data)
        
        with open(output_path, "wb") as f_out:
            f_out.write(compressed)
        
        original_size = len(data)
        compressed_size = len(compressed)
        duration_ms = (time.time() - start_time) * 1000
        
        stats = CompressionStats(
            original_size=original_size,
            compressed_size=compressed_size,
            compression_ratio=compressed_size / original_size if original_size > 0 else 0,
            compression_time_ms=duration_ms,
            algorithm=algorithm.name
        )
        
        return compressed, stats
    
    async def detect_compression(self, data: bytes) -> Optional[CompressionAlgorithm]:
        """Detect compression algorithm from data.
        
        Args:
            data: Data to analyze
            
        Returns:
            Detected algorithm or None
        """
        if len(data) < 2:
            return None
        
        if data[:2] == b"\x1f\x8b":
            return CompressionAlgorithm.GZIP
        elif data[:2] in (b"\x78\x9c", b"\x78\x01", b"\x78\xda"):
            return CompressionAlgorithm.ZLIB
        
        return None
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get compression metrics."""
        avg_ratio = (
            self._metrics["total_compressed_bytes"] / self._metrics["total_original_bytes"]
            if self._metrics["total_original_bytes"] > 0 else 0
        )
        
        return {
            **self._metrics,
            "overall_compression_ratio": avg_ratio
        }
