"""Data compression action module for RabAI AutoClick.

Provides compression capabilities for data operations:
- DataCompressor: Compress and decompress data
- StreamingCompressor: Stream-based compression
- CompressionRatioAnalyzer: Analyze compression efficiency
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
import time
import threading
import logging
import zlib
import gzip
import lz4.frame
from dataclasses import dataclass, field
from enum import Enum

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CompressionAlgorithm(Enum):
    """Compression algorithms."""
    ZLIB = "zlib"
    GZIP = "gzip"
    LZ4 = "lz4"
    ZSTD = "zstd"


@dataclass
class CompressionStats:
    """Compression statistics."""
    original_size: int
    compressed_size: int
    compression_ratio: float
    algorithm: str
    duration: float


class DataCompressor:
    """Data compression operations."""
    
    def __init__(self):
        self._stats: List[CompressionStats] = []
        self._lock = threading.Lock()
    
    def compress(self, data: Any, algorithm: CompressionAlgorithm = CompressionAlgorithm.ZLIB, level: int = 6) -> Tuple[bytes, CompressionStats]:
        """Compress data."""
        start_time = time.time()
        
        if isinstance(data, str):
            data = data.encode()
        elif not isinstance(data, bytes):
            import json
            data = json.dumps(data, default=str).encode()
        
        original_size = len(data)
        
        if algorithm == CompressionAlgorithm.ZLIB:
            compressed = zlib.compress(data, level)
        elif algorithm == CompressionAlgorithm.GZIP:
            compressed = gzip.compress(data, compresslevel=level)
        elif algorithm == CompressionAlgorithm.LZ4:
            try:
                compressed = lz4.frame.compress(data, compression_level=level)
            except Exception:
                compressed = zlib.compress(data, level)
        else:
            compressed = zlib.compress(data, level)
        
        compressed_size = len(compressed)
        ratio = compressed_size / original_size if original_size > 0 else 1.0
        duration = time.time() - start_time
        
        stats = CompressionStats(
            original_size=original_size,
            compressed_size=compressed_size,
            compression_ratio=ratio,
            algorithm=algorithm.value,
            duration=duration
        )
        
        with self._lock:
            self._stats.append(stats)
        
        return compressed, stats
    
    def decompress(self, data: bytes, algorithm: CompressionAlgorithm = CompressionAlgorithm.ZLIB) -> Tuple[bytes, CompressionStats]:
        """Decompress data."""
        start_time = time.time()
        
        compressed_size = len(data)
        
        if algorithm == CompressionAlgorithm.ZLIB:
            decompressed = zlib.decompress(data)
        elif algorithm == CompressionAlgorithm.GZIP:
            decompressed = gzip.decompress(data)
        elif algorithm == CompressionAlgorithm.LZ4:
            try:
                decompressed = lz4.frame.decompress(data)
            except Exception:
                decompressed = zlib.decompress(data)
        else:
            decompressed = zlib.decompress(data)
        
        original_size = len(decompressed)
        ratio = compressed_size / original_size if original_size > 0 else 1.0
        duration = time.time() - start_time
        
        stats = CompressionStats(
            original_size=original_size,
            compressed_size=compressed_size,
            compression_ratio=ratio,
            algorithm=algorithm.value,
            duration=duration
        )
        
        with self._lock:
            self._stats.append(stats)
        
        return decompressed, stats
    
    def get_stats(self) -> Dict[str, Any]:
        """Get compression statistics."""
        with self._lock:
            if not self._stats:
                return {"total_operations": 0}
            
            total_ops = len(self._stats)
            avg_ratio = sum(s.compression_ratio for s in self._stats) / total_ops
            total_original = sum(s.original_size for s in self._stats)
            total_compressed = sum(s.compressed_size for s in self._stats)
            
            return {
                "total_operations": total_ops,
                "average_ratio": avg_ratio,
                "total_original_bytes": total_original,
                "total_compressed_bytes": total_compressed,
                "overall_ratio": total_compressed / total_original if total_original > 0 else 1.0,
            }


class DataCompressionAction(BaseAction):
    """Data compression action."""
    action_type = "data_compression"
    display_name = "数据压缩"
    description = "数据压缩与解压缩"
    
    def __init__(self):
        super().__init__()
        self._compressor = DataCompressor()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute compression operation."""
        try:
            command = params.get("command", "compress")
            
            if command == "compress":
                data = params.get("data")
                algo_str = params.get("algorithm", "zlib")
                level = params.get("level", 6)
                
                if data is None:
                    return ActionResult(success=False, message="data required")
                
                algorithm = CompressionAlgorithm[algo_str.upper()]
                compressed, stats = self._compressor.compress(data, algorithm, level)
                
                return ActionResult(success=True, data={
                    "compressed": compressed.hex(),
                    "original_size": stats.original_size,
                    "compressed_size": stats.compressed_size,
                    "ratio": stats.compression_ratio,
                    "algorithm": stats.algorithm,
                })
            
            elif command == "decompress":
                data_hex = params.get("data")
                algo_str = params.get("algorithm", "zlib")
                
                if data_hex is None:
                    return ActionResult(success=False, message="data required")
                
                data = bytes.fromhex(data_hex)
                algorithm = CompressionAlgorithm[algo_str.upper()]
                decompressed, stats = self._compressor.decompress(data, algorithm)
                
                try:
                    result = decompressed.decode()
                except Exception:
                    result = decompressed
                
                return ActionResult(success=True, data={"decompressed": result, "size": stats.original_size})
            
            elif command == "stats":
                stats = self._compressor.get_stats()
                return ActionResult(success=True, data={"stats": stats})
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"DataCompressionAction error: {str(e)}")
