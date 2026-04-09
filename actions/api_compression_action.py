"""
API Compression Action Module

Handles gzip, deflate, and brotli compression for API requests/responses.
Content negotiation, streaming compression, and memory-efficient buffering.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import gzip
import io
import logging
import zlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class CompressionType(Enum):
    """Supported compression algorithms."""
    
    NONE = "none"
    GZIP = "gzip"
    DEFLATE = "deflate"
    BRØTLI = "brøtli"


class CompressionLevel(Enum):
    """Compression speed vs ratio tradeoffs."""
    
    BEST_SPEED = "best_speed"
    BEST_RATIO = "best_ratio"
    DEFAULT = "default"


@dataclass
class CompressionConfig:
    """Configuration for compression behavior."""
    
    enabled: bool = True
    min_size_bytes: int = 1024
    compression_type: CompressionType = CompressionType.GZIP
    compression_level: CompressionLevel = CompressionLevel.DEFAULT
    streaming_threshold: int = 65536
    include_size_header: bool = True
    include_ratio_header: bool = True


@dataclass
class CompressionStats:
    """Statistics for compression operations."""
    
    total_requests: int = 0
    compressed_requests: int = 0
    total_original_bytes: int = 0
    total_compressed_bytes: int = 0
    compression_ratio: float = 0.0
    
    def record(self, original_size: int, compressed_size: int) -> None:
        """Record a compression operation."""
        self.total_requests += 1
        if compressed_size < original_size:
            self.compressed_requests += 1
        self.total_original_bytes += original_size
        self.total_compressed_bytes += compressed_size
        if self.total_original_bytes > 0:
            self.compression_ratio = (
                self.total_original_bytes - self.total_compressed_bytes
            ) / self.total_original_bytes


class CompressionCodec:
    """Handles actual compression/decompression."""
    
    def __init__(self, config: CompressionConfig):
        self.config = config
    
    def _get_level(self) -> int:
        """Map compression level enum to actual level."""
        mapping = {
            CompressionLevel.BEST_SPEED: 1,
            CompressionLevel.DEFAULT: 6,
            CompressionLevel.BEST_RATIO: 9,
        }
        return mapping.get(self.config.compression_level, 6)
    
    def compress(self, data: Union[bytes, str]) -> bytes:
        """Compress data using configured algorithm."""
        if isinstance(data, str):
            data = data.encode("utf-8")
        
        if len(data) < self.config.min_size_bytes:
            return data
        
        if self.config.compression_type == CompressionType.GZIP:
            return self._gzip_compress(data)
        elif self.config.compression_type == CompressionType.DEFLATE:
            return self._deflate_compress(data)
        elif self.config.compression_type == CompressionType.BRØTLI:
            return self._brotli_compress(data)
        return data
    
    def _gzip_compress(self, data: bytes) -> bytes:
        """Perform gzip compression."""
        buffer = io.BytesIO()
        with gzip.GzipFile(
            fileobj=buffer,
            mode="wb",
            compresslevel=self._get_level()
        ) as f:
            f.write(data)
        return buffer.getvalue()
    
    def _deflate_compress(self, data: bytes) -> bytes:
        """Perform deflate compression."""
        return zlib.compress(data, level=self._get_level())
    
    def _brotli_compress(self, data: bytes) -> bytes:
        """Perform brotli compression."""
        try:
            import brotli
            return brotli.compress(data, quality=self._get_level())
        except ImportError:
            logger.warning("brotli not available, falling back to gzip")
            return self._gzip_compress(data)
    
    def decompress(self, data: bytes, encoding: str) -> bytes:
        """Decompress data based on encoding header."""
        if not data:
            return data
        
        encoding_lower = encoding.lower()
        
        if "gzip" in encoding_lower:
            return self._gzip_decompress(data)
        elif "deflate" in encoding_lower:
            return self._deflate_decompress(data)
        elif "br" in encoding_lower or "brotli" in encoding_lower:
            return self._brotli_decompress(data)
        return data
    
    def _gzip_decompress(self, data: bytes) -> bytes:
        """Decompress gzip data."""
        try:
            return gzip.decompress(data)
        except Exception as e:
            logger.error(f"Gzip decompression failed: {e}")
            return data
    
    def _deflate_decompress(self, data: bytes) -> bytes:
        """Decompress deflate data."""
        try:
            return zlib.decompress(data)
        except Exception:
            try:
                return zlib.decompress(data, -zlib.MAX_WBITS)
            except Exception as e:
                logger.error(f"Deflate decompression failed: {e}")
                return data
    
    def _brotli_decompress(self, data: bytes) -> bytes:
        """Decompress brotli data."""
        try:
            import brotli
            return brotli.decompress(data)
        except ImportError:
            logger.error("brotli not available for decompression")
            return data


class APICompressionAction:
    """
    Main compression action handler.
    
    Integrates with API requests/responses to automatically
    compress and decompress payloads based on client capabilities.
    """
    
    def __init__(self, config: Optional[CompressionConfig] = None):
        self.config = config or CompressionConfig()
        self.codec = CompressionCodec(self.config)
        self.stats = CompressionStats()
        self._middleware: List[Callable] = []
    
    def add_middleware(self, func: Callable) -> None:
        """Add compression middleware."""
        self._middleware.append(func)
    
    async def process_request(self, data: Any, headers: Dict) -> Dict[str, Any]:
        """Process outgoing request with compression."""
        if not self.config.enabled:
            return {"data": data, "headers": headers}
        
        for mw in self._middleware:
            result = mw({"data": data, "headers": headers})
            if asyncio.iscoroutine(result):
                result = await result
        
        return {"data": data, "headers": headers}
    
    async def process_response(
        self,
        data: Union[bytes, str],
        headers: Dict,
        accept_encoding: Optional[str] = None
    ) -> Dict[str, Any]:
        """Process incoming response with compression."""
        if not self.config.enabled:
            return {"data": data, "headers": headers}
        
        if accept_encoding is None:
            accept_encoding = headers.get("Accept-Encoding", "gzip")
        
        data_bytes = data if isinstance(data, bytes) else data.encode("utf-8")
        compressed = self.codec.compress(data_bytes)
        
        self.stats.record(len(data_bytes), len(compressed))
        
        encoding = self.config.compression_type.value
        headers["Content-Encoding"] = encoding
        
        if self.config.include_size_header:
            headers["X-Original-Size"] = str(len(data_bytes))
            headers["X-Compressed-Size"] = str(len(compressed))
        
        if self.config.include_ratio_header:
            ratio = (1 - len(compressed) / max(len(data_bytes), 1)) * 100
            headers["X-Compression-Ratio"] = f"{ratio:.1f}%"
        
        return {
            "data": compressed,
            "headers": headers,
            "compressed": len(compressed) < len(data_bytes)
        }
    
    def decompress_request(
        self,
        data: bytes,
        content_encoding: str
    ) -> bytes:
        """Decompress incoming request body."""
        return self.codec.decompress(data, content_encoding)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get compression statistics."""
        return {
            "total_requests": self.stats.total_requests,
            "compressed_requests": self.stats.compressed_requests,
            "total_original_bytes": self.stats.total_original_bytes,
            "total_compressed_bytes": self.stats.total_compressed_bytes,
            "compression_ratio": f"{self.stats.compression_ratio * 100:.1f}%",
            "space_saved_bytes": (
                self.stats.total_original_bytes - self.stats.total_compressed_bytes
            ),
        }
    
    def reset_stats(self) -> None:
        """Reset compression statistics."""
        self.stats = CompressionStats()
    
    async def stream_compress(
        self,
        generator: Callable[[], AsyncIterator[bytes]],
        chunk_size: int = 8192
    ) -> AsyncIterator[bytes]:
        """Stream-compress data from an async generator."""
        if self.config.compression_type == CompressionType.GZIP:
            buffer = io.BytesIO()
            with gzip.GzipFile(fileobj=buffer, mode="wb") as f:
                async for chunk in generator():
                    if chunk:
                        f.write(chunk)
                        if buffer.tell() >= self.config.streaming_threshold:
                            yield buffer.getvalue()
                            buffer = io.BytesIO()
            remaining = buffer.getvalue()
            if remaining:
                yield remaining
        else:
            async for chunk in generator():
                yield self.codec.compress(chunk)


from typing import AsyncIterator
