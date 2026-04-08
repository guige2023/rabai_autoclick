"""API Compression Action.

Compresses API request/response bodies for transport efficiency.
"""
from typing import Any, Callable, Dict, Optional
from dataclasses import dataclass
import gzip
import zlib


class APICompressorAction:
    """Compresses and decompresses API payloads."""

    SUPPORTED = {"gzip", "zlib", "deflate"}

    def __init__(self, default_algorithm: str = "gzip", level: int = 6) -> None:
        if default_algorithm not in self.SUPPORTED:
            raise ValueError(f"Unsupported algorithm: {default_algorithm}")
        self.default_algorithm = default_algorithm
        self.level = level
        self._stats = {"compressed_bytes": 0, "decompressed_bytes": 0, "count": 0}

    def compress(
        self,
        data: bytes,
        algorithm: Optional[str] = None,
    ) -> tuple[bytes, int]:
        algo = algorithm or self.default_algorithm
        if algo == "gzip":
            compressed = gzip.compress(data, compresslevel=self.level)
        elif algo == "zlib":
            compressed = zlib.compress(data, level=self.level)
        elif algo == "deflate":
            compressed = zlib.compress(data, level=self.level)
        else:
            raise ValueError(f"Unknown algorithm: {algo}")
        self._stats["compressed_bytes"] += len(compressed)
        self._stats["decompressed_bytes"] += len(data)
        self._stats["count"] += 1
        return compressed, len(compressed)

    def decompress(
        self,
        data: bytes,
        algorithm: Optional[str] = None,
        original_size: Optional[int] = None,
    ) -> bytes:
        algo = algorithm or self.default_algorithm
        if algo == "gzip":
            return gzip.decompress(data)
        elif algo in ("zlib", "deflate"):
            return zlib.decompress(data)
        raise ValueError(f"Unknown algorithm: {algo}")

    def get_stats(self) -> Dict[str, Any]:
        ratio = 0.0
        if self._stats["decompressed_bytes"] > 0:
            ratio = 1.0 - (self._stats["compressed_bytes"] / self._stats["decompressed_bytes"])
        return {
            "compressed_bytes": self._stats["compressed_bytes"],
            "decompressed_bytes": self._stats["decompressed_bytes"],
            "compression_ratio": ratio,
            "count": self._stats["count"],
        }
