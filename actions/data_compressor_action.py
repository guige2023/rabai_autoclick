"""
Data compressor action for efficient data storage and transmission.

Provides compression with multiple algorithms and streaming support.
"""

from typing import Any, BinaryIO, Optional
import io
import json
import base64
import zlib
import gzip


class DataCompressorAction:
    """Data compression with multiple algorithms."""

    SUPPORTED_ALGORITHMS = ("gzip", "zlib", "deflate", "lz4", "snappy")

    def __init__(
        self,
        default_algorithm: str = "gzip",
        compression_level: int = 6,
    ) -> None:
        """
        Initialize data compressor.

        Args:
            default_algorithm: Default compression algorithm
            compression_level: Compression level (1-9)
        """
        self.default_algorithm = default_algorithm
        self.compression_level = compression_level

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute compression operation.

        Args:
            params: Dictionary containing:
                - operation: 'compress', 'decompress', 'info'
                - data: Data to compress/decompress
                - algorithm: Compression algorithm
                - is_base64: Input/output is base64 encoded

        Returns:
            Dictionary with compression result
        """
        operation = params.get("operation", "compress")

        if operation == "compress":
            return self._compress(params)
        elif operation == "decompress":
            return self._decompress(params)
        elif operation == "info":
            return self._get_info(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _compress(self, params: dict[str, Any]) -> dict[str, Any]:
        """Compress data."""
        data = params.get("data", "")
        algorithm = params.get("algorithm", self.default_algorithm)
        is_base64 = params.get("is_base64", False)

        if not data:
            return {"success": False, "error": "Data is required"}

        try:
            if isinstance(data, str):
                if is_base64:
                    data = base64.b64decode(data)
                else:
                    data = data.encode("utf-8")

            original_size = len(data)

            if algorithm == "gzip":
                compressed = self._compress_gzip(data)
            elif algorithm == "zlib":
                compressed = self._compress_zlib(data)
            elif algorithm == "deflate":
                compressed = self._compress_deflate(data)
            elif algorithm == "lz4":
                compressed = self._compress_lz4(data)
            elif algorithm == "snappy":
                compressed = self._compress_snappy(data)
            else:
                return {"success": False, "error": f"Unknown algorithm: {algorithm}"}

            if is_base64:
                compressed = base64.b64encode(compressed).decode("ascii")

            compression_ratio = original_size / len(compressed) if compressed else 0

            return {
                "success": True,
                "algorithm": algorithm,
                "original_size": original_size,
                "compressed_size": len(compressed),
                "compression_ratio": round(compression_ratio, 2),
                "data": compressed if is_base64 else compressed.decode("latin-1"),
            }
        except Exception as e:
            return {"success": False, "error": f"Compression failed: {str(e)}"}

    def _decompress(self, params: dict[str, Any]) -> dict[str, Any]:
        """Decompress data."""
        data = params.get("data", "")
        algorithm = params.get("algorithm", self.default_algorithm)
        is_base64 = params.get("is_base64", False)

        if not data:
            return {"success": False, "error": "Data is required"}

        try:
            if is_base64:
                data = base64.b64decode(data)
            elif isinstance(data, str):
                data = data.encode("latin-1")

            if algorithm == "gzip":
                decompressed = self._decompress_gzip(data)
            elif algorithm == "zlib":
                decompressed = self._decompress_zlib(data)
            elif algorithm == "deflate":
                decompressed = self._decompress_deflate(data)
            elif algorithm == "lz4":
                decompressed = self._decompress_lz4(data)
            elif algorithm == "snappy":
                decompressed = self._decompress_snappy(data)
            else:
                return {"success": False, "error": f"Unknown algorithm: {algorithm}"}

            try:
                decompressed = decompressed.decode("utf-8")
            except UnicodeDecodeError:
                pass

            return {
                "success": True,
                "algorithm": algorithm,
                "decompressed_size": len(decompressed),
                "data": decompressed,
            }
        except Exception as e:
            return {"success": False, "error": f"Decompression failed: {str(e)}"}

    def _compress_gzip(self, data: bytes) -> bytes:
        """Compress using gzip."""
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=self.compression_level) as f:
            f.write(data)
        return buffer.getvalue()

    def _decompress_gzip(self, data: bytes) -> bytes:
        """Decompress gzip data."""
        buffer = io.BytesIO(data)
        with gzip.GzipFile(fileobj=buffer, mode="rb") as f:
            return f.read()

    def _compress_zlib(self, data: bytes) -> bytes:
        """Compress using zlib."""
        return zlib.compress(data, level=self.compression_level)

    def _decompress_zlib(self, data: bytes) -> bytes:
        """Decompress zlib data."""
        return zlib.decompress(data)

    def _compress_deflate(self, data: bytes) -> bytes:
        """Compress using deflate (raw)."""
        return zlib.compress(data, level=self.compression_level)[2:-4]

    def _decompress_deflate(self, data: bytes) -> bytes:
        """Decompress deflate data."""
        return zlib.decompress(data, -zlib.MAX_WBITS)

    def _compress_lz4(self, data: bytes) -> bytes:
        """Compress using LZ4 (simulated)."""
        return data

    def _decompress_lz4(self, data: bytes) -> bytes:
        """Decompress LZ4 data (simulated)."""
        return data

    def _compress_snappy(self, data: bytes) -> bytes:
        """Compress using Snappy (simulated)."""
        return data

    def _decompress_snappy(self, data: bytes) -> bytes:
        """Decompress Snappy data (simulated)."""
        return data

    def _get_info(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get compression information."""
        return {
            "success": True,
            "default_algorithm": self.default_algorithm,
            "compression_level": self.compression_level,
            "supported_algorithms": self.SUPPORTED_ALGORITHMS,
        }
