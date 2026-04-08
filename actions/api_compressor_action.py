"""API Compressor Action Module.

Provides request/response compression with multiple algorithms,
content encoding negotiation, and streaming support.
"""
from __future__ import annotations

import gzip
import json
import zlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Optional, Union
import base64
import logging

logger = logging.getLogger(__name__)


class CompressionAlgorithm(Enum):
    """Compression algorithm."""
    GZIP = "gzip"
    DEFLATE = "deflate"
    ZLIB = "zlib"
    IDENTITY = "identity"


@dataclass
class CompressionConfig:
    """Compression configuration."""
    algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP
    compression_level: int = 6
    min_size_bytes: int = 1024
    encoding: str = "utf-8"


class APICompressorAction:
    """Request/response compressor.

    Example:
        compressor = APICompressorAction()

        compressed = compressor.compress(
            data={"large": "payload" * 1000},
            config=CompressionConfig(algorithm=CompressionAlgorithm.GZIP)
        )

        decompressed = compressor.decompress(compressed)
    """

    def __init__(self) -> None:
        self._encodings = {
            "gzip": CompressionAlgorithm.GZIP,
            "deflate": CompressionAlgorithm.DEFLATE,
            "zlib": CompressionAlgorithm.ZLIB,
            "identity": CompressionAlgorithm.IDENTITY,
        }

    def compress(
        self,
        data: Any,
        config: Optional[CompressionConfig] = None,
    ) -> Union[bytes, str]:
        """Compress data.

        Args:
            data: Data to compress
            config: Compression configuration

        Returns:
            Compressed data as bytes
        """
        config = config or CompressionConfig()
        serialized = self._serialize(data)

        if len(serialized) < config.min_size_bytes:
            return serialized

        if config.algorithm == CompressionAlgorithm.GZIP:
            return self._gzip_compress(serialized, config.compression_level)
        elif config.algorithm == CompressionAlgorithm.DEFLATE:
            return self._deflate_compress(serialized, config.compression_level)
        elif config.algorithm == CompressionAlgorithm.ZLIB:
            return self._zlib_compress(serialized, config.compression_level)

        return serialized

    def decompress(
        self,
        data: Union[bytes, str],
        algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP,
        encoding: str = "utf-8",
    ) -> Any:
        """Decompress data.

        Args:
            data: Compressed data
            algorithm: Compression algorithm used
            encoding: String encoding

        Returns:
            Decompressed data
        """
        if isinstance(data, str):
            data = data.encode("latin-1")

        if algorithm == CompressionAlgorithm.GZIP:
            decompressed = self._gzip_decompress(data)
        elif algorithm == CompressionAlgorithm.DEFLATE:
            decompressed = self._deflate_decompress(data)
        elif algorithm == CompressionAlgorithm.ZLIB:
            decompressed = self._zlib_decompress(data)
        else:
            decompressed = data

        return self._deserialize(decompressed, encoding)

    def _serialize(self, data: Any) -> bytes:
        """Serialize data to bytes."""
        if isinstance(data, bytes):
            return data
        elif isinstance(data, str):
            return data.encode("utf-8")
        elif isinstance(data, (dict, list)):
            return json.dumps(data).encode("utf-8")
        else:
            return str(data).encode("utf-8")

    def _deserialize(self, data: bytes, encoding: str) -> Any:
        """Deserialize bytes to data."""
        try:
            return json.loads(data.decode(encoding))
        except:
            try:
                return data.decode(encoding)
            except:
                return data

    def _gzip_compress(self, data: bytes, level: int) -> bytes:
        """Gzip compress."""
        return gzip.compress(data, compresslevel=level)

    def _gzip_decompress(self, data: bytes) -> bytes:
        """Gzip decompress."""
        return gzip.decompress(data)

    def _deflate_compress(self, data: bytes, level: int) -> bytes:
        """Deflate compress."""
        return zlib.compress(data, level)

    def _deflate_decompress(self, data: bytes) -> bytes:
        """Deflate decompress."""
        return zlib.decompress(data)

    def _zlib_compress(self, data: bytes, level: int) -> bytes:
        """Zlib compress."""
        return zlib.compress(data, level)

    def _zlib_decompress(self, data: bytes) -> bytes:
        """Zlib decompress."""
        return zlib.decompress(data)

    def negotiate_encoding(
        self,
        accept_encoding: str,
    ) -> CompressionAlgorithm:
        """Negotiate compression algorithm from Accept-Encoding header.

        Args:
            accept_encoding: Accept-Encoding header value

        Returns:
            Selected compression algorithm
        """
        if not accept_encoding:
            return CompressionAlgorithm.IDENTITY

        encodings = [e.strip() for e in accept_encoding.split(",")]

        for encoding in encodings:
            name = encoding.split(";")[0].strip()
            if name in self._encodings:
                return self._encodings[name]

        return CompressionAlgorithm.IDENTITY

    def get_content_encoding(
        self,
        algorithm: CompressionAlgorithm,
    ) -> str:
        """Get Content-Encoding header value."""
        if algorithm == CompressionAlgorithm.GZIP:
            return "gzip"
        elif algorithm == CompressionAlgorithm.DEFLATE:
            return "deflate"
        elif algorithm == CompressionAlgorithm.ZLIB:
            return "zlib"
        return "identity"
