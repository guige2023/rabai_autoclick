"""Compression utilities for RabAI AutoClick.

Provides:
- gzip, zlib, bz2 compression helpers
- File compression/decompression
- Streaming compression
"""

import bz2
import gzip
import io
import lzma
import os
import shutil
import zlib
from typing import (
    BinaryIO,
    Optional,
    Union,
)


# ─────────────────────────────────────────────────────────────
# In-memory compression
# ─────────────────────────────────────────────────────────────

def gzip_compress(
    data: bytes,
    level: int = 6,
) -> bytes:
    """Compress data using gzip.

    Args:
        data: Data to compress.
        level: Compression level (0-9).

    Returns:
        Compressed bytes.
    """
    return gzip.compress(data, compresslevel=level)


def gzip_decompress(data: bytes) -> bytes:
    """Decompress gzip data.

    Args:
        data: Compressed data.

    Returns:
        Decompressed bytes.
    """
    return gzip.decompress(data)


def zlib_compress(
    data: bytes,
    level: int = 6,
) -> bytes:
    """Compress data using zlib.

    Args:
        data: Data to compress.
        level: Compression level (0-9).

    Returns:
        Compressed bytes.
    """
    return zlib.compress(data, level=level)


def zlib_decompress(data: bytes) -> bytes:
    """Decompress zlib data.

    Args:
        data: Compressed data.

    Returns:
        Decompressed bytes.
    """
    return zlib.decompress(data)


def bz2_compress(
    data: bytes,
    level: int = 9,
) -> bytes:
    """Compress data using bz2.

    Args:
        data: Data to compress.
        level: Compression level (1-9).

    Returns:
        Compressed bytes.
    """
    return bz2.compress(data, compresslevel=level)


def bz2_decompress(data: bytes) -> bytes:
    """Decompress bz2 data.

    Args:
        data: Compressed data.

    Returns:
        Decompressed bytes.
    """
    return bz2.decompress(data)


def lzma_compress(
    data: bytes,
    preset: int = 6,
) -> bytes:
    """Compress data using LZMA.

    Args:
        data: Data to compress.
        preset: Compression preset (0-9).

    Returns:
        Compressed bytes.
    """
    return lzma.compress(data, preset=preset)


def lzma_decompress(data: bytes) -> bytes:
    """Decompress LZMA data.

    Args:
        data: Compressed data.

    Returns:
        Decompressed bytes.
    """
    return lzma.decompress(data)


# ─────────────────────────────────────────────────────────────
# File compression
# ─────────────────────────────────────────────────────────────

def gzip_file(
    src_path: str,
    dst_path: Optional[str] = None,
    *,
    delete_src: bool = False,
) -> str:
    """Compress a file using gzip.

    Args:
        src_path: Source file path.
        dst_path: Destination path (default: src_path + '.gz').
        delete_src: If True, delete source file after compression.

    Returns:
        Path to compressed file.
    """
    if dst_path is None:
        dst_path = src_path + ".gz"

    with open(src_path, "rb") as f_in:
        with gzip.open(dst_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    if delete_src:
        os.remove(src_path)

    return dst_path


def gunzip_file(
    src_path: str,
    dst_path: Optional[str] = None,
    *,
    delete_src: bool = False,
) -> str:
    """Decompress a gzip file.

    Args:
        src_path: Source file path.
        dst_path: Destination path (default: strip .gz extension).
        delete_src: If True, delete source file after decompression.

    Returns:
        Path to decompressed file.
    """
    if dst_path is None:
        dst_path = src_path.rstrip(".gz")

    with gzip.open(src_path, "rb") as f_in:
        with open(dst_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    if delete_src:
        os.remove(src_path)

    return dst_path


def bz2_file(
    src_path: str,
    dst_path: Optional[str] = None,
    *,
    delete_src: bool = False,
) -> str:
    """Compress a file using bz2.

    Args:
        src_path: Source file path.
        dst_path: Destination path (default: src_path + '.bz2').
        delete_src: If True, delete source file after compression.

    Returns:
        Path to compressed file.
    """
    if dst_path is None:
        dst_path = src_path + ".bz2"

    with open(src_path, "rb") as f_in:
        with bz2.open(dst_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    if delete_src:
        os.remove(src_path)

    return dst_path


def bunzip2_file(
    src_path: str,
    dst_path: Optional[str] = None,
    *,
    delete_src: bool = False,
) -> str:
    """Decompress a bz2 file.

    Args:
        src_path: Source file path.
        dst_path: Destination path (default: strip .bz2 extension).
        delete_src: If True, delete source file after decompression.

    Returns:
        Path to decompressed file.
    """
    if dst_path is None:
        dst_path = src_path.rstrip(".bz2")

    with bz2.open(src_path, "rb") as f_in:
        with open(dst_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    if delete_src:
        os.remove(src_path)

    return dst_path


# ─────────────────────────────────────────────────────────────
# Streaming compression
# ─────────────────────────────────────────────────────────────

class GzipStream:
    """Streaming gzip compressor/decompressor."""

    def __init__(self, mode: str = "compress", level: int = 6) -> None:
        """Initialize gzip stream.

        Args:
            mode: "compress" or "decompress".
            level: Compression level (0-9).
        """
        self._mode = mode
        if mode == "compress":
            self._stream = gzip.GzipFile(fileobj=io.BytesIO(), mode="wb", compresslevel=level)
        else:
            self._stream = gzip.GzipFile(fileobj=io.BytesIO(), mode="rb")
        self._buffer = io.BytesIO()

    def write(self, data: bytes) -> bytes:
        """Write data to the stream.

        Args:
            data: Data to write.

        Returns:
            Compressed/decompressed output (if any).
        """
        if self._mode == "compress":
            self._stream.write(data)
        else:
            self._buffer.write(data)
            self._buffer.seek(0)
            try:
                self._stream = gzip.GzipFile(fileobj=self._buffer, mode="rb")
            except Exception:
                self._buffer = io.BytesIO()
                return b""
        return b""

    def flush(self) -> bytes:
        """Flush the stream and return final output."""
        if self._mode == "compress":
            self._stream.flush()
            result = self._stream.fileobj.getvalue()
            self._stream.close()
            return result
        return b""


class ZlibStream:
    """Streaming zlib compressor/decompressor."""

    def __init__(
        self,
        mode: str = "compress",
        level: int = 6,
    ) -> None:
        self._mode = mode
        if mode == "compress":
            self._compressor = zlib.compressobj(level)
        else:
            self._decompressor = zlib.decompressobj()

    def compress(self, data: bytes) -> bytes:
        """Compress data."""
        return self._compressor.compress(data)

    def flush(self) -> bytes:
        """Flush remaining compressed data."""
        return self._compressor.flush()

    def decompress(self, data: bytes) -> bytes:
        """Decompress data."""
        return self._decompressor.decompress(data)

    def flush_decompress(self) -> bytes:
        """Flush decompression."""
        return self._decompressor.flush()


def compressed_size(data: bytes) -> int:
    """Get compressed size of data.

    Args:
        data: Data to measure.

    Returns:
        Compressed size in bytes.
    """
    return len(zlib_compress(data))


def compression_ratio(data: bytes) -> float:
    """Calculate compression ratio.

    Args:
        data: Original data.

    Returns:
        Ratio of compressed size to original size.
    """
    compressed = len(zlib_compress(data))
    return compressed / len(data) if data else 0.0


def is_compressed(data: bytes) -> bool:
    """Check if data appears to be compressed.

    Args:
        data: Data to check.

    Returns:
        True if data appears compressed.
    """
    if len(data) < 2:
        return False

    # Check for gzip magic number
    if data[:2] == b"\x1f\x8b":
        return True

    # Check for zlib magic number
    if data[:2] in (b"\x78\x9c", b"\x78\x01", b"\x78\xda"):
        return True

    # Check for bz2 magic number
    if data[:3] == b"BZh":
        return True

    return False
