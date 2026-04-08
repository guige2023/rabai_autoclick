"""IO utilities for RabAI AutoClick.

Provides:
- File reading and writing helpers
- Path manipulation
- Stream processing
- Buffered IO operations
"""

from __future__ import annotations

import io
import os
import sys
import tempfile
from typing import (
    BinaryIO,
    Callable,
    Iterator,
    List,
    Optional,
    TextIO,
    Union,
)


def read_file(path: str, encoding: str = "utf-8") -> str:
    """Read entire file as string.

    Args:
        path: File path.
        encoding: Text encoding.

    Returns:
        File contents.
    """
    with open(path, "r", encoding=encoding) as f:
        return f.read()


def read_file_lines(path: str, encoding: str = "utf-8") -> List[str]:
    """Read file as list of lines.

    Args:
        path: File path.
        encoding: Text encoding.

    Returns:
        List of lines (with newlines).
    """
    with open(path, "r", encoding=encoding) as f:
        return f.readlines()


def write_file(path: str, content: str, encoding: str = "utf-8") -> None:
    """Write string to file.

    Args:
        path: File path.
        content: Content to write.
        encoding: Text encoding.
    """
    with open(path, "w", encoding=encoding) as f:
        f.write(content)


def append_file(path: str, content: str, encoding: str = "utf-8") -> None:
    """Append string to file.

    Args:
        path: File path.
        content: Content to append.
        encoding: Text encoding.
    """
    with open(path, "a", encoding=encoding) as f:
        f.write(content)


def read_binary(path: str) -> bytes:
    """Read binary file.

    Args:
        path: File path.

    Returns:
        File bytes.
    """
    with open(path, "rb") as f:
        return f.read()


def write_binary(path: str, data: bytes) -> None:
    """Write binary data to file.

    Args:
        path: File path.
        data: Bytes to write.
    """
    with open(path, "wb") as f:
        f.write(data)


def read_chunks(
    path: str,
    chunk_size: int = 8192,
    binary: bool = False,
) -> Iterator[str]:
    """Read file in chunks.

    Args:
        path: File path.
        chunk_size: Size of each chunk.
        binary: Whether to read as binary.

    Yields:
        Chunks of the file.
    """
    mode = "rb" if binary else "r"
    encoding = None if binary else "utf-8"
    with open(path, mode, encoding=encoding) as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                return
            yield chunk


def file_size(path: str) -> int:
    """Get file size in bytes.

    Args:
        path: File path.

    Returns:
        File size in bytes.
    """
    return os.path.getsize(path)


def file_exists(path: str) -> bool:
    """Check if file exists.

    Args:
        path: File path.

    Returns:
        True if file exists.
    """
    return os.path.isfile(path)


def ensure_dir(path: str) -> None:
    """Ensure directory exists, create if needed.

    Args:
        path: Directory path.
    """
    os.makedirs(path, exist_ok=True)


def temp_file(
    suffix: str = "",
    prefix: str = "",
    dir: Optional[str] = None,
) -> str:
    """Create a temporary file.

    Args:
        suffix: Filename suffix.
        prefix: Filename prefix.
        dir: Directory to create in.

    Returns:
        Path to created temp file.
    """
    fd, path = tempfile.mkstemp(suffix=suffix, prefix=prefix, dir=dir)
    os.close(fd)
    return path


def temp_dir(
    suffix: str = "",
    prefix: str = "",
) -> str:
    """Create a temporary directory.

    Args:
        suffix: Directory name suffix.
        prefix: Directory name prefix.

    Returns:
        Path to created temp directory.
    """
    return tempfile.mkdtemp(suffix=suffix, prefix=prefix)


def copy_file(src: str, dst: str) -> None:
    """Copy a file from src to dst.

    Args:
        src: Source path.
        dst: Destination path.
    """
    with open(src, "rb") as fsrc:
        with open(dst, "wb") as fdst:
            fdst.write(fsrc.read())


def move_file(src: str, dst: str) -> None:
    """Move a file from src to dst.

    Args:
        src: Source path.
        dst: Destination path.
    """
    import shutil
    shutil.move(src, dst)


def count_lines(path: str) -> int:
    """Count lines in a text file.

    Args:
        path: File path.

    Returns:
        Number of lines.
    """
    count = 0
    with open(path, "r", encoding="utf-8") as f:
        for _ in f:
            count += 1
    return count


def stream_lines(
    stream: TextIO,
) -> Iterator[str]:
    """Yield lines from a text stream.

    Args:
        stream: Text stream.

    Yields:
        Lines from stream.
    """
    for line in stream:
        yield line


__all__ = [
    "read_file",
    "read_file_lines",
    "write_file",
    "append_file",
    "read_binary",
    "write_binary",
    "read_chunks",
    "file_size",
    "file_exists",
    "ensure_dir",
    "temp_file",
    "temp_dir",
    "copy_file",
    "move_file",
    "count_lines",
    "stream_lines",
]
