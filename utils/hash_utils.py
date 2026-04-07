"""Hash utilities for RabAI AutoClick.

Provides:
- Common hash functions (MD5, SHA1, SHA256, etc.)
- File hashing utilities
- Incremental hashing
- Hash-based IDs and checksums
"""

import hashlib
import io
import os
from typing import (
    BinaryIO,
    Callable,
    Optional,
    Union,
)


def md5(data: Union[bytes, str], hexdigest: bool = True) -> Union[bytes, str]:
    """Compute MD5 hash.

    Args:
        data: Data to hash.
        hexdigest: If True, return hex string; else bytes.

    Returns:
        Hash as hex string or bytes.
    """
    if isinstance(data, str):
        data = data.encode()
    result = hashlib.md5(data)
    return result.hexdigest() if hexdigest else result.digest()


def sha1(data: Union[bytes, str], hexdigest: bool = True) -> Union[bytes, str]:
    """Compute SHA1 hash.

    Args:
        data: Data to hash.
        hexdigest: If True, return hex string; else bytes.

    Returns:
        Hash as hex string or bytes.
    """
    if isinstance(data, str):
        data = data.encode()
    result = hashlib.sha1(data)
    return result.hexdigest() if hexdigest else result.digest()


def sha256(data: Union[bytes, str], hexdigest: bool = True) -> Union[bytes, str]:
    """Compute SHA256 hash.

    Args:
        data: Data to hash.
        hexdigest: If True, return hex string; else bytes.

    Returns:
        Hash as hex string or bytes.
    """
    if isinstance(data, str):
        data = data.encode()
    result = hashlib.sha256(data)
    return result.hexdigest() if hexdigest else result.digest()


def sha512(data: Union[bytes, str], hexdigest: bool = True) -> Union[bytes, str]:
    """Compute SHA512 hash.

    Args:
        data: Data to hash.
        hexdigest: If True, return hex string; else bytes.

    Returns:
        Hash as hex string or bytes.
    """
    if isinstance(data, str):
        data = data.encode()
    result = hashlib.sha512(data)
    return result.hexdigest() if hexdigest else result.digest()


def sha3_256(data: Union[bytes, str], hexdigest: bool = True) -> Union[bytes, str]:
    """Compute SHA3-256 hash.

    Args:
        data: Data to hash.
        hexdigest: If True, return hex string; else bytes.

    Returns:
        Hash as hex string or bytes.
    """
    if isinstance(data, str):
        data = data.encode()
    result = hashlib.sha3_256(data)
    return result.hexdigest() if hexdigest else result.digest()


def sha3_512(data: Union[bytes, str], hexdigest: bool = True) -> Union[bytes, str]:
    """Compute SHA3-512 hash.

    Args:
        data: Data to hash.
        hexdigest: If True, return hex string; else bytes.

    Returns:
        Hash as hex string or bytes.
    """
    if isinstance(data, str):
        data = data.encode()
    result = hashlib.sha3_512(data)
    return result.hexdigest() if hexdigest else result.digest()


def blake2b(data: Union[bytes, str], hexdigest: bool = True) -> Union[bytes, str]:
    """Compute BLAKE2b hash.

    Args:
        data: Data to hash.
        hexdigest: If True, return hex string; else bytes.

    Returns:
        Hash as hex string or bytes.
    """
    if isinstance(data, str):
        data = data.encode()
    result = hashlib.blake2b(data)
    return result.hexdigest() if hexdigest else result.digest()


def blake2s(data: Union[bytes, str], hexdigest: bool = True) -> Union[bytes, str]:
    """Compute BLAKE2s hash.

    Args:
        data: Data to hash.
        hexdigest: If True, return hex string; else bytes.

    Returns:
        Hash as hex string or bytes.
    """
    if isinstance(data, str):
        data = data.encode()
    result = hashlib.blake2s(data)
    return result.hexdigest() if hexdigest else result.digest()


def file_hash(
    filepath: str,
    algorithm: str = "sha256",
    chunk_size: int = 8192,
) -> str:
    """Compute hash of a file.

    Args:
        filepath: Path to the file.
        algorithm: Hash algorithm name.
        chunk_size: Read chunk size in bytes.

    Returns:
        Hex digest of the file hash.
    """
    hasher = hashlib.new(algorithm)
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def file_hash_stream(
    stream: BinaryIO,
    algorithm: str = "sha256",
    chunk_size: int = 8192,
) -> str:
    """Compute hash of a stream.

    Args:
        stream: File-like object to hash.
        algorithm: Hash algorithm name.
        chunk_size: Read chunk size in bytes.

    Returns:
        Hex digest of the stream hash.
    """
    hasher = hashlib.new(algorithm)
    while True:
        chunk = stream.read(chunk_size)
        if not chunk:
            break
        hasher.update(chunk)
    return hasher.hexdigest()


def hash_bytes(
    data: bytes,
    algorithm: str = "sha256",
    hexdigest: bool = True,
) -> Union[bytes, str]:
    """Compute hash of bytes with a specific algorithm.

    Args:
        data: Data to hash.
        algorithm: Hash algorithm name.
        hexdigest: If True, return hex string; else bytes.

    Returns:
        Hash as hex string or bytes.
    """
    hasher = hashlib.new(algorithm)
    hasher.update(data)
    return hasher.hexdigest() if hexdigest else hasher.digest()


class IncrementalHasher:
    """Incremental hasher for streaming data."""

    def __init__(
        self,
        algorithm: str = "sha256",
    ) -> None:
        """Initialize incremental hasher.

        Args:
            algorithm: Hash algorithm name.
        """
        self._hasher = hashlib.new(algorithm)

    def update(self, data: bytes) -> None:
        """Update hash with more data.

        Args:
            data: Additional data to hash.
        """
        self._hasher.update(data)

    def digest(self) -> bytes:
        """Get raw hash digest."""
        return self._hasher.digest()

    def hexdigest(self) -> str:
        """Get hex string hash digest."""
        return self._hasher.hexdigest()

    def copy(self) -> "IncrementalHasher":
        """Create a copy of the current hasher state."""
        new_hasher = IncrementalHasher.__new__(IncrementalHasher)
        new_hasher._hasher = self._hasher.copy()
        return new_hasher


def hash_file_md5(filepath: str) -> str:
    """Compute MD5 hash of a file (convenience function).

    Args:
        filepath: Path to the file.

    Returns:
        MD5 hex digest.
    """
    return file_hash(filepath, algorithm="md5")


def hash_file_sha256(filepath: str) -> str:
    """Compute SHA256 hash of a file (convenience function).

    Args:
        filepath: Path to the file.

    Returns:
        SHA256 hex digest.
    """
    return file_hash(filepath, algorithm="sha256")


def hash_directory(
    dirpath: str,
    algorithm: str = "sha256",
    ignore_hidden: bool = True,
) -> str:
    """Compute hash of a directory (all files sorted).

    Args:
        dirpath: Path to the directory.
        algorithm: Hash algorithm name.
        ignore_hidden: If True, skip hidden files and directories.

    Returns:
        Combined hash of all files.
    """
    import os

    def _walk_and_hash():
        paths = []
        for root, dirs, files in os.walk(dirpath):
            if ignore_hidden:
                dirs[:] = [d for d in dirs if not d.startswith(".")]
                files = [f for f in files if not f.startswith(".")]

            dirs.sort()
            for filename in sorted(files):
                full_path = os.path.join(root, filename)
                rel_path = os.path.relpath(full_path, dirpath)
                paths.append((rel_path, full_path))

        hasher = IncrementalHasher(algorithm=algorithm)
        for rel_path, full_path in paths:
            hasher.update(rel_path.encode())
            hasher.update(file_hash(full_path, algorithm).encode())

        return hasher.hexdigest()

    return _walk_and_hash()


def checksum(data: Union[bytes, str], algorithm: str = "crc32") -> int:
    """Compute a simple checksum.

    Args:
        data: Data to checksum.
        algorithm: Algorithm ("crc32" or "adler32").

    Returns:
        Checksum as integer.
    """
    if isinstance(data, str):
        data = data.encode()

    if algorithm == "crc32":
        return hashlib.crc32(data) & 0xFFFFFFFF
    elif algorithm == "adler32":
        return hashlib.adler32(data) & 0xFFFFFFFF
    else:
        raise ValueError(f"Unknown algorithm: {algorithm}")


def checksum_hex(data: Union[bytes, str], algorithm: str = "crc32") -> str:
    """Compute a hex checksum.

    Args:
        data: Data to checksum.
        algorithm: Algorithm ("crc32" or "adler32").

    Returns:
        Checksum as hex string.
    """
    return f"{checksum(data, algorithm):08x}"


def hash_to_id(
    data: Union[bytes, str],
    prefix: str = "",
    length: int = 12,
) -> str:
    """Generate a short hash-based ID.

    Args:
        data: Data to hash.
        prefix: Optional prefix for the ID.
        length: Length of hash to use (in hex chars).

    Returns:
        Short ID string like "abc123" or "user_abc123".
    """
    hex_hash = sha256(data, hexdigest=True)[:length]
    if prefix:
        return f"{prefix}_{hex_hash}"
    return hex_hash
