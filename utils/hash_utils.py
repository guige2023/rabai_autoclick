"""Hash utilities for RabAI AutoClick.

Provides:
- Hash computation for various types
- Consistent hashing helpers
- Bloom filter implementation
- Hash-based data structures
"""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import (
    Any,
    Callable,
    List,
    Optional,
    Union,
)


def hash_bytes(
    data: bytes,
    algorithm: str = "sha256",
) -> str:
    """Compute hash of bytes.

    Args:
        data: Bytes to hash.
        algorithm: Hash algorithm name.

    Returns:
        Hex digest string.
    """
    hasher = hashlib.new(algorithm)
    hasher.update(data)
    return hasher.hexdigest()


def hash_string(
    text: str,
    algorithm: str = "sha256",
    encoding: str = "utf-8",
) -> str:
    """Compute hash of a string.

    Args:
        text: String to hash.
        algorithm: Hash algorithm name.
        encoding: Text encoding.

    Returns:
        Hex digest string.
    """
    return hash_bytes(text.encode(encoding), algorithm)


def hash_file(
    path: str,
    algorithm: str = "sha256",
    chunk_size: int = 8192,
) -> str:
    """Compute hash of a file.

    Args:
        path: File path.
        algorithm: Hash algorithm name.
        chunk_size: Read chunk size.

    Returns:
        Hex digest string.
    """
    hasher = hashlib.new(algorithm)
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def hash_json(
    obj: Any,
    algorithm: str = "sha256",
) -> str:
    """Compute hash of a JSON-serializable object.

    Args:
        obj: Object to hash.
        algorithm: Hash algorithm name.

    Returns:
        Hex digest string.
    """
    serialized = json.dumps(obj, sort_keys=True, default=str)
    return hash_string(serialized, algorithm)


def hmac_hash(
    key: bytes,
    message: bytes,
    algorithm: str = "sha256",
) -> str:
    """Compute HMAC of a message.

    Args:
        key: Secret key.
        message: Message bytes.
        algorithm: Hash algorithm name.

    Returns:
        Hex digest string.
    """
    return hmac.new(key, message, algorithm).hexdigest()


def md5_quick(data: Union[str, bytes]) -> str:
    """Compute MD5 hash quickly.

    Args:
        data: String or bytes.

    Returns:
        MD5 hex digest.
    """
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.md5(data).hexdigest()


def sha1_quick(data: Union[str, bytes]) -> str:
    """Compute SHA1 hash quickly.

    Args:
        data: String or bytes.

    Returns:
        SHA1 hex digest.
    """
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha1(data).hexdigest()


def sha256_quick(data: Union[str, bytes]) -> str:
    """Compute SHA256 hash quickly.

    Args:
        data: String or bytes.

    Returns:
        SHA256 hex digest.
    """
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def consistent_hash(
    key: str,
    nodes: List[str],
    hash_func: Optional[Callable[[str], int]] = None,
) -> str:
    """Map a key to a node using consistent hashing.

    Args:
        key: Key to hash.
        nodes: List of node identifiers.
        hash_func: Custom hash function. Defaults to hash().

    Returns:
        Node identifier the key maps to.
    """
    if not nodes:
        raise ValueError("nodes cannot be empty")
    if hash_func is None:
        hash_func = hash

    positions: List[Tuple[int, str]] = [
        (hash_func(f"{key}:{node}"), node) for node in nodes
    ]
    positions.sort()
    return positions[0][1]


class BloomFilter:
    """Simple Bloom filter for set membership testing.

    Args:
        size: Expected number of items.
        false_positive_rate: Desired false positive rate.
    """

    def __init__(
        self,
        size: int = 100000,
        false_positive_rate: float = 0.01,
    ) -> None:
        import math

        self._size = size
        self._hash_count = int(
            -math.log(false_positive_rate) / math.log(2)
        )
        self._bit_array = [False] * size

    def add(self, item: str) -> None:
        """Add an item to the filter."""
        for seed in range(self._hash_count):
            idx = hash(f"{seed}:{item}") % self._size
            self._bit_array[idx] = True

    def might_contain(self, item: str) -> bool:
        """Check if an item might be in the filter."""
        for seed in range(self._hash_count):
            idx = hash(f"{seed}:{item}") % self._size
            if not self._bit_array[idx]:
                return False
        return True

    def __contains__(self, item: str) -> bool:
        return self.might_contain(item)


def murmur3_hash(data: bytes, seed: int = 0) -> int:
    """Compute MurmurHash3 (simplified 32-bit version).

    Args:
        data: Bytes to hash.
        seed: Random seed.

    Returns:
        32-bit hash integer.
    """
    c1 = 0xCC9E2D51
    c2 = 0x1B873593

    length = len(data)
    h1 = seed
    rounded_end = (length & 0xFFFFFFFC)

    for i in range(0, rounded_end, 4):
        k = (
            data[i]
            | (data[i + 1] << 8)
            | (data[i + 2] << 16)
            | (data[i + 3] << 24)
        )
        k = (k * c1) & 0xFFFFFFFF
        k = ((k << 15) | (k >> 17)) & 0xFFFFFFFF
        k = (k * c2) & 0xFFFFFFFF
        h1 = (h1 ^ k) & 0xFFFFFFFF
        h1 = ((h1 << 13) | (h1 >> 19)) & 0xFFFFFFFF
        h1 = ((h1 * 5) + 0xE6546B64) & 0xFFFFFFFF

    k = 0
    tail = length & 0x03
    if tail >= 1:
        k ^= data[rounded_end]
    if tail >= 2:
        k ^= data[rounded_end + 1] << 8
    if tail >= 3:
        k ^= data[rounded_end + 2] << 16
    if tail > 0:
        k = (k * c1) & 0xFFFFFFFF
        k = ((k << 15) | (k >> 17)) & 0xFFFFFFFF
        k = (k * c2) & 0xFFFFFFFF
        h1 ^= k
        h1 ^= length

    h1 ^= h1 >> 16
    h1 = (h1 * 0x85EBCA6B) & 0xFFFFFFFF
    h1 ^= h1 >> 13
    h1 = (h1 * 0xC2B2AE35) & 0xFFFFFFFF
    h1 ^= h1 >> 16

    return h1


__all__ = [
    "hash_bytes",
    "hash_string",
    "hash_file",
    "hash_json",
    "hmac_hash",
    "md5_quick",
    "sha1_quick",
    "sha256_quick",
    "consistent_hash",
    "BloomFilter",
    "murmur3_hash",
]


from typing import Tuple  # noqa: E402
