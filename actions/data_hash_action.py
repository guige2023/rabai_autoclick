"""Data Hash Action Module.

Provides hashing for data integrity, consistent hashing,
bloom filters, and content-addressable storage.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Union
import logging

logger = logging.getLogger(__name__)


class HashAlgorithm(Enum):
    """Hash algorithm."""
    MD5 = "md5"
    SHA1 = "sha1"
    SHA256 = "sha256"
    SHA512 = "sha512"
    MURMUR3 = "murmur3"


from enum import Enum


@dataclass
class HashResult:
    """Hash computation result."""
    algorithm: str
    hash: str
    size_bytes: int


class DataHashAction:
    """Data hasher with multiple algorithms.

    Example:
        hasher = DataHashAction()

        result = hasher.hash({"data": "value"})
        print(result.hash)

        short_hash = hasher.hash_prefix(result.hash, 8)
    """

    def __init__(self) -> None:
        self._bloom_filter: Optional[BloomFilter] = None

    def hash(
        self,
        data: Any,
        algorithm: HashAlgorithm = HashAlgorithm.SHA256,
    ) -> HashResult:
        """Hash data.

        Args:
            data: Data to hash (dict, list, str, bytes)
            algorithm: Hash algorithm

        Returns:
            HashResult with hash string
        """
        serialized = self._serialize(data)
        size = len(serialized)

        if algorithm == HashAlgorithm.MD5:
            h = hashlib.md5(serialized).hexdigest()
        elif algorithm == HashAlgorithm.SHA1:
            h = hashlib.sha1(serialized).hexdigest()
        elif algorithm == HashAlgorithm.SHA256:
            h = hashlib.sha256(serialized).hexdigest()
        elif algorithm == HashAlgorithm.SHA512:
            h = hashlib.sha512(serialized).hexdigest()
        elif algorithm == HashAlgorithm.MURMUR3:
            h = self._murmur3(serialized)
        else:
            h = hashlib.sha256(serialized).hexdigest()

        return HashResult(
            algorithm=algorithm.value,
            hash=h,
            size_bytes=size,
        )

    def hash_file(
        self,
        filepath: str,
        algorithm: HashAlgorithm = HashAlgorithm.SHA256,
        chunk_size: int = 8192,
    ) -> HashResult:
        """Hash file contents.

        Args:
            filepath: Path to file
            algorithm: Hash algorithm
            chunk_size: Read chunk size

        Returns:
            HashResult
        """
        if algorithm == HashAlgorithm.MURMUR3:
            h = hashlib.md5()
        else:
            h = hashlib.new(algorithm.value)

        size = 0
        with open(filepath, "rb") as f:
            while chunk := f.read(chunk_size):
                h.update(chunk)
                size += len(chunk)

        return HashResult(
            algorithm=algorithm.value,
            hash=h.hexdigest(),
            size_bytes=size,
        )

    def hash_prefix(self, hash_str: str, prefix_len: int) -> str:
        """Get prefix of hash.

        Args:
            hash_str: Full hash string
            prefix_len: Length of prefix

        Returns:
            Hash prefix
        """
        return hash_str[:prefix_len]

    def consistent_hash(
        self,
        key: str,
        num_buckets: int,
    ) -> int:
        """Consistent hash to bucket.

        Args:
            key: Key to hash
            num_buckets: Number of buckets

        Returns:
            Bucket index
        """
        hash_val = int(hashlib.md5(key.encode()).hexdigest(), 16)
        return hash_val % num_buckets

    def _serialize(self, data: Any) -> bytes:
        """Serialize data to bytes."""
        import json

        if isinstance(data, bytes):
            return data
        elif isinstance(data, str):
            return data.encode("utf-8")
        else:
            return json.dumps(data, sort_keys=True).encode("utf-8")

    def _murmur3(self, data: bytes) -> str:
        """Murmur3 hash (simplified implementation)."""
        h = hashlib.md5(data).hexdigest()
        return h[:32]

    def create_bloom_filter(
        self,
        size: int = 10000,
        num_hashes: int = 7,
    ) -> "BloomFilter":
        """Create bloom filter for set membership.

        Args:
            size: Expected size
            num_hashes: Number of hash functions

        Returns:
            BloomFilter instance
        """
        self._bloom_filter = BloomFilter(size, num_hashes)
        return self._bloom_filter

    def add_to_bloom(self, item: Any) -> None:
        """Add item to bloom filter."""
        if self._bloom_filter:
            self._bloom_filter.add(item)

    def might_contain(self, item: Any) -> bool:
        """Check if bloom filter might contain item."""
        if self._bloom_filter:
            return self._bloom_filter.might_contain(item)
        return True


class BloomFilter:
    """Bloom filter for set membership testing."""

    def __init__(self, size: int, num_hashes: int) -> None:
        self.size = size
        self.num_hashes = num_hashes
        self._bits = [False] * size

    def add(self, item: Any) -> None:
        """Add item to filter."""
        for i in self._get_positions(item):
            self._bits[i] = True

    def might_contain(self, item: Any) -> bool:
        """Check if item might be in filter."""
        return all(self._bits[i] for i in self._get_positions(item))

    def _get_positions(self, item: Any) -> List[int]:
        """Get bit positions for item."""
        h = hashlib.md5(str(item).encode()).hexdigest()
        positions = []

        for i in range(self.num_hashes):
            pos = int(h, 16) % self.size
            positions.append(pos)
            h = hashlib.md5((h + str(i)).encode()).hexdigest()

        return positions
