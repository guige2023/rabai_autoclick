"""
Hash utilities for checksums, digests, and hash-based data structures.

Provides implementations of common hash functions, Bloom filters,
and hash ring utilities.
"""

from __future__ import annotations

import hashlib
import struct
from typing import Callable


def fnv1a(data: bytes, seed: int = 0) -> int:
    """
    Fowler-Noll-Vo hash (FNV-1a variant).

    Args:
        data: Bytes to hash
        seed: Optional seed value

    Returns:
        64-bit hash value
    """
    FNV_OFFSET_BASIS = 14695981039346656037
    FNV_PRIME = 1099511628211
    h = FNV_OFFSET_BASIS ^ seed
    for byte in data:
        h ^= byte
        h = (h * FNV_PRIME) & 0xFFFFFFFFFFFFFFFF
    return h


def murmurhash3(data: bytes, seed: int = 0) -> int:
    """
    MurmurHash3 (32-bit) implementation.

    Args:
        data: Bytes to hash
        seed: Seed value

    Returns:
        32-bit hash value
    """
    c1 = 0xcc9e2d51
    c2 = 0x1b873593
    length = len(data)
    h = seed
    rounded_end = (length // 4) * 4
    for i in range(0, rounded_end, 4):
        k = struct.unpack("<I", data[i : i + 4])[0]
        k = (k * c1) & 0xFFFFFFFF
        k = ((k << 15) | (k >> 17)) & 0xFFFFFFFF
        k = (k * c2) & 0xFFFFFFFF
        h ^= k
        h = ((h << 13) | (h >> 19)) & 0xFFFFFFFF
        h = (h * 5 + 0xe6546b64) & 0xFFFFFFFF
    k = 0
    tail_size = length % 4
    if tail_size >= 3:
        k ^= data[rounded_end + 2] << 16
    if tail_size >= 2:
        k ^= data[rounded_end + 1] << 8
    if tail_size >= 1:
        k ^= data[rounded_end]
        k = (k * c1) & 0xFFFFFFFF
        k = ((k << 15) | (k >> 17)) & 0xFFFFFFFF
        k = (k * c2) & 0xFFFFFFFF
        h ^= k
    h ^= length
    h ^= (h >> 16)
    h = (h * 0x85ebca6b) & 0xFFFFFFFF
    h ^= (h >> 13)
    h = (h * 0xc2b2ae35) & 0xFFFFFFFF
    h ^= (h >> 16)
    return h


def djb2(data: bytes) -> int:
    """DJB2 hash function (Daniel J. Bernstein)."""
    h = 5381
    for byte in data:
        h = ((h << 5) + h + byte) & 0xFFFFFFFF
    return h


def sdbm(data: bytes) -> int:
    """SDBM hash function."""
    h = 0
    for byte in data:
        h = (byte + (h << 6) + (h << 16) - h) & 0xFFFFFFFF
    return h


class BloomFilter:
    """
    Simple Bloom filter for set membership testing.

    Args:
        size: Number of bits in filter
        num_hashes: Number of hash functions
    """

    def __init__(self, size: int = 10000, num_hashes: int = 7) -> None:
        self.size = size
        self.num_hashes = num_hashes
        self.bits = [False] * size

    def _get_hash_positions(self, item: bytes) -> list[int]:
        """Get bit positions for an item."""
        h1 = murmurhash3(item) % self.size
        h2 = djb2(item) % self.size
        return [(h1 + i * h2) % self.size for i in range(self.num_hashes)]

    def add(self, item: bytes) -> None:
        """Add item to filter."""
        for pos in self._get_hash_positions(item):
            self.bits[pos] = True

    def might_contain(self, item: bytes) -> bool:
        """Check if item might be in filter (may have false positives)."""
        return all(self.bits[pos] for pos in self._get_hash_positions(item))

    def __len__(self) -> int:
        return sum(self.bits)

    def false_positive_rate(self, n: int) -> float:
        """Estimate false positive rate for n insertions."""
        k = self.num_hashes
        m = self.size
        exponent = -k * n / m
        return (1 - math.e**exponent) ** k


import math


def consistent_hash(
    key: str,
    nodes: list[str],
    replicas: int = 100,
    hash_fn: Callable[[bytes], int] | None = None,
) -> str:
    """
    Consistent hashing to determine which node a key maps to.

    Args:
        key: Key to hash
        nodes: List of node identifiers
        replicas: Number of virtual nodes per physical node
        hash_fn: Hash function (default: djb2)

    Returns:
        Node identifier that should handle the key
    """
    if not nodes:
        raise ValueError("No nodes provided")
    if hash_fn is None:
        hash_fn = lambda b: djb2(b)

    positions: dict[int, str] = {}
    for node in nodes:
        for i in range(replicas):
            pos = hash_fn(f"{node}#{i}".encode())
            positions[pos] = node

    key_pos = hash_fn(key.encode())
    sorted_positions = sorted(positions.keys())
    for pos in sorted_positions:
        if key_pos <= pos:
            return positions[pos]
    return positions[sorted_positions[0]]
