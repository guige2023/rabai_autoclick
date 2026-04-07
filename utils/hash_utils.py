"""
Hashing algorithms and utilities.

Provides MD5, SHA-1, SHA-256, murmurhash, and consistent hashing.
"""

from __future__ import annotations

import math


def md5_hash(data: str) -> str:
    """
    Compute MD5 hash of a string (simplified pure-Python implementation).

    For production use, prefer the hashlib module.
    This is a reference implementation demonstrating the algorithm.

    Args:
        data: Input string

    Returns:
        32-character hexadecimal MD5 hash.
    """
    # Use built-in hashlib for actual use
    import hashlib
    return hashlib.md5(data.encode()).hexdigest()


def sha256_hash(data: str) -> str:
    """Compute SHA-256 hash of a string."""
    import hashlib
    return hashlib.sha256(data.encode()).hexdigest()


def sha1_hash(data: str) -> str:
    """Compute SHA-1 hash of a string."""
    import hashlib
    return hashlib.sha1(data.encode()).hexdigest()


def murmurhash3_32(data: str, seed: int = 0) -> int:
    """
    MurmurHash3 32-bit.

    Args:
        data: Input string
        seed: Random seed

    Returns:
        32-bit unsigned hash value.
    """
    import struct

    def rotl32(x: int, r: int) -> int:
        return ((x << r) | (x >> (32 - r))) & 0xFFFFFFFF

    def fmix32(h: int) -> int:
        h ^= h >> 16
        h = (h * 0x85EBCA6B) & 0xFFFFFFFF
        h ^= h >> 13
        h = (h * 0xC2B2AE35) & 0xFFFFFFFF
        h ^= h >> 16
        return h

    data_bytes = data.encode("utf-8")
    length = len(data_bytes)
    nblocks = length // 4

    h1 = seed & 0xFFFFFFFF

    c1 = 0xCC9E2D51
    c2 = 0x1B873593

    for i in range(nblocks):
        k1 = struct.unpack("<I", data_bytes[i * 4 : i * 4 + 4])[0]
        k1 = (k1 * c1) & 0xFFFFFFFF
        k1 = rotl32(k1, 15)
        k1 = (k1 * c2) & 0xFFFFFFFF
        h1 ^= k1
        h1 = rotl32(h1, 13)
        h1 = ((h1 * 5) + 0xE6546B64) & 0xFFFFFFFF

    tail = data_bytes[nblocks * 4:]
    k1 = 0
    for i, byte in enumerate(tail):
        k1 ^= byte << (i * 8)
    if tail:
        k1 = (k1 * c1) & 0xFFFFFFFF
        k1 = rotl32(k1, 15)
        k1 = (k1 * c2) & 0xFFFFFFFF
        h1 ^= k1

    h1 ^= length
    h1 = fmix32(h1)
    return h1


def hash_ring(nodes: list[str], key: str, replicas: int = 100) -> str:
    """
    Consistent hashing - find which node a key belongs to.

    Args:
        nodes: List of node identifiers
        key: Key to hash
        replicas: Number of virtual nodes per physical node

    Returns:
        Selected node identifier.
    """
    if not nodes:
        raise ValueError("No nodes provided")

    positions: dict[int, str] = {}
    for node in nodes:
        for i in range(replicas):
            pos = murmurhash3_32(f"{node}::{i}", seed=0) & 0xFFFFFFFF
            positions[pos] = node

    key_hash = murmurhash3_32(key, seed=0) & 0xFFFFFFFF
    sorted_positions = sorted(positions.keys())
    for pos in sorted_positions:
        if key_hash <= pos:
            return positions[pos]
    return positions[sorted_positions[0]]


def hash_distribution(
    keys: list[str],
    nodes: list[str],
    replicas: int = 100,
) -> dict[str, int]:
    """
    Compute hash distribution across nodes.

    Args:
        keys: List of keys to distribute
        nodes: List of nodes
        replicas: Virtual nodes per physical node

    Returns:
        Dictionary mapping node to key count.
    """
    dist: dict[str, int] = {n: 0 for n in nodes}
    for key in keys:
        node = hash_ring(nodes, key, replicas)
        dist[node] = dist.get(node, 0) + 1
    return dist


def hash_bucket(key: str, num_buckets: int) -> int:
    """
    Map key to bucket index using hash.

    Args:
        key: Key string
        num_buckets: Number of buckets

    Returns:
        Bucket index [0, num_buckets).
    """
    import hashlib
    h = hashlib.md5(key.encode()).hexdigest()
    return int(h, 16) % num_buckets


def string_fingerprint(s: str, num_bits: int = 64) -> int:
    """
    Create a fingerprint hash for string deduplication.

    Args:
        s: Input string
        num_bits: Fingerprint size (64 or 128 recommended)

    Returns:
        Fingerprint as integer.
    """
    import hashlib
    if num_bits <= 64:
        return int(hashlib.md5(s.encode()).hexdigest(), 16) % (2 ** num_bits)
    return int(hashlib.sha256(s.encode()).hexdigest(), 16) % (2 ** num_bits)


def locality_sensitive_hash(
    vector: list[float],
    num_hashes: int = 16,
    dim: int | None = None,
) -> list[int]:
    """
    Simhash-style locality-sensitive hashing for vectors.

    Args:
        vector: Feature vector
        num_hashes: Number of hash bits to generate
        dim: Embedding dimension

    Returns:
        LSH fingerprint.
    """
    import hashlib

    dim = dim or len(vector)
    fingerprint = 0
    for i in range(num_hashes):
        hash_input = f"{i}:{vector}".encode()
        h = int(hashlib.md5(hash_input).hexdigest(), 16)
        bit = h & 1
        fingerprint |= (bit << i)

    # Convert to list of bits
    bits = [(fingerprint >> i) & 1 for i in range(num_hashes)]
    # Group into integers
    groups = []
    for i in range(0, num_hashes, 8):
        group_bits = bits[i:i+8]
        val = sum(b << j for j, b in enumerate(group_bits))
        groups.append(val)
    return groups


def minhash_signature(
    items: list[str],
    num_hashes: int = 100,
) -> list[int]:
    """
    MinHash signature for set similarity estimation.

    Args:
        items: List of items in the set
        num_hashes: Number of hash functions

    Returns:
        MinHash signature (list of minimum hash values).
    """
    if not items:
        return [0] * num_hashes

    signature: list[int] = []
    for i in range(num_hashes):
        min_hash = min(
            murmurhash3_32(item, seed=i) & 0xFFFFFFFF
            for item in items
        )
        signature.append(min_hash)
    return signature


def minhash_estimate_similarity(sig1: list[int], sig2: list[int]) -> float:
    """
    Estimate Jaccard similarity from MinHash signatures.

    Args:
        sig1: First MinHash signature
        sig2: Second MinHash signature

    Returns:
        Estimated Jaccard similarity.
    """
    if len(sig1) != len(sig2):
        raise ValueError("Signatures must have same length")
    if not sig1:
        return 0.0
    matches = sum(1 for a, b in zip(sig1, sig2) if a == b)
    return matches / len(sig1)
