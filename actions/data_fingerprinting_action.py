"""
Data Fingerprinting Module.

Generates unique fingerprints for datasets, files, and data streams.
Supports content-addressable storage, deduplication detection,
and data integrity verification.
"""

from __future__ import annotations

import hashlib
import zlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, BinaryIO, Callable, Optional


class HashAlgorithm(Enum):
    """Supported hashing algorithms."""
    MD5 = "md5"
    SHA1 = "sha1"
    SHA256 = "sha256"
    SHA512 = "sha512"
    XXHASH64 = "xxhash64"
    CRC32 = "crc32"


@dataclass
class Fingerprint:
    """Represents a data fingerprint."""
    algorithm: str
    hash_value: str
    size_bytes: int
    sample_rate: float = 1.0
    timestamp: float = field(default_factory=lambda: __import__("time").time())
    block_size: int = 0
    blocks: list[str] = field(default_factory=list)


@dataclass
class FingerprintMatch:
    """Represents a fingerprint match result."""
    query_fingerprint: Fingerprint
    matched_fingerprint: Fingerprint
    similarity: float
    match_type: str


class DataFingerprinter:
    """
    Generates and manages data fingerprints.

    Creates content hashes for files, datasets, and streams.
    Supports chunked hashing for large files and incremental
    updates for streaming data.

    Example:
        fp = DataFingerprinter()
        fingerprint = fp.fingerprint_file("/path/to/data.csv", algorithm=HashAlgorithm.SHA256)
        matches = fp.find_similar(new_fingerprint, threshold=0.95)
    """

    def __init__(self) -> None:
        self._registry: dict[str, Fingerprint] = {}
        self._content_index: dict[str, str] = {}

    def fingerprint_bytes(
        self,
        data: bytes,
        algorithm: HashAlgorithm = HashAlgorithm.SHA256
    ) -> Fingerprint:
        """
        Generate fingerprint for byte data.

        Args:
            data: Raw bytes to fingerprint
            algorithm: Hash algorithm to use

        Returns:
            Fingerprint object
        """
        hasher = self._get_hasher(algorithm)
        hasher.update(data)
        hash_value = hasher.hexdigest()

        return Fingerprint(
            algorithm=algorithm.value,
            hash_value=hash_value,
            size_bytes=len(data),
            sample_rate=1.0
        )

    def fingerprint_file(
        self,
        path: str,
        algorithm: HashAlgorithm = HashAlgorithm.SHA256,
        block_size: int = 8192
    ) -> Fingerprint:
        """
        Generate fingerprint for a file.

        Args:
            path: File path
            algorithm: Hash algorithm
            block_size: Block size for chunked hashing

        Returns:
            Fingerprint with block-level hashes
        """
        hasher = self._get_hasher(algorithm)
        block_hashes: list[str] = []
        total_size = 0

        with open(path, "rb") as f:
            while True:
                chunk = f.read(block_size)
                if not chunk:
                    break
                total_size += len(chunk)
                hasher.update(chunk)
                block_hasher = self._get_hasher(algorithm)
                block_hasher.update(chunk)
                block_hashes.append(block_hasher.hexdigest())

        return Fingerprint(
            algorithm=algorithm.value,
            hash_value=hasher.hexdigest(),
            size_bytes=total_size,
            block_size=block_size,
            blocks=block_hashes
        )

    def fingerprint_stream(
        self,
        stream: BinaryIO,
        algorithm: HashAlgorithm = HashAlgorithm.SHA256,
        block_size: int = 8192,
        sample_rate: float = 1.0
    ) -> Fingerprint:
        """
        Generate fingerprint for a data stream.

        Args:
            stream: Binary stream
            algorithm: Hash algorithm
            block_size: Block size
            sample_rate: Sampling rate (0.0-1.0) for large streams

        Returns:
            Fingerprint object
        """
        hasher = self._get_hasher(algorithm)
        block_hashes: list[str] = []
        total_size = 0
        import random
        random.seed(42)

        while True:
            chunk = stream.read(block_size)
            if not chunk:
                break
            total_size += len(chunk)
            hasher.update(chunk)

            if sample_rate >= 1.0 or random.random() < sample_rate:
                block_hasher = self._get_hasher(algorithm)
                block_hasher.update(chunk)
                block_hashes.append(block_hasher.hexdigest())

        return Fingerprint(
            algorithm=algorithm.value,
            hash_value=hasher.hexdigest(),
            size_bytes=total_size,
            sample_rate=sample_rate,
            block_size=block_size,
            blocks=block_hashes
        )

    def fingerprint_dict(
        self,
        data: dict[str, Any],
        algorithm: HashAlgorithm = HashAlgorithm.SHA256
    ) -> Fingerprint:
        """Generate fingerprint for a dictionary."""
        import json
        serialized = json.dumps(data, sort_keys=True, default=str)
        encoded = serialized.encode("utf-8")
        return self.fingerprint_bytes(encoded, algorithm)

    def register(
        self,
        key: str,
        fingerprint: Fingerprint
    ) -> None:
        """Register a fingerprint with a key."""
        self._registry[key] = fingerprint
        self._content_index[fingerprint.hash_value] = key

    def get(self, key: str) -> Optional[Fingerprint]:
        """Get a registered fingerprint by key."""
        return self._registry.get(key)

    def find_by_hash(self, hash_value: str) -> Optional[str]:
        """Find the key for a given hash value."""
        return self._content_index.get(hash_value)

    def find_similar(
        self,
        fingerprint: Fingerprint,
        threshold: float = 0.95
    ) -> list[FingerprintMatch]:
        """
        Find fingerprints similar to the query.

        Args:
            fingerprint: Query fingerprint
            threshold: Similarity threshold (0.0-1.0)

        Returns:
            List of matching fingerprints with similarity scores
        """
        matches: list[FingerprintMatch] = []

        for key, fp in self._registry.items():
            if key == fingerprint.hash_value:
                continue

            similarity = self._calculate_similarity(fingerprint, fp)

            if similarity >= threshold:
                matches.append(FingerprintMatch(
                    query_fingerprint=fingerprint,
                    matched_fingerprint=fp,
                    similarity=similarity,
                    match_type=self._get_match_type(fingerprint, fp, similarity)
                ))

        matches.sort(key=lambda m: m.similarity, reverse=True)
        return matches

    def _calculate_similarity(self, fp1: Fingerprint, fp2: Fingerprint) -> float:
        """Calculate similarity between two fingerprints."""
        if fp1.hash_value == fp2.hash_value:
            return 1.0

        if fp1.size_bytes != fp2.size_bytes:
            size_diff = abs(fp1.size_bytes - fp2.size_bytes)
            size_similarity = max(0, 1 - size_diff / max(fp1.size_bytes, fp2.size_bytes))
        else:
            size_similarity = 1.0

        if fp1.blocks and fp2.blocks:
            common_blocks = set(fp1.blocks) & set(fp2.blocks)
            block_similarity = len(common_blocks) / max(len(fp1.blocks), len(fp2.blocks))
            return (size_similarity * 0.3 + block_similarity * 0.7)
        else:
            return size_similarity * 0.5

    def _get_match_type(
        self,
        fp1: Fingerprint,
        fp2: Fingerprint,
        similarity: float
    ) -> str:
        """Determine the type of match."""
        if similarity >= 0.99:
            return "exact"
        elif similarity >= 0.95:
            return "near_duplicate"
        elif similarity >= 0.80:
            return "similar"
        else:
            return "related"

    def verify(
        self,
        key: str,
        data: bytes
    ) -> bool:
        """Verify data matches a registered fingerprint."""
        fp = self._registry.get(key)
        if not fp:
            return False

        computed = self.fingerprint_bytes(data, HashAlgorithm(fp.algorithm))
        return computed.hash_value == fp.hash_value

    def _get_hasher(self, algorithm: HashAlgorithm):
        """Get a hasher instance for the algorithm."""
        if algorithm == HashAlgorithm.MD5:
            return hashlib.md5()
        elif algorithm == HashAlgorithm.SHA1:
            return hashlib.sha1()
        elif algorithm == HashAlgorithm.SHA256:
            return hashlib.sha256()
        elif algorithm == HashAlgorithm.SHA512:
            return hashlib.sha512()
        elif algorithm == HashAlgorithm.CRC32:
            return _CRC32Hasher()
        else:
            return hashlib.sha256()

    def get_stats(self) -> dict[str, Any]:
        """Get fingerprint statistics."""
        return {
            "total_registered": len(self._registry),
            "unique_hashes": len(self._content_index),
            "algorithms": list(set(fp.algorithm for fp in self._registry.values()))
        }


class _CRC32Hasher:
    """CRC32 hasher wrapper."""
    def __init__(self) -> None:
        self._value = 0

    def update(self, data: bytes) -> None:
        self._value = zlib.crc32(data, self._value)

    def hexdigest(self) -> str:
        return format(self._value & 0xFFFFFFFF, "08x")
