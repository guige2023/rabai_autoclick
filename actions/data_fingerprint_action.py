"""
Data Fingerprint Action Module.

Data fingerprinting and deduplication with multiple
hashing algorithms, similarity detection, and chunking.
"""

import hashlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional
import logging

logger = logging.getLogger(__name__)


class HashAlgorithm(Enum):
    """Supported hashing algorithms."""
    MD5 = "md5"
    SHA1 = "sha1"
    SHA256 = "sha256"
    SHA512 = "sha512"
    xxHASH = "xxhash"


@dataclass
class Fingerprint:
    """
    Data fingerprint.

    Attributes:
        hash_value: Hash of the data.
        algorithm: Hash algorithm used.
        chunk_count: Number of chunks (for chunked fingerprinting).
        size_bytes: Original data size.
    """
    hash_value: str
    algorithm: HashAlgorithm
    chunk_count: int = 1
    size_bytes: int = 0


@dataclass
class SimilarityMatch:
    """Similarity match result."""
    fingerprint1: str
    fingerprint2: str
    similarity_score: float
    match_type: str


class DataFingerprintAction:
    """
    Data fingerprinting and deduplication.

    Example:
        fingerprinter = DataFingerprintAction()
        fp = fingerprinter.fingerprint(data, algorithm=HashAlgorithm.SHA256)
        fingerprinter.is_duplicate(fp)
    """

    def __init__(self):
        """Initialize data fingerprint action."""
        self._fingerprints: dict[str, Fingerprint] = {}
        self._seen_hashes: set = set()

    def fingerprint(
        self,
        data: Any,
        algorithm: HashAlgorithm = HashAlgorithm.SHA256,
        chunk_size: Optional[int] = None
    ) -> Fingerprint:
        """
        Generate fingerprint for data.

        Args:
            data: Data to fingerprint.
            algorithm: Hash algorithm to use.
            chunk_size: Chunk size for large data (None for single hash).

        Returns:
            Fingerprint object.
        """
        if isinstance(data, str):
            data_bytes = data.encode("utf-8")
        elif isinstance(data, bytes):
            data_bytes = data
        else:
            data_bytes = str(data).encode("utf-8")

        size_bytes = len(data_bytes)

        if chunk_size and size_bytes > chunk_size:
            hash_value, chunk_count = self._chunked_hash(data_bytes, algorithm, chunk_size)
        else:
            hash_value = self._hash_bytes(data_bytes, algorithm)
            chunk_count = 1

        fp = Fingerprint(
            hash_value=hash_value,
            algorithm=algorithm,
            chunk_count=chunk_count,
            size_bytes=size_bytes
        )

        self._fingerprints[hash_value] = fp
        self._seen_hashes.add(hash_value)

        return fp

    def _hash_bytes(
        self,
        data: bytes,
        algorithm: HashAlgorithm
    ) -> str:
        """Hash bytes with specified algorithm."""
        if algorithm == HashAlgorithm.MD5:
            return hashlib.md5(data).hexdigest()
        elif algorithm == HashAlgorithm.SHA1:
            return hashlib.sha1(data).hexdigest()
        elif algorithm == HashAlgorithm.SHA256:
            return hashlib.sha256(data).hexdigest()
        elif algorithm == HashAlgorithm.SHA512:
            return hashlib.sha512(data).hexdigest()
        elif algorithm == HashAlgorithm.xxHASH:
            try:
                import xxhash
                return xxhash.xxh64(data).hexdigest()
            except ImportError:
                return hashlib.sha256(data).hexdigest()

        return hashlib.sha256(data).hexdigest()

    def _chunked_hash(
        self,
        data: bytes,
        algorithm: HashAlgorithm,
        chunk_size: int
    ) -> tuple[str, int]:
        """Hash data in chunks (for large data)."""
        chunk_hashes = []

        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            chunk_hash = self._hash_bytes(chunk, algorithm)
            chunk_hashes.append(chunk_hash)

        combined = "".join(chunk_hashes).encode()
        total_hash = self._hash_bytes(combined, HashAlgorithm.SHA256)

        return total_hash, len(chunk_hashes)

    def is_duplicate(
        self,
        fingerprint: Fingerprint,
        check_similar: bool = False
    ) -> tuple[bool, Optional[str]]:
        """
        Check if fingerprint is duplicate.

        Args:
            fingerprint: Fingerprint to check.
            check_similar: Also check for similar fingerprints.

        Returns:
            Tuple of (is_duplicate, original_hash).
        """
        if fingerprint.hash_value in self._seen_hashes:
            for hash_val, fp in self._fingerprints.items():
                if fp.hash_value == fingerprint.hash_value and hash_val != fingerprint.hash_value:
                    return True, hash_val

        return False, None

    def find_similar(
        self,
        fingerprint: Fingerprint,
        threshold: float = 0.9
    ) -> list[SimilarityMatch]:
        """
        Find similar fingerprints.

        Args:
            fingerprint: Fingerprint to compare.
            threshold: Similarity threshold (0-1).

        Returns:
            List of SimilarityMatch objects.
        """
        matches = []

        for hash_val, fp in self._fingerprints.items():
            if hash_val == fingerprint.hash_value:
                continue

            score = self._calculate_similarity(fingerprint, fp)

            if score >= threshold:
                matches.append(SimilarityMatch(
                    fingerprint1=fingerprint.hash_value,
                    fingerprint2=hash_val,
                    similarity_score=score,
                    match_type="partial" if score < 1.0 else "exact"
                ))

        return matches

    def _calculate_similarity(
        self,
        fp1: Fingerprint,
        fp2: Fingerprint
    ) -> float:
        """Calculate similarity between two fingerprints."""
        if fp1.size_bytes == 0 or fp2.size_bytes == 0:
            return 0.0

        size_ratio = min(fp1.size_bytes, fp2.size_bytes) / max(fp1.size_bytes, fp2.size_bytes)

        hash_distance = self._hamming_distance(fp1.hash_value, fp2.hash_value)
        max_distance = len(fp1.hash_value) * 4

        hash_similarity = 1.0 - (hash_distance / max_distance)

        return (size_ratio + hash_similarity) / 2.0

    def _hamming_distance(self, hash1: str, hash2: str) -> int:
        """Calculate Hamming distance between two hashes."""
        if len(hash1) != len(hash2):
            return abs(len(hash1) - len(hash2)) * 4

        distance = 0
        for c1, c2 in zip(hash1, hash2):
            if c1 != c2:
                distance += 1

        return distance

    def dedupe_list(
        self,
        items: list,
        key_func: Optional[Callable[[Any], Any]] = None
    ) -> list:
        """
        Deduplicate list while preserving order.

        Args:
            items: List of items.
            key_func: Optional function to extract comparison key.

        Returns:
            Deduplicated list.
        """
        seen = set()
        result = []

        for item in items:
            key = key_func(item) if key_func else item

            if key not in seen:
                seen.add(key)
                result.append(item)

        logger.info(f"Deduplicated {len(items)} -> {len(result)} items")
        return result

    def content_define_chunking(
        self,
        data: bytes,
        chunk_min: int = 512,
        chunk_max: int = 4096
    ) -> list[tuple[int, bytes]]:
        """
        Content-defined chunking (CDC) for deduplication.

        Args:
            data: Data to chunk.
            chunk_min: Minimum chunk size.
            chunk_max: Maximum chunk size.

        Returns:
            List of (offset, chunk_data) tuples.
        """
        chunks = []
        i = 0

        while i < len(data):
            chunk_end = min(i + chunk_max, len(data))

            if chunk_end - i < chunk_min:
                chunk_end = min(i + chunk_min, len(data))

            chunk = data[i:chunk_end]
            chunks.append((i, chunk))

            i = chunk_end

        return chunks

    def get_stats(self) -> dict:
        """Get fingerprint statistics."""
        return {
            "total_fingerprints": len(self._fingerprints),
            "unique_hashes": len(self._seen_hashes)
        }

    def clear(self) -> None:
        """Clear all fingerprints."""
        self._fingerprints.clear()
        self._seen_hashes.clear()
