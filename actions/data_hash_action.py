"""Data Hashing and Fingerprinting.

This module provides data hashing capabilities:
- Multiple hash algorithms (MD5, SHA-256, xxHash)
- Record fingerprinting
- Content deduplication
- Secure comparisons

Example:
    >>> from actions.data_hash_action import DataHasher
    >>> hasher = DataHasher()
    >>> hash_val = hasher.hash_record(record)
"""

from __future__ import annotations

import hashlib
import logging
import threading
from typing import Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class DataHasher:
    """Hashes data for deduplication and fingerprinting."""

    SUPPORTED_ALGORITHMS = ["md5", "sha1", "sha256", "sha512", "xxhash64"]

    def __init__(
        self,
        algorithm: str = "sha256",
        ignore_fields: Optional[list[str]] = None,
    ) -> None:
        """Initialize the data hasher.

        Args:
            algorithm: Hash algorithm name.
            ignore_fields: Fields to exclude from hashing.
        """
        if algorithm not in self.SUPPORTED_ALGORITHMS:
            raise ValueError(f"Unsupported algorithm: {algorithm}")

        self._algorithm = algorithm
        self._ignore_fields = set(ignore_fields or [])
        self._lock = threading.Lock()
        self._stats = {"records_hashed": 0, "hash_collisions": 0}

    def hash_bytes(self, data: bytes) -> str:
        """Hash bytes data.

        Args:
            data: Bytes to hash.

        Returns:
            Hex digest string.
        """
        if self._algorithm == "md5":
            return hashlib.md5(data).hexdigest()
        elif self._algorithm == "sha1":
            return hashlib.sha1(data).hexdigest()
        elif self._algorithm == "sha256":
            return hashlib.sha256(data).hexdigest()
        elif self._algorithm == "sha512":
            return hashlib.sha512(data).hexdigest()
        elif self._algorithm == "xxhash64":
            try:
                import xxhash
                return xxhash.xxh64(data).hexdigest()
            except ImportError:
                return hashlib.sha256(data).hexdigest()
        return hashlib.sha256(data).hexdigest()

    def hash_string(self, s: str) -> str:
        """Hash a string.

        Args:
            s: String to hash.

        Returns:
            Hex digest string.
        """
        return self.hash_bytes(s.encode("utf-8"))

    def hash_record(self, record: dict[str, Any]) -> str:
        """Hash a record dict.

        Args:
            record: Record to hash.

        Returns:
            Hex digest string.
        """
        filtered = {k: v for k, v in record.items() if k not in self._ignore_fields}
        normalized = self._normalize(filtered)
        data = str(normalized).encode("utf-8")

        with self._lock:
            self._stats["records_hashed"] += 1

        return self.hash_bytes(data)

    def hash_record_fingerprint(self, record: dict[str, Any]) -> str:
        """Create a stable fingerprint for a record.

        Args:
            record: Record to fingerprint.

        Returns:
            Fingerprint string (16 chars).
        """
        full_hash = self.hash_record(record)
        return full_hash[:16]

    def _normalize(self, obj: Any) -> Any:
        """Normalize an object for consistent hashing."""
        if isinstance(obj, dict):
            return tuple(sorted((k, self._normalize(v)) for k, v in obj.items()))
        elif isinstance(obj, (list, tuple)):
            return tuple(self._normalize(v) for v in obj)
        elif isinstance(obj, set):
            return tuple(sorted(self._normalize(v) for v in obj))
        elif isinstance(obj, float):
            return round(obj, 10)
        return obj

    def verify_record(self, record: dict[str, Any], expected_hash: str) -> bool:
        """Verify a record matches an expected hash.

        Args:
            record: Record to verify.
            expected_hash: Expected hash value.

        Returns:
            True if hash matches.
        """
        actual = self.hash_record(record)
        return actual == expected_hash

    def compute_similarities(
        self,
        records: list[dict[str, Any]],
    ) -> list[tuple[int, int, float]]:
        """Compute pairwise similarity between records using hash distance.

        Args:
            records: List of records.

        Returns:
            List of (i, j, similarity) tuples.
        """
        hashes = [(i, self.hash_record(r)) for i, r in enumerate(records)]
        similarities = []

        for i, (idx_i, hash_i) in enumerate(hashes):
            for j, (idx_j, hash_j) in enumerate(hashes[i + 1:], start=i + 1):
                sim = self._hash_similarity(hash_i, hash_j)
                similarities.append((idx_i, idx_j, sim))

        return similarities

    def _hash_similarity(self, hash1: str, hash2: str) -> float:
        """Compute similarity between two hashes (0-1)."""
        if hash1 == hash2:
            return 1.0

        matching = sum(1 for a, b in zip(hash1, hash2) if a == b)
        return matching / max(len(hash1), len(hash2))

    def get_stats(self) -> dict[str, int]:
        """Get hashing statistics."""
        with self._lock:
            return dict(self._stats)
