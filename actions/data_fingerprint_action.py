"""
Data Fingerprint Action Module.

Generates content fingerprints for deduplication,
change detection, and data integrity verification.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


class HashAlgorithm(Enum):
    """Supported hash algorithms."""

    MD5 = "md5"
    SHA1 = "sha1"
    SHA256 = "sha256"
    XXHASH64 = "xxhash64"


@dataclass
class FingerprintResult:
    """Result of fingerprint generation."""

    fingerprint: str
    algorithm: str
    record_count: int = 0
    content_hash: str = ""
    metadata_hash: str = ""


class DataFingerprintAction:
    """
    Generates fingerprints for data deduplication and integrity.

    Features:
    - Multiple hash algorithms
    - Content and metadata separation
    - Batch fingerprinting
    - Change detection

    Example:
        fp = DataFingerprintAction()
        result = fp.fingerprint(data_list)
        if fp.has_changed(new_fingerprint, old_fingerprint):
            await process_changes()
    """

    def __init__(
        self,
        default_algorithm: HashAlgorithm = HashAlgorithm.SHA256,
        include_metadata: bool = True,
    ) -> None:
        """
        Initialize fingerprint action.

        Args:
            default_algorithm: Default hash algorithm.
            include_metadata: Include metadata in fingerprint.
        """
        self.default_algorithm = default_algorithm
        self.include_metadata = include_metadata
        self._fingerprint_cache: dict[str, str] = {}

    def fingerprint(
        self,
        data: list[dict[str, Any]],
        algorithm: Optional[HashAlgorithm] = None,
        key_field: str = "id",
    ) -> FingerprintResult:
        """
        Generate a fingerprint for a dataset.

        Args:
            data: List of records to fingerprint.
            algorithm: Hash algorithm to use.
            key_field: Field to use as record key.

        Returns:
            FingerprintResult with fingerprint and metadata.
        """
        algo = algorithm or self.default_algorithm

        sorted_data = sorted(data, key=lambda x: str(x.get(key_field, "")))

        content_parts = []
        for record in sorted_data:
            record_str = json.dumps(record, sort_keys=True, default=str)
            content_parts.append(record_str)

        content_combined = "|".join(content_parts)
        content_hash = self._hash_string(content_combined, algo)

        fingerprint = content_hash[:16]

        result = FingerprintResult(
            fingerprint=fingerprint,
            algorithm=algo.value,
            record_count=len(data),
            content_hash=content_hash,
        )

        logger.debug(f"Generated fingerprint: {fingerprint} for {len(data)} records")
        return result

    def fingerprint_record(
        self,
        record: dict[str, Any],
        algorithm: Optional[HashAlgorithm] = None,
    ) -> str:
        """
        Generate fingerprint for a single record.

        Args:
            record: Record to fingerprint.
            algorithm: Hash algorithm.

        Returns:
            Record fingerprint string.
        """
        algo = algorithm or self.default_algorithm
        record_str = json.dumps(record, sort_keys=True, default=str)
        return self._hash_string(record_str, algo)

    def detect_changes(
        self,
        old_data: list[dict[str, Any]],
        new_data: list[dict[str, Any]],
        key_field: str = "id",
    ) -> dict[str, Any]:
        """
        Detect changes between two datasets.

        Args:
            old_data: Original dataset.
            new_data: New dataset.
            key_field: Key field for comparison.

        Returns:
            Dictionary with added, removed, and modified records.
        """
        old_map = {r.get(key_field): r for r in old_data}
        new_map = {r.get(key_field): r for r in new_data}

        old_keys = set(old_map.keys())
        new_keys = set(new_map.keys())

        added_keys = new_keys - old_keys
        removed_keys = old_keys - new_keys
        common_keys = old_keys & new_keys

        modified_keys = []
        for key in common_keys:
            old_fingerprint = self.fingerprint_record(old_map[key])
            new_fingerprint = self.fingerprint_record(new_map[key])
            if old_fingerprint != new_fingerprint:
                modified_keys.append(key)

        return {
            "added": [new_map[k] for k in added_keys],
            "removed": [old_map[k] for k in removed_keys],
            "modified": [new_map[k] for k in modified_keys],
            "unchanged": len(common_keys) - len(modified_keys),
            "added_count": len(added_keys),
            "removed_count": len(removed_keys),
            "modified_count": len(modified_keys),
        }

    def has_changed(
        self,
        new_fingerprint: str,
        old_fingerprint: str,
    ) -> bool:
        """
        Check if fingerprint has changed.

        Args:
            new_fingerprint: New fingerprint.
            old_fingerprint: Old fingerprint.

        Returns:
            True if fingerprints differ.
        """
        return new_fingerprint != old_fingerprint

    def cache_fingerprint(self, key: str, fingerprint: str) -> None:
        """
        Cache a fingerprint.

        Args:
            key: Cache key.
            fingerprint: Fingerprint to cache.
        """
        self._fingerprint_cache[key] = fingerprint

    def get_cached_fingerprint(self, key: str) -> Optional[str]:
        """
        Get a cached fingerprint.

        Args:
            key: Cache key.

        Returns:
            Cached fingerprint or None.
        """
        return self._fingerprint_cache.get(key)

    def verify_integrity(
        self,
        data: list[dict[str, Any]],
        expected_fingerprint: str,
        algorithm: Optional[HashAlgorithm] = None,
    ) -> bool:
        """
        Verify data integrity against expected fingerprint.

        Args:
            data: Data to verify.
            expected_fingerprint: Expected fingerprint.
            algorithm: Hash algorithm.

        Returns:
            True if fingerprint matches.
        """
        result = self.fingerprint(data, algorithm)
        return result.fingerprint == expected_fingerprint or result.content_hash == expected_fingerprint

    def _hash_string(self, text: str, algorithm: HashAlgorithm) -> str:
        """Hash a string using the specified algorithm."""
        if algorithm == HashAlgorithm.MD5:
            return hashlib.md5(text.encode()).hexdigest()
        elif algorithm == HashAlgorithm.SHA1:
            return hashlib.sha1(text.encode()).hexdigest()
        elif algorithm == HashAlgorithm.SHA256:
            return hashlib.sha256(text.encode()).hexdigest()
        else:
            return hashlib.sha256(text.encode()).hexdigest()

    def clear_cache(self) -> None:
        """Clear fingerprint cache."""
        self._fingerprint_cache.clear()
