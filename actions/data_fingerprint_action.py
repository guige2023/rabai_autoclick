"""Data Fingerprint Action.

Generates cryptographic fingerprints for datasets to enable
deduplication, change detection, and integrity verification.
"""
from __future__ import annotations

import hashlib
import zlib
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple


class HashAlgorithm(Enum):
    """Supported hashing algorithms."""
    MD5 = "md5"
    SHA1 = "sha1"
    SHA256 = "sha256"
    XXHASH64 = "xxhash64"
    CRC32 = "crc32"


@dataclass
class FieldFingerprint:
    """Fingerprint for a single field."""
    field_name: str
    value_hash: str
    null_count: int = 0
    distinct_count: int = 0


@dataclass
class DatasetFingerprint:
    """Complete fingerprint of a dataset."""
    dataset_name: str
    record_count: int
    field_count: int
    content_hash: str
    structure_hash: str
    field_fingerprints: List[FieldFingerprint] = field(default_factory=list)
    min_value_hash: Optional[str] = None
    max_value_hash: Optional[str] = None
    algorithm: str = "sha256"
    generated_at: datetime = field(default_factory=datetime.now)


@dataclass
class FingerprintMatch:
    """A match between two fingerprints."""
    fingerprint_a: str
    fingerprint_b: str
    similarity: float
    match_type: str


class DataFingerprintAction:
    """Generates and compares data fingerprints."""

    def __init__(
        self,
        algorithm: HashAlgorithm = HashAlgorithm.SHA256,
        include_structure: bool = True,
    ) -> None:
        self.algorithm = algorithm
        self.include_structure = include_structure
        self._min_record_bytes = 64
        self._max_record_bytes = 4096

    def _normalize_value(self, value: Any) -> str:
        """Normalize a value for consistent hashing."""
        if value is None:
            return "NULL"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, (list, tuple)):
            return ",".join(self._normalize_value(v) for v in value)
        if isinstance(value, dict):
            items = sorted((k, self._normalize_value(v)) for k, v in value.items())
            return ";".join(f"{k}={v}" for k, v in items)
        return str(value)

    def _hash_value(self, value: str) -> str:
        """Hash a normalized value."""
        if self.algorithm == HashAlgorithm.MD5:
            return hashlib.md5(value.encode()).hexdigest()
        elif self.algorithm == HashAlgorithm.SHA1:
            return hashlib.sha1(value.encode()).hexdigest()
        elif self.algorithm == HashAlgorithm.SHA256:
            return hashlib.sha256(value.encode()).hexdigest()
        elif self.algorithm == HashAlgorithm.CRC32:
            return str(zlib.crc32(value.encode()) & 0xFFFFFFFF)
        return hashlib.sha256(value.encode()).hexdigest()

    def fingerprint_field(
        self,
        values: Sequence[Any],
        field_name: str = "field",
    ) -> FieldFingerprint:
        """Generate a fingerprint for a single field."""
        normalized = [self._normalize_value(v) for v in values]
        non_null = [n for n in normalized if n != "NULL"]

        hashes = [self._hash_value(n) for n in non_null]
        content_hash = self._hash_value("|".join(sorted(hashes))) if hashes else ""

        return FieldFingerprint(
            field_name=field_name,
            value_hash=content_hash,
            null_count=len(normalized) - len(non_null),
            distinct_count=len(set(non_null)),
        )

    def fingerprint_dataset(
        self,
        data: List[Dict[str, Any]],
        dataset_name: str = "dataset",
    ) -> DatasetFingerprint:
        """Generate a complete fingerprint for a dataset."""
        if not data:
            return DatasetFingerprint(
                dataset_name=dataset_name,
                record_count=0,
                field_count=0,
                content_hash="",
                structure_hash="",
            )

        all_fields = set()
        for record in data:
            all_fields.update(record.keys())

        sorted_fields = sorted(all_fields)
        field_fps: List[FieldFingerprint] = []

        all_value_hashes: List[str] = []
        min_value = None
        max_value = None

        for field_name in sorted_fields:
            values = [r.get(field_name) for r in data]
            fp = self.fingerprint_field(values, field_name)
            field_fps.append(fp)
            all_value_hashes.append(fp.value_hash)

        if all_value_hashes:
            min_idx = min(range(len(all_value_hashes)), key=lambda i: all_value_hashes[i])
            max_idx = max(range(len(all_value_hashes)), key=lambda i: all_value_hashes[i])
            min_value = all_value_hashes[min_idx]
            max_value = all_value_hashes[max_idx]

        record_hashes: List[str] = []
        for record in data:
            field_values = [self._normalize_value(record.get(f, None)) for f in sorted_fields]
            record_hash = self._hash_value("|".join(sorted(field_values)))
            record_hashes.append(record_hash)

        content_hash = self._hash_value(";".join(sorted(record_hashes)))
        structure_hash = self._hash_value("|".join(sorted_fields))

        return DatasetFingerprint(
            dataset_name=dataset_name,
            record_count=len(data),
            field_count=len(sorted_fields),
            content_hash=content_hash,
            structure_hash=structure_hash,
            field_fingerprints=field_fps,
            min_value_hash=min_value,
            max_value_hash=max_value,
            algorithm=self.algorithm.value,
        )

    def fingerprint_bytes(self, data: bytes) -> str:
        """Generate a fingerprint for raw bytes."""
        return self._hash_value(data.decode("latin-1") if isinstance(data, bytes) else str(data))

    def compare_fingerprints(
        self,
        fp_a: DatasetFingerprint,
        fp_b: DatasetFingerprint,
    ) -> FingerprintMatch:
        """Compare two fingerprints."""
        content_match = fp_a.content_hash == fp_b.content_hash
        structure_match = fp_a.structure_hash == fp_b.structure_hash

        if content_match and structure_match:
            return FingerprintMatch(
                fingerprint_a=fp_a.content_hash[:16],
                fingerprint_b=fp_b.content_hash[:16],
                similarity=1.0,
                match_type="identical",
            )

        if structure_match:
            field_matches = sum(
                1 for fa, fb in zip(fp_a.field_fingerprints, fp_b.field_fingerprints)
                if fa.value_hash == fb.value_hash
            )
            similarity = field_matches / max(len(fp_a.field_fingerprints), 1)

            return FingerprintMatch(
                fingerprint_a=fp_a.content_hash[:16],
                fingerprint_b=fp_b.content_hash[:16],
                similarity=similarity,
                match_type="structure_match",
            )

        all_hashes_a = set(f.value_hash for f in fp_a.field_fingerprints)
        all_hashes_b = set(f.value_hash for f in fp_b.field_fingerprints)

        intersection = len(all_hashes_a & all_hashes_b)
        union = len(all_hashes_a | all_hashes_b)
        jaccard = intersection / union if union > 0 else 0.0

        return FingerprintMatch(
            fingerprint_a=fp_a.content_hash[:16],
            fingerprint_b=fp_b.content_hash[:16],
            similarity=jaccard,
            match_type="partial",
        )

    def detect_duplicates(
        self,
        fingerprints: List[DatasetFingerprint],
    ) -> List[Tuple[int, int]]:
        """Detect duplicate datasets from fingerprints."""
        duplicates: List[Tuple[int, int]] = []

        for i in range(len(fingerprints)):
            for j in range(i + 1, len(fingerprints)):
                match = self.compare_fingerprints(fingerprints[i], fingerprints[j])
                if match.similarity >= 0.95:
                    duplicates.append((i, j))

        return duplicates

    def generate_sample_fingerprint(
        self,
        data: List[Dict[str, Any]],
        sample_size: int = 1000,
    ) -> str:
        """Generate a fingerprint from a random sample."""
        if len(data) <= sample_size:
            fp = self.fingerprint_dataset(data)
            return fp.content_hash

        indices = set(random.sample(range(len(data)), sample_size))
        sample = [data[i] for i in sorted(indices)]
        fp = self.fingerprint_dataset(sample)
        return fp.content_hash

    def verify_integrity(
        self,
        data: List[Dict[str, Any]],
        expected_fingerprint: str,
    ) -> bool:
        """Verify data integrity against expected fingerprint."""
        fp = self.fingerprint_dataset(data)
        return fp.content_hash == expected_fingerprint

    def rolling_fingerprint(
        self,
        data: List[Dict[str, Any]],
        window_size: int = 100,
    ) -> List[str]:
        """Generate rolling fingerprints over sliding windows."""
        fingerprints: List[str] = []

        for i in range(len(data) - window_size + 1):
            window = data[i:i + window_size]
            fp = self.fingerprint_dataset(window)
            fingerprints.append(fp.content_hash[:16])

        return fingerprints
