"""
Data Fingerprint Action Module

Generates and verifies data fingerprints for integrity
checking, change detection, and deduplication.

Author: RabAi Team
"""

from __future__ import annotations

import hashlib
import json
import zlib
from collections import Counter
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import logging

logger = logging.getLogger(__name__)


class HashAlgorithm(Enum):
    """Supported hashing algorithms."""

    MD5 = auto()
    SHA1 = auto()
    SHA256 = auto()
    SHA512 = auto()
    MURMUR3 = auto()
    CITYHASH = auto()


class FingerprintType(Enum):
    """Types of fingerprints."""

    CONTENT = auto()
    STRUCTURE = auto()
    SEMANTIC = auto()
    STATISTICAL = auto()
    BLOOM = auto()


@dataclass
class DataFingerprint:
    """A data fingerprint with metadata."""

    fingerprint: str
    algorithm: HashAlgorithm
    fingerprint_type: FingerprintType
    length: int
    checksum: Optional[str] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    sample_rate: float = 1.0
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def matches(self, other: DataFingerprint) -> bool:
        """Check if two fingerprints match."""
        return self.fingerprint == other.fingerprint


@dataclass
class BloomFilter:
    """Bloom filter for set membership testing."""

    size_bits: int
    hash_count: int
    bit_array: int = 0
    inserted_count: int = 0

    def add(self, item: str) -> None:
        """Add an item to the bloom filter."""
        for seed in range(self.hash_count):
            idx = self._hash(item, seed) % self.size_bits
            self.bit_array |= (1 << idx)
        self.inserted_count += 1

    def might_contain(self, item: str) -> bool:
        """Check if an item might be in the set."""
        for seed in range(self.hash_count):
            idx = self._hash(item, seed) % self.size_bits
            if not (self.bit_array & (1 << idx)):
                return False
        return True

    def _hash(self, item: str, seed: int) -> int:
        """Hash an item with given seed."""
        return int(hashlib.md5(f"{seed}:{item}".encode()).hexdigest(), 16)


@dataclass
class FingerprintReport:
    """Report of fingerprint analysis."""

    fingerprint: DataFingerprint
    changes_detected: bool
    change_summary: Dict[str, Any] = field(default_factory=dict)
    similarity_score: float = 0.0


class FingerprintGenerator:
    """Generates various types of data fingerprints."""

    def __init__(self) -> None:
        pass

    def _serialize(
        self,
        data: Any,
        sort_keys: bool = True,
    ) -> str:
        """Serialize data to deterministic string."""
        if isinstance(data, dict):
            items = sorted(data.items()) if sort_keys else data.items()
            parts = []
            for k, v in items:
                parts.append(f"{k}:{self._serialize(v, sort_keys)}")
            return "{" + ",".join(parts) + "}"
        elif isinstance(data, (list, tuple)):
            items = sorted(data) if sort_keys else data
            return "[" + ",".join(self._serialize(i, sort_keys) for i in items) + "]"
        elif isinstance(data, str):
            return data
        elif isinstance(data, bytes):
            return data.hex()
        else:
            return str(data)

    def _compute_hash(
        self,
        content: str,
        algorithm: HashAlgorithm,
    ) -> str:
        """Compute hash of content using specified algorithm."""
        if algorithm == HashAlgorithm.MD5:
            return hashlib.md5(content.encode()).hexdigest()
        elif algorithm == HashAlgorithm.SHA1:
            return hashlib.sha1(content.encode()).hexdigest()
        elif algorithm == HashAlgorithm.SHA256:
            return hashlib.sha256(content.encode()).hexdigest()
        elif algorithm == HashAlgorithm.SHA512:
            return hashlib.sha512(content.encode()).hexdigest()
        elif algorithm == HashAlgorithm.MURMUR3:
            return hashlib.md5(content.encode()).hexdigest()[:16]
        else:
            return hashlib.sha256(content.encode()).hexdigest()

    def content_fingerprint(
        self,
        data: Union[Dict[str, Any], List[Any], str],
        algorithm: HashAlgorithm = HashAlgorithm.SHA256,
    ) -> DataFingerprint:
        """Generate a content fingerprint of data."""
        serialized = self._serialize(data)
        fingerprint = self._compute_hash(serialized, algorithm)

        length = len(serialized)
        checksum = zlib.crc32(serialized.encode())

        row_count = len(data) if isinstance(data, (list, tuple)) else None
        col_count = len(data) if isinstance(data, dict) else None

        return DataFingerprint(
            fingerprint=fingerprint,
            algorithm=algorithm,
            fingerprint_type=FingerprintType.CONTENT,
            length=length,
            checksum=str(checksum),
            row_count=row_count,
            column_count=col_count,
        )

    def structure_fingerprint(
        self,
        data: Union[Dict[str, Any], List[Any]],
        algorithm: HashAlgorithm = HashAlgorithm.SHA256,
    ) -> DataFingerprint:
        """Generate a fingerprint based on data structure only."""
        if isinstance(data, dict):
            structure = sorted(data.keys())
        elif isinstance(data, list):
            if data and isinstance(data[0], dict):
                structure = sorted(data[0].keys())
            else:
                structure = type(data).__name__
        else:
            structure = type(data).__name__

        serialized = json.dumps(structure, sort_keys=True)
        fingerprint = self._compute_hash(serialized, algorithm)

        return DataFingerprint(
            fingerprint=fingerprint,
            algorithm=algorithm,
            fingerprint_type=FingerprintType.STRUCTURE,
            length=len(serialized),
            metadata={"structure": structure},
        )

    def statistical_fingerprint(
        self,
        records: List[Dict[str, Any]],
        algorithm: HashAlgorithm = HashAlgorithm.SHA256,
        sample_rate: float = 1.0,
    ) -> DataFingerprint:
        """Generate a statistical fingerprint using sampling."""
        if not records:
            return DataFingerprint(
                fingerprint="",
                algorithm=algorithm,
                fingerprint_type=FingerprintType.STATISTICAL,
                length=0,
            )

        sample_size = max(1, int(len(records) * sample_rate))
        sampled = records[:sample_size]

        stats: Dict[str, Any] = {
            "row_count": len(records),
            "sample_count": sample_size,
        }

        if records and isinstance(records[0], dict):
            for field_name in records[0].keys():
                values = [r.get(field_name) for r in sampled if r.get(field_name) is not None]
                numeric_values = [v for v in values if isinstance(v, (int, float))]

                field_stats: Dict[str, Any] = {
                    "count": len(values),
                    "unique": len(set(str(v) for v in values)),
                }

                if numeric_values:
                    field_stats.update({
                        "min": min(numeric_values),
                        "max": max(numeric_values),
                        "mean": sum(numeric_values) / len(numeric_values),
                    })

                if isinstance(values[0], str) if values else False:
                    counter = Counter(values)
                    field_stats["top_5"] = counter.most_common(5)

                stats[field_name] = field_stats

        serialized = json.dumps(stats, sort_keys=True)
        fingerprint = self._compute_hash(serialized, algorithm)

        return DataFingerprint(
            fingerprint=fingerprint,
            algorithm=algorithm,
            fingerprint_type=FingerprintType.STATISTICAL,
            length=len(serialized),
            sample_rate=sample_rate,
            row_count=len(records),
            metadata=stats,
        )


class DataFingerprintAction:
    """Action class for data fingerprinting operations."""

    def __init__(self) -> None:
        self.generator = FingerprintGenerator()
        self._history: Dict[str, List[DataFingerprint]] = {}

    def fingerprint(
        self,
        data: Any,
        algorithm: HashAlgorithm = HashAlgorithm.SHA256,
        fingerprint_type: FingerprintType = FingerprintType.CONTENT,
    ) -> DataFingerprint:
        """Generate a fingerprint of data."""
        if fingerprint_type == FingerprintType.CONTENT:
            fp = self.generator.content_fingerprint(data, algorithm)
        elif fingerprint_type == FingerprintType.STRUCTURE:
            fp = self.generator.structure_fingerprint(data, algorithm)
        elif fingerprint_type == FingerprintType.STATISTICAL:
            fp = self.generator.statistical_fingerprint(data, algorithm)
        else:
            fp = self.generator.content_fingerprint(data, algorithm)

        return fp

    def track_fingerprint(
        self,
        entity_id: str,
        data: Any,
        algorithm: HashAlgorithm = HashAlgorithm.SHA256,
    ) -> DataFingerprint:
        """Generate and track a fingerprint for an entity."""
        fp = self.fingerprint(data, algorithm)

        if entity_id not in self._history:
            self._history[entity_id] = []
        self._history[entity_id].append(fp)

        return fp

    def detect_changes(
        self,
        entity_id: str,
        new_data: Any,
        algorithm: HashAlgorithm = HashAlgorithm.SHA256,
    ) -> FingerprintReport:
        """Detect changes in data by comparing fingerprints."""
        new_fp = self.fingerprint(new_data, algorithm)

        if entity_id in self._history and self._history[entity_id]:
            old_fp = self._history[entity_id][-1]
            changed = not new_fp.matches(old_fp)
            similarity = 100.0 if not changed else 0.0

            return FingerprintReport(
                fingerprint=new_fp,
                changes_detected=changed,
                change_summary={
                    "previous": old_fp.fingerprint,
                    "current": new_fp.fingerprint,
                    "same": not changed,
                },
                similarity_score=similarity,
            )

        return FingerprintReport(
            fingerprint=new_fp,
            changes_detected=True,
            change_summary={"first_fingerprint": True},
            similarity_score=0.0,
        )

    def build_bloom_filter(
        self,
        items: List[str],
        size_bits: int = 10000,
        hash_count: int = 7,
    ) -> BloomFilter:
        """Build a bloom filter from items."""
        bloom = BloomFilter(size_bits=size_bits, hash_count=hash_count)
        for item in items:
            bloom.add(str(item))
        return bloom

    def get_fingerprint_history(self, entity_id: str) -> List[DataFingerprint]:
        """Get fingerprint history for an entity."""
        return self._history.get(entity_id, []).copy()
