"""
Data Fingerprinting and Deduplication Module.

Generates content-addressable fingerprints for data records,
enables deduplication, change detection, and similarity matching.

Author: AutoGen
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class FingerprintAlgorithm(Enum):
    MD5 = auto()
    SHA1 = auto()
    SHA256 = auto()
    MURMUR3 = auto()
    SIMHASH = auto()
    MINHASH = auto()


@dataclass(frozen=True)
class DataFingerprint:
    """Immutable fingerprint of a data record."""
    fingerprint: str
    algorithm: str
    content_hash: str
    field_hash: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    record_id: Optional[str] = None


@dataclass
class DuplicateGroup:
    """A group of duplicate records."""
    group_id: str
    canonical_id: str
    members: List[str] = field(default_factory=list)
    fingerprint: str = ""
    confidence: float = 1.0


class FingerprintGenerator:
    """Generates various types of fingerprints for data."""

    HASH_FUNCS = {
        FingerprintAlgorithm.MD5: lambda d: hashlib.md5(d).hexdigest(),
        FingerprintAlgorithm.SHA1: lambda d: hashlib.sha1(d).hexdigest(),
        FingerprintAlgorithm.SHA256: lambda d: hashlib.sha256(d).hexdigest(),
    }

    def __init__(self, algorithm: FingerprintAlgorithm = FingerprintAlgorithm.SHA256):
        self.algorithm = algorithm

    def fingerprint_bytes(self, data: bytes) -> str:
        if self.algorithm in self.HASH_FUNCS:
            return self.HASH_FUNCS[self.algorithm](data)
        if self.algorithm == FingerprintAlgorithm.MURMUR3:
            return self._murmur3(data)
        return self._sha256_fallback(data)

    def fingerprint_string(self, text: str, encoding: str = "utf-8") -> str:
        return self.fingerprint_bytes(text.encode(encoding))

    def fingerprint_record(
        self, record: Dict[str, Any], fields: Optional[List[str]] = None
    ) -> Tuple[str, str, str]:
        """
        Generate fingerprints for a data record.

        Returns (content_hash, field_hash, combined_hash).
        """
        if fields:
            sorted_fields = sorted(fields)
            field_data = {k: record.get(k) for k in sorted_fields if k in record}
        else:
            field_data = {k: record[k] for k in sorted(record.keys())}

        field_json = json.dumps(field_data, sort_keys=True, default=str)
        content_hash = self.fingerprint_string(field_json)

        field_hash = self.fingerprint_string(json.dumps(sorted(record.keys()), sort_keys=True))

        combined = f"{content_hash}:{field_hash}"
        combined_hash = self.fingerprint_string(combined)

        return (content_hash, field_hash, combined_hash)

    def _sha256_fallback(self, data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def _murmur3(self, data: bytes) -> str:
        try:
            import mmh3
            return str(abs(mmh3.hash128(data)))
        except ImportError:
            return self._sha256_fallback(data)


class SimHashCalculator:
    """SimHash for near-duplicate detection."""

    def __init__(self, num_features: int = 128):
        self.num_features = num_features

    def calculate(self, text: str) -> int:
        import struct

        words = re.findall(r"\w+", text.lower())
        vectors: List[List[int]] = []

        for word in set(words):
            v = self._word_to_vector(word)
            vectors.append(v)

        if not vectors:
            return 0

        summed = [0] * self.num_features
        for v in vectors:
            for i, bit in enumerate(v):
                summed[i] += bit if bit == 1 else -1

        fingerprint = 0
        for i, val in enumerate(summed):
            if val > 0:
                fingerprint |= 1 << (self.num_features - 1 - i)

        return fingerprint

    def _word_to_vector(self, word: str) -> List[int]:
        import struct
        try:
            digest = int(hashlib.md5(word.encode()).hexdigest()[:16], 16)
        except Exception:
            digest = abs(hash(word))

        v = []
        for i in range(self.num_features):
            v.append(1 if (digest >> i) & 1 else 0)
        return v

    def hamming_distance(self, h1: int, h2: int) -> int:
        xor = h1 ^ h2
        return bin(xor).count("1")

    def find_near_duplicates(
        self, fingerprints: Dict[str, int], threshold: int = 3
    ) -> List[Tuple[str, str, int]]:
        """Find near-duplicate pairs from a set of fingerprints."""
        duplicates: List[Tuple[str, str, int]] = []
        items = list(fingerprints.items())
        for i, (id1, fp1) in enumerate(items):
            for id2, fp2 in items[i + 1:]:
                dist = self.hamming_distance(fp1, fp2)
                if dist <= threshold:
                    duplicates.append((id1, id2, dist))
        return duplicates


class MinHashCalculator:
    """MinHash for set similarity estimation."""

    def __init__(self, num_hashes: int = 128, seed: int = 42):
        self.num_hashes = num_hashes
        self.seed = seed

    def calculate(self, items: Set[str]) -> List[int]:
        import random
        random.seed(self.seed)
        minhash = [float("inf")] * self.num_hashes

        a_values = [random.randint(0, 2**31 - 1) for _ in range(self.num_hashes)]
        b_values = [random.randint(0, 2**31 - 1) for _ in range(self.num_hashes)]

        for item in items:
            try:
                digest = int(hashlib.sha256(item.encode()).hexdigest()[:8], 16)
            except Exception:
                digest = abs(hash(item))

            for i in range(self.num_hashes):
                h = (a_values[i] * digest + b_values[i]) % (2**31 - 1)
                minhash[i] = min(minhash[i], h)

        return minhash

    def jaccard_estimate(self, mh1: List[int], mh2: List[int]) -> float:
        if len(mh1) != len(mh2):
            return 0.0
        matches = sum(1 for a, b in zip(mh1, mh2) if a == b)
        return matches / len(mh1)


class DataDeduplicator:
    """
    Detects and manages duplicate data records.
    """

    def __init__(
        self,
        algorithm: FingerprintAlgorithm = FingerprintAlgorithm.SHA256,
        similarity_threshold: float = 0.8,
    ):
        self.fp_gen = FingerprintGenerator(algorithm)
        self.simhash = SimHashCalculator()
        self.minhash = MinHashCalculator()
        self.similarity_threshold = similarity_threshold

        self._exact_index: Dict[str, List[str]] = defaultdict(list)
        self._simhash_index: Dict[str, int] = {}
        self._minhash_index: Dict[str, List[int]] = {}
        self._record_fingerprints: Dict[str, DataFingerprint] = {}

    def add_record(
        self, record_id: str, record: Dict[str, Any], fields: Optional[List[str]] = None
    ) -> DataFingerprint:
        """Add a record and generate its fingerprint."""
        content_hash, field_hash, combined = self.fp_gen.fingerprint_record(
            record, fields
        )

        fp = DataFingerprint(
            fingerprint=combined,
            algorithm=self.fp_gen.algorithm.name,
            content_hash=content_hash,
            field_hash=field_hash,
            record_id=record_id,
        )

        self._exact_index[combined].append(record_id)
        self._record_fingerprints[record_id] = fp

        text_repr = json.dumps(record, sort_keys=True, default=str)
        sim_fp = self.simhash.calculate(text_repr)
        self._simhash_index[record_id] = sim_fp

        tokens = set(re.findall(r"\w+", text_repr.lower()))
        if tokens:
            self._minhash_index[record_id] = self.minhash.calculate(tokens)

        return fp

    def find_exact_duplicates(self, record_id: str) -> List[str]:
        """Find exact duplicates of a record by fingerprint."""
        fp = self._record_fingerprints.get(record_id)
        if not fp:
            return []
        return [
            rid for rid in self._exact_index.get(fp.fingerprint, [])
            if rid != record_id
        ]

    def find_similar(
        self, record_id: str, top_k: int = 10
    ) -> List[Tuple[str, float]]:
        """Find similar records using MinHash."""
        mh = self._minhash_index.get(record_id)
        if not mh:
            return []

        similarities: List[Tuple[str, float]] = []
        for other_id, other_mh in self._minhash_index.items():
            if other_id == record_id:
                continue
            sim = self.minhash.jaccard_estimate(mh, other_mh)
            if sim >= self.similarity_threshold:
                similarities.append((other_id, sim))

        similarities.sort(key=lambda x: x[1], reverse=True)
        return similarities[:top_k]

    def find_near_duplicates(self, threshold: int = 3) -> List[DuplicateGroup]:
        """Find all near-duplicate groups using SimHash."""
        pairs = self.simhash.find_near_duplicates(self._simhash_index, threshold)

        groups: Dict[str, Set[str]] = defaultdict(set)
        for id1, id2, _ in pairs:
            key = min(id1, id2)
            groups[key].add(id1)
            groups[key].add(id2)

        result: List[DuplicateGroup] = []
        for i, (canonical, members) in enumerate(groups.items()):
            group = DuplicateGroup(
                group_id=f"dup_group_{i}",
                canonical_id=canonical,
                members=list(members),
                fingerprint=self._record_fingerprints.get(canonical, DataFingerprint("", "", "")).fingerprint,
            )
            result.append(group)

        return result

    def get_fingerprint(self, record_id: str) -> Optional[DataFingerprint]:
        return self._record_fingerprints.get(record_id)

    def get_stats(self) -> Dict[str, Any]:
        total_records = len(self._record_fingerprints)
        exact_dup_groups = sum(
            max(0, len(members) - 1) for members in self._exact_index.values()
        )
        return {
            "total_records": total_records,
            "exact_duplicate_count": exact_dup_groups,
            "simhash_indexed": len(self._simhash_index),
            "minhash_indexed": len(self._minhash_index),
            "unique_fingerprints": len(self._exact_index),
        }
