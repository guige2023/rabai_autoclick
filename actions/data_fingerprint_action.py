"""Data Fingerprint Action Module.

Provides data fingerprinting and deduplication using content-based
hashing, similarity detection, and near-duplicate identification.
"""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class HashAlgorithm(Enum):
    MD5 = "md5"
    SHA1 = "sha1"
    SHA256 = "sha256"
    xxHASH = "xxhash"
    SIMHASH = "simhash"
    MINHASH = "minhash"


@dataclass
class FingerprintResult:
    hash_value: str
    algorithm: HashAlgorithm
    size_bytes: int
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DeduplicationResult:
    total_records: int
    duplicate_groups: int
    unique_records: int
    duplicate_count: int
    groups: List[List[int]] = field(default_factory=list)


@dataclass
class NearDuplicateResult:
    record_index: int
    near_duplicates: List[Tuple[int, float]] = field(default_factory=list)


class DataFingerprint:
    def __init__(self, algorithm: HashAlgorithm = HashAlgorithm.SHA256):
        self.algorithm = algorithm

    def fingerprint(self, data: Any) -> FingerprintResult:
        if isinstance(data, dict):
            content = self._normalize_dict(data)
        elif isinstance(data, (list, set)):
            content = self._normalize_collection(data)
        elif isinstance(data, str):
            content = data
        else:
            content = str(data)

        if isinstance(content, dict):
            content_str = str(sorted(content.items()))
        elif isinstance(content, (list, set)):
            content_str = str(sorted(content, key=str))
        else:
            content_str = str(content)

        hash_value = self._compute_hash(content_str.encode('utf-8'))

        return FingerprintResult(
            hash_value=hash_value,
            algorithm=self.algorithm,
            size_bytes=len(content_str.encode('utf-8')),
        )

    def _normalize_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        normalized = {}
        for k, v in data.items():
            if isinstance(v, dict):
                normalized[k] = self._normalize_dict(v)
            elif isinstance(v, (list, set)):
                normalized[k] = self._normalize_collection(v)
            elif isinstance(v, str):
                normalized[k] = v.strip().lower()
            else:
                normalized[k] = v
        return normalized

    def _normalize_collection(self, data: Any) -> List[Any]:
        result = []
        for item in data:
            if isinstance(item, dict):
                result.append(self._normalize_dict(item))
            elif isinstance(item, str):
                result.append(item.strip().lower())
            else:
                result.append(item)
        return result

    def _compute_hash(self, data: bytes) -> str:
        if self.algorithm == HashAlgorithm.MD5:
            return hashlib.md5(data).hexdigest()
        elif self.algorithm == HashAlgorithm.SHA1:
            return hashlib.sha1(data).hexdigest()
        elif self.algorithm == HashAlgorithm.SHA256:
            return hashlib.sha256(data).hexdigest()
        else:
            return hashlib.sha256(data).hexdigest()


class SimHash:
    def __init__(self, hash_size: int = 64):
        self.hash_size = hash_size

    def compute(self, text: str) -> int:
        tokens = self._tokenize(text)
        v = [0] * self.hash_size

        for token in tokens:
            h = int(hashlib.md5(token.encode()).hexdigest(), 16)
            for i in range(self.hash_size):
                bit = (h >> i) & 1
                v[i] += 1 if bit else -1

        fingerprint = 0
        for i in range(self.hash_size):
            if v[i] > 0:
                fingerprint |= (1 << i)

        return fingerprint

    def _tokenize(self, text: str) -> List[str]:
        text = text.lower()
        tokens = re.findall(r'\w+', text)
        return tokens

    def hamming_distance(self, hash1: int, hash2: int) -> int:
        xor = hash1 ^ hash2
        return bin(xor).count('1')

    def find_similar(
        self,
        target: int,
        hashes: List[Tuple[int, Any]],
        threshold: int = 3,
    ) -> List[Tuple[int, float, Any]]:
        results = []
        for idx, (h, data) in enumerate(hashes):
            dist = self.hamming_distance(target, h)
            if dist <= threshold:
                similarity = 1.0 - (dist / self.hash_size)
                results.append((idx, similarity, data))
        return results


class DataDeduplicator:
    def __init__(self, algorithm: HashAlgorithm = HashAlgorithm.SHA256):
        self.fingerprint = DataFingerprint(algorithm)
        self._seen_hashes: Dict[str, List[int]] = {}

    def find_duplicates(
        self,
        records: List[Dict[str, Any]],
    ) -> DeduplicationResult:
        self._seen_hashes.clear()
        groups: List[List[int]] = []

        for i, record in enumerate(records):
            fp = self.fingerprint.fingerprint(record)
            h = fp.hash_value

            if h in self._seen_hashes:
                self._seen_hashes[h].append(i)
            else:
                self._seen_hashes[h] = [i]

        for indices in self._seen_hashes.values():
            if len(indices) > 1:
                groups.append(indices)

        total_dupes = sum(len(g) - 1 for g in groups)

        return DeduplicationResult(
            total_records=len(records),
            duplicate_groups=len(groups),
            unique_records=len(records) - total_dupes,
            duplicate_count=total_dupes,
            groups=groups,
        )

    def deduplicate(
        self,
        records: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        seen = set()
        result = []

        for record in records:
            fp = self.fingerprint.fingerprint(record)
            h = fp.hash_value

            if h not in seen:
                seen.add(h)
                result.append(record)

        return result


def fingerprint_text(text: str, algorithm: HashAlgorithm = HashAlgorithm.SHA256) -> str:
    fp = DataFingerprint(algorithm)
    return fp.fingerprint(text).hash_value


def find_near_duplicates(
    texts: List[str],
    threshold: float = 0.9,
) -> List[Tuple[int, int, float]]:
    if not texts:
        return []

    simhash = SimHash()
    hashes = [(simhash.compute(t), t) for t in texts]
    results = []

    for i, (target_hash, target_text) in enumerate(hashes):
        for j in range(i + 1, len(hashes)):
            other_hash, other_text = hashes[j]
            dist = simhash.hamming_distance(target_hash, other_hash)
            similarity = 1.0 - (dist / simhash.hash_size)

            if similarity >= threshold:
                results.append((i, j, similarity))

    return results
