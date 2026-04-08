"""
Data fingerprinting module for content identification and deduplication.

Supports multiple fingerprinting algorithms, similarity detection,
and content-based addressing.
"""
from __future__ import annotations

import hashlib
import json
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, BinaryIO, Optional


class FingerprintAlgorithm(Enum):
    """Fingerprinting algorithms."""
    MD5 = "md5"
    SHA1 = "sha1"
    SHA256 = "sha256"
    XXHASH64 = "xxhash64"
    SIMHASH = "simhash"
    MINHASH = "minhash"


@dataclass
class Fingerprint:
    """A data fingerprint."""
    value: str
    algorithm: FingerprintAlgorithm
    size_bytes: int
    created_at: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)


@dataclass
class ContentRecord:
    """Record of stored content."""
    id: str
    fingerprint: str
    content_hash: str
    size_bytes: int
    created_at: float = field(default_factory=time.time)
    access_count: int = 0
    last_accessed_at: Optional[float] = None
    metadata: dict = field(default_factory=dict)


class DataFingerprinter:
    """
    Data fingerprinting service for content identification.

    Supports multiple fingerprinting algorithms, similarity detection,
    and content-based addressing.
    """

    def __init__(self, default_algorithm: FingerprintAlgorithm = FingerprintAlgorithm.SHA256):
        self.default_algorithm = default_algorithm
        self._fingerprints: dict[str, Fingerprint] = {}
        self._content_index: dict[str, str] = {}
        self._hash_to_ids: dict[str, list[str]] = defaultdict(list)

    def compute_fingerprint(
        self,
        data: Any,
        algorithm: Optional[FingerprintAlgorithm] = None,
    ) -> Fingerprint:
        """Compute a fingerprint for data."""
        algorithm = algorithm or self.default_algorithm

        if isinstance(data, str):
            data_bytes = data.encode("utf-8")
        elif isinstance(data, bytes):
            data_bytes = data
        elif hasattr(data, "read"):
            data_bytes = data.read()
        else:
            data_bytes = json.dumps(data, sort_keys=True).encode("utf-8")

        if algorithm == FingerprintAlgorithm.MD5:
            hash_value = hashlib.md5(data_bytes).hexdigest()
        elif algorithm == FingerprintAlgorithm.SHA1:
            hash_value = hashlib.sha1(data_bytes).hexdigest()
        elif algorithm == FingerprintAlgorithm.SHA256:
            hash_value = hashlib.sha256(data_bytes).hexdigest()
        elif algorithm == FingerprintAlgorithm.SIMHASH:
            hash_value = self._compute_simhash(data_bytes)
        elif algorithm == FingerprintAlgorithm.MINHASH:
            hash_value = self._compute_minhash(data_bytes)
        else:
            hash_value = hashlib.sha256(data_bytes).hexdigest()

        fingerprint = Fingerprint(
            value=hash_value,
            algorithm=algorithm,
            size_bytes=len(data_bytes),
        )

        self._fingerprints[hash_value] = fingerprint
        return fingerprint

    def _compute_simhash(self, data: bytes) -> str:
        """Compute SimHash for near-duplicate detection."""
        import struct

        features = [data[i:i+8] for i in range(0, min(len(data), 256), 8)]
        v = [0] * 64

        for feature in features:
            h = int.from_bytes(hashlib.md5(feature).digest()[:8], "big")
            for i in range(64):
                if h & (1 << i):
                    v[i] += 1
                else:
                    v[i] -= 1

        result = 0
        for i in range(64):
            if v[i] > 0:
                result |= (1 << i)

        return format(result, "016x")

    def _compute_minhash(self, data: bytes) -> str:
        """Compute MinHash for similarity estimation."""
        import struct

        num_hashes = 128
        hashes = []

        for i in range(num_hashes):
            seed = i * 1000
            h = int.from_bytes(hashlib.sha256(data + str(seed).encode()).digest()[:8], "big")
            hashes.append(h)

        minhash = min(hashes)
        return format(minhash, "016x")

    def store_content(
        self,
        data: Any,
        content_id: Optional[str] = None,
        algorithm: Optional[FingerprintAlgorithm] = None,
        metadata: Optional[dict] = None,
    ) -> tuple[str, Fingerprint]:
        """Store content and return its ID and fingerprint."""
        content_id = content_id or str(uuid.uuid4())[:12]
        fingerprint = self.compute_fingerprint(data, algorithm)

        record = ContentRecord(
            id=content_id,
            fingerprint=fingerprint.value,
            content_hash=self._content_hash(data),
            size_bytes=fingerprint.size_bytes,
            metadata=metadata or {},
        )

        self._content_index[content_id] = fingerprint.value
        self._hash_to_ids[fingerprint.value].append(content_id)

        return content_id, fingerprint

    def _content_hash(self, data: Any) -> str:
        """Compute a content hash."""
        if isinstance(data, str):
            data_bytes = data.encode("utf-8")
        elif isinstance(data, bytes):
            data_bytes = data
        else:
            data_bytes = json.dumps(data, sort_keys=True).encode("utf-8")

        return hashlib.sha256(data_bytes).hexdigest()

    def find_duplicates(
        self,
        data: Any,
        algorithm: Optional[FingerprintAlgorithm] = None,
    ) -> list[str]:
        """Find duplicate content IDs."""
        fingerprint = self.compute_fingerprint(data, algorithm)
        return self._hash_to_ids.get(fingerprint.value, [])

    def find_similar(
        self,
        data: Any,
        threshold: float = 0.8,
        max_results: int = 10,
    ) -> list[tuple[str, float]]:
        """Find similar content using SimHash or MinHash."""
        fingerprint = self.compute_fingerprint(data, FingerprintAlgorithm.SIMHASH)

        similarities = []

        for stored_fp_value, stored_fp in self._fingerprints.items():
            if stored_fp.algorithm != FingerprintAlgorithm.SIMHASH:
                continue

            if stored_fp_value == fingerprint.value:
                continue

            similarity = self._hamming_similarity(fingerprint.value, stored_fp_value)

            if similarity >= threshold:
                content_ids = self._hash_to_ids.get(stored_fp_value, [])
                for cid in content_ids:
                    similarities.append((cid, similarity))

        similarities.sort(key=lambda x: x[1], reverse=True)
        return similarities[:max_results]

    def _hamming_similarity(self, hash1: str, hash2: str) -> float:
        """Calculate similarity between two hashes using Hamming distance."""
        if len(hash1) != len(hash2):
            return 0.0

        h1 = int(hash1, 16)
        h2 = int(hash2, 16)

        xor = h1 ^ h2
        distance = bin(xor).count("1")
        max_distance = len(hash1) * 4

        return 1.0 - (distance / max_distance)

    def get_content_id(self, fingerprint_value: str) -> Optional[str]:
        """Get content ID by fingerprint."""
        ids = self._hash_to_ids.get(fingerprint_value, [])
        return ids[0] if ids else None

    def get_fingerprint(self, content_id: str) -> Optional[Fingerprint]:
        """Get fingerprint by content ID."""
        fp_value = self._content_index.get(content_id)
        return self._fingerprints.get(fp_value) if fp_value else None

    def record_access(self, content_id: str) -> bool:
        """Record an access to content."""
        if content_id not in self._content_index:
            return False

        return True

    def list_fingerprints(
        self,
        algorithm: Optional[FingerprintAlgorithm] = None,
        limit: int = 100,
    ) -> list[Fingerprint]:
        """List stored fingerprints."""
        fps = list(self._fingerprints.values())

        if algorithm:
            fps = [f for f in fps if f.algorithm == algorithm]

        return fps[:limit]

    def get_stats(self) -> dict:
        """Get fingerprinting statistics."""
        by_algorithm = defaultdict(int)
        total_size = 0

        for fp in self._fingerprints.values():
            by_algorithm[fp.algorithm.value] += 1
            total_size += fp.size_bytes

        return {
            "total_fingerprints": len(self._fingerprints),
            "total_content": len(self._content_index),
            "by_algorithm": dict(by_algorithm),
            "total_size_bytes": total_size,
        }
