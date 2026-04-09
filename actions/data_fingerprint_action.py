"""Data Fingerprint Action module.

Generates fingerprints/hashes for data integrity verification,
change detection, and deduplication.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

try:
    import xxhash
    HAS_XXHASH = True
except ImportError:
    HAS_XXHASH = False


@dataclass
class Fingerprint:
    """A data fingerprint."""

    value: str
    algorithm: str
    size_bytes: int
    created_at: datetime = field(default_factory=datetime.now)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return self.value

    def __len__(self) -> int:
        return len(self.value)


class FingerprintGenerator:
    """Generates fingerprints for various data types."""

    def __init__(self, default_algorithm: str = "xxhash64"):
        self.default_algorithm = default_algorithm

    def fingerprint_bytes(
        self,
        data: bytes,
        algorithm: Optional[str] = None,
    ) -> Fingerprint:
        """Generate fingerprint for bytes.

        Args:
            data: Bytes to fingerprint
            algorithm: Hash algorithm to use

        Returns:
            Fingerprint
        """
        algo = algorithm or self.default_algorithm

        if algo == "xxhash64":
            if not HAS_XXHASH:
                algo = "md5"
            return self._xxhash64(data)
        elif algo == "xxhash128":
            if not HAS_XXHASH:
                algo = "sha256"
            return self._xxhash128(data)
        elif algo == "md5":
            return self._md5(data)
        elif algo == "sha1":
            return self._sha1(data)
        elif algo == "sha256":
            return self._sha256(data)
        else:
            return self._sha256(data)

    def _xxhash64(self, data: bytes) -> Fingerprint:
        """Generate xxhash64 fingerprint."""
        h = xxhash.xxh64(data)
        return Fingerprint(
            value=h.hexdigest(),
            algorithm="xxhash64",
            size_bytes=len(data),
        )

    def _xxhash128(self, data: bytes) -> Fingerprint:
        """Generate xxhash128 fingerprint."""
        h = xxhash.xxh128(data)
        return Fingerprint(
            value=h.hexdigest(),
            algorithm="xxhash128",
            size_bytes=len(data),
        )

    def _md5(self, data: bytes) -> Fingerprint:
        """Generate MD5 fingerprint."""
        return Fingerprint(
            value=hashlib.md5(data).hexdigest(),
            algorithm="md5",
            size_bytes=len(data),
        )

    def _sha1(self, data: bytes) -> Fingerprint:
        """Generate SHA1 fingerprint."""
        return Fingerprint(
            value=hashlib.sha1(data).hexdigest(),
            algorithm="sha1",
            size_bytes=len(data),
        )

    def _sha256(self, data: bytes) -> Fingerprint:
        """Generate SHA256 fingerprint."""
        return Fingerprint(
            value=hashlib.sha256(data).hexdigest(),
            algorithm="sha256",
            size_bytes=len(data),
        )

    def fingerprint_string(
        self,
        text: str,
        algorithm: Optional[str] = None,
    ) -> Fingerprint:
        """Generate fingerprint for string."""
        return self.fingerprint_bytes(text.encode("utf-8"), algorithm)

    def fingerprint_dict(
        self,
        data: dict[str, Any],
        algorithm: Optional[str] = None,
        sort_keys: bool = True,
    ) -> Fingerprint:
        """Generate fingerprint for dictionary."""
        json_str = json.dumps(data, sort_keys=sort_keys, default=str)
        return self.fingerprint_string(json_str, algorithm)

    def fingerprint_list(
        self,
        data: list[Any],
        algorithm: Optional[str] = None,
    ) -> Fingerprint:
        """Generate fingerprint for list."""
        json_str = json.dumps(data, sort_keys=True, default=str)
        return self.fingerprint_string(json_str, algorithm)


@dataclass
class RollingHash:
    """Rolling hash for streaming/chunked fingerprinting."""

    window_size: int = 64
    prime: int = 1000000007
    base: int = 31

    _hash: int = 0
    _power: int = 0
    _buffer: list[int] = field(default_factory=list)

    def __post_init__(self):
        self._power = pow(self.base, self.window_size - 1, self.prime)

    def update(self, byte_value: int) -> None:
        """Update hash with a new byte."""
        self._hash = (self._hash * self.base + byte_value) % self.prime
        self._buffer.append(byte_value)

        if len(self._buffer) > self.window_size:
            removed = self._buffer.pop(0)
            self._hash = (self._hash - removed * self._power) % self.prime
            if self._hash < 0:
                self._hash += self.prime

    def value(self) -> int:
        """Get current hash value."""
        return self._hash

    def reset(self) -> None:
        """Reset the hash."""
        self._hash = 0
        self._buffer.clear()


@dataclass
class ContentDefinedChunker:
    """Content-defined chunking for deduplication.

    Splits data into variable-sized chunks based on content
    rather than fixed offsets.
    """

    chunk_min_size: int = 512
    chunk_max_size: int = 8192
    average_size: int = 4096
    mask: int = 0xFFF

    def chunk_bytes(self, data: bytes) -> list[bytes]:
        """Split data into chunks.

        Args:
            data: Data to chunk

        Returns:
            List of chunk bytes
        """
        chunks = []
        i = 0
        n = len(data)

        while i < n:
            window_end = min(i + self.chunk_max_size, n)

            if window_end - i < self.chunk_min_size:
                chunk_end = min(i + self.average_size, n)
            else:
                chunk_end = self._find_chunk_end(data, i, window_end)

            chunks.append(data[i:chunk_end])
            i = chunk_end

        return chunks

    def _find_chunk_end(self, data: bytes, start: int, end: int) -> int:
        """Find chunk boundary using rolling hash."""
        for i in range(start + self.chunk_min_size, end):
            if i + 1 >= end:
                return end

            window = data[i - self.chunk_min_size + 1:i + 1]
            if len(window) < self.chunk_min_size:
                continue

            rolling = RollingHash(window_size=self.chunk_min_size)
            for b in window:
                rolling.update(b)

            if rolling.value() & self.mask == 0:
                return i + 1

        return end


@dataclass
class SimilarityFingerprint:
    """Simhash-style similarity fingerprint."""

    value: int
    dimensions: int = 64

    def hamming_distance(self, other: "SimilarityFingerprint") -> int:
        """Calculate Hamming distance to another fingerprint."""
        xor = self.value ^ other.value
        distance = 0
        while xor:
            distance += 1
            xor &= xor - 1
        return distance

    def is_similar(
        self,
        other: "SimilarityFingerprint",
        threshold: int = 3,
    ) -> bool:
        """Check if fingerprints are similar."""
        return self.hamming_distance(other) <= threshold


def generate_simhash(data: list[int]) -> SimilarityFingerprint:
    """Generate simhash from feature vector.

    Args:
        data: List of feature hashes

    Returns:
        SimilarityFingerprint
    """
    v = [0] * 64

    for feature_hash in data:
        for i in range(64):
            if feature_hash & (1 << i):
                v[i] += 1
            else:
                v[i] -= 1

    fingerprint = 0
    for i in range(64):
        if v[i] > 0:
            fingerprint |= 1 << i

    return SimilarityFingerprint(value=fingerprint)
