"""
Data Fingerprint Action.

Generates cryptographic fingerprints for data assets to enable
deduplication, integrity verification, and change detection.

Author: rabai_autoclick
License: MIT
"""

from __future__ import annotations

import hashlib
import json
import zlib
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum, auto

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self


class FingerprintAlgorithm(Enum):
    """Supported fingerprinting algorithms."""
    MD5 = auto()
    SHA1 = auto()
    SHA256 = auto()
    SHA512 = auto()
    BLAKE2B = auto()
    BLAKE3 = auto()
    XXHASH64 = auto()


@dataclass(frozen=True)
class DataFingerprint:
    """Immutable fingerprint result."""
    value: str
    algorithm: FingerprintAlgorithm
    length: int
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    source_type: str = "unknown"  # file, stream, dict, etc.
    source_id: Optional[str] = None


@dataclass
class FingerprintConfig:
    """Configuration for fingerprint generation."""
    algorithm: FingerprintAlgorithm = FingerprintAlgorithm.SHA256
    normalize: bool = True  # Sort keys before hashing dicts
    include_schema: bool = True  # Include field names in hash
    sample_rate: float = 1.0  # 1.0 = 100% of data
    chunk_size: int = 8192
    exclude_fields: List[str] = field(default_factory=list)


class DataFingerprinter:
    """
    Generate cryptographic fingerprints for data of any type.

    Example:
        fingerprinter = DataFingerprinter()
        fp = fingerprinter.fingerprint({"user_id": 1, "name": "Alice"})
        print(fp.value)  # e.g. "a3f2b8c1..."
    """

    ALGORITHM_NAMES = {
        "md5": FingerprintAlgorithm.MD5,
        "sha1": FingerprintAlgorithm.SHA1,
        "sha256": FingerprintAlgorithm.SHA256,
        "sha512": FingerprintAlgorithm.SHA512,
        "blake2b": FingerprintAlgorithm.BLAKE2B,
        "blake3": FingerprintAlgorithm.BLAKE3,
        "xxhash64": FingerprintAlgorithm.XXHASH64,
    }

    def __init__(self, config: Optional[FingerprintConfig] = None) -> None:
        self._config = config or FingerprintConfig()

    def fingerprint(self, data: Any, source_type: str = "unknown",
                    source_id: Optional[str] = None) -> DataFingerprint:
        """
        Generate fingerprint for any data type.

        Supports: str, bytes, dict, list, int, float, bool, None
        """
        raw = self._serialize(data)
        digest = self._hash(raw)
        return DataFingerprint(
            value=digest,
            algorithm=self._config.algorithm,
            length=len(raw),
            source_type=source_type,
            source_id=source_id,
        )

    def fingerprint_file(self, path: str) -> DataFingerprint:
        """Fingerprint a file by streaming its contents."""
        hasher = self._create_hasher()
        total_read = 0
        with open(path, "rb") as f:
            while True:
                chunk = f.read(self._config.chunk_size)
                if not chunk:
                    break
                hasher.update(chunk)
                total_read += len(chunk)
        digest = hasher.hexdigest()
        return DataFingerprint(
            value=digest,
            algorithm=self._config.algorithm,
            length=total_read,
            source_type="file",
            source_id=path,
        )

    def fingerprint_stream(self, stream: bytes,
                           chunk_size: Optional[int] = None) -> DataFingerprint:
        """Fingerprint a byte stream in chunks."""
        hasher = self._create_hasher()
        cs = chunk_size or self._config.chunk_size
        total = 0
        for i in range(0, len(stream), cs):
            chunk = stream[i:i + cs]
            hasher.update(chunk)
            total += len(chunk)
        return DataFingerprint(
            value=hasher.hexdigest(),
            algorithm=self._config.algorithm,
            length=total,
            source_type="stream",
        )

    def fingerprint_dict(self, data: Dict[str, Any],
                          normalize: bool = True) -> DataFingerprint:
        """Fingerprint a dictionary with optional key normalization."""
        if normalize and self._config.normalize:
            data = self._normalize_dict(data)
        raw = self._serialize(data)
        return DataFingerprint(
            value=self._hash(raw),
            algorithm=self._config.algorithm,
            length=len(raw),
            source_type="dict",
        )

    def fingerprint_set(self, items: List[Any]) -> DataFingerprint:
        """Fingerprint a set-like collection (order-independent)."""
        normalized = sorted(self._serialize(item) for item in items)
        combined = b"".join(n.encode() for n in normalized)
        return DataFingerprint(
            value=self._hash(combined),
            algorithm=self._config.algorithm,
            length=len(combined),
            source_type="set",
        )

    def rolling_fingerprint(self, data: bytes, window_size: int = 4) -> List[str]:
        """
        Generate rolling fingerprints using a sliding window.
        Useful for finding identical subsequences in large data.
        """
        fingerprints = []
        for i in range(len(data) - window_size + 1):
            window = data[i:i + window_size]
            fingerprints.append(self._hash(window))
        return fingerprints

    def compare(self, fp1: DataFingerprint, fp2: DataFingerprint) -> bool:
        """Compare two fingerprints for equality."""
        return fp1.value == fp2.value and fp1.algorithm == fp2.algorithm

    def verify(self, data: Any, expected: DataFingerprint) -> bool:
        """Verify data matches an expected fingerprint."""
        actual = self.fingerprint(data, source_type=expected.source_type)
        return self.compare(actual, expected)

    def _normalize_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively normalize dict by sorting keys and filtering excluded fields."""
        if not isinstance(data, dict):
            return data
        result = {}
        for key in sorted(data.keys()):
            if key in self._config.exclude_fields:
                continue
            value = data[key]
            if isinstance(value, dict):
                result[key] = self._normalize_dict(value)
            elif isinstance(value, list):
                result[key] = [self._normalize_dict(v) if isinstance(v, dict) else v for v in value]
            else:
                result[key] = value
        return result

    def _serialize(self, data: Any) -> str:
        """Serialize data to a canonical string representation."""
        if data is None:
            return "null"
        if isinstance(data, bool):
            return "bool:" + str(data)
        if isinstance(data, (int, float)):
            return f"{type(data).__name__}:{data}"
        if isinstance(data, str):
            return f"str:{data}"
        if isinstance(data, bytes):
            return f"bytes:{data.decode('utf-8', errors='replace')}"
        if isinstance(data, dict):
            parts = [f"{k}={self._serialize(v)}" for k, v in sorted(data.items())]
            return "dict:{" + ",".join(parts) + "}"
        if isinstance(data, (list, tuple)):
            return "list:[" + ",".join(self._serialize(item) for item in data) + "]"
        return f"unknown:{repr(data)}"

    def _create_hasher(self) -> Any:
        """Create a hasher for the configured algorithm."""
        alg = self._config.algorithm
        if alg == FingerprintAlgorithm.MD5:
            return hashlib.md5()
        elif alg == FingerprintAlgorithm.SHA1:
            return hashlib.sha1()
        elif alg == FingerprintAlgorithm.SHA256:
            return hashlib.sha256()
        elif alg == FingerprintAlgorithm.SHA512:
            return hashlib.sha512()
        elif alg == FingerprintAlgorithm.BLAKE2B:
            return hashlib.blake2b()
        elif alg == FingerprintAlgorithm.BLAKE3:
            try:
                import blake3
                return blake3.blake3()
            except ImportError:
                return hashlib.blake3_256()
        elif alg == FingerprintAlgorithm.XXHASH64:
            try:
                import xxhash
                h = xxhash.xxh64()
                return h
            except ImportError:
                return hashlib.sha256()  # Fallback
        return hashlib.sha256()

    def _hash(self, data: Union[str, bytes]) -> str:
        """Hash data using the configured algorithm."""
        if isinstance(data, str):
            data = data.encode("utf-8")
        hasher = self._create_hasher()
        hasher.update(data)
        return hasher.hexdigest()


def fingerprint_bytes(data: bytes, algorithm: str = "sha256") -> str:
    """Convenience function for quick fingerprinting."""
    alg = DataFingerprinter.ALGORITHM_NAMES.get(algorithm, FingerprintAlgorithm.SHA256)
    config = FingerprintConfig(algorithm=alg)
    fp = DataFingerprinter(config).fingerprint_stream(data)
    return fp.value


def content_address(data: bytes, algorithm: str = "sha256") -> str:
    """Generate a content-addressable identifier (CID-like)."""
    prefix = algorithm[:7].encode() if isinstance(algorithm, str) else b"sha256"
    raw_hash = fingerprint_bytes(data, algorithm)
    return f"{prefix.decode()}-{raw_hash}"
