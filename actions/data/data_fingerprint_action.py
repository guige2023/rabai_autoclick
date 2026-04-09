"""Data fingerprinting for integrity verification and deduplication.

Provides content-addressable storage, deduplication, and integrity
checking through cryptographic fingerprints.
"""

from __future__ import annotations

import hashlib
import json
import mmh3
import threading
import time
import uuid
import zlib
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import copy


class HashAlgorithm(Enum):
    """Supported hash algorithms."""
    MD5 = "md5"
    SHA1 = "sha1"
    SHA256 = "sha256"
    SHA512 = "sha512"
    MURMUR3 = "murmur3"
    XXHASH = "xxhash"
    CRC32 = "crc32"


@dataclass
class Fingerprint:
    """A data fingerprint."""
    fingerprint_id: str
    content_hash: str
    algorithm: HashAlgorithm
    size_bytes: int
    created_at: float = field(default_factory=time.time)
    chunk_count: int = 1
    chunk_hashes: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DeduplicationResult:
    """Result of deduplication check."""
    is_duplicate: bool
    original_fingerprint_id: Optional[str] = None
    similarity: float = 1.0
    matching_chunks: int = 0


class ContentHasher:
    """Computes various types of content fingerprints."""

    @staticmethod
    def md5(data: Union[str, bytes]) -> str:
        """Compute MD5 hash."""
        if isinstance(data, str):
            data = data.encode('utf-8')
        return hashlib.md5(data).hexdigest()

    @staticmethod
    def sha1(data: Union[str, bytes]) -> str:
        """Compute SHA1 hash."""
        if isinstance(data, str):
            data = data.encode('utf-8')
        return hashlib.sha1(data).hexdigest()

    @staticmethod
    def sha256(data: Union[str, bytes]) -> str:
        """Compute SHA256 hash."""
        if isinstance(data, str):
            data = data.encode('utf-8')
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def sha512(data: Union[str, bytes]) -> str:
        """Compute SHA512 hash."""
        if isinstance(data, str):
            data = data.encode('utf-8')
        return hashlib.sha512(data).hexdigest()

    @staticmethod
    def murmur3_32(data: Union[str, bytes]) -> str:
        """Compute MurmurHash3 (32-bit)."""
        if isinstance(data, str):
            data = data.encode('utf-8')
        return format(mmh3.hash(data), '08x')

    @staticmethod
    def crc32(data: Union[str, bytes]) -> str:
        """Compute CRC32 checksum."""
        if isinstance(data, str):
            data = data.encode('utf-8')
        return format(zlib.crc32(data) & 0xFFFFFFFF, '08x')

    @classmethod
    def compute(
        cls,
        data: Union[str, bytes],
        algorithm: HashAlgorithm,
    ) -> str:
        """Compute hash using specified algorithm."""
        if algorithm == HashAlgorithm.MD5:
            return cls.md5(data)
        elif algorithm == HashAlgorithm.SHA1:
            return cls.sha1(data)
        elif algorithm == HashAlgorithm.SHA256:
            return cls.sha256(data)
        elif algorithm == HashAlgorithm.SHA512:
            return cls.sha512(data)
        elif algorithm == HashAlgorithm.MURMUR3:
            return cls.murmur3_32(data)
        elif algorithm == HashAlgorithm.CRC32:
            return cls.crc32(data)
        else:
            return cls.sha256(data)


class ChunkingStrategy:
    """Content chunking for large data deduplication."""

    FIXED = "fixed"
    RABIN = "rabin"
    CONTENTDEFINED = "content_defined"

    @staticmethod
    def fixed_chunk(
        data: Union[str, bytes],
        chunk_size: int = 1024,
    ) -> List[bytes]:
        """Split data into fixed-size chunks."""
        if isinstance(data, str):
            data = data.encode('utf-8')
        return [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]

    @staticmethod
    def content_defined_chunk(
        data: Union[str, bytes],
        min_size: int = 256,
        max_size: int = 4096,
        avg_size: int = 1024,
    ) -> List[bytes]:
        """Content-defined chunking using rolling hash."""
        if isinstance(data, str):
            data = data.encode('utf-8')

        chunks = []
        i = 0
        data_len = len(data)

        while i < data_len:
            window_size = min(avg_size, data_len - i)
            end = min(i + max_size, data_len)

            chunk_end = i + min_size
            best_cut = chunk_end

            for j in range(chunk_end, min(end, i + avg_size + 64)):
                if j >= data_len:
                    break
                byte_val = data[j] if j < data_len else 0
                if j >= chunk_end and (byte_val & 0x03) == 0:
                    best_cut = j
                    break

            chunk_end = min(best_cut, data_len)
            chunks.append(data[i:chunk_end])
            i = chunk_end

        return chunks


class FingerprintStore:
    """Storage for fingerprints with deduplication support."""

    def __init__(self):
        self._fingerprints: Dict[str, Fingerprint] = {}
        self._content_index: Dict[str, str] = {}
        self._lock = threading.RLock()

    def add(self, fingerprint: Fingerprint) -> bool:
        """Add a fingerprint to the store. Returns True if new, False if duplicate."""
        with self._lock:
            if fingerprint.content_hash in self._content_index:
                return False

            self._fingerprints[fingerprint.fingerprint_id] = fingerprint
            self._content_index[fingerprint.content_hash] = fingerprint.fingerprint_id
            return True

    def get(self, fingerprint_id: str) -> Optional[Fingerprint]:
        """Get a fingerprint by ID."""
        with self._lock:
            return copy.deepcopy(self._fingerprints.get(fingerprint_id))

    def get_by_hash(self, content_hash: str) -> Optional[Fingerprint]:
        """Get a fingerprint by content hash."""
        with self._lock:
            fingerprint_id = self._content_index.get(content_hash)
            if fingerprint_id:
                return copy.deepcopy(self._fingerprints.get(fingerprint_id))
            return None

    def exists(self, content_hash: str) -> bool:
        """Check if content hash already exists."""
        with self._lock:
            return content_hash in self._content_index

    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        with self._lock:
            total_size = sum(f.size_bytes for f in self._fingerprints.values())
            unique_chunks = sum(f.chunk_count for f in self._fingerprints.values())
            return {
                "total_fingerprints": len(self._fingerprints),
                "total_size_bytes": total_size,
                "total_chunks": unique_chunks,
                "avg_chunk_size": total_size / unique_chunks if unique_chunks > 0 else 0,
            }


class AutomationFingerprintAction:
    """Action providing data fingerprinting for automation workflows."""

    def __init__(
        self,
        store: Optional[FingerprintStore] = None,
        default_algorithm: str = "sha256",
    ):
        self._store = store or FingerprintStore()
        try:
            self._default_algorithm = HashAlgorithm(default_algorithm.lower())
        except ValueError:
            self._default_algorithm = HashAlgorithm.SHA256

    def compute(
        self,
        data: Any,
        algorithm: str = "sha256",
        chunk: bool = False,
        chunk_size: int = 1024,
    ) -> Dict[str, Any]:
        """Compute fingerprint for data.

        Args:
            data: Data to fingerprint
            algorithm: Hash algorithm to use
            chunk: Whether to chunk large data
            chunk_size: Size of chunks if chunking

        Returns:
            Fingerprint information including ID, hash, and metadata
        """
        try:
            algo = HashAlgorithm(algorithm.lower())
        except ValueError:
            algo = self._default_algorithm

        if chunk:
            chunks = ChunkingStrategy.fixed_chunk(str(data), chunk_size)
            chunk_hashes = [
                ContentHasher.compute(chunk_data, algo)
                for chunk_data in chunks
            ]
            combined = ''.join(chunk_hashes)
            content_hash = ContentHasher.compute(combined, algo)
            chunk_count = len(chunks)
        else:
            content_hash = ContentHasher.compute(str(data), algo)
            chunk_hashes = []
            chunk_count = 1

        data_bytes = str(data).encode('utf-8')
        size_bytes = len(data_bytes)

        fingerprint = Fingerprint(
            fingerprint_id=str(uuid.uuid4())[:16],
            content_hash=content_hash,
            algorithm=algo,
            size_bytes=size_bytes,
            chunk_count=chunk_count,
            chunk_hashes=chunk_hashes,
        )

        is_new = self._store.add(fingerprint)

        return {
            "fingerprint_id": fingerprint.fingerprint_id,
            "content_hash": content_hash,
            "algorithm": algo.value,
            "size_bytes": size_bytes,
            "chunk_count": chunk_count,
            "is_new": is_new,
            "created_at": datetime.fromtimestamp(fingerprint.created_at).isoformat(),
        }

    def verify(
        self,
        data: Any,
        fingerprint_id: Optional[str] = None,
        content_hash: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Verify data matches a fingerprint."""
        if fingerprint_id:
            stored = self._store.get(fingerprint_id)
            if not stored:
                return {"verified": False, "error": "Fingerprint not found"}
            expected_hash = stored.content_hash
            algorithm = stored.algorithm
        elif content_hash:
            expected_hash = content_hash
            algorithm = self._default_algorithm
        else:
            return {"verified": False, "error": "Must provide fingerprint_id or content_hash"}

        actual_hash = ContentHasher.compute(str(data), algorithm)

        return {
            "verified": actual_hash == expected_hash,
            "expected_hash": expected_hash,
            "actual_hash": actual_hash,
            "algorithm": algorithm.value,
        }

    def deduplicate(
        self,
        data: Any,
        algorithm: str = "sha256",
    ) -> DeduplicationResult:
        """Check if data is a duplicate of existing content."""
        try:
            algo = HashAlgorithm(algorithm.lower())
        except ValueError:
            algo = self._default_algorithm

        content_hash = ContentHasher.compute(str(data), algo)

        existing = self._store.get_by_hash(content_hash)
        if existing:
            return DeduplicationResult(
                is_duplicate=True,
                original_fingerprint_id=existing.fingerprint_id,
                similarity=1.0,
                matching_chunks=existing.chunk_count,
            )

        return DeduplicationResult(
            is_duplicate=False,
            similarity=0.0,
            matching_chunks=0,
        )

    def execute(
        self,
        context: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute a fingerprinting operation.

        Required params:
            operation: str - 'compute', 'verify', or 'deduplicate'
            data: Any - Data to process

        Optional params:
            algorithm: str - Hash algorithm (default: sha256)
            fingerprint_id: str - For verify operation
            content_hash: str - For verify operation
            chunk: bool - Whether to chunk data
            chunk_size: int - Chunk size
        """
        operation = params.get("operation")
        data = params.get("data")

        if not data:
            raise ValueError("data is required")

        algorithm = params.get("algorithm", "sha256")
        chunk = params.get("chunk", False)
        chunk_size = params.get("chunk_size", 1024)

        if operation == "compute":
            return self.compute(data, algorithm, chunk, chunk_size)

        elif operation == "verify":
            fingerprint_id = params.get("fingerprint_id")
            content_hash = params.get("content_hash")
            return self.verify(data, fingerprint_id, content_hash)

        elif operation == "deduplicate":
            return {
                "is_duplicate": self.deduplicate(data, algorithm).is_duplicate,
                "original_fingerprint_id": self.deduplicate(data, algorithm).original_fingerprint_id,
            }

        else:
            raise ValueError(f"Unknown operation: {operation}")

    def get_fingerprint(self, fingerprint_id: str) -> Optional[Dict[str, Any]]:
        """Get fingerprint details by ID."""
        fp = self._store.get(fingerprint_id)
        if not fp:
            return None
        return {
            "fingerprint_id": fp.fingerprint_id,
            "content_hash": fp.content_hash,
            "algorithm": fp.algorithm.value,
            "size_bytes": fp.size_bytes,
            "chunk_count": fp.chunk_count,
            "chunk_hashes": fp.chunk_hashes,
            "created_at": datetime.fromtimestamp(fp.created_at).isoformat(),
            "metadata": fp.metadata,
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        return self._store.get_stats()
