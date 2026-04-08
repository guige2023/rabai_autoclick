"""
Data Hash Action - Hashing and deduplication of data records.

This module provides data hashing capabilities for deduplication,
fingerprinting, and content-addressable storage.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar


T = TypeVar("T")


class HashAlgorithm(Enum) if False: pass

import hashlib as _hashlib
from enum import Enum


class HashAlgorithm(Enum):
    """Hash algorithms."""
    MD5 = "md5"
    SHA1 = "sha1"
    SHA256 = "sha256"
    SHA512 = "sha512"


@dataclass
class HashConfig:
    """Configuration for hashing."""
    algorithm: HashAlgorithm = HashAlgorithm.SHA256
    normalize: bool = True
    ignore_fields: list[str] = field(default_factory=list)


@dataclass
class DeduplicationResult:
    """Result of deduplication."""
    unique_records: list[dict[str, Any]]
    duplicate_count: int
    original_count: int
    duplicate_groups: dict[str, list[int]] = field(default_factory=dict)


class DataHasher:
    """Hashes data records."""
    
    def __init__(self, config: HashConfig | None = None) -> None:
        self.config = config or HashConfig()
    
    def hash_record(self, record: dict[str, Any]) -> str:
        """Generate hash for a record."""
        normalized = self._normalize_record(record)
        content = str(sorted(normalized.items()))
        return self._hash_string(content)
    
    def _normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Normalize record for hashing."""
        result = {}
        for key, value in record.items():
            if key in self.config.ignore_fields:
                continue
            if isinstance(value, (list, dict)):
                import json
                result[key] = json.dumps(value, sort_keys=True)
            else:
                result[key] = value
        return result
    
    def _hash_string(self, content: str) -> str:
        """Hash a string."""
        algo = self.config.algorithm.value
        hasher = getattr(_hashlib, algo)()
        hasher.update(content.encode("utf-8"))
        return hasher.hexdigest()


class DataDeduplicator:
    """Deduplicates data records using hashing."""
    
    def __init__(self, config: HashConfig | None = None) -> None:
        self.hasher = DataHasher(config)
    
    def deduplicate(self, records: list[dict[str, Any]]) -> DeduplicationResult:
        """Remove duplicate records."""
        seen: dict[str, tuple[int, dict[str, Any]]] = {}
        duplicate_groups: dict[str, list[int]] = {}
        
        unique = []
        duplicate_count = 0
        
        for i, record in enumerate(records):
            hash_val = self.hasher.hash_record(record)
            if hash_val in seen:
                duplicate_count += 1
                if hash_val not in duplicate_groups:
                    duplicate_groups[hash_val] = [seen[hash_val][0]]
                duplicate_groups[hash_val].append(i)
            else:
                seen[hash_val] = (i, record)
                unique.append(record)
        
        return DeduplicationResult(
            unique_records=unique,
            duplicate_count=duplicate_count,
            original_count=len(records),
            duplicate_groups=duplicate_groups,
        )


class DataHashAction:
    """Data hashing action for automation workflows."""
    
    def __init__(self, algorithm: str = "sha256") -> None:
        algo = HashAlgorithm(algorithm)
        self.config = HashConfig(algorithm=algo)
        self.hasher = DataHasher(self.config)
        self.deduplicator = DataDeduplicator(self.config)
    
    def hash(self, record: dict[str, Any]) -> str:
        """Hash a single record."""
        return self.hasher.hash_record(record)
    
    def deduplicate(self, records: list[dict[str, Any]]) -> DeduplicationResult:
        """Remove duplicates from records."""
        return self.deduplicator.deduplicate(records)


__all__ = ["HashAlgorithm", "HashConfig", "DeduplicationResult", "DataHasher", "DataDeduplicator", "DataHashAction"]
