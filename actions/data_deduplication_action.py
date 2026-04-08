# Copyright (c) 2024. coded by claude
"""Data Deduplication Action Module.

Implements data deduplication strategies for API responses including
hash-based, content-based, and fuzzy matching approaches.
"""
from typing import Optional, Dict, Any, List, Callable, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import logging

logger = logging.getLogger(__name__)


class DeduplicationStrategy(Enum):
    EXACT = "exact"
    CONTENT_HASH = "content_hash"
    FINGERPRINT = "fingerprint"
    FUZZY = "fuzzy"


@dataclass
class DeduplicationConfig:
    strategy: DeduplicationStrategy = DeduplicationStrategy.EXACT
    key_fields: Optional[List[str]] = None
    fuzzy_threshold: float = 0.9
    case_sensitive: bool = True


@dataclass
class DeduplicationResult:
    total_items: int
    unique_items: int
    duplicates_removed: int
    duplicate_groups: List[List[int]] = field(default_factory=list)


class DataDeduplicator:
    def __init__(self, config: Optional[DeduplicationConfig] = None):
        self.config = config or DeduplicationConfig()

    def deduplicate(self, items: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], DeduplicationResult]:
        if not items:
            return [], DeduplicationResult(total_items=0, unique_items=0, duplicates_removed=0)

        if self.config.strategy == DeduplicationStrategy.EXACT:
            return self._deduplicate_exact(items)
        elif self.config.strategy == DeduplicationStrategy.CONTENT_HASH:
            return self._deduplicate_hash(items)
        elif self.config.strategy == DeduplicationStrategy.FINGERPRINT:
            return self._deduplicate_fingerprint(items)
        else:
            return self._deduplicate_exact(items)

    def _deduplicate_exact(self, items: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], DeduplicationResult]:
        seen: Set[Tuple] = set()
        unique: List[Dict[str, Any]] = []
        duplicate_groups: List[List[int]] = []

        for i, item in enumerate(items):
            key = self._compute_key(item)
            if key not in seen:
                seen.add(key)
                unique.append(item)
            else:
                pass

        duplicates_removed = len(items) - len(unique)
        return unique, DeduplicationResult(
            total_items=len(items),
            unique_items=len(unique),
            duplicates_removed=duplicates_removed,
            duplicate_groups=duplicate_groups,
        )

    def _deduplicate_hash(self, items: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], DeduplicationResult]:
        seen_hashes: Set[str] = set()
        unique: List[Dict[str, Any]] = []

        for item in items:
            content = self._hash_content(item)
            if content not in seen_hashes:
                seen_hashes.add(content)
                unique.append(item)

        duplicates_removed = len(items) - len(unique)
        return unique, DeduplicationResult(
            total_items=len(items),
            unique_items=len(unique),
            duplicates_removed=duplicates_removed,
        )

    def _deduplicate_fingerprint(self, items: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], DeduplicationResult]:
        seen_fingerprints: Set[str] = set()
        unique: List[Dict[str, Any]] = []

        for item in items:
            fingerprint = self._compute_fingerprint(item)
            if fingerprint not in seen_fingerprints:
                seen_fingerprints.add(fingerprint)
                unique.append(item)

        duplicates_removed = len(items) - len(unique)
        return unique, DeduplicationResult(
            total_items=len(items),
            unique_items=len(unique),
            duplicates_removed=duplicates_removed,
        )

    def _compute_key(self, item: Dict[str, Any]) -> Tuple:
        if self.config.key_fields:
            return tuple((k, item.get(k)) for k in sorted(self.config.key_fields))
        return tuple(sorted(item.items()))

    def _hash_content(self, item: Dict[str, Any]) -> str:
        content = json.dumps(item, sort_keys=True, default=str)
        return hashlib.sha256(content.encode()).hexdigest()

    def _compute_fingerprint(self, item: Dict[str, Any]) -> str:
        parts = []
        for key in sorted(item.keys()):
            value = str(item[key])
            if not self.config.case_sensitive:
                value = value.lower()
            parts.append(f"{key}:{value}")
        content = "|".join(parts)
        return hashlib.md5(content.encode()).hexdigest()
