"""Data Deduplicator Action Module.

Provides record deduplication using multiple strategies:
exact match, fuzzy match, hash-based, and custom key functions.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import logging

logger = logging.getLogger(__name__)


class DedupStrategy(Enum):
    """Deduplication strategy."""
    EXACT = "exact"
    FUZZY = "fuzzy"
    HASH_BASED = "hash_based"
    CUSTOM_KEY = "custom_key"


@dataclass
class DedupConfig:
    """Deduplication configuration."""
    strategy: DedupStrategy = DedupStrategy.EXACT
    fields: Optional[List[str]] = None
    hash_fields: Optional[List[str]] = None
    custom_key: Optional[Callable[[Dict], Any]] = None
    fuzzy_threshold: float = 0.85
    keep: str = "first"


class DataDeduplicatorAction:
    """Record deduplicator with multiple strategies.

    Example:
        dedup = DataDeduplicatorAction()

        result = dedup.deduplicate(
            data=[
                {"id": 1, "email": "alice@example.com"},
                {"id": 2, "email": "alice@example.com"},
                {"id": 3, "email": "bob@example.com"},
            ],
            config=DedupConfig(
                strategy=DedupStrategy.EXACT,
                fields=["email"],
                keep="first"
            )
        )
    """

    def __init__(self) -> None:
        self._seen: Dict[Any, Any] = {}

    def deduplicate(
        self,
        data: List[Dict[str, Any]],
        config: DedupConfig,
    ) -> List[Dict[str, Any]]:
        """Deduplicate data.

        Args:
            data: List of records
            config: Deduplication configuration

        Returns:
            Deduplicated list
        """
        if not data:
            return []

        self._seen.clear()

        if config.strategy == DedupStrategy.EXACT:
            return self._deduplicate_exact(data, config)
        elif config.strategy == DedupStrategy.HASH_BASED:
            return self._deduplicate_hash(data, config)
        elif config.strategy == DedupStrategy.CUSTOM_KEY:
            return self._deduplicate_custom_key(data, config)
        elif config.strategy == DedupStrategy.FUZZY:
            return self._deduplicate_fuzzy(data, config)

        return data

    def _deduplicate_exact(
        self,
        data: List[Dict[str, Any]],
        config: DedupConfig,
    ) -> List[Dict[str, Any]]:
        """Deduplicate by exact field match."""
        result: List[Dict[str, Any]] = []

        for record in data:
            key = self._make_key(record, config.fields or [])

            if key not in self._seen:
                self._seen[key] = record
                result.append(record)
            else:
                logger.debug(f"Duplicate found: {key}")

        return result

    def _deduplicate_hash(
        self,
        data: List[Dict[str, Any]],
        config: DedupConfig,
    ) -> List[Dict[str, Any]]:
        """Deduplicate by hash of specified fields."""
        result: List[Dict[str, Any]] = []

        for record in data:
            hash_value = self._compute_hash(record, config.hash_fields or list(record.keys()))

            if hash_value not in self._seen:
                self._seen[hash_value] = record
                result.append(record)

        return result

    def _deduplicate_custom_key(
        self,
        data: List[Dict[str, Any]],
        config: DedupConfig,
    ) -> List[Dict[str, Any]]:
        """Deduplicate using custom key function."""
        result: List[Dict[str, Any]] = []

        for record in data:
            key = config.custom_key(record) if config.custom_key else record

            if key not in self._seen:
                self._seen[key] = record
                result.append(record)

        return result

    def _deduplicate_fuzzy(
        self,
        data: List[Dict[str, Any]],
        config: DedupConfig,
    ) -> List[Dict[str, Any]]:
        """Deduplicate using fuzzy matching."""
        result: List[Dict[str, Any]] = []
        seen_values: List[Tuple[str, Dict]] = []

        for record in data:
            key_value = self._make_key(record, config.fields or [])

            is_duplicate = False
            for seen_value, seen_record in seen_values:
                similarity = self._calculate_similarity(str(key_value), str(seen_value))
                if similarity >= config.fuzzy_threshold:
                    is_duplicate = True
                    logger.debug(f"Fuzzy duplicate: {key_value} ~ {seen_value}")
                    break

            if not is_duplicate:
                seen_values.append((str(key_value), record))
                result.append(record)

        return result

    def _make_key(self, record: Dict, fields: List[str]) -> Tuple:
        """Make lookup key from fields."""
        return tuple(record.get(f) for f in fields)

    def _compute_hash(self, record: Dict, fields: List[str]) -> str:
        """Compute hash of specified fields."""
        content = "|".join(str(record.get(f, "")) for f in fields)
        return hashlib.sha256(content.encode()).hexdigest()

    def _calculate_similarity(self, s1: str, s2: str) -> float:
        """Calculate string similarity (Jaccard)."""
        set1 = set(s1.lower().split())
        set2 = set(s2.lower().split())

        if not set1 and not set2:
            return 1.0

        intersection = len(set1 & set2)
        union = len(set1 | set2)

        return intersection / union if union > 0 else 0.0

    def find_duplicates(
        self,
        data: List[Dict[str, Any]],
        config: DedupConfig,
    ) -> List[List[Dict[str, Any]]]:
        """Find all duplicate groups.

        Returns:
            List of duplicate groups
        """
        self._seen.clear()
        groups: List[List[Dict[str, Any]]] = []

        for record in data:
            if config.strategy == DedupStrategy.EXACT:
                key = self._make_key(record, config.fields or [])
            elif config.strategy == DedupStrategy.HASH_BASED:
                key = self._compute_hash(record, config.hash_fields or list(record.keys()))
            else:
                key = config.custom_key(record) if config.custom_key else record

            if key not in self._seen:
                self._seen[key] = []
            self._seen[key].append(record)

        return [group for group in self._seen.values() if len(group) > 1]
