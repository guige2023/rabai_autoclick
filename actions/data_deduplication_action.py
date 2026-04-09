"""Data deduplication action for removing duplicate records.

Provides various deduplication strategies including
exact match, fuzzy match, and subset matching.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class DeduplicationStrategy(Enum):
    EXACT = "exact"
    FUZZY = "fuzzy"
    SUBSET = "subset"
    SEMANTIC = "semantic"


@dataclass
class DuplicateGroup:
    group_id: str
    record_ids: list[str]
    similarity: float
    representative_id: Optional[str] = None


class DataDeduplicationAction:
    """Remove duplicate records from datasets.

    Args:
        default_strategy: Default deduplication strategy.
        similarity_threshold: Similarity threshold for fuzzy matching.
    """

    def __init__(
        self,
        default_strategy: DeduplicationStrategy = DeduplicationStrategy.EXACT,
        similarity_threshold: float = 0.85,
    ) -> None:
        self._default_strategy = default_strategy
        self._similarity_threshold = similarity_threshold
        self._duplicate_groups: list[DuplicateGroup] = []
        self._stats = {
            "total_checked": 0,
            "duplicates_found": 0,
            "records_removed": 0,
        }

    def find_duplicates(
        self,
        records: list[dict[str, Any]],
        key_fields: list[str],
        strategy: Optional[DeduplicationStrategy] = None,
    ) -> list[DuplicateGroup]:
        """Find duplicate records.

        Args:
            records: List of records.
            key_fields: Fields to use for comparison.
            strategy: Deduplication strategy.

        Returns:
            List of duplicate groups.
        """
        strategy = strategy or self._default_strategy

        if strategy == DeduplicationStrategy.EXACT:
            return self._find_exact_duplicates(records, key_fields)
        elif strategy == DeduplicationStrategy.FUZZY:
            return self._find_fuzzy_duplicates(records, key_fields)
        elif strategy == DeduplicationStrategy.SUBSET:
            return self._find_subset_duplicates(records, key_fields)

        return []

    def _find_exact_duplicates(
        self,
        records: list[dict[str, Any]],
        key_fields: list[str],
    ) -> list[DuplicateGroup]:
        """Find exact duplicate records.

        Args:
            records: List of records.
            key_fields: Fields to use for comparison.

        Returns:
            List of duplicate groups.
        """
        seen: dict[str, list[str]] = {}
        duplicate_groups: list[DuplicateGroup] = []

        for i, record in enumerate(records):
            key = self._compute_key(record, key_fields)
            if key in seen:
                seen[key].append(str(i))
            else:
                seen[key] = [str(i)]

        for key, record_ids in seen.items():
            if len(record_ids) > 1:
                group = DuplicateGroup(
                    group_id=f"exact_{key[:16]}",
                    record_ids=record_ids,
                    similarity=1.0,
                    representative_id=record_ids[0],
                )
                duplicate_groups.append(group)

        self._duplicate_groups = duplicate_groups
        self._stats["duplicates_found"] = sum(len(g.record_ids) for g in duplicate_groups)
        return duplicate_groups

    def _find_fuzzy_duplicates(
        self,
        records: list[dict[str, Any]],
        key_fields: list[str],
    ) -> list[DuplicateGroup]:
        """Find fuzzy duplicate records.

        Args:
            records: List of records.
            key_fields: Fields to use for comparison.

        Returns:
            List of duplicate groups.
        """
        duplicate_groups: list[DuplicateGroup] = []
        assigned: set[str] = set()

        for i, record in enumerate(records):
            record_id = str(i)
            if record_id in assigned:
                continue

            current_key = self._compute_key(record, key_fields)
            similar: list[str] = [record_id]

            for j, other in enumerate(records[i + 1:], start=i + 1):
                other_id = str(j)
                if other_id in assigned:
                    continue

                other_key = self._compute_key(other, key_fields)
                similarity = self._compute_similarity(current_key, other_key)

                if similarity >= self._similarity_threshold:
                    similar.append(other_id)
                    assigned.add(other_id)

            if len(similar) > 1:
                assigned.add(record_id)
                group = DuplicateGroup(
                    group_id=f"fuzzy_{len(duplicate_groups)}",
                    record_ids=similar,
                    similarity=self._similarity_threshold,
                    representative_id=similar[0],
                )
                duplicate_groups.append(group)

        self._duplicate_groups = duplicate_groups
        self._stats["duplicates_found"] = sum(len(g.record_ids) for g in duplicate_groups)
        return duplicate_groups

    def _find_subset_duplicates(
        self,
        records: list[dict[str, Any]],
        key_fields: list[str],
    ) -> list[DuplicateGroup]:
        """Find records that are subsets of others.

        Args:
            records: List of records.
            key_fields: Fields to use for comparison.

        Returns:
            List of duplicate groups.
        """
        duplicate_groups: list[DuplicateGroup] = []

        for i, record in enumerate(records):
            record_id = str(i)
            key = self._compute_key(record, key_fields)

            for j, other in enumerate(records[i + 1:], start=i + 1):
                other_key = self._compute_key(other, key_fields)

                if self._is_subset(key, other_key):
                    group = DuplicateGroup(
                        group_id=f"subset_{i}_{j}",
                        record_ids=[record_id, str(j)],
                        similarity=0.8,
                        representative_id=str(j),
                    )
                    duplicate_groups.append(group)

        self._duplicate_groups = duplicate_groups
        self._stats["duplicates_found"] = sum(len(g.record_ids) for g in duplicate_groups)
        return duplicate_groups

    def deduplicate(
        self,
        records: list[dict[str, Any]],
        key_fields: list[str],
        strategy: Optional[DeduplicationStrategy] = None,
        keep: str = "first",
    ) -> list[dict[str, Any]]:
        """Remove duplicates from records.

        Args:
            records: List of records.
            key_fields: Fields to use for comparison.
            strategy: Deduplication strategy.
            keep: Which record to keep ('first', 'last').

        Returns:
            Deduplicated records.
        """
        duplicate_groups = self.find_duplicates(records, key_fields, strategy)

        indices_to_remove: set[int] = set()
        for group in duplicate_groups:
            keep_idx = 0 if keep == "first" else len(group.record_ids) - 1
            for idx, record_id in enumerate(group.record_ids):
                if idx != keep_idx:
                    indices_to_remove.add(int(record_id))

        self._stats["records_removed"] = len(indices_to_remove)

        result = [r for i, r in enumerate(records) if i not in indices_to_remove]
        return result

    def _compute_key(
        self,
        record: dict[str, Any],
        key_fields: list[str],
    ) -> str:
        """Compute a comparison key for a record.

        Args:
            record: Record.
            key_fields: Fields to use.

        Returns:
            Comparison key string.
        """
        values = [str(record.get(f, "")) for f in key_fields]
        return "|".join(values).lower()

    def _compute_similarity(self, key1: str, key2: str) -> float:
        """Compute similarity between two keys.

        Args:
            key1: First key.
            key2: Second key.

        Returns:
            Similarity score (0-1).
        """
        if key1 == key2:
            return 1.0

        words1 = set(key1.split())
        words2 = set(key2.split())

        if not words1 or not words2:
            return 0.0

        intersection = len(words1 & words2)
        union = len(words1 | words2)

        return intersection / union if union > 0 else 0.0

    def _is_subset(self, key1: str, key2: str) -> bool:
        """Check if key1 is a subset of key2.

        Args:
            key1: Potential subset key.
            key2: Superset key.

        Returns:
            True if key1 is subset of key2.
        """
        words1 = set(key1.split())
        words2 = set(key2.split())
        return words1.issubset(words2) and words1 != words2

    def get_duplicate_groups(self) -> list[DuplicateGroup]:
        """Get found duplicate groups.

        Returns:
            List of duplicate groups.
        """
        return self._duplicate_groups

    def get_stats(self) -> dict[str, Any]:
        """Get deduplication statistics.

        Returns:
            Dictionary with stats.
        """
        return {
            **self._stats,
            "default_strategy": self._default_strategy.value,
            "similarity_threshold": self._similarity_threshold,
            "duplicate_groups": len(self._duplicate_groups),
        }

    def reset_stats(self) -> None:
        """Reset statistics counters."""
        self._stats = {
            "total_checked": 0,
            "duplicates_found": 0,
            "records_removed": 0,
        }
        self._duplicate_groups.clear()
