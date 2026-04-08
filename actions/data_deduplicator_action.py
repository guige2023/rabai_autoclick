"""
Data Deduplicator Action Module.

Detects and removes duplicate records from datasets using
 configurable keys and similarity matching.
"""

from __future__ import annotations

from typing import Any, Callable, Optional
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


class DeduplicationStrategy(Enum):
    """Strategy for handling duplicates."""
    KEEP_FIRST = "keep_first"
    KEEP_LAST = "keep_last"
    KEEP_MOST_COMPLETE = "keep_most_complete"


@dataclass
class DuplicateGroup:
    """A group of duplicate records."""
    records: list[dict[str, Any]]
    key: str
    primary_index: int = 0


@dataclass
class DeduplicationResult:
    """Result of deduplication operation."""
    deduplicated: list[dict[str, Any]]
    duplicates_removed: int = 0
    duplicate_groups: int = 0


class DataDeduplicatorAction:
    """
    Dataset deduplication with multiple strategies.

    Detects and removes duplicate records using exact matching
    on key fields or fuzzy similarity matching.

    Example:
        dedup = DataDeduplicatorAction()
        dedup.add_key("email")
        dedup.add_key("phone")
        result = dedup.deduplicate(data)
    """

    def __init__(
        self,
        strategy: DeduplicationStrategy = DeduplicationStrategy.KEEP_FIRST,
    ) -> None:
        self.strategy = strategy
        self._key_fields: list[str] = []
        self._similarity_threshold: float = 0.0
        self._similarity_func: Optional[Callable[[dict, dict], float]] = None

    def add_key(self, field: str) -> "DataDeduplicatorAction":
        """Add a key field for deduplication."""
        self._key_fields.append(field)
        return self

    def set_similarity(
        self,
        threshold: float,
        similarity_func: Optional[Callable[[dict, dict], float]] = None,
    ) -> "DataDeduplicatorAction":
        """Set similarity-based deduplication."""
        self._similarity_threshold = threshold
        self._similarity_func = similarity_func
        return self

    def deduplicate(
        self,
        data: list[dict[str, Any]],
    ) -> DeduplicationResult:
        """Remove duplicates from dataset."""
        if not data:
            return DeduplicationResult(deduplicated=[], duplicates_removed=0)

        if self._similarity_func:
            return self._deduplicate_similar(data)

        return self._deduplicate_exact(data)

    def _deduplicate_exact(
        self,
        data: list[dict[str, Any]],
    ) -> DeduplicationResult:
        """Deduplicate using exact key matching."""
        seen: dict[str, list[tuple[int, dict[str, Any]]]] = {}
        duplicate_groups = 0

        for idx, record in enumerate(data):
            key = self._make_key(record)

            if key not in seen:
                seen[key] = []

            seen[key].append((idx, record))

        result: list[dict[str, Any]] = []
        removed = 0

        for key, records in seen.values():
            if len(records) > 1:
                duplicate_groups += 1
                removed += len(records) - 1

            if self.strategy == DeduplicationStrategy.KEEP_FIRST:
                result.append(records[0][1])
            elif self.strategy == DeduplicationStrategy.KEEP_LAST:
                result.append(records[-1][1])
            elif self.strategy == DeduplicationStrategy.KEEP_MOST_COMPLETE:
                most_complete = max(records, key=lambda r: self._completeness_score(r[1]))
                result.append(most_complete[1])

        return DeduplicationResult(
            deduplicated=result,
            duplicates_removed=removed,
            duplicate_groups=duplicate_groups,
        )

    def _deduplicate_similar(
        self,
        data: list[dict[str, Any]],
    ) -> DeduplicationResult:
        """Deduplicate using similarity matching."""
        result: list[dict[str, Any]] = []
        removed = 0

        for record in data:
            is_duplicate = False

            for existing in result:
                if self._similarity_func:
                    similarity = self._similarity_func(existing, record)
                    if similarity >= self._similarity_threshold:
                        is_duplicate = True
                        break

            if is_duplicate:
                removed += 1
            else:
                result.append(record)

        return DeduplicationResult(
            deduplicated=result,
            duplicates_removed=removed,
            duplicate_groups=removed,
        )

    def _make_key(self, record: dict[str, Any]) -> str:
        """Create a deduplication key from record."""
        values: list[str] = []

        for field_name in self._key_fields:
            value = record.get(field_name)
            values.append(str(value) if value is not None else "_null_")

        return "|".join(values)

    def _completeness_score(self, record: dict[str, Any]) -> int:
        """Score record by completeness (non-null fields)."""
        return sum(1 for v in record.values() if v is not None)


from enum import Enum
