"""
Data Deduplication Action Module.

Provides data deduplication with various
matching strategies and duplicate handling.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class MatchStrategy(Enum):
    """Match strategies for deduplication."""
    EXACT = "exact"
    FUZZY = "fuzzy"
    SIGNATURE = "signature"
    COMPOSITE = "composite"


@dataclass
class DuplicateGroup:
    """Group of duplicate records."""
    group_id: str
    records: List[Dict[str, Any]]
    canonical_record: Optional[Dict[str, Any]] = None
    confidence: float = 1.0


@dataclass
class DeduplicationResult:
    """Result of deduplication."""
    original_count: int
    unique_count: int
    duplicate_count: int
    duplicate_groups: List[DuplicateGroup]


class ExactMatcher:
    """Exact match deduplication."""

    def __init__(self, key_fields: List[str]):
        self.key_fields = key_fields

    def _make_key(self, record: Dict[str, Any]) -> Tuple:
        """Create key from record."""
        return tuple(record.get(field) for field in self.key_fields)

    def find_duplicates(self, data: List[Dict[str, Any]]) -> List[Set[int]]:
        """Find duplicate record indices."""
        key_to_indices: Dict[Tuple, List[int]] = {}

        for i, record in enumerate(data):
            key = self._make_key(record)
            if key not in key_to_indices:
                key_to_indices[key] = []
            key_to_indices[key].append(i)

        duplicates = [
            set(indices)
            for indices in key_to_indices.values()
            if len(indices) > 1
        ]

        return duplicates


class SignatureMatcher:
    """Signature-based fuzzy matching."""

    def __init__(self, normalize_func: Optional[Callable] = None):
        self.normalize_func = normalize_func or (lambda x: x.lower().strip())

    def _create_signature(self, text: str) -> Set[str]:
        """Create signature tokens from text."""
        import re
        tokens = re.findall(r'\w+', self.normalize_func(text))
        return set(tokens)

    def _jaccard_similarity(self, sig1: Set[str], sig2: Set[str]) -> float:
        """Calculate Jaccard similarity."""
        if not sig1 or not sig2:
            return 0.0
        intersection = len(sig1 & sig2)
        union = len(sig1 | sig2)
        return intersection / union if union > 0 else 0.0

    def find_similar(
        self,
        data: List[Dict[str, Any]],
        field: str,
        threshold: float = 0.8
    ) -> List[Tuple[int, int, float]]:
        """Find similar records."""
        signatures = [
            (i, self._create_signature(str(record.get(field, ""))))
            for i, record in enumerate(data)
        ]

        similar_pairs = []

        for i, (idx1, sig1) in enumerate(signatures):
            for idx2, sig2 in signatures[i + 1:]:
                similarity = self._jaccard_similarity(sig1, sig2)
                if similarity >= threshold:
                    similar_pairs.append((idx1, idx2, similarity))

        return similar_pairs


class DataDeduper:
    """Main deduplication orchestrator."""

    def __init__(self, match_strategy: MatchStrategy = MatchStrategy.EXACT):
        self.match_strategy = match_strategy
        self.matchers = {}

    def add_exact_matcher(self, key_fields: List[str]):
        """Add exact matcher."""
        self.matchers["exact"] = ExactMatcher(key_fields)

    def add_signature_matcher(self, normalize_func: Optional[Callable] = None):
        """Add signature matcher."""
        self.matchers["signature"] = SignatureMatcher(normalize_func)

    def deduplicate(
        self,
        data: List[Dict[str, Any]],
        **kwargs
    ) -> DeduplicationResult:
        """Deduplicate data."""
        duplicate_indices = []

        if self.match_strategy == MatchStrategy.EXACT:
            if "exact" in self.matchers:
                duplicate_indices = self.matchers["exact"].find_duplicates(data)
            else:
                key_fields = kwargs.get("key_fields", ["id"])
                matcher = ExactMatcher(key_fields)
                duplicate_indices = matcher.find_duplicates(data)

        duplicate_groups = []
        seen = set()
        group_id = 0

        for indices in duplicate_indices:
            group_records = []
            for idx in indices:
                if idx not in seen:
                    group_records.append(data[idx])
                    seen.add(idx)

            if group_records:
                duplicate_groups.append(DuplicateGroup(
                    group_id=str(group_id),
                    records=group_records,
                    canonical_record=group_records[0]
                ))
                group_id += 1

        unique_records = [
            data[i] for i in range(len(data))
            if i not in seen
        ]

        return DeduplicationResult(
            original_count=len(data),
            unique_count=len(unique_records) + len(duplicate_groups),
            duplicate_count=len(data) - (len(unique_records) + len(duplicate_groups)),
            duplicate_groups=duplicate_groups
        )

    def remove_duplicates(
        self,
        data: List[Dict[str, Any]],
        keep: str = "first"
    ) -> List[Dict[str, Any]]:
        """Remove duplicates from data."""
        result = self.deduplicate(data)
        seen = set()

        duplicates_to_remove = set()
        for group in result.duplicate_groups:
            canonical_idx = None
            for idx, record in enumerate(data):
                if record in group.records:
                    if canonical_idx is None:
                        canonical_idx = idx
                    else:
                        duplicates_to_remove.add(idx)

        if keep == "first":
            return [
                data[i] for i in range(len(data))
                if i not in duplicates_to_remove
            ]
        else:
            return [
                data[i] for i in range(len(data))
                if i not in duplicates_to_remove
            ]


def main():
    """Demonstrate deduplication."""
    data = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
        {"id": 3, "name": "Alice", "email": "alice@example.com"},
        {"id": 4, "name": "Charlie", "email": "charlie@example.com"},
    ]

    deduper = DataDeduper(MatchStrategy.EXACT)
    result = deduper.deduplicate(data, key_fields=["email"])

    print(f"Original: {result.original_count}")
    print(f"Unique: {result.unique_count}")
    print(f"Duplicates: {result.duplicate_count}")


if __name__ == "__main__":
    main()
