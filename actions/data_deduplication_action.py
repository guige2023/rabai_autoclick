"""Data deduplication action for identifying and removing duplicate data.

Provides multiple strategies for detecting duplicates including
exact match, fuzzy match, and custom key-based deduplication.
"""

import hashlib
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class DeduplicationStrategy(Enum := __import__("enum").Enum):
    """Strategy for detecting duplicates."""
    EXACT = "exact"
    FINGERPRINT = "fingerprint"
    FUZZY = "fuzzy"
    KEY_BASED = "key_based"


@dataclass
class DuplicateGroup:
    """A group of identified duplicate items."""
    representative: Any
    duplicates: list[Any]
    match_score: float = 1.0


@dataclass
class DeduplicationResult:
    """Result of deduplication operation."""
    unique_items: list[Any]
    duplicate_groups: list[DuplicateGroup]
    duplicates_removed: int
    processing_time_ms: float


@dataclass
class DeduplicationStats:
    """Statistics for deduplication."""
    items_processed: int = 0
    duplicates_found: int = 0
    unique_remaining: int = 0


class DataDeduplicationAction:
    """Remove duplicate items from data collections.

    Args:
        strategy: Deduplication strategy to use.
        fuzzy_threshold: Similarity threshold for fuzzy matching (0-1).

    Example:
        >>> dedup = DataDeduplicationAction(strategy=DeduplicationStrategy.EXACT)
        >>> result = dedup.deduplicate(items)
    """

    def __init__(
        self,
        strategy: DeduplicationStrategy = DeduplicationStrategy.EXACT,
        fuzzy_threshold: float = 0.85,
    ) -> None:
        self.strategy = strategy
        self.fuzzy_threshold = fuzzy_threshold
        self._stats = DeduplicationStats()

    def set_key_func(self, func: Callable[[Any], Any]) -> None:
        """Set key function for KEY_BASED deduplication.

        Args:
            func: Function to extract comparison key from items.
        """
        self._key_func = func

    def deduplicate(
        self,
        items: list[Any],
        key_func: Optional[Callable[[Any], Any]] = None,
    ) -> DeduplicationResult:
        """Remove duplicates from a list of items.

        Args:
            items: List of items to deduplicate.
            key_func: Optional key extraction function.

        Returns:
            Deduplication result with unique items and groups.
        """
        import time
        start_time = time.time()

        if not items:
            return DeduplicationResult(
                unique_items=[],
                duplicate_groups=[],
                duplicates_removed=0,
                processing_time_ms=0.0,
            )

        if self.strategy == DeduplicationStrategy.EXACT:
            unique, groups = self._deduplicate_exact(items)
        elif self.strategy == DeduplicationStrategy.FINGERPRINT:
            unique, groups = self._deduplicate_fingerprint(items)
        elif self.strategy == DeduplicationStrategy.KEY_BASED:
            unique, groups = self._deduplicate_key_based(items, key_func)
        else:
            unique, groups = self._deduplicate_fuzzy(items)

        self._stats.items_processed = len(items)
        self._stats.duplicates_found = len(items) - len(unique)
        self._stats.unique_remaining = len(unique)

        return DeduplicationResult(
            unique_items=unique,
            duplicate_groups=groups,
            duplicates_removed=len(items) - len(unique),
            processing_time_ms=(time.time() - start_time) * 1000,
        )

    def _deduplicate_exact(self, items: list[Any]) -> tuple[list[Any], list[DuplicateGroup]]:
        """Deduplicate using exact matching.

        Args:
            items: Items to deduplicate.

        Returns:
            Tuple of (unique items, duplicate groups).
        """
        seen: set[Any] = set()
        unique: list[Any] = []
        groups: list[DuplicateGroup] = []

        for item in items:
            if item not in seen:
                unique.append(item)
                seen.add(item)
            else:
                logger.debug(f"Found duplicate: {item}")

        return unique, groups

    def _deduplicate_fingerprint(self, items: list[Any]) -> tuple[list[Any], list[DuplicateGroup]]:
        """Deduplicate using fingerprint hashing.

        Args:
            items: Items to deduplicate.

        Returns:
            Tuple of (unique items, duplicate groups).
        """
        fingerprints: dict[str, list[Any]] = {}
        unique: list[Any] = []

        for item in items:
            fp = self._fingerprint(item)
            if fp not in fingerprints:
                fingerprints[fp] = [item]
                unique.append(item)
            else:
                fingerprints[fp].append(item)

        groups = [
            DuplicateGroup(rep=group[0], duplicates=group[1:])
            for group in fingerprints.values()
            if len(group) > 1
        ]

        return unique, groups

    def _deduplicate_key_based(
        self,
        items: list[Any],
        key_func: Optional[Callable[[Any], Any]],
    ) -> tuple[list[Any], list[DuplicateGroup]]:
        """Deduplicate using a key function.

        Args:
            items: Items to deduplicate.
            key_func: Function to extract comparison key.

        Returns:
            Tuple of (unique items, duplicate groups).
        """
        if not key_func:
            key_func = self._key_func if hasattr(self, "_key_func") else str

        key_map: dict[Any, list[Any]] = {}
        unique: list[Any] = []

        for item in items:
            key = key_func(item)
            if key not in key_map:
                key_map[key] = [item]
                unique.append(item)
            else:
                key_map[key].append(item)

        groups = [
            DuplicateGroup(rep=group[0], duplicates=group[1:])
            for group in key_map.values()
            if len(group) > 1
        ]

        return unique, groups

    def _deduplicate_fuzzy(self, items: list[Any]) -> tuple[list[Any], list[DuplicateGroup]]:
        """Deduplicate using fuzzy string matching.

        Args:
            items: Items to deduplicate.

        Returns:
            Tuple of (unique items, duplicate groups).
        """
        unique: list[Any] = []
        groups: list[DuplicateGroup] = []

        for item in items:
            item_str = str(item)
            best_match: Optional[Any] = None
            best_score = 0.0

            for u in unique:
                score = self._fuzzy_score(item_str, str(u))
                if score > best_score:
                    best_score = score
                    best_match = u

            if best_match and best_score >= self.fuzzy_threshold:
                groups.append(DuplicateGroup(
                    representative=best_match,
                    duplicates=[item],
                    match_score=best_score,
                ))
            else:
                unique.append(item)

        return unique, groups

    def _fingerprint(self, item: Any) -> str:
        """Generate fingerprint for an item.

        Args:
            item: Item to fingerprint.

        Returns:
            Fingerprint string.
        """
        content = str(item).encode("utf-8")
        return hashlib.sha256(content).hexdigest()

    def _fuzzy_score(self, s1: str, s2: str) -> float:
        """Calculate fuzzy similarity score.

        Args:
            s1: First string.
            s2: Second string.

        Returns:
            Similarity score between 0 and 1.
        """
        if s1 == s2:
            return 1.0

        longer = max(len(s1), len(s2))
        if longer == 0:
            return 1.0

        distance = self._levenshtein_distance(s1.lower(), s2.lower())
        return 1.0 - (distance / longer)

    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """Calculate Levenshtein edit distance.

        Args:
            s1: First string.
            s2: Second string.

        Returns:
            Edit distance.
        """
        if len(s1) < len(s2):
            return self._levenshtein_distance(s2, s1)

        if len(s2) == 0:
            return len(s1)

        previous_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row

        return previous_row[-1]

    def get_stats(self) -> DeduplicationStats:
        """Get deduplication statistics.

        Returns:
            Current statistics.
        """
        return self._stats
