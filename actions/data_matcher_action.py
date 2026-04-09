"""
Data Matcher Action Module.

Provides fuzzy matching and deduplication with
similarity scoring and threshold filtering.
"""

import asyncio
import difflib
import hashlib
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, TypeVar
from collections import defaultdict

T = TypeVar("T")


class MatchAlgorithm(Enum):
    """Matching algorithms."""
    EXACT = "exact"
    SIMILARITY = "similarity"
    LEVENSHTEIN = "levenshtein"
    JARO_WINKLER = "jaro_winkler"
    FINGERPRINT = "fingerprint"


@dataclass
class MatchResult:
    """Match result."""
    record1: Any
    record2: Any
    score: float
    matched: bool
    algorithm: MatchAlgorithm = MatchAlgorithm.SIMILARITY


@dataclass
class DedupeConfig:
    """Deduplication configuration."""
    algorithm: MatchAlgorithm = MatchAlgorithm.SIMILARITY
    threshold: float = 0.85
    group_fields: list[str] = field(default_factory=list)
    ignore_fields: list[str] = field(default_factory=list)


def levenshtein_distance(s1: str, s2: str) -> int:
    """Calculate Levenshtein distance."""
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)

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


def jaro_similarity(s1: str, s2: str) -> float:
    """Calculate Jaro similarity."""
    if s1 == s2:
        return 1.0

    len1, len2 = len(s1), len(s2)
    if len1 == 0 or len2 == 0:
        return 0.0

    match_distance = max(len1, len2) // 2 - 1
    match_distance = max(0, match_distance)

    s1_matches = [False] * len1
    s2_matches = [False] * len2

    matches = 0
    transpositions = 0

    for i in range(len1):
        start = max(0, i - match_distance)
        end = min(i + match_distance + 1, len2)

        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    k = 0
    for i in range(len1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1

    return (matches / len1 + matches / len2 +
            (matches - transpositions / 2) / matches) / 3


def jaro_winkler_similarity(s1: str, s2: str, p: float = 0.1) -> float:
    """Calculate Jaro-Winkler similarity."""
    jaro = jaro_similarity(s1, s2)
    prefix = 0
    for i in range(min(len(s1), len(s2), 4)):
        if s1[i] == s2[i]:
            prefix += 1
        else:
            break

    return jaro + prefix * p * (1 - jaro)


def fingerprint(text: str) -> str:
    """Generate text fingerprint for matching."""
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)
    words = sorted(set(text.split()))
    return ' '.join(words)


class DataMatcher:
    """Fuzzy data matcher."""

    def __init__(self, config: DedupeConfig):
        self.config = config

    def _get_field_value(self, record: dict, field_path: str) -> Any:
        """Get field value from record."""
        parts = field_path.split(".")
        value = record
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
        return value

    def _normalize_value(self, value: Any) -> str:
        """Normalize value for comparison."""
        if value is None:
            return ""
        return str(value).lower().strip()

    def _calculate_similarity(
        self,
        value1: str,
        value2: str
    ) -> float:
        """Calculate similarity score."""
        if self.config.algorithm == MatchAlgorithm.EXACT:
            return 1.0 if value1 == value2 else 0.0

        elif self.config.algorithm == MatchAlgorithm.SIMILARITY:
            return difflib.SequenceMatcher(
                None, value1, value2
            ).ratio()

        elif self.config.algorithm == MatchAlgorithm.LEVENSHTEIN:
            max_len = max(len(value1), len(value2))
            if max_len == 0:
                return 1.0
            distance = levenshtein_distance(value1, value2)
            return 1.0 - (distance / max_len)

        elif self.config.algorithm == MatchAlgorithm.JARO_WINKLER:
            return jaro_winkler_similarity(value1, value2)

        elif self.config.algorithm == MatchAlgorithm.FINGERPRINT:
            fp1 = fingerprint(value1)
            fp2 = fingerprint(value2)
            return 1.0 if fp1 == fp2 else 0.0

        return 0.0

    def match(
        self,
        record1: dict,
        record2: dict,
        fields: Optional[list[str]] = None
    ) -> MatchResult:
        """Match two records."""
        fields = fields or self.config.group_fields
        if not fields:
            fields = list(set(record1.keys()) & set(record2.keys()))

        scores = []
        for field_name in fields:
            value1 = self._normalize_value(
                self._get_field_value(record1, field_name)
            )
            value2 = self._normalize_value(
                self._get_field_value(record2, field_name)
            )

            if not value1 and not value2:
                continue

            score = self._calculate_similarity(value1, value2)
            scores.append(score)

        avg_score = sum(scores) / len(scores) if scores else 0.0

        return MatchResult(
            record1=record1,
            record2=record2,
            score=avg_score,
            matched=avg_score >= self.config.threshold,
            algorithm=self.config.algorithm
        )


class DataDeduplicator:
    """Data deduplication."""

    def __init__(self, config: DedupeConfig):
        self.config = config
        self._matcher = DataMatcher(config)

    def deduplicate(
        self,
        records: list[dict],
        group_key: Optional[str] = None
    ) -> tuple[list[dict], list[list[dict]]]:
        """Remove duplicate records."""
        if not records:
            return [], []

        unique_records = []
        duplicate_groups = []

        for record in records:
            is_duplicate = False
            for unique_record in unique_records:
                result = self._matcher.match(record, unique_record)
                if result.matched:
                    is_duplicate = True
                    duplicate_groups.append([record, unique_record])
                    break

            if not is_duplicate:
                unique_records.append(record)

        return unique_records, duplicate_groups

    def find_duplicates(
        self,
        records: list[dict],
        fields: Optional[list[str]] = None
    ) -> list[tuple[int, int, float]]:
        """Find all duplicate pairs."""
        duplicates = []

        for i in range(len(records)):
            for j in range(i + 1, len(records)):
                result = self._matcher.match(records[i], records[j], fields)
                if result.matched:
                    duplicates.append((i, j, result.score))

        return duplicates


class DataMatcherAction:
    """
    Fuzzy matching and deduplication.

    Example:
        matcher = DataMatcherAction(
            algorithm=MatchAlgorithm.JARO_WINKLER,
            threshold=0.9
        )

        result = matcher.match(record1, record2)
        unique, duplicates = matcher.deduplicate(records)
    """

    def __init__(
        self,
        algorithm: MatchAlgorithm = MatchAlgorithm.SIMILARITY,
        threshold: float = 0.85
    ):
        config = DedupeConfig(algorithm=algorithm, threshold=threshold)
        self._matcher = DataMatcher(config)
        self._deduplicator = DataDeduplicator(config)

    def match(
        self,
        record1: dict,
        record2: dict,
        fields: Optional[list[str]] = None
    ) -> MatchResult:
        """Match two records."""
        return self._matcher.match(record1, record2, fields)

    def deduplicate(
        self,
        records: list[dict],
        group_key: Optional[str] = None
    ) -> tuple[list[dict], list[list[dict]]]:
        """Remove duplicates."""
        return self._deduplicator.deduplicate(records, group_key)

    def find_duplicates(
        self,
        records: list[dict],
        fields: Optional[list[str]] = None
    ) -> list[tuple[int, int, float]]:
        """Find duplicate pairs."""
        return self._deduplicator.find_duplicates(records, fields)
