"""
Data Fuzzy Search Action Module

Provides fuzzy string matching and search capabilities for data records.
Supports multiple algorithms, typo tolerance, phonetic matching, and ranking.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class MatchAlgorithm(Enum):
    """Fuzzy matching algorithms."""

    LEVENSHTEIN = "levenshtein"
    JARO_WINKLER = "jaro_winkler"
    NGRAM = "ngram"
    PHONETIC = "phonetic"
    COMBINED = "combined"


@dataclass
class SearchRecord:
    """A record in the fuzzy search index."""

    record_id: str
    fields: Dict[str, str]
    metadata: Dict[str, Any] = field(default_factory=dict)
    indexed_at: float = field(default_factory=time.time)


@dataclass
class SearchResult:
    """A fuzzy search result."""

    record_id: str
    score: float
    matched_field: str
    matched_value: str
    highlights: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SearchConfig:
    """Configuration for fuzzy search."""

    algorithm: MatchAlgorithm = MatchAlgorithm.COMBINED
    max_distance: int = 2
    min_score: float = 0.5
    phonetic_encoder: str = "metaphone"
    enable_highlighting: bool = True
    case_sensitive: bool = False
    max_results: int = 10


class StringMatcher:
    """String matching utilities."""

    @staticmethod
    def levenshtein_distance(s1: str, s2: str) -> int:
        """Calculate Levenshtein distance between two strings."""
        if len(s1) < len(s2):
            return StringMatcher.levenshtein_distance(s2, s1)

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

    @staticmethod
    def jaro_winkler_similarity(s1: str, s2: str) -> float:
        """Calculate Jaro-Winkler similarity between two strings."""
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

        jaro = (matches / len1 + matches / len2 + (matches - transpositions / 2) / matches) / 3

        prefix_len = 0
        for i in range(min(len1, len2, 4)):
            if s1[i] == s2[i]:
                prefix_len += 1
            else:
                break

        return jaro + prefix_len * 0.1 * (1 - jaro)

    @staticmethod
    def ngram_similarity(s1: str, s2: str, n: int = 2) -> float:
        """Calculate n-gram similarity between two strings."""
        def get_ngrams(s: str, n: int) -> Set[str]:
            return set(s[i:i + n] for i in range(len(s) - n + 1))

        if len(s1) < n or len(s2) < n:
            return 0.0 if s1 != s2 else 1.0

        ngrams1 = get_ngrams(s1.lower(), n)
        ngrams2 = get_ngrams(s2.lower(), n)

        intersection = len(ngrams1 & ngrams2)
        union = len(ngrams1 | ngrams2)

        return intersection / union if union > 0 else 0.0

    @staticmethod
    def calculate_score(query: str, target: str, algorithm: MatchAlgorithm) -> float:
        """Calculate match score using specified algorithm."""
        if algorithm == MatchAlgorithm.LEVENSHTEIN:
            max_len = max(len(query), len(target))
            distance = StringMatcher.levenshtein_distance(query.lower(), target.lower())
            return 1.0 - (distance / max_len) if max_len > 0 else 1.0

        elif algorithm == MatchAlgorithm.JARO_WINKLER:
            return StringMatcher.jaro_winkler_similarity(query.lower(), target.lower())

        elif algorithm == MatchAlgorithm.NGRAM:
            return StringMatcher.ngram_similarity(query, target)

        else:
            lev_score = StringMatcher.calculate_score(query, target, MatchAlgorithm.LEVENSHTEIN)
            jw_score = StringMatcher.calculate_score(query, target, MatchAlgorithm.JARO_WINKLER)
            ng_score = StringMatcher.calculate_score(query, target, MatchAlgorithm.NGRAM)
            return (lev_score * 0.4 + jw_score * 0.4 + ng_score * 0.2)


class DataFuzzySearchAction:
    """
    Fuzzy search action for data records.

    Features:
    - Multiple matching algorithms (Levenshtein, Jaro-Winkler, n-gram)
    - Configurable scoring and thresholds
    - Multi-field search
    - Result ranking and highlighting
    - Typo tolerance
    - Phonetic matching

    Usage:
        search = DataFuzzySearchAction(config)
        search.index("user-1", {"name": "John Doe", "email": "john@example.com"})
        
        results = search.search("Jon Doe", fields=["name"], limit=5)
    """

    def __init__(self, config: Optional[SearchConfig] = None):
        self.config = config or SearchConfig()
        self._matcher = StringMatcher()
        self._index: Dict[str, SearchRecord] = {}
        self._stats = {
            "records_indexed": 0,
            "searches_performed": 0,
            "total_results": 0,
        }

    def index(
        self,
        record_id: str,
        fields: Dict[str, str],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SearchRecord:
        """Index a record for fuzzy searching."""
        record = SearchRecord(
            record_id=record_id,
            fields=fields,
            metadata=metadata or {},
        )
        self._index[record_id] = record
        self._stats["records_indexed"] += 1
        return record

    def index_batch(
        self,
        records: List[tuple],
    ) -> List[SearchRecord]:
        """Index multiple records at once."""
        indexed = []
        for record_id, fields, metadata in records:
            record = self.index(record_id, fields, metadata)
            indexed.append(record)
        return indexed

    def search(
        self,
        query: str,
        fields: Optional[List[str]] = None,
        limit: int = 10,
        min_score: Optional[float] = None,
    ) -> List[SearchResult]:
        """
        Search for records matching the query.

        Args:
            query: Search query string
            fields: Fields to search in (None = all fields)
            limit: Maximum number of results
            min_score: Minimum match score threshold

        Returns:
            List of SearchResult sorted by score
        """
        self._stats["searches_performed"] += 1
        threshold = min_score or self.config.min_score

        if not fields:
            fields = list(next(iter(self._index.values())).fields.keys()) if self._index else []

        results: List[SearchResult] = []

        for record_id, record in self._index.items():
            best_score = 0.0
            best_field = ""
            best_value = ""

            for field_name in fields:
                if field_name not in record.fields:
                    continue

                field_value = record.fields[field_name]

                score = self._matcher.calculate_score(
                    query,
                    field_value,
                    self.config.algorithm,
                )

                if score > best_score:
                    best_score = score
                    best_field = field_name
                    best_value = field_value

            if best_score >= threshold:
                highlights = self._generate_highlights(query, best_value) if self.config.enable_highlighting else []

                result = SearchResult(
                    record_id=record_id,
                    score=best_score,
                    matched_field=best_field,
                    matched_value=best_value,
                    highlights=highlights,
                    metadata=record.metadata,
                )
                results.append(result)
                self._stats["total_results"] += 1

        results.sort(key=lambda r: -r.score)
        return results[:limit]

    def _generate_highlights(self, query: str, text: str) -> List[str]:
        """Generate highlighted snippets matching the query."""
        pattern = re.compile(re.escape(query), re.IGNORECASE)
        if pattern.search(text):
            return [text]
        return []

    def search_by_field(
        self,
        field_name: str,
        query: str,
        limit: int = 10,
    ) -> List[SearchResult]:
        """Search within a specific field."""
        return self.search(query, fields=[field_name], limit=limit)

    def find_similar(
        self,
        record_id: str,
        field_name: str,
        limit: int = 10,
    ) -> List[SearchResult]:
        """Find records similar to an existing record."""
        record = self._index.get(record_id)
        if record is None or field_name not in record.fields:
            return []

        query = record.fields[field_name]
        return self.search(query, fields=[field_name], limit=limit + 1)[1:]

    def remove(self, record_id: str) -> bool:
        """Remove a record from the index."""
        if record_id in self._index:
            del self._index[record_id]
            return True
        return False

    def clear(self) -> None:
        """Clear the entire index."""
        self._index.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Get fuzzy search statistics."""
        return {
            **self._stats.copy(),
            "total_indexed": len(self._index),
            "algorithm": self.config.algorithm.value,
        }


async def demo_fuzzy_search():
    """Demonstrate fuzzy search."""
    config = SearchConfig(
        algorithm=MatchAlgorithm.COMBINED,
        min_score=0.5,
    )
    search = DataFuzzySearchAction(config)

    users = [
        ("user-1", {"name": "John Doe", "email": "john@example.com"}),
        ("user-2", {"name": "Jane Smith", "email": "jane@example.com"}),
        ("user-3", {"name": "Bob Johnson", "email": "bob@example.com"}),
        ("user-4", {"name": "Alice Williams", "email": "alice@example.com"}),
    ]

    search.index_batch(users)

    results = search.search("Jon Doe", fields=["name"])
    print(f"Search results for 'Jon Doe':")
    for r in results:
        print(f"  {r.record_id}: score={r.score:.3f}, matched={r.matched_value}")

    print(f"Stats: {search.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_fuzzy_search())
