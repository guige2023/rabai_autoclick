"""
Data Matcher Action Module.

Provides fuzzy matching, record linkage, deduplication,
and similarity search capabilities.

Author: rabai_autoclick team
"""

import logging
from typing import (
    Optional, Dict, Any, List, Tuple, Callable,
    Set, Union
)
from dataclasses import dataclass, field
from enum import Enum
import re

logger = logging.getLogger(__name__)


class MatchType(Enum):
    """Types of matching."""
    EXACT = "exact"
    FUZZY = "fuzzy"
    SIMILARITY = "similarity"
    SEMANTIC = "semantic"
    PHONETIC = "phonetic"


@dataclass
class MatchResult:
    """Result of a matching operation."""
    record_id: str
    matched_id: Optional[str]
    score: float
    match_type: MatchType
    confidence: float

    @property
    def is_match(self) -> bool:
        """Check if result is a confident match."""
        return self.matched_id is not None and self.confidence >= 0.8


@dataclass
class DedupeConfig:
    """Configuration for deduplication."""
    match_type: MatchType = MatchType.FUZZY
    threshold: float = 0.8
    blocking_fields: List[str] = field(default_factory=list)
    compare_fields: List[str] = field(default_factory=list)
    score_weights: Dict[str, float] = field(default_factory=dict)


class DataMatcherAction:
    """
    Fuzzy Matching and Deduplication Engine.

    Provides multiple matching strategies including exact,
    fuzzy string matching, phonetic matching, and more.

    Example:
        >>> matcher = DataMatcherAction()
        >>> result = matcher.find_similar("John Smith", candidates)
    """

    def __init__(self, config: Optional[DedupeConfig] = None):
        self.config = config or DedupeConfig()

    # String Similarity Metrics

    def levenshtein_distance(self, s1: str, s2: str) -> int:
        """
        Calculate Levenshtein distance between two strings.

        Args:
            s1: First string
            s2: Second string

        Returns:
            Edit distance
        """
        if len(s1) < len(s2):
            return self.levenshtein_distance(s2, s1)

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

    def jaro_similarity(self, s1: str, s2: str) -> float:
        """
        Calculate Jaro similarity between two strings.

        Args:
            s1: First string
            s2: Second string

        Returns:
            Similarity score (0-1)
        """
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

        return (
            matches / len1
            + matches / len2
            + (matches - transpositions / 2) / matches
        ) / 3

    def jaro_winkler_similarity(
        self,
        s1: str,
        s2: str,
        prefix_weight: float = 0.1,
    ) -> float:
        """
        Calculate Jaro-Winkler similarity.

        Args:
            s1: First string
            s2: Second string
            prefix_weight: Weight for common prefix (default 0.1)

        Returns:
            Similarity score (0-1)
        """
        jaro = self.jaro_similarity(s1, s2)

        prefix_len = 0
        for i in range(min(len(s1), len(s2), 4)):
            if s1[i] == s2[i]:
                prefix_len += 1
            else:
                break

        return jaro + prefix_len * prefix_weight * (1 - jaro)

    def cosine_similarity(self, s1: str, s2: str) -> float:
        """
        Calculate cosine similarity between two strings.

        Args:
            s1: First string
            s2: Second string

        Returns:
            Similarity score (0-1)
        """
        tokens1 = self._tokenize(s1)
        tokens2 = self._tokenize(s2)

        all_tokens = set(tokens1) | set(tokens2)
        vector1 = [1 if t in tokens1 else 0 for t in all_tokens]
        vector2 = [1 if t in tokens2 else 0 for t in all_tokens]

        dot_product = sum(a * b for a, b in zip(vector1, vector2))
        norm1 = sum(a * a for a in vector1) ** 0.5
        norm2 = sum(b * b for b in vector2) ** 0.5

        if norm1 == 0 or norm2 == 0:
            return 0.0

        return dot_product / (norm1 * norm2)

    def _tokenize(self, text: str) -> Set[str]:
        """Tokenize text into words."""
        return set(re.findall(r"\w+", text.lower()))

    def dice_coefficient(self, s1: str, s2: str) -> float:
        """
        Calculate Dice coefficient (bigram similarity).

        Args:
            s1: First string
            s2: Second string

        Returns:
            Similarity score (0-1)
        """
        bigrams1 = self._get_bigrams(s1)
        bigrams2 = self._get_bigrams(s2)

        intersection = bigrams1 & bigrams2

        if not bigrams1 or not bigrams2:
            return 0.0

        return 2 * len(intersection) / (len(bigrams1) + len(bigrams2))

    def _get_bigrams(self, text: str) -> Set[str]:
        """Get bigrams from text."""
        text = text.lower()
        return {text[i : i + 2] for i in range(len(text) - 1)}

    def _phonetic_hash(self, text: str) -> str:
        """Generate phonetic hash (simplified Soundex)."""
        text = text.upper()
        soundex = text[0]

        mapping = {
            "BFPV": "1",
            "CGJKQSXZ": "2",
            "DT": "3",
            "L": "4",
            "MN": "5",
            "R": "6",
        }

        prev_code = ""
        for char in text[1:]:
            for chars, code in mapping.items():
                if char in chars:
                    if code != prev_code:
                        soundex += code
                        prev_code = code
                    break

        soundex = soundex[:4].ljust(4, "0")
        return soundex

    def compare_strings(
        self,
        s1: str,
        s2: str,
        method: MatchType = MatchType.SIMILARITY,
    ) -> float:
        """
        Compare two strings and return similarity score.

        Args:
            s1: First string
            s2: Second string
            method: Matching method

        Returns:
            Similarity score (0-1)
        """
        s1_lower = s1.lower().strip()
        s2_lower = s2.lower().strip()

        if s1_lower == s2_lower:
            return 1.0

        if method == MatchType.EXACT:
            return 1.0 if s1_lower == s2_lower else 0.0

        if method == MatchType.FUZZY:
            jw = self.jaro_winkler_similarity(s1_lower, s2_lower)
            dice = self.dice_coefficient(s1_lower, s2_lower)
            return (jw + dice) / 2

        if method == MatchType.SIMILARITY:
            jw = self.jaro_winkler_similarity(s1_lower, s2_lower)
            lev_dist = self.levenshtein_distance(s1_lower, s2_lower)
            max_len = max(len(s1_lower), len(s2_lower))
            lev_sim = 1 - (lev_dist / max_len) if max_len > 0 else 1.0
            return (jw + lev_sim) / 2

        if method == MatchType.PHONETIC:
            return 1.0 if self._phonetic_hash(s1) == self._phonetic_hash(s2) else 0.0

        return 0.0

    # Record Matching

    def find_similar(
        self,
        record: Dict[str, Any],
        candidates: List[Dict[str, Any]],
        fields: Optional[List[str]] = None,
        threshold: float = 0.8,
    ) -> List[MatchResult]:
        """
        Find similar records in candidate list.

        Args:
            record: Reference record
            candidates: List of candidate records
            fields: Fields to compare
            threshold: Match threshold

        Returns:
            List of match results
        """
        if not fields:
            fields = list(record.keys())

        results = []

        for candidate in candidates:
            score = self._compare_records(record, candidate, fields)
            confidence = score

            if score >= threshold:
                results.append(
                    MatchResult(
                        record_id=str(record.get("id", id(record))),
                        matched_id=str(candidate.get("id", id(candidate))),
                        score=score,
                        match_type=MatchType.FUZZY,
                        confidence=confidence,
                    )
                )

        return sorted(results, key=lambda r: r.score, reverse=True)

    def _compare_records(
        self,
        record1: Dict[str, Any],
        record2: Dict[str, Any],
        fields: List[str],
    ) -> float:
        """Compare two records across specified fields."""
        scores = []
        weights = []

        for field in fields:
            if field in record1 and field in record2:
                val1 = str(record1[field])
                val2 = str(record2[field])

                if val1 and val2:
                    score = self.jaro_winkler_similarity(val1, val2)
                    scores.append(score)
                    weights.append(self.config.score_weights.get(field, 1.0))

        if not scores:
            return 0.0

        total_weight = sum(weights)
        if total_weight == 0:
            return sum(scores) / len(scores)

        return sum(s * w for s, w in zip(scores, weights)) / total_weight

    # Deduplication

    def deduplicate(
        self,
        records: List[Dict[str, Any]],
        config: Optional[DedupeConfig] = None,
    ) -> List[List[Dict[str, Any]]]:
        """
        Find duplicate record groups.

        Args:
            records: List of records to deduplicate
            config: Deduplication configuration

        Returns:
            List of duplicate groups (each group is a list of records)
        """
        cfg = config or self.config
        groups: List[List[Dict[str, Any]]] = []
        processed: Set[int] = set()

        if cfg.blocking_fields:
            blocks = self._create_blocks(records, cfg.blocking_fields)

            for block_records in blocks.values():
                for i, record in enumerate(block_records):
                    if id(record) in processed:
                        continue

                    group = [record]
                    processed.add(id(record))

                    for j, other in enumerate(block_records[i + 1 :], i + 1):
                        if id(other) in processed:
                            continue

                        score = self._compare_records(
                            record, other, cfg.compare_fields
                        )

                        if score >= cfg.threshold:
                            group.append(other)
                            processed.add(id(other))

                    if len(group) > 1:
                        groups.append(group)

        else:
            for i, record in enumerate(records):
                if id(record) in processed:
                    continue

                group = [record]
                processed.add(id(record))

                for j, other in enumerate(records[i + 1 :], i + 1):
                    if id(other) in processed:
                        continue

                    score = self._compare_records(
                        record, other, cfg.compare_fields
                    )

                    if score >= cfg.threshold:
                        group.append(other)
                        processed.add(id(other))

                if len(group) > 1:
                    groups.append(group)

        return groups

    def _create_blocks(
        self,
        records: List[Dict[str, Any]],
        blocking_fields: List[str],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Create blocking keys for records."""
        blocks: Dict[str, List[Dict[str, Any]]] = {}

        for record in records:
            for field in blocking_fields:
                value = str(record.get(field, "")).lower()[:3]
                if value:
                    blocks.setdefault(value, []).append(record)

        return blocks

    def deduplicate_best(
        self,
        records: List[Dict[str, Any]],
        config: Optional[DedupeConfig] = None,
    ) -> List[Dict[str, Any]]:
        """
        Deduplicate and return only the best record from each group.

        Args:
            records: List of records
            config: Deduplication configuration

        Returns:
            List of deduplicated records
        """
        groups = self.deduplicate(records, config)
        duplicates = set()

        for group in groups:
            for record in group[1:]:
                duplicates.add(id(record))

        return [r for r in records if id(r) not in duplicates]

    # Record Linkage

    def link_records(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        left_keys: List[str],
        right_keys: List[str],
        threshold: float = 0.8,
    ) -> List[Tuple[Dict[str, Any], Dict[str, Any], float]]:
        """
        Link records between two datasets.

        Args:
            left: Left dataset
            right: Right dataset
            left_keys: Keys to match on left
            right_keys: Keys to match on right
            threshold: Match threshold

        Returns:
            List of (left_record, right_record, score) tuples
        """
        links = []

        for l_record in left:
            best_match = None
            best_score = 0.0

            for r_record in right:
                score = self._compare_records(l_record, r_record, left_keys + right_keys)

                if score >= threshold and score > best_score:
                    best_score = score
                    best_match = r_record

            if best_match:
                links.append((l_record, best_match, best_score))

        return links
