"""Data fuzzy matching and string similarity action."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Sequence


@dataclass
class MatchResult:
    """Result of a fuzzy match operation."""

    source: str
    target: str
    score: float  # 0-1, higher = better match
    matched: bool
    source_index: Optional[int] = None
    target_index: Optional[int] = None


class DataFuzzyMatchAction:
    """Performs fuzzy matching between strings and records."""

    def __init__(
        self,
        default_threshold: float = 0.8,
        case_sensitive: bool = False,
    ):
        """Initialize fuzzy matcher.

        Args:
            default_threshold: Default match threshold (0-1).
            case_sensitive: Whether matching is case sensitive.
        """
        self._default_threshold = default_threshold
        self._case_sensitive = case_sensitive

    def _normalize(self, s: str) -> str:
        """Normalize string for comparison."""
        if not self._case_sensitive:
            s = s.lower()
        return " ".join(s.split())

    def levenshtein_distance(self, s1: str, s2: str) -> int:
        """Calculate Levenshtein distance between two strings."""
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

    def levenshtein_score(self, s1: str, s2: str) -> float:
        """Calculate similarity score based on Levenshtein distance."""
        max_len = max(len(s1), len(s2))
        if max_len == 0:
            return 1.0
        distance = self.levenshtein_distance(s1, s2)
        return 1.0 - (distance / max_len)

    def jaro_winkler_score(self, s1: str, s2: str) -> float:
        """Calculate Jaro-Winkler similarity score."""
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

        jaro = (
            matches / len1
            + matches / len2
            + (matches - transpositions / 2) / matches
        ) / 3

        prefix_len = 0
        for i in range(min(len1, len2, 4)):
            if s1[i] == s2[i]:
                prefix_len += 1

        return jaro + prefix_len * 0.1 * (1 - jaro)

    def match_strings(
        self,
        source: str,
        targets: Sequence[str],
        threshold: Optional[float] = None,
        method: str = "levenshtein",
    ) -> list[MatchResult]:
        """Match a source string against multiple targets.

        Args:
            source: Source string.
            targets: Target strings to match against.
            threshold: Minimum match threshold.
            method: Scoring method ("levenshtein" or "jaro_winkler").

        Returns:
            List of MatchResult sorted by score.
        """
        threshold = threshold or self._default_threshold
        results = []

        source_normalized = self._normalize(source)

        for target in targets:
            target_normalized = self._normalize(target)

            if method == "levenshtein":
                score = self.levenshtein_score(source_normalized, target_normalized)
            else:
                score = self.jaro_winkler_score(source_normalized, target_normalized)

            results.append(
                MatchResult(
                    source=source,
                    target=target,
                    score=score,
                    matched=score >= threshold,
                )
            )

        return sorted(results, key=lambda r: r.score, reverse=True)

    def match_records(
        self,
        source_records: Sequence[dict[str, Any]],
        target_records: Sequence[dict[str, Any]],
        source_key: str,
        target_key: str,
        threshold: Optional[float] = None,
        method: str = "levenshtein",
    ) -> list[MatchResult]:
        """Match records between two datasets.

        Args:
            source_records: Source records.
            target_records: Target records.
            source_key: Field name in source records.
            target_key: Field name in target records.

        Returns:
            List of MatchResult for best matches.
        """
        threshold = threshold or self._default_threshold
        results = []

        for src_idx, src_record in enumerate(source_records):
            source_value = str(src_record.get(source_key, ""))

            best_match: Optional[MatchResult] = None
            for tgt_idx, tgt_record in enumerate(target_records):
                target_value = str(tgt_record.get(target_key, ""))

                if method == "levenshtein":
                    score = self.levenshtein_score(
                        self._normalize(source_value),
                        self._normalize(target_value),
                    )
                else:
                    score = self.jaro_winkler_score(
                        self._normalize(source_value),
                        self._normalize(target_value),
                    )

                match_result = MatchResult(
                    source=source_value,
                    target=target_value,
                    score=score,
                    matched=score >= threshold,
                    source_index=src_idx,
                    target_index=tgt_idx,
                )

                if best_match is None or score > best_match.score:
                    best_match = match_result

            if best_match:
                results.append(best_match)

        return results

    def find_duplicates(
        self,
        records: Sequence[dict[str, Any]],
        key_field: str,
        threshold: Optional[float] = None,
    ) -> list[tuple[int, int, float]]:
        """Find potential duplicate records.

        Returns:
            List of (index1, index2, score) tuples.
        """
        threshold = threshold or self._default_threshold
        duplicates = []

        record_list = list(records)
        for i in range(len(record_list)):
            for j in range(i + 1, len(record_list)):
                s1 = self._normalize(str(record_list[i].get(key_field, "")))
                s2 = self._normalize(str(record_list[j].get(key_field, "")))

                score = self.jaro_winkler_score(s1, s2)

                if score >= threshold:
                    duplicates.append((i, j, score))

        return sorted(duplicates, key=lambda x: x[2], reverse=True)
