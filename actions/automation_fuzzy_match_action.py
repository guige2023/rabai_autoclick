"""Automation Fuzzy Match Action Module.

Provides fuzzy string matching and similarity scoring
for element identification in UI automation.

Author: RabAi Team
"""

from __future__ import annotations

import re
import sys
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class MatchResult:
    """Result of fuzzy match operation."""
    matched: bool
    score: float
    matched_indices: List[int] = field(default_factory=list)
    method: str = ""


@dataclass
class FuzzyConfig:
    """Configuration for fuzzy matching."""
    threshold: float = 0.6
    method: str = "levenshtein"
    case_sensitive: bool = False
    ignore_whitespace: bool = True


def levenshtein_distance(s1: str, s2: str) -> int:
    """Calculate Levenshtein distance between two strings."""
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


def levenshtein_similarity(s1: str, s2: str) -> float:
    """Calculate similarity based on Levenshtein distance."""
    if not s1 and not s2:
        return 1.0

    max_len = max(len(s1), len(s2))
    if max_len == 0:
        return 1.0

    distance = levenshtein_distance(s1, s2)
    return 1.0 - (distance / max_len)


def jaro_winkler_similarity(s1: str, s2: str) -> float:
    """Calculate Jaro-Winkler similarity."""
    if not s1 and not s2:
        return 1.0

    if s1 == s2:
        return 1.0

    len1, len2 = len(s1), len(s2)
    match_distance = max(len1, len2) // 2 - 1

    if match_distance < 0:
        match_distance = 0

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
        matches / len1 +
        matches / len2 +
        (matches - transpositions / 2) / matches
    ) / 3

    prefix_len = 0
    for i in range(min(len1, len2, 4)):
        if s1[i] == s2[i]:
            prefix_len += 1
        else:
            break

    return jaro + prefix_len * 0.1 * (1 - jaro)


def token_similarity(s1: str, s2: str) -> float:
    """Calculate similarity based on token overlap."""
    if not s1 and not s2:
        return 1.0

    tokens1 = set(s1.lower().split())
    tokens2 = set(s2.lower().split())

    if not tokens1 or not tokens2:
        return 0.0

    intersection = tokens1 & tokens2
    union = tokens1 | tokens2

    return len(intersection) / len(union)


def subsequence_score(s: str, pattern: str) -> float:
    """Calculate how well pattern appears as subsequence in s."""
    if not pattern:
        return 1.0 if not s else 0.5

    s_idx = 0
    pattern_idx = 0
    matched = 0

    while s_idx < len(s) and pattern_idx < len(pattern):
        if s[s_idx].lower() == pattern[pattern_idx].lower():
            matched += 1
            pattern_idx += 1
        s_idx += 1

    if pattern_idx == len(pattern):
        return matched / len(pattern)

    return matched / (len(pattern) + len(s))


class FuzzyMatcher:
    """Fuzzy string matching engine."""

    def __init__(self, config: FuzzyConfig):
        self.config = config

    def _preprocess(self, s: str) -> str:
        """Preprocess string for matching."""
        if self.config.ignore_whitespace:
            s = " ".join(s.split())

        if not self.config.case_sensitive:
            s = s.lower()

        return s

    def _get_indices(self, s: str, pattern: str) -> List[int]:
        """Get indices in s that match pattern."""
        indices = []
        p_idx = 0

        for i, c in enumerate(s):
            if p_idx < len(pattern) and c == pattern[p_idx]:
                indices.append(i)
                p_idx += 1

        return indices

    def match(self, text: str, pattern: str) -> MatchResult:
        """Perform fuzzy match between text and pattern."""
        text = self._preprocess(text)
        pattern = self._preprocess(pattern)

        if not pattern:
            return MatchResult(
                matched=True,
                score=1.0,
                method=self.config.method
            )

        if not text:
            return MatchResult(
                matched=False,
                score=0.0,
                method=self.config.method
            )

        if self.config.method == "levenshtein":
            score = levenshtein_similarity(text, pattern)
        elif self.config.method == "jaro_winkler":
            score = jaro_winkler_similarity(text, pattern)
        elif self.config.method == "token":
            score = token_similarity(text, pattern)
        elif self.config.method == "subsequence":
            score = subsequence_score(text, pattern)
        else:
            score = levenshtein_similarity(text, pattern)

        indices = self._get_indices(text, pattern) if score > 0 else []

        matched = score >= self.config.threshold

        return MatchResult(
            matched=matched,
            score=score,
            matched_indices=indices,
            method=self.config.method
        )

    def match_best(
        self,
        text: str,
        patterns: List[str]
    ) -> Tuple[Optional[str], float]:
        """Find best matching pattern for text."""
        best_pattern = None
        best_score = 0.0

        for pattern in patterns:
            result = self.match(text, pattern)
            if result.score > best_score:
                best_score = result.score
                best_pattern = pattern

        return best_pattern, best_score

    def filter_by_threshold(
        self,
        text: str,
        patterns: List[str],
        threshold: Optional[float] = None
    ) -> List[Tuple[str, float]]:
        """Filter patterns that meet threshold."""
        thresh = threshold or self.config.threshold
        results = []

        for pattern in patterns:
            result = self.match(text, pattern)
            if result.score >= thresh:
                results.append((pattern, result.score))

        return sorted(results, key=lambda x: x[1], reverse=True)


class AutomationFuzzyMatchAction(BaseAction):
    """Action for fuzzy matching operations."""

    def __init__(self):
        super().__init__("automation_fuzzy_match")
        self._config = FuzzyConfig()
        self._matcher = FuzzyMatcher(self._config)

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute fuzzy match action."""
        try:
            operation = params.get("operation", "match")

            if operation == "match":
                return self._match(params)
            elif operation == "best_match":
                return self._best_match(params)
            elif operation == "filter":
                return self._filter(params)
            elif operation == "configure":
                return self._configure(params)
            elif operation == "distance":
                return self._distance(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _match(self, params: Dict[str, Any]) -> ActionResult:
        """Match text against pattern."""
        text = params.get("text", "")
        pattern = params.get("pattern", "")

        result = self._matcher.match(text, pattern)

        return ActionResult(
            success=result.matched,
            data={
                "matched": result.matched,
                "score": result.score,
                "matched_indices": result.matched_indices,
                "method": result.method
            }
        )

    def _best_match(self, params: Dict[str, Any]) -> ActionResult:
        """Find best matching pattern."""
        text = params.get("text", "")
        patterns = params.get("patterns", [])

        best_pattern, score = self._matcher.match_best(text, patterns)

        return ActionResult(
            success=best_pattern is not None,
            data={
                "pattern": best_pattern,
                "score": score
            }
        )

    def _filter(self, params: Dict[str, Any]) -> ActionResult:
        """Filter patterns by threshold."""
        text = params.get("text", "")
        patterns = params.get("patterns", [])
        threshold = params.get("threshold")

        results = self._matcher.filter_by_threshold(text, patterns, threshold)

        return ActionResult(
            success=True,
            data={
                "matches": [
                    {"pattern": p, "score": s}
                    for p, s in results
                ]
            }
        )

    def _configure(self, params: Dict[str, Any]) -> ActionResult:
        """Configure fuzzy matching."""
        self._config.threshold = params.get("threshold", 0.6)
        self._config.method = params.get("method", "levenshtein")
        self._config.case_sensitive = params.get("case_sensitive", False)
        self._config.ignore_whitespace = params.get("ignore_whitespace", True)

        self._matcher = FuzzyMatcher(self._config)

        return ActionResult(success=True, message="Configuration updated")

    def _distance(self, params: Dict[str, Any]) -> ActionResult:
        """Calculate Levenshtein distance."""
        s1 = params.get("s1", "")
        s2 = params.get("s2", "")

        distance = levenshtein_distance(s1, s2)
        similarity = levenshtein_similarity(s1, s2)

        return ActionResult(
            success=True,
            data={
                "distance": distance,
                "similarity": similarity
            }
        )
