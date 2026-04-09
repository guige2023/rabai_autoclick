"""
Text finding and matching utilities for UI automation.

This module provides utilities for finding text in UI elements,
including fuzzy matching, regex search, and accessibility-based
text retrieval.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Pattern, Tuple, Dict, Any
from enum import Enum, auto


class MatchStrategy(Enum):
    """Text matching strategies."""
    EXACT = auto()
    CONTAINS = auto()
    STARTS_WITH = auto()
    ENDS_WITH = auto()
    REGEX = auto()
    FUZZY = auto()
    CASE_INSENSITIVE = auto()


@dataclass
class TextMatch:
    """
    Represents a text match result.

    Attributes:
        text: The matched text.
        start: Start index of match.
        end: End index of match.
        score: Match confidence score (0.0-1.0).
        context: Surrounding context text.
    """
    text: str
    start: int = 0
    end: int = 0
    score: float = 1.0
    context: str = ""

    @property
    def length(self) -> int:
        """Get length of the matched text."""
        return self.end - self.start

    @property
    def matched(self) -> str:
        """Get the matched substring."""
        return self.text[self.start:self.end]


@dataclass
class TextSearchResult:
    """
    Result of a text search operation.

    Attributes:
        found: Whether text was found.
        matches: List of text matches.
        element: The element containing the match.
        path: Path to the element.
    """
    found: bool = False
    matches: List[TextMatch] = field(default_factory=list)
    element: Optional[str] = None
    path: str = ""


class TextMatcher:
    """
    Provides text matching capabilities with multiple strategies.

    Supports exact, substring, wildcard, regex, and fuzzy matching.
    """

    def __init__(self) -> None:
        self._fuzzy_threshold: float = 0.7

    def set_fuzzy_threshold(self, threshold: float) -> TextMatcher:
        """Set minimum score for fuzzy matches (0.0-1.0)."""
        self._fuzzy_threshold = max(0.0, min(1.0, threshold))
        return self

    def match(
        self,
        text: str,
        pattern: str,
        strategy: MatchStrategy = MatchStrategy.EXACT,
    ) -> Optional[TextMatch]:
        """
        Match text using the specified strategy.

        Returns TextMatch if found, None otherwise.
        """
        if strategy == MatchStrategy.EXACT:
            return self._match_exact(text, pattern)
        elif strategy == MatchStrategy.CONTAINS:
            return self._match_contains(text, pattern)
        elif strategy == MatchStrategy.STARTS_WITH:
            return self._match_starts_with(text, pattern)
        elif strategy == MatchStrategy.ENDS_WITH:
            return self._match_ends_with(text, pattern)
        elif strategy == MatchStrategy.REGEX:
            return self._match_regex(text, pattern)
        elif strategy == MatchStrategy.FUZZY:
            return self._match_fuzzy(text, pattern)
        elif strategy == MatchStrategy.CASE_INSENSITIVE:
            return self._match_case_insensitive(text, pattern)
        return None

    def _match_exact(self, text: str, pattern: str) -> Optional[TextMatch]:
        """Exact string match."""
        if text == pattern:
            return TextMatch(text=text, start=0, end=len(text), score=1.0)
        return None

    def _match_contains(self, text: str, pattern: str) -> Optional[TextMatch]:
        """Substring match."""
        idx = text.find(pattern)
        if idx >= 0:
            return TextMatch(text=text, start=idx, end=idx + len(pattern), score=1.0)
        return None

    def _match_starts_with(self, text: str, pattern: str) -> Optional[TextMatch]:
        """Match if text starts with pattern."""
        if text.startswith(pattern):
            return TextMatch(text=text, start=0, end=len(pattern), score=1.0)
        return None

    def _match_ends_with(self, text: str, pattern: str) -> Optional[TextMatch]:
        """Match if text ends with pattern."""
        if text.endswith(pattern):
            return TextMatch(text=text, start=len(text) - len(pattern), end=len(text), score=1.0)
        return None

    def _match_regex(self, text: str, pattern: str) -> Optional[TextMatch]:
        """Regular expression match."""
        try:
            compiled: Pattern[str] = re.compile(pattern)
            match = compiled.search(text)
            if match:
                return TextMatch(
                    text=text,
                    start=match.start(),
                    end=match.end(),
                    score=1.0,
                )
        except re.error:
            pass
        return None

    def _match_fuzzy(self, text: str, pattern: str) -> Optional[TextMatch]:
        """Fuzzy string match with threshold."""
        score = self._calculate_similarity(text, pattern)
        if score >= self._fuzzy_threshold:
            # Find best matching substring
            start, end = self._find_best_substring(text, pattern)
            return TextMatch(
                text=text,
                start=start,
                end=end,
                score=score,
            )
        return None

    def _match_case_insensitive(self, text: str, pattern: str) -> Optional[TextMatch]:
        """Case-insensitive exact match."""
        lower_text = text.lower()
        lower_pattern = pattern.lower()
        if lower_text == lower_pattern:
            return TextMatch(text=text, start=0, end=len(text), score=1.0)
        return None

    def _calculate_similarity(self, s1: str, s2: str) -> float:
        """Calculate string similarity using simple scoring."""
        if not s1 or not s2:
            return 0.0

        # Simple Levenshtein-like ratio
        longer = s1 if len(s1) >= len(s2) else s2
        shorter = s2 if len(s1) >= len(s2) else s1

        if len(longer) == 0:
            return 1.0

        matches = sum(1 for c in shorter if c in longer)
        return matches / len(longer)

    def _find_best_substring(self, text: str, pattern: str) -> Tuple[int, int]:
        """Find the substring of text that best matches pattern."""
        if pattern in text:
            idx = text.find(pattern)
            return idx, idx + len(pattern)

        # Find longest common subsequence window
        best_len = 0
        best_start = 0
        best_end = 0

        for i in range(len(text)):
            for j in range(i + 1, len(text) + 1):
                substring = text[i:j]
                score = self._calculate_similarity(substring, pattern)
                if score > self._fuzzy_threshold and len(substring) > best_len:
                    best_len = len(substring)
                    best_start = i
                    best_end = j

        return best_start, best_end

    def find_all(
        self,
        text: str,
        pattern: str,
        strategy: MatchStrategy = MatchStrategy.CONTAINS,
    ) -> List[TextMatch]:
        """Find all occurrences of pattern in text."""
        matches: List[TextMatch] = []

        if strategy == MatchStrategy.CONTAINS:
            idx = 0
            while True:
                idx = text.find(pattern, idx)
                if idx < 0:
                    break
                matches.append(TextMatch(
                    text=text,
                    start=idx,
                    end=idx + len(pattern),
                    score=1.0,
                ))
                idx += 1

        elif strategy == MatchStrategy.REGEX:
            try:
                compiled = re.compile(pattern)
                for match in compiled.finditer(text):
                    matches.append(TextMatch(
                        text=text,
                        start=match.start(),
                        end=match.end(),
                        score=1.0,
                    ))
            except re.error:
                pass

        return matches


class TextFinder:
    """
    Finds text within UI elements using accessibility APIs.

    Searches through element text content, titles, labels,
    and values to locate specified text.
    """

    def __init__(self) -> None:
        self._matcher = TextMatcher()
        self._cache: Dict[str, List[TextMatch]] = {}

    def set_fuzzy_threshold(self, threshold: float) -> TextFinder:
        """Set fuzzy match threshold."""
        self._matcher.set_fuzzy_threshold(threshold)
        return self

    def find_in_element(
        self,
        element: Dict[str, Any],
        text: str,
        strategy: MatchStrategy = MatchStrategy.CONTAINS,
    ) -> TextSearchResult:
        """
        Search for text within a UI element.

        Searches element text, title, value, and label.
        """
        result = TextSearchResult()

        # Search in various text fields
        text_sources = [
            element.get("title", ""),
            element.get("value", ""),
            element.get("text", ""),
            element.get("label", ""),
            element.get("description", ""),
        ]

        for source in text_sources:
            if not isinstance(source, str):
                continue

            match = self._matcher.match(source, text, strategy)
            if match:
                result.found = True
                result.matches.append(match)

        result.element = element.get("elementId", "")
        result.path = element.get("path", "")
        return result

    def find_all_in_tree(
        self,
        elements: List[Dict[str, Any]],
        text: str,
        strategy: MatchStrategy = MatchStrategy.CONTAINS,
    ) -> List[TextSearchResult]:
        """Search for text across all elements in a tree."""
        results: List[TextSearchResult] = []

        for element in elements:
            result = self.find_in_element(element, text, strategy)
            if result.found:
                results.append(result)

        return results

    def find_by_text(
        self,
        elements: List[Dict[str, Any]],
        text: str,
        strategy: MatchStrategy = MatchStrategy.CONTAINS,
    ) -> List[Dict[str, Any]]:
        """Find all elements containing the specified text."""
        matching: List[Dict[str, Any]] = []

        for element in elements:
            result = self.find_in_element(element, text, strategy)
            if result.found:
                matching.append(element)

        return matching


def normalize_text(text: str) -> str:
    """Normalize text for comparison by removing extra whitespace."""
    return " ".join(text.split())


def extract_text_segments(text: str) -> List[str]:
    """Split text into segments (words, punctuation, whitespace)."""
    return re.findall(r'\S+|\s+', text)
