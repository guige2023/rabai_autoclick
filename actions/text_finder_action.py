"""
Text Finder Action Module

Provides text detection, OCR-based text finding, and
string matching utilities for UI automation.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class MatchType(Enum):
    """Text match types."""

    EXACT = "exact"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    REGEX = "regex"
    FUZZY = "fuzzy"


@dataclass
class TextMatch:
    """Represents a text match result."""

    text: str
    bounds: Optional[Tuple[int, int, int, int]] = None
    confidence: float = 1.0
    match_type: MatchType = MatchType.EXACT
    index: int = 0


@dataclass
class TextFinderConfig:
    """Configuration for text finder."""

    case_sensitive: bool = False
    trim_whitespace: bool = True
    normalize_unicode: bool = True
    min_fuzzy_score: float = 0.8
    ocr_enabled: bool = True
    ocr_languages: List[str] = field(default_factory=lambda: ["eng"])


class TextFinder:
    """
    Finds text in UI elements and images.

    Supports exact matching, regex, fuzzy matching,
    and OCR-based text detection from screenshots.
    """

    def __init__(
        self,
        config: Optional[TextFinderConfig] = None,
        ocr_handler: Optional[Callable[[Tuple[int, int, int, int]], List[TextMatch]]] = None,
    ):
        self.config = config or TextFinderConfig()
        self.ocr_handler = ocr_handler
        self._cache: Dict[str, List[TextMatch]] = {}

    def find_in_text(
        self,
        text: str,
        pattern: str,
        match_type: MatchType = MatchType.CONTAINS,
    ) -> List[TextMatch]:
        """
        Find pattern in text.

        Args:
            text: Source text
            pattern: Pattern to find
            match_type: Type of matching

        Returns:
            List of TextMatch results
        """
        if self.config.trim_whitespace:
            text = text.strip()
            pattern = pattern.strip()

        if self.config.normalize_unicode:
            import unicodedata
            text = unicodedata.normalize("NFKC", text)
            pattern = unicodedata.normalize("NFKC", pattern)

        matches: List[TextMatch] = []

        if match_type == MatchType.EXACT:
            if self._match_case(text, pattern):
                matches.append(TextMatch(text=pattern, match_type=MatchType.EXACT))
        elif match_type == MatchType.CONTAINS:
            matches = self._find_all_occurrences(text, pattern)
        elif match_type == MatchType.STARTS_WITH:
            if text.startswith(pattern):
                matches.append(TextMatch(text=pattern, match_type=MatchType.STARTS_WITH))
        elif match_type == MatchType.ENDS_WITH:
            if text.endswith(pattern):
                matches.append(TextMatch(text=pattern, match_type=MatchType.ENDS_WITH))
        elif match_type == MatchType.REGEX:
            matches = self._find_regex(text, pattern)
        elif match_type == MatchType.FUZZY:
            matches = self._find_fuzzy(text, pattern)

        return matches

    def _match_case(self, text: str, pattern: str) -> bool:
        """Check if text matches pattern (case handling)."""
        if self.config.case_sensitive:
            return text == pattern
        return text.lower() == pattern.lower()

    def _find_all_occurrences(self, text: str, pattern: str) -> List[TextMatch]:
        """Find all occurrences of pattern in text."""
        matches = []
        search_text = text if self.config.case_sensitive else text.lower()
        search_pattern = pattern if self.config.case_sensitive else pattern.lower()

        start = 0
        index = 0
        while True:
            pos = search_text.find(search_pattern, start)
            if pos == -1:
                break
            matches.append(
                TextMatch(
                    text=text[pos:pos + len(pattern)],
                    match_type=MatchType.CONTAINS,
                    index=index,
                )
            )
            start = pos + 1
            index += 1

        return matches

    def _find_regex(self, text: str, pattern: str) -> List[TextMatch]:
        """Find all regex matches in text."""
        matches = []
        flags = 0 if self.config.case_sensitive else re.IGNORECASE

        try:
            for i, match in enumerate(re.finditer(pattern, text, flags)):
                matches.append(
                    TextMatch(
                        text=match.group(),
                        bounds=(match.start(), 0, match.end(), 0),
                        match_type=MatchType.REGEX,
                        index=i,
                    )
                )
        except re.error as e:
            logger.error(f"Invalid regex pattern: {e}")

        return matches

    def _find_fuzzy(self, text: str, pattern: str) -> List[TextMatch]:
        """Find fuzzy matches in text."""
        matches = []

        words = text.split()
        pattern_lower = pattern.lower()

        for i, word in enumerate(words):
            score = self._calculate_similarity(word.lower(), pattern_lower)
            if score >= self.config.min_fuzzy_score:
                matches.append(
                    TextMatch(
                        text=word,
                        confidence=score,
                        match_type=MatchType.FUZZY,
                        index=i,
                    )
                )

        return matches

    def _calculate_similarity(self, s1: str, s2: str) -> float:
        """Calculate similarity score between two strings."""
        if s1 == s2:
            return 1.0

        len1, len2 = len(s1), len(s2)
        if len1 == 0 or len2 == 0:
            return 0.0

        distance = self._levenshtein_distance(s1, s2)
        max_len = max(len1, len2)

        return 1.0 - (distance / max_len)

    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """Calculate Levenshtein distance between two strings."""
        if len(s1) < len(s2):
            return self._levenshtein_distance(s2, s1)

        if len(s2) == 0:
            return len(s1)

        previous_row = list(range(len(s2) + 1))

        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row

        return previous_row[-1]

    def find_in_element(
        self,
        element_text: str,
        pattern: str,
        match_type: MatchType = MatchType.CONTAINS,
    ) -> bool:
        """
        Check if pattern exists in element text.

        Args:
            element_text: Text content of element
            pattern: Pattern to find
            match_type: Type of matching

        Returns:
            True if match found
        """
        matches = self.find_in_text(element_text, pattern, match_type)
        return len(matches) > 0

    def find_with_ocr(
        self,
        image_source: Any,
        pattern: str,
        match_type: MatchType = MatchType.CONTAINS,
    ) -> List[TextMatch]:
        """
        Find text in image using OCR.

        Args:
            image_source: Image data or screenshot
            pattern: Text pattern to find
            match_type: Type of matching

        Returns:
            List of TextMatch results with positions
        """
        if not self.config.ocr_enabled:
            logger.warning("OCR is not enabled")
            return []

        if self.ocr_handler:
            raw_matches = self.ocr_handler(image_source)
            return [
                m for m in raw_matches
                if self.find_in_text(m.text, pattern, match_type)
            ]

        logger.debug("No OCR handler configured")
        return []

    def extract_numbers(self, text: str) -> List[float]:
        """Extract all numbers from text."""
        pattern = r"-?\d+\.?\d*"
        matches = re.findall(pattern, text)
        return [float(m) for m in matches]

    def extract_urls(self, text: str) -> List[str]:
        """Extract all URLs from text."""
        pattern = r"https?://[^\s<>\"{}|\\^`\[\]]+"
        return re.findall(pattern, text)

    def extract_emails(self, text: str) -> List[str]:
        """Extract all email addresses from text."""
        pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
        return re.findall(pattern, text)

    def highlight_regions(
        self,
        text: str,
        pattern: str,
        match_type: MatchType = MatchType.REGEX,
    ) -> str:
        """
        Highlight matched patterns in text.

        Args:
            text: Source text
            pattern: Pattern to highlight
            match_type: Type of matching

        Returns:
            Text with highlighted matches
        """
        matches = self.find_in_text(text, pattern, match_type)

        if not matches:
            return text

        result = text
        offset = 0

        for match in matches:
            if match.bounds:
                start = match.bounds[0] + offset
                end = match.bounds[2] + offset
                result = f"{result[:start]}**{result[start:end]}**{result[end:]}"
                offset += 4

        return result

    def clear_cache(self) -> None:
        """Clear the text cache."""
        self._cache.clear()


def create_text_finder(
    config: Optional[TextFinderConfig] = None,
) -> TextFinder:
    """Factory function to create a TextFinder."""
    return TextFinder(config=config)
