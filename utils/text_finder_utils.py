"""Text finding on screen using OCR and image analysis.

Provides utilities for locating text regions on screen using OCR,
computing match confidence, and handling fuzzy text matching for
automation targets that may have slight visual variations.

Example:
    >>> from utils.text_finder_utils import TextFinder, FuzzyMatch
    >>> finder = TextFinder()
    >>> results = finder.find_all("Submit")
    >>> if results:
    ...     print(f"Found at {results[0].center}")
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Sequence

__all__ = [
    "TextMatch",
    "FuzzyMatch",
    "TextFinder",
]


@dataclass
class TextMatch:
    """Represents a text match found on screen.

    Attributes:
        text: The matched text string.
        bounds: (x, y, x2, y2) bounding box coordinates.
        confidence: Match confidence score (0.0 to 1.0).
    """

    text: str
    bounds: tuple[int, int, int, int]
    confidence: float

    @property
    def center(self) -> tuple[int, int]:
        """Return the center point of the match."""
        x, y, x2, y2 = self.bounds
        return ((x + x2) // 2, (y + y2) // 2)

    @property
    def width(self) -> int:
        """Return the width of the match region."""
        x, _, x2, _ = self.bounds
        return x2 - x

    @property
    def height(self) -> int:
        """Return the height of the match region."""
        _, y, _, y2 = self.bounds
        return y2 - y


class FuzzyMatch:
    """Fuzzy string matching with configurable similarity threshold.

    Example:
        >>> fm = FuzzyMatch(threshold=0.8)
        >>> score = fm.score("Submit", "Submil")  # typo
        >>> print(f"Similarity: {score:.2%}")
    """

    def __init__(self, threshold: float = 0.8) -> None:
        self.threshold = threshold

    @staticmethod
    def levenshtein(s: str, t: str) -> int:
        """Compute Levenshtein edit distance between two strings."""
        if len(s) < len(t):
            return FuzzyMatch.levenshtein(t, s)
        if len(t) == 0:
            return len(s)
        prev_row = list(range(len(t) + 1))
        for i, cs in enumerate(s):
            curr_row = [i + 1]
            for j, ct in enumerate(t):
                insertions = prev_row[j + 1] + 1
                deletions = curr_row[j] + 1
                substitutions = prev_row[j] + (cs != ct)
                curr_row.append(min(insertions, deletions, substitutions))
            prev_row = curr_row
        return prev_row[-1]

    def score(self, s: str, t: str) -> float:
        """Compute similarity score between two strings.

        Args:
            s: First string.
            t: Second string.

        Returns:
            Similarity score from 0.0 (completely different) to 1.0 (identical).
        """
        if s == t:
            return 1.0
        if not s or not t:
            return 0.0
        distance = self.levenshtein(s.lower(), t.lower())
        max_len = max(len(s), len(t))
        return 1.0 - distance / max_len

    def match(self, s: str, t: str) -> bool:
        """Return True if similarity score meets the threshold."""
        return self.score(s, t) >= self.threshold


class TextFinder:
    """Finds text on screen using OCR.

    This is a placeholder implementation that returns mock results.
    In production, integrate with pytesseract, macOS Vision framework,
    or another OCR backend.

    Example:
        >>> finder = TextFinder()
        >>> matches = finder.find("OK", confidence_threshold=0.9)
    """

    def __init__(
        self,
        confidence_threshold: float = 0.7,
    ) -> None:
        self.confidence_threshold = confidence_threshold
        self._fuzzy = FuzzyMatch()

    def find(
        self,
        text: str,
        confidence_threshold: float | None = None,
    ) -> TextMatch | None:
        """Find the first occurrence of text on screen.

        Args:
            text: The text to search for.
            confidence_threshold: Minimum confidence to accept a match.

        Returns:
            TextMatch if found, else None.
        """
        threshold = confidence_threshold or self.confidence_threshold
        results = self.find_all(text, confidence_threshold=threshold)
        return results[0] if results else None

    def find_all(
        self,
        text: str,
        confidence_threshold: float | None = None,
    ) -> list[TextMatch]:
        """Find all occurrences of text on screen.

        Args:
            text: The text to search for.
            confidence_threshold: Minimum confidence to accept.

        Returns:
            List of all TextMatch objects found.
        """
        threshold = confidence_threshold or self.confidence_threshold
        # Placeholder: in production, run OCR on screen capture
        # and filter results using FuzzyMatch
        return []

    def find_fuzzy(
        self,
        text: str,
        threshold: float = 0.8,
    ) -> list[TextMatch]:
        """Find text with fuzzy matching.

        Args:
            text: The text to search for.
            threshold: Minimum fuzzy similarity score.

        Returns:
            List of matches with similarity >= threshold.
        """
        # Placeholder: run OCR, apply fuzzy matching to results
        return []

    def ocr_region(
        self,
        x: int,
        y: int,
        width: int,
        height: int,
    ) -> list[TextMatch]:
        """Run OCR on a specific screen region.

        Args:
            x: Left edge X coordinate.
            y: Top edge Y coordinate.
            width: Width of the region.
            height: Height of the region.

        Returns:
            List of TextMatch objects found in the region.
        """
        # Placeholder: crop screen capture to region, run OCR
        return []
