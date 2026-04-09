"""Text finding action for UI automation.

Finds text in UI elements and on screen:
- Text element search
- OCR-based text finding
- Text comparison and fuzzy matching
- Text extraction from images
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable


class TextSearchStrategy(Enum):
    """Text search strategies."""
    EXACT = auto()
    CONTAINS = auto()
    STARTS_WITH = auto()
    ENDS_WITH = auto()
    FUZZY = auto()
    REGEX = auto()
    CASE_INSENSITIVE = auto()


@dataclass
class TextMatch:
    """Text match result."""
    text: str
    bounds: tuple[int, int, int, int]  # x, y, width, height
    confidence: float = 1.0
    element_id: str | None = None
    metadata: dict = field(default_factory=dict)


@dataclass
class TextSearchCriteria:
    """Criteria for text search."""
    text: str
    strategy: TextSearchStrategy = TextSearchStrategy.CONTAINS
    max_results: int = 10
    timeout: float = 5.0
    case_sensitive: bool = False


class TextFinder:
    """Finds text in UI elements.

    Features:
    - Direct element text search
    - OCR-based text finding
    - Fuzzy text matching
    - Text extraction from images
    """

    def __init__(self):
        self._ocr_func: Callable | None = None
        self._element_query_func: Callable | None = None

    def set_ocr_func(self, func: Callable) -> None:
        """Set OCR function.

        Args:
            func: Function(image_bytes) -> list[TextMatch]
        """
        self._ocr_func = func

    def set_element_query_func(self, func: Callable) -> None:
        """Set element query function.

        Args:
            func: Function(xpath or selector) -> element or None
        """
        self._element_query_func = func

    def find_text_in_image(self, image_data: bytes, criteria: TextSearchCriteria) -> list[TextMatch]:
        """Find text in image using OCR.

        Args:
            image_data: Image bytes
            criteria: Search criteria

        Returns:
            List of text matches
        """
        if not self._ocr_func:
            return []

        start = time.time()
        all_matches = self._ocr_func(image_data)

        results = []
        for match in all_matches:
            if time.time() - start > criteria.timeout:
                break
            if self._text_matches(match.text, criteria):
                results.append(match)
                if len(results) >= criteria.max_results:
                    break

        return results

    def find_text_in_element(self, element_id: str, text: str) -> str | None:
        """Get text content from element.

        Args:
            element_id: Element identifier
            text: Text to find (for validation)

        Returns:
            Element text or None
        """
        if not self._element_query_func:
            return None

        element = self._element_query_func(element_id)
        if not element:
            return None

        return element.get("text") or element.get("value")

    def search(self, criteria: TextSearchCriteria) -> list[TextMatch]:
        """Search for text with criteria.

        Args:
            criteria: Search criteria

        Returns:
            List of matches
        """
        # This is a simplified interface
        # Real implementation would search accessible elements
        return []

    def _text_matches(self, text: str, criteria: TextSearchCriteria) -> bool:
        """Check if text matches criteria."""
        if not text:
            return False

        search_text = criteria.text
        if not criteria.case_sensitive:
            text = text.lower()
            search_text = search_text.lower()

        if criteria.strategy == TextSearchStrategy.EXACT:
            return text == search_text
        elif criteria.strategy == TextSearchStrategy.CONTAINS:
            return search_text in text
        elif criteria.strategy == TextSearchStrategy.STARTS_WITH:
            return text.startswith(search_text)
        elif criteria.strategy == TextSearchStrategy.ENDS_WITH:
            return text.endswith(search_text)
        elif criteria.strategy == TextSearchStrategy.REGEX:
            flags = 0 if criteria.case_sensitive else re.IGNORECASE
            return bool(re.search(criteria.text, text, flags))
        elif criteria.strategy == TextSearchStrategy.FUZZY:
            # Simplified fuzzy match
            return self._fuzzy_match(text, search_text)
        elif criteria.strategy == TextSearchStrategy.CASE_INSENSITIVE:
            return search_text in text

        return False

    def _fuzzy_match(self, text: str, pattern: str, threshold: float = 0.8) -> bool:
        """Simple fuzzy matching using character overlap."""
        if not text or not pattern:
            return False

        # Count character overlap
        text_chars = set(text.lower())
        pattern_chars = set(pattern.lower())

        overlap = len(text_chars & pattern_chars)
        ratio = overlap / len(pattern_chars) if pattern_chars else 0

        return ratio >= threshold

    def extract_text_regions(
        self,
        image_data: bytes,
        min_confidence: float = 0.5,
    ) -> list[TextMatch]:
        """Extract all text regions from image.

        Args:
            image_data: Image bytes
            min_confidence: Minimum confidence threshold

        Returns:
            List of text matches above confidence
        """
        if not self._ocr_func:
            return []

        all_matches = self._ocr_func(image_data)
        return [m for m in all_matches if m.confidence >= min_confidence]

    def get_text_at_position(
        self,
        image_data: bytes,
        x: int,
        y: int,
        tolerance: int = 10,
    ) -> TextMatch | None:
        """Get text at specific position.

        Args:
            image_data: Image bytes
            x: X coordinate
            y: Y coordinate
            tolerance: Search tolerance in pixels

        Returns:
            Text match at position or None
        """
        if not self._ocr_func:
            return None

        all_matches = self._ocr_func(image_data)

        for match in all_matches:
            bx, by, bw, bh = match.bounds
            if (bx - tolerance <= x <= bx + bw + tolerance and
                by - tolerance <= y <= by + bh + tolerance):
                return match

        return None


def create_text_finder() -> TextFinder:
    """Create text finder."""
    return TextFinder()
