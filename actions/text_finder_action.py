"""
Text Finder Action Module.

Finds and locates text content within UI elements and DOM
structures using various matching strategies including exact,
fuzzy, and regex-based search.
"""

import re
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class TextMatch:
    """Represents a text match result."""
    text: str
    element_id: Optional[str]
    bounds: tuple[int, int, int, int]
    confidence: float = 1.0
    match_type: str = "exact"


class TextFinder:
    """Finds text content in UI structures."""

    def __init__(self, case_sensitive: bool = False):
        """
        Initialize text finder.

        Args:
            case_sensitive: Whether matching is case-sensitive.
        """
        self.case_sensitive = case_sensitive

    def find_all(
        self,
        elements: list[dict],
        search_text: str,
        match_type: str = "exact",
    ) -> list[TextMatch]:
        """
        Find all occurrences of text in elements.

        Args:
            elements: List of DOM elements.
            search_text: Text to find.
            match_type: 'exact', 'contains', 'starts_with', 'ends_with', 'regex'.

        Returns:
            List of TextMatch objects.
        """
        matches = []

        for elem in self._flatten(elements):
            found = self._search_element(elem, search_text, match_type)
            if found:
                matches.extend(found)

        return matches

    def find_first(
        self,
        elements: list[dict],
        search_text: str,
        match_type: str = "contains",
    ) -> Optional[TextMatch]:
        """
        Find first occurrence of text.

        Args:
            elements: List of elements.
            search_text: Text to find.
            match_type: Matching strategy.

        Returns:
            First TextMatch or None.
        """
        for elem in self._flatten(elements):
            found = self._search_element(elem, search_text, match_type)
            if found:
                return found[0]
        return None

    def fuzzy_find(
        self,
        elements: list[dict],
        search_text: str,
        threshold: float = 0.8,
    ) -> list[TextMatch]:
        """
        Find text with fuzzy matching.

        Args:
            elements: List of elements.
            search_text: Text to find.
            threshold: Similarity threshold (0-1).

        Returns:
            List of matches above threshold.
        """
        matches = []
        search = search_text if self.case_sensitive else search_text.lower()

        for elem in self._flatten(elements):
            text = elem.get("text", "")
            compare = text if self.case_sensitive else text.lower()

            similarity = self._string_similarity(search, compare)
            if similarity >= threshold:
                match = TextMatch(
                    text=text,
                    element_id=elem.get("id"),
                    bounds=elem.get("bounds", (0, 0, 0, 0)),
                    confidence=similarity,
                    match_type="fuzzy",
                )
                matches.append(match)

        matches.sort(key=lambda m: m.confidence, reverse=True)
        return matches

    def _search_element(
        self,
        element: dict,
        search_text: str,
        match_type: str,
    ) -> list[TextMatch]:
        """Search a single element for text."""
        matches = []
        text = element.get("text", "")

        if not self.case_sensitive:
            text = text.lower()
            search = search_text.lower()
        else:
            search = search_text

        found = False

        if match_type == "exact":
            found = text == search
        elif match_type == "contains":
            found = search in text
        elif match_type == "starts_with":
            found = text.startswith(search)
        elif match_type == "ends_with":
            found = text.endswith(search)
        elif match_type == "regex":
            flags = 0 if self.case_sensitive else re.IGNORECASE
            found = bool(re.search(search_text, element.get("text", ""), flags))

        if found:
            matches.append(TextMatch(
                text=element.get("text", ""),
                element_id=element.get("id"),
                bounds=element.get("bounds", (0, 0, 0, 0)),
                match_type=match_type,
            ))

        return matches

    @staticmethod
    def _string_similarity(s1: str, s2: str) -> float:
        """Compute simple string similarity (basic Jaccard on words)."""
        if not s1 or not s2:
            return 0.0

        words1 = set(s1.split())
        words2 = set(s2.split())

        if not words1 or not words2:
            return 0.0

        intersection = words1 & words2
        union = words1 | words2

        return len(intersection) / len(union) if union else 0.0

    def _flatten(self, elements: list[dict]) -> list[dict]:
        """Flatten DOM tree."""
        result = []
        for elem in elements:
            result.append(elem)
            result.extend(self._flatten(elem.get("children", [])))
        return result
