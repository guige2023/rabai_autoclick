"""Element matching utilities.

This module provides utilities for matching UI elements
by various criteria including role, name, attributes, and position.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass


@dataclass
class ElementCriteria:
    """Criteria for matching an element."""
    role: Optional[str] = None
    name: Optional[str] = None
    name_pattern: Optional[str] = None
    value: Optional[str] = None
    label: Optional[str] = None
    xpath: Optional[str] = None
    attributes: Optional[Dict[str, Any]] = None
    index: Optional[int] = None
    visible_only: bool = True
    enabled_only: bool = False


class ElementMatcher:
    """Matches UI elements against criteria."""

    def __init__(self) -> None:
        self._element_store: List[Dict[str, Any]] = []

    def add_element(self, element: Dict[str, Any]) -> None:
        """Add an element to the store."""
        self._element_store.append(element)

    def add_elements(self, elements: List[Dict[str, Any]]) -> None:
        """Add multiple elements to the store."""
        self._element_store.extend(elements)

    def clear(self) -> None:
        """Clear all elements."""
        self._element_store.clear()

    def find_all(self, criteria: ElementCriteria) -> List[Dict[str, Any]]:
        """Find all elements matching criteria.

        Args:
            criteria: ElementCriteria to match against.

        Returns:
            List of matching elements.
        """
        results = []
        for elem in self._element_store:
            if self._matches(elem, criteria):
                results.append(elem)
        return results

    def find_first(self, criteria: ElementCriteria) -> Optional[Dict[str, Any]]:
        """Find first element matching criteria.

        Args:
            criteria: ElementCriteria to match against.

        Returns:
            First matching element or None.
        """
        for elem in self._element_store:
            if self._matches(elem, criteria):
                return elem
        return None

    def _matches(self, elem: Dict[str, Any], criteria: ElementCriteria) -> bool:
        if criteria.role:
            if elem.get("role") != criteria.role:
                return False

        if criteria.name is not None:
            if elem.get("name") != criteria.name:
                return False

        if criteria.name_pattern is not None:
            name = elem.get("name", "")
            import re
            if not re.search(criteria.name_pattern, name):
                return False

        if criteria.value is not None:
            if elem.get("value") != criteria.value:
                return False

        if criteria.label is not None:
            if elem.get("label") != criteria.label:
                return False

        if criteria.attributes:
            for k, v in criteria.attributes.items():
                if elem.get(k) != v:
                    return False

        if criteria.visible_only:
            if not elem.get("visible", True):
                return False

        if criteria.enabled_only:
            if not elem.get("enabled", False):
                return False

        return True


def fuzzy_match(text: str, pattern: str, threshold: float = 0.6) -> bool:
    """Fuzzy match text against a pattern.

    Args:
        text: Text to match.
        pattern: Pattern to match against.
        threshold: Minimum similarity score.

    Returns:
        True if match score exceeds threshold.
    """
    import difflib
    score = difflib.SequenceMatcher(None, text.lower(), pattern.lower()).ratio()
    return score >= threshold


def levenshtein_distance(s1: str, s2: str) -> int:
    """Compute Levenshtein distance between two strings.

    Args:
        s1: First string.
        s2: Second string.

    Returns:
        Edit distance.
    """
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


__all__ = [
    "ElementCriteria",
    "ElementMatcher",
    "fuzzy_match",
    "levenshtein_distance",
]
