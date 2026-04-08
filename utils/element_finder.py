"""Element finder utilities for UI automation.

Provides pattern-based element finding with fuzzy matching
and ranking for ambiguous element lookups.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional, Sequence


class MatchMode(Enum):
    """Element matching modes."""
    EXACT = auto()
    CONTAINS = auto()
    STARTS_WITH = auto()
    ENDS_WITH = auto()
    FUZZY = auto()
    REGEX = auto()


@dataclass
class ElementMatch:
    """A match result for element finding.

    Attributes:
        element_id: ID of matched element.
        score: Match confidence score (0.0-1.0).
        matched_on: The property/attribute that matched.
        matched_value: The value that was matched against.
    """
    element_id: str
    score: float = 0.0
    matched_on: str = ""
    matched_value: str = ""


@dataclass
class ElementQuery:
    """A query for finding elements.

    Attributes:
        role: Element role to match.
        name: Element name to match.
        label: Element label to match.
        value: Element value to match.
        match_mode: How to match string values.
        timeout: Maximum time to search.
        rank_by: Property to rank results by.
    """
    role: str = ""
    name: str = ""
    label: str = ""
    value: str = ""
    match_mode: MatchMode = MatchMode.CONTAINS
    timeout: float = 10.0
    rank_by: str = "score"


class ElementFinder:
    """Finds UI elements using flexible matching criteria.

    Supports multiple match modes, ranking, and fuzzy matching
    for robust element lookup in dynamic UIs.
    """

    def __init__(self) -> None:
        """Initialize element finder."""
        self._elements: dict[str, dict[str, Any]] = {}
        self._match_callbacks: list[Callable[[ElementQuery], list[dict]]] = []

    def register_element(
        self,
        element_id: str,
        role: str = "",
        name: str = "",
        label: str = "",
        value: Any = None,
        **attrs: Any,
    ) -> None:
        """Register an element for lookup."""
        self._elements[element_id] = {
            "id": element_id,
            "role": role,
            "name": name,
            "label": label,
            "value": value,
            **attrs,
        }

    def unregister_element(self, element_id: str) -> bool:
        """Remove an element. Returns True if found."""
        if element_id in self._elements:
            del self._elements[element_id]
            return True
        return False

    def find_one(self, query: ElementQuery) -> Optional[str]:
        """Find the best matching element ID.

        Returns the ID of the best match, or None if no match.
        """
        matches = self.find(query)
        return matches[0].element_id if matches else None

    def find(self, query: ElementQuery) -> list[ElementMatch]:
        """Find all elements matching the query.

        Returns list sorted by match score.
        """
        results: list[ElementMatch] = []

        for elem_id, elem in self._elements.items():
            score = self._compute_match_score(elem, query)
            if score > 0:
                results.append(ElementMatch(
                    element_id=elem_id,
                    score=score,
                    matched_on=self._best_match_property(elem, query),
                    matched_value=self._best_match_value(elem, query),
                ))

        results.sort(key=lambda m: m.score, reverse=True)
        return results

    def _compute_match_score(
        self,
        elem: dict[str, Any],
        query: ElementQuery,
    ) -> float:
        """Compute match score for an element."""
        score = 0.0
        total_weight = 0.0

        if query.role:
            total_weight += 3.0
            if elem.get("role", "").lower() == query.role.lower():
                score += 3.0

        if query.name:
            total_weight += 2.0
            score += self._string_score(
                elem.get("name", ""), query.name, query.match_mode
            ) * 2.0

        if query.label:
            total_weight += 1.5
            score += self._string_score(
                elem.get("label", ""), query.label, query.match_mode
            ) * 1.5

        if total_weight == 0:
            return 0.0
        return score / total_weight

    def _string_score(
        self,
        text: str,
        pattern: str,
        mode: MatchMode,
    ) -> float:
        """Compute string match score (0.0-1.0)."""
        if not text or not pattern:
            return 0.0

        text_lower = text.lower()
        pattern_lower = pattern.lower()

        if mode == MatchMode.EXACT:
            return 1.0 if text_lower == pattern_lower else 0.0

        if mode == MatchMode.CONTAINS:
            return 1.0 if pattern_lower in text_lower else 0.0

        if mode == MatchMode.STARTS_WITH:
            return 1.0 if text_lower.startswith(pattern_lower) else 0.0

        if mode == MatchMode.ENDS_WITH:
            return 1.0 if text_lower.endswith(pattern_lower) else 0.0

        if mode == MatchMode.REGEX:
            try:
                return 1.0 if re.search(pattern, text, re.I) else 0.0
            except Exception:
                return 0.0

        if mode == MatchMode.FUZZY:
            return self._fuzzy_score(text_lower, pattern_lower)

        return 0.0

    def _fuzzy_score(self, text: str, pattern: str) -> float:
        """Simple fuzzy matching score."""
        if pattern in text:
            return 1.0
        if all(c in text for c in pattern):
            return 0.7
        if any(c in text for c in pattern):
            return 0.3
        return 0.0

    def _best_match_property(
        self,
        elem: dict[str, Any],
        query: ElementQuery,
    ) -> str:
        """Return the property that matched best."""
        if query.role and elem.get("role", "").lower() == query.role.lower():
            return "role"
        if query.name and query.name.lower() in elem.get("name", "").lower():
            return "name"
        if query.label and query.label.lower() in elem.get("label", "").lower():
            return "label"
        return ""

    def _best_match_value(
        self,
        elem: dict[str, Any],
        query: ElementQuery,
    ) -> str:
        """Return the value that was matched against."""
        prop = self._best_match_property(elem, query)
        return str(elem.get(prop, ""))

    @property
    def element_count(self) -> int:
        """Return number of registered elements."""
        return len(self._elements)
