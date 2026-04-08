"""Element matching utilities for UI automation.

Provides advanced element matching strategies including
fuzzy matching, similarity scoring, and rank-based selection.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class MatchStrategy(Enum):
    """Strategy for element matching."""
    EXACT = auto()
    CONTAINS = auto()
    STARTS_WITH = auto()
    ENDS_WITH = auto()
    REGEX = auto()
    FUZZY = auto()
    SIMILARITY = auto()


@dataclass
class ElementCandidate:
    """A candidate element for matching.

    Attributes:
        element_id: Unique element identifier.
        attributes: Element attributes dictionary.
        score: Match score (0.0-1.0).
        matched_by: The attribute that matched.
        matched_value: The value that was matched against.
    """
    element_id: str
    attributes: dict[str, Any]
    score: float = 0.0
    matched_by: str = ""
    matched_value: str = ""


class ElementMatcher:
    """Advanced element matching with multiple strategies.

    Supports exact, substring, regex, fuzzy, and similarity-based
    matching for robust element finding.
    """

    def __init__(self) -> None:
        """Initialize element matcher."""
        self._elements: dict[str, dict[str, Any]] = {}

    def add_element(
        self,
        element_id: str,
        role: str = "",
        name: str = "",
        label: str = "",
        value: Any = None,
        text: str = "",
        **attrs: Any,
    ) -> None:
        """Add an element for matching."""
        self._elements[element_id] = {
            "id": element_id,
            "role": role,
            "name": name,
            "label": label,
            "value": value,
            "text": text,
            **attrs,
        }

    def remove_element(self, element_id: str) -> bool:
        """Remove an element."""
        if element_id in self._elements:
            del self._elements[element_id]
            return True
        return False

    def find_one(
        self,
        role: str = "",
        name: str = "",
        label: str = "",
        value: Any = None,
        text: str = "",
        strategy: MatchStrategy = MatchStrategy.CONTAINS,
        min_score: float = 0.5,
    ) -> Optional[str]:
        """Find the best matching element ID.

        Returns the ID of the best match, or None if no match above min_score.
        """
        candidates = self.find_all(
            role=role, name=name, label=label,
            value=value, text=text, strategy=strategy
        )
        if not candidates:
            return None
        best = max(candidates, key=lambda c: c.score)
        if best.score >= min_score:
            return best.element_id
        return None

    def find_all(
        self,
        role: str = "",
        name: str = "",
        label: str = "",
        value: Any = None,
        text: str = "",
        strategy: MatchStrategy = MatchStrategy.CONTAINS,
    ) -> list[ElementCandidate]:
        """Find all elements matching the criteria."""
        results: list[ElementCandidate] = []

        for elem_id, elem in self._elements.items():
            score, matched_by, matched_value = self._compute_best_match(
                elem, role, name, label, value, text, strategy
            )
            if score > 0:
                results.append(ElementCandidate(
                    element_id=elem_id,
                    attributes=elem,
                    score=score,
                    matched_by=matched_by,
                    matched_value=matched_value,
                ))

        results.sort(key=lambda c: c.score, reverse=True)
        return results

    def _compute_best_match(
        self,
        elem: dict[str, Any],
        role: str,
        name: str,
        label: str,
        value: Any,
        text: str,
        strategy: MatchStrategy,
    ) -> tuple[float, str, str]:
        """Compute the best match score for an element."""
        candidates = [
            ("role", role, elem.get("role", "")),
            ("name", name, elem.get("name", "")),
            ("label", label, elem.get("label", "")),
            ("text", text, elem.get("text", "")),
            ("value", str(value) if value is not None else "", str(elem.get("value", ""))),
        ]

        best_score = 0.0
        best_matched_by = ""
        best_matched_value = ""

        for attr_name, query, elem_value in candidates:
            if not query:
                continue

            score = self._match_value(
                str(query), str(elem_value), strategy
            )
            weight = self._attribute_weight(attr_name)
            score *= weight

            if score > best_score:
                best_score = score
                best_matched_by = attr_name
                best_matched_value = str(elem_value)

        return best_score, best_matched_by, best_matched_value

    def _attribute_weight(self, attr_name: str) -> float:
        """Return importance weight for an attribute."""
        weights = {
            "role": 1.5,
            "name": 1.2,
            "label": 1.0,
            "text": 0.8,
            "value": 0.9,
        }
        return weights.get(attr_name, 1.0)

    def _match_value(
        self,
        query: str,
        value: str,
        strategy: MatchStrategy,
    ) -> float:
        """Compute match score using the given strategy."""
        if not value:
            return 0.0

        query_lower = query.lower()
        value_lower = value.lower()

        if strategy == MatchStrategy.EXACT:
            return 1.0 if query_lower == value_lower else 0.0

        if strategy == MatchStrategy.CONTAINS:
            return 1.0 if query_lower in value_lower else 0.0

        if strategy == MatchStrategy.STARTS_WITH:
            return 1.0 if value_lower.startswith(query_lower) else 0.0

        if strategy == MatchStrategy.ENDS_WITH:
            return 1.0 if value_lower.endswith(query_lower) else 0.0

        if strategy == MatchStrategy.REGEX:
            try:
                return 1.0 if re.search(query, value, re.I) else 0.0
            except Exception:
                return 0.0

        if strategy == MatchStrategy.FUZZY:
            return self._fuzzy_score(query_lower, value_lower)

        if strategy == MatchStrategy.SIMILARITY:
            return self._similarity_score(query_lower, value_lower)

        return 0.0

    def _fuzzy_score(self, query: str, value: str) -> float:
        """Compute fuzzy match score."""
        if query in value:
            return 1.0
        if all(c in value for c in query):
            return 0.7
        common = sum(1 for c in query if c in value)
        if common > 0:
            return common / max(len(query), len(value))
        return 0.0

    def _similarity_score(self, query: str, value: str) -> float:
        """Compute similarity score using simple character overlap."""
        if query == value:
            return 1.0
        if not query or not value:
            return 0.0

        query_chars = set(query)
        value_chars = set(value)
        intersection = len(query_chars & value_chars)
        union = len(query_chars | value_chars)

        return intersection / union if union > 0 else 0.0

    @property
    def element_count(self) -> int:
        """Return number of registered elements."""
        return len(self._elements)
