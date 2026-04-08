"""
Element matcher utilities for matching elements with multiple criteria.

Provides flexible element matching based on properties like
role, label, position, size, and custom predicates.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class ElementMatchCriteria:
    """Criteria for matching elements."""
    role: Optional[str] = None
    label: Optional[str] = None
    title: Optional[str] = None
    value: Optional[str] = None
    min_width: Optional[float] = None
    max_width: Optional[float] = None
    min_height: Optional[float] = None
    max_height: Optional[float] = None
    is_enabled: Optional[bool] = None
    is_visible: Optional[bool] = None
    predicate: Optional[Callable[[dict], bool]] = None
    index: Optional[int] = None  # Match by index in list


@dataclass
class MatchResult:
    """Result of an element match operation."""
    element_id: Optional[str]
    matched_criteria: list[str]
    score: float
    is_match: bool


class ElementMatcher:
    """Matches elements against criteria."""

    def __init__(self):
        self._last_match_result: Optional[MatchResult] = None

    def match(
        self,
        criteria: ElementMatchCriteria,
        elements: list[dict],
    ) -> list[tuple[str, float]]:
        """Match elements against criteria and return scored results.

        Args:
            criteria: Match criteria to apply
            elements: List of element dictionaries

        Returns:
            List of (element_id, score) tuples, sorted by score descending
        """
        results = []

        for i, elem in enumerate(elements):
            element_id = elem.get("id", elem.get("element_id", str(i)))
            matched, score, details = self._score_element(criteria, elem, i)

            if matched:
                results.append((element_id, score))

        # Sort by score descending
        results.sort(key=lambda x: x[1], reverse=True)
        return results

    def find_best_match(
        self,
        criteria: ElementMatchCriteria,
        elements: list[dict],
    ) -> Optional[str]:
        """Find the best matching element."""
        results = self.match(criteria, elements)
        if results:
            best_id, best_score = results[0]
            if best_score >= 0.5:
                return best_id
        return None

    def find_all_matches(
        self,
        criteria: ElementMatchCriteria,
        elements: list[dict],
        min_score: float = 0.5,
    ) -> list[str]:
        """Find all elements matching the criteria."""
        results = self.match(criteria, elements)
        return [eid for eid, score in results if score >= min_score]

    def _score_element(
        self,
        criteria: ElementMatchCriteria,
        elem: dict,
        index: int,
    ) -> tuple[bool, float, list[str]]:
        """Score a single element against criteria.

        Returns:
            (is_match, score, list of matched criteria names)
        """
        score = 0.0
        matched_criteria = []
        total_weight = 0.0

        # Role
        if criteria.role is not None:
            total_weight += 0.2
            if elem.get("role", "").lower() == criteria.role.lower():
                score += 0.2
                matched_criteria.append("role")

        # Label
        if criteria.label is not None:
            total_weight += 0.15
            elem_label = elem.get("label", elem.get("name", "")).lower()
            if criteria.label.lower() in elem_label or elem_label in criteria.label.lower():
                score += 0.15
                matched_criteria.append("label")

        # Title
        if criteria.title is not None:
            total_weight += 0.15
            elem_title = elem.get("title", "").lower()
            if criteria.title.lower() in elem_title:
                score += 0.15
                matched_criteria.append("title")

        # Width constraints
        width = elem.get("width", elem.get("bounds", (0, 0, 0, 0))[2] if isinstance(elem.get("bounds"), tuple) else 0)
        if criteria.min_width is not None:
            total_weight += 0.05
            if width >= criteria.min_width:
                score += 0.05
                matched_criteria.append("min_width")

        if criteria.max_width is not None:
            total_weight += 0.05
            if width <= criteria.max_width:
                score += 0.05
                matched_criteria.append("max_width")

        # Height constraints
        height = elem.get("height", elem.get("bounds", (0, 0, 0, 0))[3] if isinstance(elem.get("bounds"), tuple) else 0)
        if criteria.min_height is not None:
            total_weight += 0.05
            if height >= criteria.min_height:
                score += 0.05
                matched_criteria.append("min_height")

        if criteria.max_height is not None:
            total_weight += 0.05
            if height <= criteria.max_height:
                score += 0.05
                matched_criteria.append("max_height")

        # Enabled state
        if criteria.is_enabled is not None:
            total_weight += 0.1
            if elem.get("is_enabled", True) == criteria.is_enabled:
                score += 0.1
                matched_criteria.append("is_enabled")

        # Visible state
        if criteria.is_visible is not None:
            total_weight += 0.1
            if elem.get("is_visible", True) == criteria.is_visible:
                score += 0.1
                matched_criteria.append("is_visible")

        # Index
        if criteria.index is not None:
            total_weight += 0.05
            if index == criteria.index:
                score += 0.05
                matched_criteria.append("index")

        # Custom predicate
        if criteria.predicate is not None:
            total_weight += 0.1
            try:
                if criteria.predicate(elem):
                    score += 0.1
                    matched_criteria.append("predicate")
            except Exception:
                pass

        # Normalize score
        if total_weight > 0:
            normalized_score = score / total_weight
        else:
            normalized_score = 0.0

        is_match = len(matched_criteria) > 0 and normalized_score >= 0.5

        return (is_match, normalized_score, matched_criteria)


__all__ = ["ElementMatcher", "ElementMatchCriteria", "MatchResult"]
