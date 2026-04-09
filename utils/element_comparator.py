"""
Element comparator utilities for UI hierarchy comparison.

Compares UI elements by various attributes to find
matches, duplicates, or differences.

Author: AutoClick Team
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class ElementMatch:
    """
    Represents a comparison match between elements.

    Attributes:
        score: Similarity score 0.0-1.0
        matched_attributes: List of matching attribute names
        different_attributes: Dict of attribute -> (expected, actual)
    """

    score: float
    matched_attributes: list[str]
    different_attributes: dict[str, tuple[Any, Any]]


class ElementComparator:
    """
    Compares UI elements for matching/similarity.

    Supports multiple comparison strategies and
    threshold-based matching.

    Example:
        comparator = ElementComparator(threshold=0.8)
        result = comparator.compare(element_a, element_b)
        if result.score > 0.9:
            print("Elements are nearly identical")
    """

    def __init__(
        self,
        threshold: float = 0.7,
        ignore_fields: list[str] | None = None,
        attribute_weights: dict[str, float] | None = None,
    ) -> None:
        """
        Initialize comparator.

        Args:
            threshold: Minimum score to consider a match
            ignore_fields: Field names to skip in comparison
            attribute_weights: Per-attribute importance weights
        """
        self._threshold = threshold
        self._ignore_fields = set(ignore_fields or [])
        self._weights = attribute_weights or {}

    def compare(
        self,
        element_a: dict[str, Any],
        element_b: dict[str, Any],
    ) -> ElementMatch:
        """
        Compare two UI elements.

        Args:
            element_a: First element dict
            element_b: Second element dict

        Returns:
            ElementMatch with similarity score
        """
        all_keys = set(element_a.keys()) | set(element_b.keys())
        relevant_keys = all_keys - self._ignore_fields

        matched: list[str] = []
        different: dict[str, tuple[Any, Any]] = {}

        total_weight = 0.0
        matched_weight = 0.0

        for key in relevant_keys:
            weight = self._weights.get(key, 1.0)
            total_weight += weight

            if key not in element_a or key not in element_b:
                different[key] = (element_a.get(key), element_b.get(key))
            elif element_a[key] == element_b[key]:
                matched.append(key)
                matched_weight += weight
            else:
                different[key] = (element_a[key], element_b[key])

        score = matched_weight / total_weight if total_weight > 0 else 0.0

        return ElementMatch(
            score=score,
            matched_attributes=matched,
            different_attributes=different,
        )

    def is_match(self, element_a: dict[str, Any], element_b: dict[str, Any]) -> bool:
        """Check if elements meet the similarity threshold."""
        return self.compare(element_a, element_b).score >= self._threshold

    def find_duplicates(
        self,
        elements: list[dict[str, Any]],
    ) -> list[list[int]]:
        """
        Find groups of duplicate elements.

        Args:
            elements: List of element dicts

        Returns:
            List of index groups, each containing indices of duplicate elements
        """
        groups: list[list[int]] = []
        used: set[int] = set()

        for i, element in enumerate(elements):
            if i in used:
                continue

            group = [i]
            for j, other in enumerate(elements[i + 1 :], start=i + 1):
                if j in used:
                    continue
                if self.is_match(element, other):
                    group.append(j)
                    used.add(j)

            if len(group) > 1:
                groups.append(group)
            used.add(i)

        return groups

    def diff_elements(
        self,
        element_a: dict[str, Any],
        element_b: dict[str, Any],
    ) -> dict[str, tuple[Any, Any]]:
        """Get only the differing attributes between two elements."""
        return self.compare(element_a, element_b).different_attributes


def compare_by_role(
    element: dict[str, Any],
    expected_role: str,
    expected_title: str | None = None,
) -> float:
    """
    Simple role/title comparison scoring.

    Args:
        element: Element to check
        expected_role: Expected role value
        expected_title: Optional expected title value

    Returns:
        Score 0.0-1.0
    """
    score = 0.0

    if element.get("role") == expected_role:
        score += 0.7
        if expected_title and element.get("title") == expected_title:
            score += 0.3
    elif element.get("subrole") == expected_role:
        score += 0.5

    return min(score, 1.0)
