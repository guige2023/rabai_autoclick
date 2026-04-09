"""
Element Comparison Utilities for UI Automation.

This module provides utilities for comparing UI elements
across different snapshots and states.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any, Tuple
from enum import Enum


class ComparisonType(Enum):
    """Type of element comparison."""
    STRUCTURAL = "structural"
    VISUAL = "visual"
    SEMANTIC = "semantic"
    BEHAVIORAL = "behavioral"


@dataclass
class ElementAttribute:
    """Single element attribute for comparison."""
    name: str
    value: Any
    weight: float = 1.0


@dataclass
class ComparisonResult:
    """Result of element comparison."""
    is_similar: bool
    similarity_score: float
    comparison_type: ComparisonType
    differences: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ElementDiff:
    """Represents a difference between two elements."""
    attribute: str
    expected: Any
    actual: Any
    severity: str = "minor"


class ElementComparator:
    """
    Compare UI elements for similarity and differences.
    """

    def __init__(
        self,
        comparison_type: ComparisonType = ComparisonType.STRUCTURAL,
        threshold: float = 0.8
    ):
        """
        Initialize element comparator.

        Args:
            comparison_type: Type of comparison to perform
            threshold: Similarity threshold for "similar"判定
        """
        self.comparison_type = comparison_type
        self.threshold = threshold

    def compare(
        self,
        element1: Dict[str, Any],
        element2: Dict[str, Any]
    ) -> ComparisonResult:
        """
        Compare two elements.

        Args:
            element1: First element data
            element2: Second element data

        Returns:
            ComparisonResult
        """
        if self.comparison_type == ComparisonType.STRUCTURAL:
            return self._compare_structural(element1, element2)
        elif self.comparison_type == ComparisonType.SEMANTIC:
            return self._compare_semantic(element1, element2)
        else:
            return self._compare_structural(element1, element2)

    def _compare_structural(
        self,
        element1: Dict[str, Any],
        element2: Dict[str, Any]
    ) -> ComparisonResult:
        """Structural comparison of elements."""
        differences = []
        matching_attrs = 0
        total_attrs = 0

        all_keys = set(element1.keys()) | set(element2.keys())

        for key in all_keys:
            val1 = element1.get(key)
            val2 = element2.get(key)
            total_attrs += 1

            if val1 != val2:
                differences.append(f"{key}: {val1} != {val2}")
            else:
                matching_attrs += 1

        similarity = matching_attrs / total_attrs if total_attrs > 0 else 0.0

        return ComparisonResult(
            is_similar=similarity >= self.threshold,
            similarity_score=similarity,
            comparison_type=self.comparison_type,
            differences=differences
        )

    def _compare_semantic(
        self,
        element1: Dict[str, Any],
        element2: Dict[str, Any]
    ) -> ComparisonResult:
        """Semantic comparison of elements."""
        differences = []
        matching_attrs = 0
        total_attrs = 0

        semantic_keys = ["role", "label", "title", "value", "description"]

        for key in semantic_keys:
            val1 = element1.get(key)
            val2 = element2.get(key)
            if val1 is not None or val2 is not None:
                total_attrs += 1
                if val1 == val2:
                    matching_attrs += 1
                elif val1 is not None and val2 is not None:
                    differences.append(f"semantic.{key}: {val1} != {val2}")

        similarity = matching_attrs / total_attrs if total_attrs > 0 else 1.0

        return ComparisonResult(
            is_similar=similarity >= self.threshold,
            similarity_score=similarity,
            comparison_type=self.comparison_type,
            differences=differences
        )


def compare_element_lists(
    list1: List[Dict[str, Any]],
    list2: List[Dict[str, Any]],
    comparator: Optional[ElementComparator] = None
) -> Tuple[List[int], List[int], List[Tuple[int, int]]]:
    """
    Compare two element lists.

    Args:
        list1: First element list
        list2: Second element list
        comparator: Element comparator to use

    Returns:
        Tuple of (added_indices, removed_indices, matched_pairs)
    """
    comparator = comparator or ElementComparator()
    added = []
    removed = []
    matched = []

    matched_in_list2 = set()

    for i, elem1 in enumerate(list1):
        best_match_idx = -1
        best_score = 0.0

        for j, elem2 in enumerate(list2):
            if j in matched_in_list2:
                continue

            result = comparator.compare(elem1, elem2)
            if result.is_similar and result.similarity_score > best_score:
                best_score = result.similarity_score
                best_match_idx = j

        if best_match_idx >= 0:
            matched.append((i, best_match_idx))
            matched_in_list2.add(best_match_idx)
        else:
            removed.append(i)

    for j, elem2 in enumerate(list2):
        if j not in matched_in_list2:
            added.append(j)

    return added, removed, matched


def calculate_element_hash(element: Dict[str, Any]) -> str:
    """
    Calculate a hash for an element.

    Args:
        element: Element data

    Returns:
        Hash string
    """
    import hashlib
    import json

    stable = {
        "role": element.get("role"),
        "label": element.get("label"),
        "value": element.get("value"),
    }

    stable_str = json.dumps(stable, sort_keys=True)
    return hashlib.sha256(stable_str.encode()).hexdigest()[:16]
