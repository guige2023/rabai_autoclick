"""
Element matching utilities for UI automation.

This module provides utilities for matching UI elements using
various criteria including accessibility properties, visual
characteristics, and semantic attributes.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Dict, Any, Set, Tuple
from enum import Enum, auto


class MatchConfidence(Enum):
    """Confidence levels for element matches."""
    EXACT = auto()
    HIGH = auto()
    MEDIUM = auto()
    LOW = auto()
    NONE = auto()


@dataclass
class MatchCriteria:
    """
    Criteria for element matching.

    Attributes:
        role: Required accessibility role.
        label: Required accessibility label (supports wildcards).
        title: Required element title.
        value: Required element value.
        identifier: Required element identifier.
        enabled: Required enabled state.
        focused: Required focused state.
        attributes: Additional attribute requirements.
    """
    role: Optional[str] = None
    label: Optional[str] = None
    title: Optional[str] = None
    value: Optional[str] = None
    identifier: Optional[str] = None
    enabled: Optional[bool] = None
    focused: Optional[bool] = None
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MatchResult:
    """
    Result of an element match operation.

    Attributes:
        matched: Whether match succeeded.
        confidence: Match confidence level.
        element: The matched element.
        score: Numerical score (0.0-1.0).
        matched_criteria: List of criteria that matched.
        unmatched_criteria: List of criteria that didn't match.
    """
    matched: bool = False
    confidence: MatchConfidence = MatchConfidence.NONE
    element: Optional[Dict[str, Any]] = None
    score: float = 0.0
    matched_criteria: List[str] = field(default_factory=list)
    unmatched_criteria: List[str] = field(default_factory=list)


class ElementMatcher:
    """
    Matches UI elements against criteria with scoring.

    Provides flexible element matching using accessibility
    properties, visual characteristics, and custom predicates.
    """

    def __init__(self) -> None:
        self._weight_role: float = 1.0
        self._weight_label: float = 0.9
        self._weight_title: float = 0.8
        self._weight_value: float = 0.6
        self._weight_identifier: float = 1.0

    def set_weights(
        self,
        role: float = 1.0,
        label: float = 0.9,
        title: float = 0.8,
        value: float = 0.6,
        identifier: float = 1.0,
    ) -> ElementMatcher:
        """Set match weights for different criteria."""
        self._weight_role = max(0.0, min(1.0, role))
        self._weight_label = max(0.0, min(1.0, label))
        self._weight_title = max(0.0, min(1.0, title))
        self._weight_value = max(0.0, min(1.0, value))
        self._weight_identifier = max(0.0, min(1.0, identifier))
        return self

    def match(
        self,
        element: Dict[str, Any],
        criteria: MatchCriteria,
    ) -> MatchResult:
        """
        Match an element against criteria.

        Returns MatchResult with confidence and scoring details.
        """
        result = MatchResult(element=element)
        total_score = 0.0
        total_weight = 0.0

        # Match role
        if criteria.role is not None:
            total_weight += self._weight_role
            if self._match_role(element, criteria.role):
                result.matched_criteria.append("role")
                total_score += self._weight_role
            else:
                result.unmatched_criteria.append("role")

        # Match label
        if criteria.label is not None:
            total_weight += self._weight_label
            match_score = self._match_text(element.get("AXLabel", ""), criteria.label)
            if match_score > 0:
                result.matched_criteria.append("label")
                total_score += self._weight_label * match_score
            else:
                result.unmatched_criteria.append("label")

        # Match title
        if criteria.title is not None:
            total_weight += self._weight_title
            match_score = self._match_text(element.get("title", ""), criteria.title)
            if match_score > 0:
                result.matched_criteria.append("title")
                total_score += self._weight_title * match_score
            else:
                result.unmatched_criteria.append("title")

        # Match value
        if criteria.value is not None:
            total_weight += self._weight_value
            if self._match_exact(element.get("value", ""), criteria.value):
                result.matched_criteria.append("value")
                total_score += self._weight_value
            else:
                result.unmatched_criteria.append("value")

        # Match identifier
        if criteria.identifier is not None:
            total_weight += self._weight_identifier
            if self._match_exact(element.get("identifier", ""), criteria.identifier):
                result.matched_criteria.append("identifier")
                total_score += self._weight_identifier
            else:
                result.unmatched_criteria.append("identifier")

        # Match enabled state
        if criteria.enabled is not None:
            total_weight += 0.3
            if element.get("enabled", True) == criteria.enabled:
                result.matched_criteria.append("enabled")
                total_score += 0.3
            else:
                result.unmatched_criteria.append("enabled")

        # Match focused state
        if criteria.focused is not None:
            total_weight += 0.3
            if element.get("focused", False) == criteria.focused:
                result.matched_criteria.append("focused")
                total_score += 0.3
            else:
                result.unmatched_criteria.append("focused")

        # Match custom attributes
        for key, expected in criteria.attributes.items():
            total_weight += 0.5
            actual = element.get(key)
            if actual == expected:
                result.matched_criteria.append(key)
                total_score += 0.5
            else:
                result.unmatched_criteria.append(key)

        # Calculate final score
        result.score = total_score / total_weight if total_weight > 0 else 0.0
        result.matched = len(result.unmatched_criteria) == 0

        # Set confidence
        if result.score >= 0.95:
            result.confidence = MatchConfidence.EXACT
        elif result.score >= 0.8:
            result.confidence = MatchConfidence.HIGH
        elif result.score >= 0.5:
            result.confidence = MatchConfidence.MEDIUM
        elif result.score > 0:
            result.confidence = MatchConfidence.LOW
        else:
            result.confidence = MatchConfidence.NONE

        return result

    def _match_role(self, element: Dict[str, Any], expected: str) -> bool:
        """Match accessibility role."""
        actual = element.get("role", "") or element.get("AXRole", "")
        return actual.lower() == expected.lower()

    def _match_text(self, actual: str, pattern: str) -> float:
        """
        Match text with wildcard support.

        Returns score from 0.0 to 1.0.
        """
        if not actual and not pattern:
            return 1.0
        if not actual or not pattern:
            return 0.0

        # Check for wildcards
        if "*" in pattern or "?" in pattern:
            regex_pattern = pattern.replace("*", ".*").replace("?", ".")
            if re.match(f"^{regex_pattern}$", actual, re.IGNORECASE):
                return 1.0
            return 0.0

        # Exact match
        if actual.lower() == pattern.lower():
            return 1.0

        # Contains
        if pattern.lower() in actual.lower():
            return 0.8

        # Partial match
        pattern_words = pattern.lower().split()
        actual_words = actual.lower().split()
        matches = sum(1 for w in pattern_words if w in actual_words)
        if matches:
            return matches / len(pattern_words) * 0.5

        return 0.0

    def _match_exact(self, actual: Any, expected: Any) -> bool:
        """Exact match for non-text criteria."""
        return actual == expected


class ElementMatcherBuilder:
    """
    Builder for constructing element matchers fluently.

    Provides a chainable API for specifying match criteria.
    """

    def __init__(self) -> None:
        self._criteria = MatchCriteria()

    def with_role(self, role: str) -> ElementMatcherBuilder:
        """Set required role."""
        self._criteria.role = role
        return self

    def with_label(self, label: str) -> ElementMatcherBuilder:
        """Set required label."""
        self._criteria.label = label
        return self

    def with_title(self, title: str) -> ElementMatcherBuilder:
        """Set required title."""
        self._criteria.title = title
        return self

    def with_value(self, value: str) -> ElementMatcherBuilder:
        """Set required value."""
        self._criteria.value = value
        return self

    def with_id(self, identifier: str) -> ElementMatcherBuilder:
        """Set required identifier."""
        self._criteria.identifier = identifier
        return self

    def enabled(self, enabled: bool = True) -> ElementMatcherBuilder:
        """Set required enabled state."""
        self._criteria.enabled = enabled
        return self

    def focused(self, focused: bool = True) -> ElementMatcherBuilder:
        """Set required focused state."""
        self._criteria.focused = focused
        return self

    def with_attribute(self, key: str, value: Any) -> ElementMatcherBuilder:
        """Set custom attribute requirement."""
        self._criteria.attributes[key] = value
        return self

    def build(self) -> MatchCriteria:
        """Build the match criteria."""
        return self._criteria


def match_button(label: Optional[str] = None, title: Optional[str] = None) -> MatchCriteria:
    """Create criteria for matching a button."""
    return ElementMatcherBuilder().with_role("AXButton").with_label(label or "").with_title(title or "").build()


def match_text_field(label: Optional[str] = None) -> MatchCriteria:
    """Create criteria for matching a text field."""
    return ElementMatcherBuilder().with_role("AXTextField").with_label(label or "").build()


def match_checkbox(label: Optional[str] = None) -> MatchCriteria:
    """Create criteria for matching a checkbox."""
    return ElementMatcherBuilder().with_role("AXCheckBox").with_label(label or "").build()
