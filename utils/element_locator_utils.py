"""
Element locator strategies for UI automation.

This module provides various strategies for locating UI elements,
including XPath, CSS selectors, accessibility attributes, and
heuristic-based matching.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, Optional, List, Dict, Any, Tuple
from enum import Enum, auto


class LocatorType(Enum):
    """Supported element locator types."""
    XPATH = auto()
    CSS = auto()
    ACCESSIBILITY_ID = auto()
    ACCESSIBILITY_LABEL = auto()
    ACCESSIBILITY_ROLE = auto()
    TEXT = auto()
    PARTIAL_TEXT = auto()
    IMAGE = auto()
    FUZZY = auto()


@dataclass
class Locator:
    """
    Element locator with multiple strategy fallbacks.

    Attributes:
        type: The primary locator strategy type.
        value: The locator value/expression.
        index: Element index for multiple matches (0 = first).
        timeout: Maximum wait time in seconds.
        required: Whether element must be found.
        fallbacks: Alternative locators if primary fails.
    """
    type: LocatorType
    value: str
    index: int = 0
    timeout: float = 10.0
    required: bool = True
    fallbacks: List[Locator] = field(default_factory=list)

    def __str__(self) -> str:
        suffix = f"[{self.index}]" if self.index > 0 else ""
        return f"{self.type.name}:{self.value}{suffix}"


@dataclass
class LocatorResult:
    """Result of a locator operation."""
    found: bool
    element: Optional[Any] = None
    locator_used: Optional[Locator] = None
    error: Optional[str] = None
    attempts: int = 0


class XPathBuilder:
    """
    Builder for constructing XPath expressions.

    Supports chaining of conditions, axis navigation,
    and common UI element patterns.
    """

    def __init__(self, expression: str = "*") -> None:
        self._parts: List[str] = [expression]

    @property
    def xpath(self) -> str:
        """Get the constructed XPath expression."""
        return "/".join(self._parts)

    def tag(self, tag_name: str) -> XPathBuilder:
        """Match element by tag name."""
        self._parts.append(tag_name)
        return self

    def with_id(self, element_id: str) -> XPathBuilder:
        """Match element with @id attribute."""
        self._parts.append(f'[@id="{element_id}"]')
        return self

    def with_class(self, class_name: str) -> XPathBuilder:
        """Match element with @class containing name."""
        self._parts.append(f'[contains(@class,"{class_name}")]')
        return self

    def with_text(self, text: str, exact: bool = True) -> XPathBuilder:
        """Match element by text content."""
        if exact:
            self._parts.append(f'[text()="{text}"]')
        else:
            self._parts.append(f'[contains(text(),"{text}")]')
        return self

    def with_attribute(
        self,
        attr: str,
        value: str,
        operator: str = "=",
    ) -> XPathBuilder:
        """Match element with arbitrary attribute."""
        self._parts.append(f'[@{attr}{operator}"{value}"]')
        return self

    def with_role(self, role: str) -> XPathBuilder:
        """Match element by accessibility role."""
        self._parts.append(f'[@AXRole="{role}"]')
        return self

    def with_label(self, label: str) -> XPathBuilder:
        """Match element with accessibility label."""
        self._parts.append(f'[@AXLabel="{label}"]')
        return self

    def ancestor(self, tag: str = "*") -> XPathBuilder:
        """Navigate to ancestor element."""
        self._parts.append(f"ancestor::{tag}")
        return self

    def descendant(self, tag: str = "*") -> XPathBuilder:
        """Navigate to descendant element."""
        self._parts.append(f"descendant::{tag}")
        return self

    def parent(self) -> XPathBuilder:
        """Navigate to parent element."""
        self._parts.append("..")
        return self

    def following_sibling(self, tag: str = "*") -> XPathBuilder:
        """Navigate to following sibling element."""
        self._parts.append(f"following-sibling::{tag}")
        return self

    def position(self, index: int) -> XPathBuilder:
        """Match element by position (1-based)."""
        self._parts.append(f"[position()={index}]")
        return self

    def last(self) -> XPathBuilder:
        """Match last element."""
        self._parts.append("[last()]")
        return self

    def first(self) -> XPathBuilder:
        """Match first element."""
        return self.position(1)


class LocatorEvaluator:
    """
    Evaluates and scores element locators for fuzzy matching.

    Provides similarity scoring between locators and candidate
    elements to handle dynamic UI changes.
    """

    def __init__(self) -> None:
        self._cache: Dict[str, float] = {}

    def score(
        self,
        locator: Locator,
        candidate: Dict[str, Any],
    ) -> float:
        """
        Calculate match score between locator and candidate.

        Returns:
            Score from 0.0 (no match) to 1.0 (perfect match).
        """
        if locator.type == LocatorType.TEXT:
            return self._score_text(locator.value, candidate)
        elif locator.type == LocatorType.ACCESSIBILITY_LABEL:
            return self._score_label(locator.value, candidate)
        elif locator.type == LocatorType.ACCESSIBILITY_ROLE:
            return self._score_role(locator.value, candidate)
        return 0.0

    def _score_text(self, text: str, candidate: Dict[str, Any]) -> float:
        """Score text match with fuzzy matching."""
        candidate_text = candidate.get("text", "") or ""
        if text == candidate_text:
            return 1.0
        if text.lower() in candidate_text.lower():
            return 0.8
        # Levenshtein-like scoring
        max_len = max(len(text), len(candidate_text))
        if max_len == 0:
            return 0.0
        similarity = self._string_similarity(text, candidate_text)
        return similarity * 0.6

    def _score_label(self, label: str, candidate: Dict[str, Any]) -> float:
        """Score accessibility label match."""
        candidate_label = candidate.get("AXLabel", "") or ""
        if label.lower() == candidate_label.lower():
            return 1.0
        if label.lower() in candidate_label.lower():
            return 0.9
        return self._string_similarity(label, candidate_label) * 0.7

    def _score_role(self, role: str, candidate: Dict[str, Any]) -> float:
        """Score accessibility role match."""
        candidate_role = candidate.get("AXRole", "") or ""
        if role.lower() == candidate_role.lower():
            return 1.0
        return 0.0

    def _string_similarity(self, s1: str, s2: str) -> float:
        """Calculate string similarity using simple ratio."""
        if not s1 or not s2:
            return 0.0
        longer = s1 if len(s1) >= len(s2) else s2
        shorter = s2 if len(s1) >= len(s2) else s1
        matches = sum(1 for c in shorter if c in longer)
        return matches / len(longer)


class ElementLocator:
    """
    Main element locator with strategy pattern support.

    Supports multiple locator strategies with automatic
    fallback and retry logic.
    """

    def __init__(self) -> None:
        self._strategies: Dict[LocatorType, Callable] = {}
        self._evaluator = LocatorEvaluator()
        self._cache: Dict[str, Any] = {}

    def register_strategy(
        self,
        locator_type: LocatorType,
        strategy: Callable[[Locator], Any],
    ) -> None:
        """Register a lookup strategy for a locator type."""
        self._strategies[locator_type] = strategy

    def locate(self, locator: Locator) -> LocatorResult:
        """Locate element using primary locator and fallbacks."""
        result = LocatorResult(found=False, attempts=0)

        # Try primary locator
        result = self._try_locate(locator)
        if result.found or not locator.fallbacks:
            return result

        # Try fallbacks in order
        for fallback in locator.fallbacks:
            result.attempts += 1
            fb_result = self._try_locate(fallback)
            if fb_result.found:
                return fb_result

        return result

    def _try_locate(self, locator: Locator) -> LocatorResult:
        """Attempt to locate using a single locator."""
        result = LocatorResult(found=False, locator_used=locator, attempts=1)

        if locator.type not in self._strategies:
            result.error = f"No strategy registered for {locator.type}"
            return result

        try:
            element = self._strategies[locator.type](locator)
            result.found = element is not None
            result.element = element
        except Exception as e:
            result.error = str(e)

        return result

    def locate_multiple(self, locator: Locator) -> List[Any]:
        """Locate all elements matching locator."""
        if locator.type not in self._strategies:
            return []
        try:
            elements = self._strategies[locator.type](locator)
            return elements if isinstance(elements, list) else [elements]
        except Exception:
            return []


def xpath(tag: str = "*") -> XPathBuilder:
    """Create new XPath builder."""
    return XPathBuilder(tag)


def fuzzy_match(text: str, threshold: float = 0.7) -> Locator:
    """Create fuzzy text match locator."""
    return Locator(
        type=LocatorType.FUZZY,
        value=text,
        fallbacks=[
            Locator(type=LocatorType.TEXT, value=text, exact=False),
            Locator(type=LocatorType.PARTIAL_TEXT, value=text),
        ],
    )
