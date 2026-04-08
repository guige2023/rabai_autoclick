"""
Element Matcher Utilities

Provides utilities for matching UI elements
based on various criteria in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable
import re


class ElementMatcher:
    """
    Matches UI elements based on criteria.
    
    Provides flexible matching with support for
    exact, pattern, and custom matching modes.
    """

    def __init__(self) -> None:
        self._matchers: dict[str, Callable[[Any], bool]] = {}

    def register_matcher(
        self,
        name: str,
        matcher: Callable[[Any], bool],
    ) -> None:
        """Register a named matcher function."""
        self._matchers[name] = matcher

    def match(
        self,
        element: dict[str, Any],
        criteria: dict[str, Any],
    ) -> bool:
        """
        Match an element against criteria.
        
        Args:
            element: Element to match.
            criteria: Matching criteria.
            
        Returns:
            True if element matches all criteria.
        """
        for key, expected in criteria.items():
            actual = element.get(key)
            if isinstance(expected, str) and "*" in expected:
                pattern = expected.replace("*", ".*")
                if not re.match(pattern, str(actual)):
                    return False
            elif actual != expected:
                return False
        return True

    def match_any(
        self,
        element: dict[str, Any],
        criteria_list: list[dict[str, Any]],
    ) -> bool:
        """Match if element matches any criteria."""
        return any(self.match(element, c) for c in criteria_list)

    def match_all(
        self,
        element: dict[str, Any],
        criteria_list: list[dict[str, Any]],
    ) -> bool:
        """Match if element matches all criteria."""
        return all(self.match(element, c) for c in criteria_list)


def match_by_id(element: dict[str, Any], expected_id: str) -> bool:
    """Match element by ID."""
    return element.get("id") == expected_id


def match_by_text(
    element: dict[str, Any],
    text: str,
    exact: bool = False,
) -> bool:
    """Match element by text content."""
    element_text = element.get("text", "")
    if exact:
        return element_text == text
    return text in element_text


def match_by_class(
    element: dict[str, Any],
    class_name: str,
) -> bool:
    """Match element by class name."""
    classes = element.get("class", "").split()
    return class_name in classes


def match_by_visible(element: dict[str, Any]) -> bool:
    """Match element by visibility."""
    return element.get("visible", True)
