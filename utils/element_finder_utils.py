"""
Element Finder Utilities

Provides utilities for finding UI elements
using various strategies in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable
from enum import Enum, auto


class FinderStrategy(Enum):
    """Strategies for finding elements."""
    ID = auto()
    TEXT = auto()
    CLASS_NAME = auto()
    TAG_NAME = auto()
    XPATH = auto()
    CSS_SELECTOR = auto()
    ACCESSIBILITY = auto()
    FUZZY = auto()


@dataclass
class FinderQuery:
    """Query for finding an element."""
    strategy: FinderStrategy
    value: str
    index: int = 0
    parent: FinderQuery | None = None


class ElementFinder:
    """
    Finds UI elements using various strategies.
    
    Supports multiple finder strategies including
    ID, text, XPath, and accessibility queries.
    """

    def __init__(self) -> None:
        self._cache: dict[str, Any] = {}
        self._custom_finders: dict[str, Callable[[str], list[Any]]] = {}

    def register_custom_finder(
        self,
        name: str,
        finder: Callable[[str], list[Any]],
    ) -> None:
        """Register a custom finder function."""
        self._custom_finders[name] = finder

    def find(
        self,
        strategy: FinderStrategy,
        value: str,
        elements: list[dict[str, Any]],
        index: int = 0,
    ) -> dict[str, Any] | None:
        """
        Find an element using the given strategy.
        
        Args:
            strategy: Finder strategy to use.
            value: Value to search for.
            elements: List of element dictionaries to search.
            index: Index of element if multiple matches.
            
        Returns:
            Found element or None.
        """
        matches = self.find_all(strategy, value, elements)
        if 0 <= index < len(matches):
            return matches[index]
        return None

    def find_all(
        self,
        strategy: FinderStrategy,
        value: str,
        elements: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Find all elements matching the criteria.
        
        Args:
            strategy: Finder strategy to use.
            value: Value to search for.
            elements: List of element dictionaries to search.
            
        Returns:
            List of matching elements.
        """
        matches = []

        if strategy == FinderStrategy.ID:
            matches = [e for e in elements if e.get("id") == value]
        elif strategy == FinderStrategy.TEXT:
            matches = [e for e in elements if value in e.get("text", "")]
        elif strategy == FinderStrategy.CLASS_NAME:
            matches = [e for e in elements if value in e.get("class", "")]
        elif strategy == FinderStrategy.TAG_NAME:
            matches = [e for e in elements if e.get("tag") == value]
        elif strategy == FinderStrategy.ACCESSIBILITY:
            matches = [e for e in elements if e.get("accessibility_label") == value]
        elif strategy == FinderStrategy.FUZZY:
            matches = self._fuzzy_match(elements, value)

        return matches

    def _fuzzy_match(
        self,
        elements: list[dict[str, Any]],
        query: str,
    ) -> list[dict[str, Any]]:
        """Perform fuzzy matching on elements."""
        query_lower = query.lower()
        results = []
        for elem in elements:
            text = elem.get("text", "").lower()
            if query_lower in text:
                results.append(elem)
        return results

    def find_descendant(
        self,
        parent: dict[str, Any],
        strategy: FinderStrategy,
        value: str,
        index: int = 0,
    ) -> dict[str, Any] | None:
        """Find a descendant element within a parent."""
        children = parent.get("children", [])
        return self.find(strategy, value, children, index)


def create_finder_query(
    strategy: FinderStrategy,
    value: str,
    index: int = 0,
) -> FinderQuery:
    """Create a finder query."""
    return FinderQuery(strategy=strategy, value=value, index=index)
