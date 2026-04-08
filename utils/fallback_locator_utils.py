"""
Fallback Locator Utilities

Provides fallback locator strategies for
resilient element finding in automation.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable
from dataclasses import dataclass
from enum import Enum, auto


class LocatorType(Enum):
    """Types of element locators."""
    ID = auto()
    TEXT = auto()
    CSS = auto()
    XPATH = auto()
    ACCESSIBILITY = auto()
    IMAGE = auto()


@dataclass
class LocatorResult:
    """Result of a locator attempt."""
    locator_type: LocatorType
    value: str
    success: bool
    found_element: dict[str, Any] | None = None


class FallbackLocator:
    """
    Provides fallback locator strategies.
    
    Tries multiple locator strategies in order
    until an element is found.
    """

    def __init__(self) -> None:
        self._strategies: list[tuple[LocatorType, Callable[[str], Any | None]]] = []
        self._default_strategies: list[LocatorType] = [
            LocatorType.ID,
            LocatorType.TEXT,
            LocatorType.CSS,
            LocatorType.XPATH,
        ]

    def register_strategy(
        self,
        locator_type: LocatorType,
        strategy: Callable[[str], Any | None],
    ) -> None:
        """Register a strategy for a locator type."""
        self._strategies.append((locator_type, strategy))

    def find_with_fallback(
        self,
        value: str,
        locator_types: list[LocatorType] | None = None,
    ) -> LocatorResult | None:
        """
        Try to find element using fallback strategies.
        
        Args:
            value: Value to search for.
            locator_types: Types of locators to try.
            
        Returns:
            LocatorResult if found, None otherwise.
        """
        types_to_try = locator_types or self._default_strategies
        for locator_type in types_to_try:
            for registered_type, strategy in self._strategies:
                if registered_type == locator_type:
                    result = strategy(value)
                    if result is not None:
                        return LocatorResult(
                            locator_type=locator_type,
                            value=value,
                            success=True,
                            found_element=result,
                        )
        return LocatorResult(
            locator_type=types_to_try[0],
            value=value,
            success=False,
        )

    def find_best_match(
        self,
        candidates: list[dict[str, Any]],
        criteria: dict[str, Any],
    ) -> dict[str, Any] | None:
        """
        Find best matching element from candidates.
        
        Args:
            candidates: List of candidate elements.
            criteria: Matching criteria.
            
        Returns:
            Best matching element or None.
        """
        if not candidates:
            return None
        scores = []
        for candidate in candidates:
            score = self._calculate_match_score(candidate, criteria)
            scores.append((score, candidate))
        scores.sort(key=lambda x: -x[0])
        if scores and scores[0][0] > 0:
            return scores[0][1]
        return None

    def _calculate_match_score(
        self,
        element: dict[str, Any],
        criteria: dict[str, Any],
    ) -> float:
        """Calculate match score for an element."""
        score = 0.0
        for key, expected in criteria.items():
            actual = element.get(key)
            if actual == expected:
                score += 1.0
        return score
