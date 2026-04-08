"""
Quality Checker Utilities

Provides utilities for quality checks on UI elements
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any


class QualityChecker:
    """
    Performs quality checks on UI elements.
    
    Validates element properties against
    quality criteria.
    """

    def __init__(self) -> None:
        self._checks: dict[str, callable] = {}

    def register_check(
        self,
        name: str,
        check: callable,
    ) -> None:
        """Register a quality check."""
        self._checks[name] = check

    def check_element(
        self,
        element: dict[str, Any],
    ) -> dict[str, bool]:
        """
        Run all checks on an element.
        
        Returns:
            Dict of check name to pass/fail.
        """
        results = {}
        for name, check in self._checks.items():
            try:
                results[name] = bool(check(element))
            except Exception:
                results[name] = False
        return results

    def is_usable(self, element: dict[str, Any]) -> bool:
        """Check if element is usable."""
        visible = element.get("visible", True)
        enabled = element.get("enabled", True)
        has_bounds = "bounds" in element
        return visible and enabled and has_bounds

    def get_quality_score(
        self,
        element: dict[str, Any],
    ) -> float:
        """Calculate quality score 0.0 - 1.0."""
        score = 0.0
        if element.get("visible", True):
            score += 0.3
        if element.get("enabled", True):
            score += 0.3
        if "bounds" in element:
            score += 0.2
        if element.get("name") or element.get("text"):
            score += 0.2
        return score
