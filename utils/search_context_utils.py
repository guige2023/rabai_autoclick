"""
Search Context Utilities

Provides utilities for managing search context
in UI automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any


class SearchContext:
    """
    Manages context during element searches.
    
    Tracks search scope, filters, and
    results for complex searches.
    """

    def __init__(self, scope: list[dict[str, Any]] | None = None) -> None:
        self._scope = scope or []
        self._filters: list[callable] = []
        self._results: list[dict[str, Any]] = []

    def set_scope(self, elements: list[dict[str, Any]]) -> None:
        """Set the search scope."""
        self._scope = elements

    def add_filter(self, filter_func: callable) -> None:
        """Add a filter function."""
        self._filters.append(filter_func)

    def search(
        self,
        predicate: callable,
    ) -> list[dict[str, Any]]:
        """Search scope with predicate and filters."""
        results = [e for e in self._scope if predicate(e)]
        for filter_func in self._filters:
            results = [e for e in results if filter_func(e)]
        self._results = results
        return results

    def get_results(self) -> list[dict[str, Any]]:
        """Get last search results."""
        return list(self._results)

    def get_first(self) -> dict[str, Any] | None:
        """Get first result or None."""
        return self._results[0] if self._results else None

    def clear(self) -> None:
        """Clear results and filters."""
        self._results.clear()
        self._filters.clear()
