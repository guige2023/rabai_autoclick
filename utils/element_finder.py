"""
Element Finder Utility

Provides flexible element finding with multiple strategies.
Supports fallback chains when primary finding method fails.

Example:
    >>> finder = ElementFinder(accessibility_tree)
    >>> element = finder.find_one(role="button", name="Submit")
    >>> elements = finder.find_all(role="textfield")
"""

from __future__ import annotations

from typing import Any, Callable, Optional


class ElementFinder:
    """
    Flexible element finder with strategy chaining.

    Args:
        elements: List of accessibility elements to search.
    """

    def __init__(self, elements: Optional[list[dict]] = None) -> None:
        self.elements: list[dict] = elements or []

    def set_elements(self, elements: list[dict]) -> None:
        """Replace the element list."""
        self.elements = elements

    def find_one(
        self,
        role: Optional[str] = None,
        name: Optional[str] = None,
        value: Optional[str] = None,
        enabled: Optional[bool] = None,
        focused: Optional[bool] = None,
        index: Optional[int] = None,
        predicate: Optional[Callable[[dict], bool]] = None,
    ) -> Optional[dict]:
        """
        Find a single element matching criteria.

        Args:
            role: Element role to match.
            name: Element name to match (partial match).
            value: Element value to match.
            enabled: Element enabled state.
            focused: Element focused state.
            index: Return nth matching element (0-indexed).
            predicate: Custom predicate function.

        Returns:
            First matching element dict, or None.
        """
        results = self._filter(
            role=role,
            name=name,
            value=value,
            enabled=enabled,
            focused=focused,
            predicate=predicate,
        )

        if index is not None and 0 <= index < len(results):
            return results[index]

        return results[0] if results else None

    def find_all(
        self,
        role: Optional[str] = None,
        name: Optional[str] = None,
        value: Optional[str] = None,
        enabled: Optional[bool] = None,
        focused: Optional[bool] = None,
        predicate: Optional[Callable[[dict], bool]] = None,
        limit: Optional[int] = None,
    ) -> list[dict]:
        """
        Find all elements matching criteria.

        Args:
            role: Element role to match.
            name: Element name to match (partial match).
            value: Element value to match.
            enabled: Element enabled state.
            focused: Element focused state.
            predicate: Custom predicate function.
            limit: Maximum number of results.

        Returns:
            List of matching element dicts.
        """
        results = self._filter(
            role=role,
            name=name,
            value=value,
            enabled=enabled,
            focused=focused,
            predicate=predicate,
        )

        if limit:
            return results[:limit]
        return results

    def _filter(
        self,
        role: Optional[str] = None,
        name: Optional[str] = None,
        value: Optional[str] = None,
        enabled: Optional[bool] = None,
        focused: Optional[bool] = None,
        predicate: Optional[Callable[[dict], bool]] = None,
    ) -> list[dict]:
        """Internal filter implementation."""
        results: list[dict] = []

        for element in self.elements:
            # Role filter
            if role is not None:
                if element.get("role", "").lower() != role.lower():
                    continue

            # Name filter (partial match)
            if name is not None:
                el_name = element.get("name", "") or ""
                if name.lower() not in el_name.lower():
                    continue

            # Value filter
            if value is not None:
                el_value = element.get("value", "") or ""
                if value.lower() not in el_value.lower():
                    continue

            # Enabled filter
            if enabled is not None:
                if element.get("enabled", True) != enabled:
                    continue

            # Focused filter
            if focused is not None:
                if element.get("focused", False) != focused:
                    continue

            # Custom predicate
            if predicate is not None and not predicate(element):
                continue

            results.append(element)

        return results

    def find_by_role(self, role: str) -> list[dict]:
        """Find all elements with given role."""
        return self.find_all(role=role)

    def find_by_name(self, name: str) -> list[dict]:
        """Find all elements whose name contains given string."""
        return self.find_all(name=name)

    def find_focused(self) -> Optional[dict]:
        """Find the currently focused element."""
        return self.find_one(focused=True)

    def find_enabled(self) -> list[dict]:
        """Find all enabled elements."""
        return self.find_all(enabled=True)

    def find_in_bounds(
        self,
        x: int,
        y: int,
        width: int,
        height: int,
    ) -> list[dict]:
        """Find elements within given bounds."""
        def in_bounds(el: dict) -> bool:
            bounds = el.get("bounds", [0, 0, 0, 0])
            if len(bounds) < 4:
                return False
            bx, by, bw, bh = bounds

            # Check if element center is within bounds
            cx, cy = bx + bw // 2, by + bh // 2
            return (x <= cx <= x + width) and (y <= cy <= y + height)

        return self.find_all(predicate=in_bounds)

    def count(self, role: Optional[str] = None) -> int:
        """Count elements matching criteria."""
        return len(self.find_all(role=role))
