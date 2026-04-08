"""
Element Tree Utilities

Provides utilities for traversing and manipulating
UI element trees in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable, Iterator


class ElementTree:
    """
    Represents a tree of UI elements.
    
    Provides traversal, search, and manipulation
    operations on element hierarchies.
    """

    def __init__(self, root: dict[str, Any]) -> None:
        self._root = root

    def root(self) -> dict[str, Any]:
        """Get the root element."""
        return self._root

    def children(self, element: dict[str, Any]) -> list[dict[str, Any]]:
        """Get children of an element."""
        return element.get("children", [])

    def parent(self, element: dict[str, Any], root: dict[str, Any] | None = None) -> dict[str, Any] | None:
        """Find parent of an element."""
        root = root or self._root
        return self._find_parent(root, element)

    def _find_parent(
        self,
        current: dict[str, Any],
        target: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Recursively find parent."""
        for child in current.get("children", []):
            if child is target:
                return current
            found = self._find_parent(child, target)
            if found:
                return found
        return None

    def traverse(
        self,
        element: dict[str, Any] | None = None,
        order: str = "pre",
    ) -> Iterator[dict[str, Any]]:
        """
        Traverse element tree.
        
        Args:
            element: Starting element (default: root).
            order: Traversal order ("pre" or "post").
            
        Yields:
            Each element in traversal order.
        """
        elem = elem or self._root
        if order == "pre":
            yield elem
        for child in self.children(elem):
            yield from self.traverse(child, order)
        if order == "post":
            yield elem

    def find_all(
        self,
        predicate: Callable[[dict[str, Any]], bool],
        element: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Find all elements matching predicate."""
        results = []
        for elem in self.traverse(element):
            if predicate(elem):
                results.append(elem)
        return results

    def find_first(
        self,
        predicate: Callable[[dict[str, Any]], bool],
        element: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Find first element matching predicate."""
        for elem in self.traverse(element):
            if predicate(elem):
                return elem
        return None

    def depth(self, element: dict[str, Any], current_depth: int = 0) -> int:
        """Get depth of element in tree."""
        parent = self.parent(element)
        if parent is None:
            return current_depth
        return self.depth(parent, current_depth + 1)

    def path_to(self, element: dict[str, Any]) -> list[dict[str, Any]]:
        """Get path from root to element."""
        path = []
        current = element
        while current is not None:
            path.append(current)
            current = self.parent(current)
        return list(reversed(path))
