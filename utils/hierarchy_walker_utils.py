"""
Hierarchy Walker Utilities

Provides utilities for walking UI element hierarchies
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable, Iterator


class HierarchyWalker:
    """
    Walks UI element hierarchies.
    
    Provides traversal methods including
    depth-first and breadth-first search.
    """

    def __init__(self, root: dict[str, Any]) -> None:
        self._root = root

    def get_children(self, element: dict[str, Any]) -> list[dict[str, Any]]:
        """Get children of an element."""
        return element.get("children", [])

    def walk_depth_first(
        self,
        element: dict[str, Any] | None = None,
        callback: Callable[[dict[str, Any]], bool | None] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """
        Depth-first traversal of hierarchy.
        
        Args:
            element: Starting element (default: root).
            callback: Optional callback returning False to skip children.
            
        Yields:
            Elements in depth-first order.
        """
        elem = elem or self._root
        should_continue = True
        if callback:
            should_continue = callback(elem)
        yield elem
        if should_continue is not False:
            for child in self.get_children(elem):
                yield from self.walk_depth_first(child, callback)

    def walk_breadth_first(
        self,
        element: dict[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """
        Breadth-first traversal of hierarchy.
        
        Args:
            element: Starting element (default: root).
            
        Yields:
            Elements in breadth-first order.
        """
        from collections import deque
        elem = elem or self._root
        queue = deque([elem])
        while queue:
            current = queue.popleft()
            yield current
            queue.extend(self.get_children(current))

    def find_depth_first(
        self,
        predicate: Callable[[dict[str, Any]], bool],
        element: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Find first element matching predicate (depth-first)."""
        for elem in self.walk_depth_first(element):
            if predicate(elem):
                return elem
        return None

    def find_all(
        self,
        predicate: Callable[[dict[str, Any]], bool],
        element: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Find all elements matching predicate."""
        return [e for e in self.walk_depth_first(element) if predicate(e)]

    def count_elements(
        self,
        element: dict[str, Any] | None = None,
    ) -> int:
        """Count total elements in hierarchy."""
        return sum(1 for _ in self.walk_depth_first(element))
