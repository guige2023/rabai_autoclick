"""
Breadcrumb Utilities

Provides breadcrumb navigation and path tracking
for UI automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class BreadcrumbNode:
    """Represents a node in a breadcrumb trail."""
    label: str
    path: str
    element_id: str | None = None
    data: dict[str, Any] | None = None


class BreadcrumbTrail:
    """
    Manages breadcrumb navigation trails.
    
    Tracks navigation path and provides methods
    for traversing and manipulating breadcrumbs.
    """

    def __init__(self) -> None:
        self._nodes: list[BreadcrumbNode] = []
        self._max_depth: int = 50

    def push(
        self,
        label: str,
        path: str,
        element_id: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> BreadcrumbNode:
        """Push a new node onto the trail."""
        node = BreadcrumbNode(
            label=label,
            path=path,
            element_id=element_id,
            data=data,
        )
        self._nodes.append(node)
        if len(self._nodes) > self._max_depth:
            self._nodes.pop(0)
        return node

    def pop(self) -> BreadcrumbNode | None:
        """Pop the last node from the trail."""
        if self._nodes:
            return self._nodes.pop()
        return None

    def peek(self) -> BreadcrumbNode | None:
        """Peek at the last node without removing it."""
        if self._nodes:
            return self._nodes[-1]
        return None

    def get_trail(self) -> list[BreadcrumbNode]:
        """Get the full breadcrumb trail."""
        return list(self._nodes)

    def get_path_string(self, separator: str = " > ") -> str:
        """Get breadcrumb trail as a string."""
        return separator.join(node.label for node in self._nodes)

    def navigate_to(self, index: int) -> BreadcrumbNode | None:
        """Navigate to a specific index in the trail."""
        if 0 <= index < len(self._nodes):
            self._nodes = self._nodes[:index + 1]
            return self._nodes[-1]
        return None

    def clear(self) -> None:
        """Clear the breadcrumb trail."""
        self._nodes.clear()

    def get_depth(self) -> int:
        """Get current depth of the trail."""
        return len(self._nodes)

    def set_max_depth(self, max_depth: int) -> None:
        """Set maximum trail depth."""
        self._max_depth = max_depth
        while len(self._nodes) > self._max_depth:
            self._nodes.pop(0)
