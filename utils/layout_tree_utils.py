"""
Layout tree utilities for traversing and analyzing UI hierarchy.

Provides tree data structure and traversal algorithms for
UI element hierarchy analysis.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Iterator, Optional


@dataclass
class LayoutNode:
    """Node in the layout tree."""
    element_id: str
    name: str = ""
    bounds: tuple[float, float, float, float] = (0, 0, 0, 0)  # x, y, w, h
    children: list[LayoutNode] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)

    @property
    def x(self) -> float:
        return self.bounds[0]

    @property
    def y(self) -> float:
        return self.bounds[1]

    @property
    def width(self) -> float:
        return self.bounds[2]

    @property
    def height(self) -> float:
        return self.bounds[3]

    @property
    def x2(self) -> float:
        return self.x + self.width

    @property
    def y2(self) -> float:
        return self.y + self.height

    @property
    def center_x(self) -> float:
        return self.x + self.width / 2

    @property
    def center_y(self) -> float:
        return self.y + self.height / 2

    @property
    def area(self) -> float:
        return self.width * self.height

    def is_leaf(self) -> bool:
        return len(self.children) == 0

    def add_child(self, child: LayoutNode) -> None:
        self.children.append(child)


class LayoutTree:
    """Tree structure for UI layout hierarchy."""

    def __init__(self, root: Optional[LayoutNode] = None):
        self.root = root

    def find_by_id(self, element_id: str) -> Optional[LayoutNode]:
        """Find a node by element ID."""
        if not self.root:
            return None
        return self._find_recursive(self.root, element_id)

    def _find_recursive(self, node: LayoutNode, element_id: str) -> Optional[LayoutNode]:
        if node.element_id == element_id:
            return node
        for child in node.children:
            found = self._find_recursive(child, element_id)
            if found:
                return found
        return None

    def find_by_predicate(
        self,
        predicate: Callable[[LayoutNode], bool],
    ) -> list[LayoutNode]:
        """Find all nodes matching a predicate."""
        if not self.root:
            return []
        results = []
        self._find_predicate_recursive(self.root, predicate, results)
        return results

    def _find_predicate_recursive(
        self,
        node: LayoutNode,
        predicate: Callable[[LayoutNode], bool],
        results: list[LayoutNode],
    ) -> None:
        if predicate(node):
            results.append(node)
        for child in node.children:
            self._find_predicate_recursive(child, predicate, results)

    def traverse_bfs(self) -> Iterator[LayoutNode]:
        """Breadth-first traversal."""
        if not self.root:
            return
        queue = [self.root]
        while queue:
            node = queue.pop(0)
            yield node
            queue.extend(node.children)

    def traverse_dfs(self) -> Iterator[LayoutNode]:
        """Depth-first traversal (pre-order)."""
        if not self.root:
            return
        stack = [self.root]
        while stack:
            node = stack.pop()
            yield node
            stack.extend(reversed(node.children))

    def depth_of(self, element_id: str) -> int:
        """Get depth of a node in the tree."""
        if not self.root:
            return -1
        return self._depth_recursive(self.root, element_id, 0)

    def _depth_recursive(self, node: LayoutNode, element_id: str, depth: int) -> int:
        if node.element_id == element_id:
            return depth
        for child in node.children:
            result = self._depth_recursive(child, element_id, depth + 1)
            if result >= 0:
                return result
        return -1

    def siblings_of(self, element_id: str) -> list[LayoutNode]:
        """Get sibling nodes."""
        parent = self._find_parent_of(element_id)
        if not parent:
            return []
        return [c for c in parent.children if c.element_id != element_id]

    def _find_parent_of(self, element_id: str) -> Optional[LayoutNode]:
        """Find parent of a node."""
        if not self.root:
            return None
        return self._find_parent_recursive(self.root, element_id, None)

    def _find_parent_recursive(
        self,
        node: LayoutNode,
        element_id: str,
        parent: Optional[LayoutNode],
    ) -> Optional[LayoutNode]:
        if node.element_id == element_id:
            return parent
        for child in node.children:
            found = self._find_parent_recursive(child, element_id, node)
            if found is not None:
                return found
        return None

    def leaf_nodes(self) -> list[LayoutNode]:
        """Get all leaf nodes."""
        return self.find_by_predicate(lambda n: n.is_leaf())

    def node_count(self) -> int:
        """Count total nodes."""
        return sum(1 for _ in self.traverse_dfs())


__all__ = ["LayoutTree", "LayoutNode"]
