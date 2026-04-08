"""Accessibility tree traversal and querying utilities.

This module provides utilities for parsing, traversing, and querying
accessibility trees from GUI frameworks.
"""

from __future__ import annotations

from typing import Any, Callable, Iterator, List, Optional, TypeVar

T = TypeVar("T")


class AXNode:
    """Represents a node in an accessibility tree."""

    def __init__(
        self,
        role: str,
        name: Optional[str] = None,
        value: Optional[str] = None,
        children: Optional[List["AXNode"]] = None,
        attributes: Optional[dict] = None,
    ) -> None:
        self.role = role
        self.name = name or ""
        self.value = value or ""
        self.children = children or []
        self.attributes = attributes or {}

    def __repr__(self) -> str:
        return f"AXNode(role={self.role!r}, name={self.name!r})"


class AccessibilityTree:
    """An accessibility tree structure."""

    def __init__(self, root: Optional[AXNode] = None) -> None:
        self.root = root

    @classmethod
    def from_dict(cls, data: dict) -> "AccessibilityTree":
        """Build tree from dictionary representation.

        Args:
            data: Dictionary with 'role', 'name', 'value', 'children'.

        Returns:
            AccessibilityTree instance.
        """
        def parse(d: dict) -> AXNode:
            return AXNode(
                role=d.get("role", "unknown"),
                name=d.get("name"),
                value=d.get("value"),
                children=[parse(c) for c in d.get("children", [])],
                attributes=d.get("attributes", {}),
            )
        return cls(root=parse(data) if data else None)

    def find_all(self, predicate: Callable[[AXNode], bool]) -> List[AXNode]:
        """Find all nodes matching predicate.

        Args:
            predicate: Function that returns True for matching nodes.

        Returns:
            List of matching nodes.
        """
        results: List[AXNode] = []
        if self.root:
            self._find_recursive(self.root, predicate, results)
        return results

    def _find_recursive(
        self,
        node: AXNode,
        predicate: Callable[[AXNode], bool],
        results: List[AXNode],
    ) -> None:
        if predicate(node):
            results.append(node)
        for child in node.children:
            self._find_recursive(child, predicate, results)

    def find_first(self, predicate: Callable[[AXNode], bool]) -> Optional[AXNode]:
        """Find first node matching predicate.

        Args:
            predicate: Function that returns True for matching node.

        Returns:
            First matching node or None.
        """
        for node in self.iter_nodes():
            if predicate(node):
                return node
        return None

    def iter_nodes(self) -> Iterator[AXNode]:
        """Iterate over all nodes in tree order.

        Yields:
            Each node in the tree.
        """
        if self.root:
            yield from self._iter_recursive(self.root)

    def _iter_recursive(self, node: AXNode) -> Iterator[AXNode]:
        yield node
        for child in node.children:
            yield from self._iter_recursive(child)

    def nodes_by_role(self, role: str) -> List[AXNode]:
        """Get all nodes with specific role.

        Args:
            role: Role name to filter by.

        Returns:
            List of nodes with that role.
        """
        return self.find_all(lambda n: n.role == role)

    def leaves(self) -> List[AXNode]:
        """Get all leaf nodes (nodes with no children).

        Returns:
            List of leaf nodes.
        """
        return self.find_all(lambda n: len(n.children) == 0)


def build_tree_from_flat(
    nodes: List[dict],
    id_field: str = "id",
    parent_field: str = "parent_id",
    root_id: Optional[str] = None,
) -> AccessibilityTree:
    """Build accessibility tree from flat node list.

    Args:
        nodes: List of node dictionaries.
        id_field: Field name for node ID.
        parent_field: Field name for parent ID.
        root_id: ID of root node (first with null parent if not specified).

    Returns:
        AccessibilityTree with constructed tree.
    """
    by_id: dict[str, AXNode] = {}
    for n in nodes:
        ax_node = AXNode(
            role=n.get("role", "unknown"),
            name=n.get("name"),
            value=n.get("value"),
            attributes=n,
        )
        by_id[n[id_field]] = ax_node

    for n in nodes:
        pid = n.get(parent_field)
        if pid and pid in by_id:
            by_id[pid].children.append(by_id[n[id_field]])

    root = None
    if root_id and root_id in by_id:
        root = by_id[root_id]
    else:
        for n in nodes:
            if not n.get(parent_field):
                root = by_id[n[id_field]]
                break

    return AccessibilityTree(root=root)


__all__ = [
    "AXNode",
    "AccessibilityTree",
    "build_tree_from_flat",
]
