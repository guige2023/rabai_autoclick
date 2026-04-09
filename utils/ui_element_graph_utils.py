"""
UI Element Graph Utilities

Build and query a graph representation of the UI element hierarchy,
enabling graph-based queries like finding elements by relationship
(parent, child, sibling, ancestor, descendant).

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, List, Dict


@dataclass
class ElementNode:
    """A node in the UI element graph."""
    element_id: str
    role: str
    label: str
    parent_id: Optional[str] = None
    child_ids: list[str] = field(default_factory=list)
    attributes: dict = field(default_factory=dict)


class UIElementGraph:
    """A graph representation of the UI element hierarchy."""

    def __init__(self):
        self._nodes: Dict[str, ElementNode] = {}
        self._roots: List[str] = []

    def add_node(
        self,
        element_id: str,
        role: str,
        label: str,
        parent_id: Optional[str] = None,
        attributes: Optional[dict] = None,
    ) -> None:
        """Add a node to the graph."""
        node = ElementNode(
            element_id=element_id,
            role=role,
            label=label,
            parent_id=parent_id,
            attributes=attributes or {},
        )
        self._nodes[element_id] = node

        if parent_id and parent_id in self._nodes:
            if element_id not in self._nodes[parent_id].child_ids:
                self._nodes[parent_id].child_ids.append(element_id)
        elif parent_id is None:
            if element_id not in self._roots:
                self._roots.append(element_id)

    def get_node(self, element_id: str) -> Optional[ElementNode]:
        """Get a node by ID."""
        return self._nodes.get(element_id)

    def get_children(self, element_id: str) -> List[ElementNode]:
        """Get all direct children of a node."""
        node = self._nodes.get(element_id)
        if not node:
            return []
        return [self._nodes[cid] for cid in node.child_ids if cid in self._nodes]

    def get_ancestors(self, element_id: str) -> List[ElementNode]:
        """Get all ancestors of a node (path to root)."""
        ancestors = []
        current_id = element_id
        visited = set()
        while current_id:
            if current_id in visited:
                break
            visited.add(current_id)
            node = self._nodes.get(current_id)
            if not node or not node.parent_id:
                break
            parent = self._nodes.get(node.parent_id)
            if parent:
                ancestors.append(parent)
            current_id = node.parent_id
        return ancestors

    def get_descendants(self, element_id: str) -> List[ElementNode]:
        """Get all descendants of a node (recursive children)."""
        result = []
        to_visit = list(self._nodes.get(element_id, ElementNode(element_id="", role="", label="")).child_ids)
        visited = set()
        while to_visit:
            cid = to_visit.pop(0)
            if cid in visited:
                continue
            visited.add(cid)
            node = self._nodes.get(cid)
            if node:
                result.append(node)
                to_visit.extend(node.child_ids)
        return result

    def get_siblings(self, element_id: str) -> List[ElementNode]:
        """Get all siblings of a node (same parent)."""
        node = self._nodes.get(element_id)
        if not node or not node.parent_id:
            return []
        parent = self._nodes.get(node.parent_id)
        if not parent:
            return []
        return [self._nodes[cid] for cid in parent.child_ids
                if cid in self._nodes and cid != element_id]

    def find_by_role(self, role: str) -> List[ElementNode]:
        """Find all nodes with a specific role."""
        return [n for n in self._nodes.values() if n.role == role]

    def clear(self) -> None:
        """Clear all nodes from the graph."""
        self._nodes.clear()
        self._roots.clear()
