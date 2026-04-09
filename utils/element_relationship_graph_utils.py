"""Element Relationship Graph Utilities.

Builds and traverses element relationship graphs for UI automation.

Example:
    >>> from element_relationship_graph_utils import ElementGraph
    >>> graph = ElementGraph()
    >>> graph.add_element("btn", parent="dialog")
    >>> parents = graph.get_parents("btn")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set


@dataclass
class GraphNode:
    """A node in the element graph."""
    element_id: str
    element_type: str = ""
    attributes: Dict[str, Any] = field(default_factory=dict)
    children: Set[str] = field(default_factory=set)


class ElementGraph:
    """Graph of UI element relationships."""

    def __init__(self):
        """Initialize an empty element graph."""
        self._nodes: Dict[str, GraphNode] = {}
        self._parent_map: Dict[str, Set[str]] = {}

    def add_element(
        self,
        element_id: str,
        parent: Optional[str] = None,
        element_type: str = "",
        **attributes: Any,
    ) -> None:
        """Add an element to the graph.

        Args:
            element_id: Unique element identifier.
            parent: Optional parent element ID.
            element_type: Element type (button, input, etc).
            **attributes: Additional element attributes.
        """
        if element_id not in self._nodes:
            self._nodes[element_id] = GraphNode(
                element_id=element_id,
                element_type=element_type,
                attributes=attributes,
            )

        if parent:
            self._parent_map.setdefault(element_id, set()).add(parent)
            if parent in self._nodes:
                self._nodes[parent].children.add(element_id)

    def get_parents(self, element_id: str) -> List[str]:
        """Get parent elements of an element.

        Args:
            element_id: Element ID.

        Returns:
            List of parent element IDs.
        """
        return list(self._parent_map.get(element_id, set()))

    def get_children(self, element_id: str) -> List[str]:
        """Get child elements of an element.

        Args:
            element_id: Element ID.

        Returns:
            List of child element IDs.
        """
        if element_id in self._nodes:
            return list(self._nodes[element_id].children)
        return []

    def get_ancestors(self, element_id: str) -> List[str]:
        """Get all ancestors of an element.

        Args:
            element_id: Element ID.

        Returns:
            List of ancestor element IDs (bottom-up).
        """
        ancestors = []
        visited: Set[str] = set()
        queue = self.get_parents(element_id)

        while queue:
            parent = queue.pop(0)
            if parent in visited:
                continue
            visited.add(parent)
            ancestors.append(parent)
            queue.extend(self.get_parents(parent))

        return ancestors

    def get_descendants(self, element_id: str) -> List[str]:
        """Get all descendants of an element.

        Args:
            element_id: Element ID.

        Returns:
            List of descendant element IDs.
        """
        descendants = []
        visited: Set[str] = set()
        queue = self.get_children(element_id)

        while queue:
            child = queue.pop(0)
            if child in visited:
                continue
            visited.add(child)
            descendants.append(child)
            queue.extend(self.get_children(child))

        return descendants

    def find_by_type(self, element_type: str) -> List[str]:
        """Find elements by type.

        Args:
            element_type: Element type to search.

        Returns:
            List of matching element IDs.
        """
        return [
            eid for eid, node in self._nodes.items()
            if node.element_type == element_type
        ]

    def find_by_attribute(self, key: str, value: Any) -> List[str]:
        """Find elements by attribute value.

        Args:
            key: Attribute key.
            value: Attribute value to match.

        Returns:
            List of matching element IDs.
        """
        return [
            eid for eid, node in self._nodes.items()
            if node.attributes.get(key) == value
        ]

    def to_dict(self) -> Dict[str, Any]:
        """Export graph as dictionary."""
        return {
            "nodes": {
                eid: {
                    "type": node.element_type,
                    "attributes": node.attributes,
                    "children": list(node.children),
                }
                for eid, node in self._nodes.items()
            }
        }
