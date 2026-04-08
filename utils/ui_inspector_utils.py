"""UI inspector utilities.

This module provides utilities for inspecting UI elements,
extracting properties, and navigating element hierarchies.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field


@dataclass
class InspectorNode:
    """A node in the UI hierarchy for inspection."""
    element_id: str
    role: str
    name: str
    value: str = ""
    enabled: bool = True
    visible: bool = True
    focused: bool = False
    children: List["InspectorNode"] = field(default_factory=list)
    attributes: Dict[str, Any] = field(default_factory=dict)
    parent_id: Optional[str] = None


class UIHierarchyInspector:
    """Inspects and navigates UI element hierarchies."""

    def __init__(self) -> None:
        self._root: Optional[InspectorNode] = None
        self._node_map: Dict[str, InspectorNode] = {}

    def set_root(self, root: InspectorNode) -> None:
        self._root = root
        self._rebuild_map(root)

    def _rebuild_map(self, node: InspectorNode) -> None:
        self._node_map[node.element_id] = node
        for child in node.children:
            child.parent_id = node.element_id
            self._rebuild_map(child)

    def get_node(self, element_id: str) -> Optional[InspectorNode]:
        return self._node_map.get(element_id)

    def get_children(self, element_id: str) -> List[InspectorNode]:
        node = self._node_map.get(element_id)
        return node.children if node else []

    def get_parent(self, element_id: str) -> Optional[InspectorNode]:
        node = self._node_map.get(element_id)
        if node and node.parent_id:
            return self._node_map.get(node.parent_id)
        return None

    def get_ancestors(self, element_id: str) -> List[InspectorNode]:
        ancestors: List[InspectorNode] = []
        current = self.get_parent(element_id)
        while current:
            ancestors.append(current)
            current = self.get_parent(current.element_id)
        return ancestors

    def get_descendants(self, element_id: str) -> List[InspectorNode]:
        node = self._node_map.get(element_id)
        if not node:
            return []
        result: List[InspectorNode] = []
        self._collect_descendants(node, result)
        return result

    def _collect_descendants(self, node: InspectorNode, result: List[InspectorNode]) -> None:
        for child in node.children:
            result.append(child)
            self._collect_descendants(child, result)

    def search(
        self,
        predicate: Callable[[InspectorNode], bool],
    ) -> List[InspectorNode]:
        if not self._root:
            return []
        result: List[InspectorNode] = []
        self._search_recursive(self._root, predicate, result)
        return result

    def _search_recursive(
        self,
        node: InspectorNode,
        predicate: Callable[[InspectorNode], bool],
        result: List[InspectorNode],
    ) -> None:
        if predicate(node):
            result.append(node)
        for child in node.children:
            self._search_recursive(child, predicate, result)

    def filter_by_role(self, role: str) -> List[InspectorNode]:
        return self.search(lambda n: n.role == role)

    def filter_by_name(self, name: str) -> List[InspectorNode]:
        return self.search(lambda n: n.name == name)

    def filter_by_attribute(
        self,
        key: str,
        value: Any,
    ) -> List[InspectorNode]:
        return self.search(lambda n: n.attributes.get(key) == value)


def extract_text_content(node: InspectorNode) -> str:
    """Extract all text content from a node and its children.

    Args:
        node: InspectorNode to extract from.

    Returns:
        Concatenated text content.
    """
    parts = []
    if node.name:
        parts.append(node.name)
    if node.value:
        parts.append(node.value)
    for child in node.children:
        parts.append(extract_text_content(child))
    return " ".join(parts)


def to_dict(node: InspectorNode) -> Dict[str, Any]:
    """Convert InspectorNode to dictionary.

    Args:
        node: Node to convert.

    Returns:
        Dictionary representation.
    """
    return {
        "element_id": node.element_id,
        "role": node.role,
        "name": node.name,
        "value": node.value,
        "enabled": node.enabled,
        "visible": node.visible,
        "focused": node.focused,
        "attributes": node.attributes,
        "children": [to_dict(c) for c in node.children],
    }


__all__ = [
    "InspectorNode",
    "UIHierarchyInspector",
    "extract_text_content",
    "to_dict",
]
