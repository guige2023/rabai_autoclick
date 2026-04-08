"""Widget tree utilities for UI automation.

Provides tree data structure and traversal operations
for widget hierarchies in UI frameworks.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator, Optional


@dataclass
class WidgetNode:
    """A node in a widget tree.

    Attributes:
        widget_id: Unique identifier.
        widget_type: Type/kind of widget.
        name: Widget name or label.
        children: Child widget nodes.
        parent: Parent widget node.
        properties: Widget properties dictionary.
        is_expanded: Whether this node is expanded (for tree views).
        is_visible: Whether the widget is visible.
        is_enabled: Whether the widget is enabled.
    """
    widget_type: str
    widget_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    children: list[WidgetNode] = field(default_factory=list)
    parent: Optional[WidgetNode] = None
    properties: dict[str, Any] = field(default_factory=dict)
    is_expanded: bool = True
    is_visible: bool = True
    is_enabled: bool = True

    def add_child(self, child: WidgetNode) -> None:
        """Add a child widget."""
        child.parent = self
        self.children.append(child)

    def remove_child(self, widget_id: str) -> bool:
        """Remove a child by ID. Returns True if found."""
        for i, child in enumerate(self.children):
            if child.widget_id == widget_id:
                child.parent = None
                self.children.pop(i)
                return True
        return False

    def get_child(self, widget_id: str) -> Optional[WidgetNode]:
        """Get a child by ID."""
        for child in self.children:
            if child.widget_id == widget_id:
                return child
        return None

    def get_property(self, key: str, default: Any = None) -> Any:
        """Get a widget property."""
        return self.properties.get(key, default)

    def set_property(self, key: str, value: Any) -> None:
        """Set a widget property."""
        self.properties[key] = value

    def find_by_id(self, widget_id: str) -> Optional[WidgetNode]:
        """Find a widget by ID in this subtree."""
        if self.widget_id == widget_id:
            return self
        for child in self.children:
            result = child.find_by_id(widget_id)
            if result:
                return result
        return None

    def find_by_type(self, widget_type: str) -> list[WidgetNode]:
        """Find all widgets of a given type in this subtree."""
        results: list[WidgetNode] = []
        if self.widget_type == widget_type:
            results.append(self)
        for child in self.children:
            results.extend(child.find_by_type(widget_type))
        return results

    def find_by_name(self, name: str) -> list[WidgetNode]:
        """Find all widgets with a matching name."""
        results: list[WidgetNode] = []
        if self.name == name:
            results.append(self)
        for child in self.children:
            results.extend(child.find_by_name(name))
        return results

    def walk(self) -> Iterator[WidgetNode]:
        """Iterate over all nodes in this subtree (depth-first)."""
        yield self
        for child in self.children:
            yield from child.walk()

    @property
    def depth(self) -> int:
        """Return depth of this node in the tree."""
        depth = 0
        current = self.parent
        while current:
            depth += 1
            current = current.parent
        return depth

    @property
    def subtree_size(self) -> int:
        """Return total number of nodes in this subtree."""
        return sum(1 for _ in self.walk())


class WidgetTree:
    """A complete widget tree."""

    def __init__(self, root: Optional[WidgetNode] = None) -> None:
        """Initialize widget tree with optional root."""
        self._root = root
        self._nodes_by_id: dict[str, WidgetNode] = {}
        if root:
            self._index_node(root)

    def set_root(self, root: WidgetNode) -> None:
        """Set the root node."""
        self._root = root
        self._nodes_by_id.clear()
        self._index_node(root)

    def get_root(self) -> Optional[WidgetNode]:
        """Return the root node."""
        return self._root

    def get_node(self, widget_id: str) -> Optional[WidgetNode]:
        """Get a node by ID."""
        return self._nodes_by_id.get(widget_id)

    def _index_node(self, node: WidgetNode) -> None:
        """Index a node and all its descendants."""
        self._nodes_by_id[node.widget_id] = node
        for child in node.children:
            self._index_node(child)

    def walk(self) -> Iterator[WidgetNode]:
        """Iterate over all nodes (depth-first)."""
        if self._root:
            yield from self._root.walk()

    def find_by_type(self, widget_type: str) -> list[WidgetNode]:
        """Find all widgets of a type in the tree."""
        if self._root:
            return self._root.find_by_type(widget_type)
        return []

    def find_by_name(self, name: str) -> list[WidgetNode]:
        """Find all widgets by name."""
        if self._root:
            return self._root.find_by_name(name)
        return []

    @property
    def size(self) -> int:
        """Return total number of nodes."""
        return len(self._nodes_by_id)

    @property
    def is_empty(self) -> bool:
        """Return True if tree has no nodes."""
        return self._root is None
