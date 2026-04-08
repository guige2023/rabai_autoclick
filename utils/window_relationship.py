"""Window relationship tracking for UI automation.

Tracks parent-child and sibling relationships between windows,
modal dialog detection, and window hierarchy traversal.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Optional


class WindowRelationship(Enum):
    """Types of window relationships."""
    PARENT = auto()
    CHILD = auto()
    SIBLING = auto()
    MODAL_PARENT = auto()
    MODAL_CHILD = auto()
    OWNER = auto()
    OWNED = auto()
    TRANSIENT = auto()


class WindowType(Enum):
    """Classifications of window types."""
    MAIN = auto()
    DIALOG = auto()
    MODAL_DIALOG = auto()
    POPUP = auto()
    TOOLTIP = auto()
    MENU = auto()
    DROPDOWN = auto()
    TOOL_WINDOW = auto()
    FLOATING = auto()
    UNKNOWN = auto()


@dataclass
class WindowNode:
    """Represents a window in a relationship graph.

    Attributes:
        window_id: Unique identifier for this window.
        title: Window title text.
        process_name: Name of the owning process.
        window_type: Classification of window type.
        parent_id: ID of the parent window, if any.
        owner_id: ID of the owning window (for owned/popup windows).
        is_modal: Whether this is a modal window.
        is_visible: Whether the window is currently visible.
        is_enabled: Whether the window accepts input.
        z_order: Z-order position (higher = on top).
        metadata: Additional window properties.
    """
    window_id: str
    title: str
    process_name: str = ""
    window_type: WindowType = WindowType.UNKNOWN
    parent_id: Optional[str] = None
    owner_id: Optional[str] = None
    is_modal: bool = False
    is_visible: bool = True
    is_enabled: bool = True
    z_order: int = 0
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    metadata: dict = field(default_factory=dict)

    def is_dialog(self) -> bool:
        """Return True if this is a dialog window."""
        return self.window_type in (
            WindowType.DIALOG,
            WindowType.MODAL_DIALOG,
            WindowType.POPUP,
        )

    def is_modal_dialog(self) -> bool:
        """Return True if this is a modal dialog."""
        return self.window_type == WindowType.MODAL_DIALOG or self.is_modal

    def is_accessory(self) -> bool:
        """Return True if this is an accessory window (tooltip, menu, etc)."""
        return self.window_type in (
            WindowType.TOOLTIP,
            WindowType.MENU,
            WindowType.DROPDOWN,
        )


class WindowRelationshipGraph:
    """Graph of window relationships.

    Maintains parent-child and owner-owned relationships,
    supports modal window detection and hierarchy traversal.
    """

    def __init__(self) -> None:
        """Initialize an empty relationship graph."""
        self._nodes: dict[str, WindowNode] = {}
        self._children: dict[Optional[str], list[str]] = {None: []}
        self._owned: dict[Optional[str], list[str]] = {None: []}
        self._on_change_callbacks: list[Callable[[str, WindowRelationship], None]] = []

    def add(self, node: WindowNode) -> str:
        """Add a window node to the graph. Returns node ID."""
        self._nodes[node.id] = node
        self._children.setdefault(node.id, [])
        self._children.setdefault(node.parent_id, [])
        self._children[node.parent_id].append(node.id)
        self._owned.setdefault(node.id, [])
        self._owned.setdefault(node.owner_id, [])
        if node.owner_id:
            self._owned[node.owner_id].append(node.id)
        self._emit_change(node.id, WindowRelationship.OWNER)
        return node.id

    def remove(self, node_id: str) -> bool:
        """Remove a window node. Returns True if found."""
        if node_id not in self._nodes:
            return False
        node = self._nodes[node_id]
        self._children.pop(node_id, None)
        self._owned.pop(node_id, None)
        for children_list in self._children.values():
            if node_id in children_list:
                children_list.remove(node_id)
        for owned_list in self._owned.values():
            if node_id in owned_list:
                owned_list.remove(node_id)
        del self._nodes[node_id]
        return True

    def get(self, node_id: str) -> Optional[WindowNode]:
        """Retrieve a window node by ID."""
        return self._nodes.get(node_id)

    def find_by_title(self, title: str, exact: bool = False) -> list[WindowNode]:
        """Find windows by title."""
        if exact:
            return [n for n in self._nodes.values() if n.title == title]
        title_lower = title.lower()
        return [
            n for n in self._nodes.values()
            if title_lower in n.title.lower()
        ]

    def find_by_process(self, process_name: str) -> list[WindowNode]:
        """Find all windows belonging to a process."""
        name_lower = process_name.lower()
        return [
            n for n in self._nodes.values()
            if n.process_name.lower() == name_lower
        ]

    def find_by_type(self, window_type: WindowType) -> list[WindowNode]:
        """Find all windows of a given type."""
        return [n for n in self._nodes.values() if n.window_type == window_type]

    def get_children(self, node_id: str) -> list[WindowNode]:
        """Get direct child windows."""
        child_ids = self._children.get(node_id, [])
        return [self._nodes[nid] for nid in child_ids if nid in self._nodes]

    def get_parents(self, node_id: str) -> list[WindowNode]:
        """Walk up the parent chain to the root."""
        result: list[WindowNode] = []
        current = self._nodes.get(node_id)
        while current and current.parent_id:
            parent = self._nodes.get(current.parent_id)
            if parent:
                result.append(parent)
                current = parent
            else:
                break
        return result

    def get_owned_windows(self, node_id: str) -> list[WindowNode]:
        """Get windows owned by this window (popups, dialogs)."""
        owned_ids = self._owned.get(node_id, [])
        return [self._nodes[nid] for nid in owned_ids if nid in self._nodes]

    def get_siblings(self, node_id: str) -> list[WindowNode]:
        """Get windows that share the same parent."""
        node = self._nodes.get(node_id)
        if not node or not node.parent_id:
            return []
        return [
            n for nid in self._children.get(node.parent_id, [])
            if nid != node_id and nid in self._nodes
        ]

    def get_root_windows(self) -> list[WindowNode]:
        """Get top-level windows (no parent)."""
        return [n for n in self._nodes.values() if n.parent_id is None]

    def get_modal_chain(self, node_id: str) -> list[WindowNode]:
        """Get the chain of modal dialogs from a window up to the root."""
        result: list[WindowNode] = []
        current = self._nodes.get(node_id)
        while current:
            if current.is_modal_dialog():
                result.append(current)
            current_id = current.parent_id
            current = self._nodes.get(current_id) if current_id else None
        return result

    def get_topmost_window(self) -> Optional[WindowNode]:
        """Get the highest-z-order visible window."""
        visible = [n for n in self._nodes.values() if n.is_visible]
        if not visible:
            return None
        return max(visible, key=lambda n: n.z_order)

    def get_ancestors(self, node_id: str) -> list[WindowNode]:
        """Get all ancestors (parent chain)."""
        return self.get_parents(node_id)

    def get_descendants(self, node_id: str) -> list[WindowNode]:
        """Get all descendants (child subtree)."""
        result: list[WindowNode] = []
        stack = list(self.get_children(node_id))
        while stack:
            child = stack.pop()
            result.append(child)
            stack.extend(self.get_children(child.id))
        return result

    def is_blocked_by_modal(self, node_id: str) -> bool:
        """Return True if there is a modal dialog blocking this window."""
        node = self._nodes.get(node_id)
        if not node:
            return False
        ancestors = self.get_ancestors(node_id)
        for ancestor in ancestors:
            if ancestor.is_modal_dialog() and ancestor.is_visible:
                return True
        return False

    def on_change(
        self, callback: Callable[[str, WindowRelationship], None]
    ) -> None:
        """Register a callback for relationship changes."""
        self._on_change_callbacks.append(callback)

    def _emit_change(self, node_id: str, rel: WindowRelationship) -> None:
        """Emit change event to all callbacks."""
        for cb in self._on_change_callbacks:
            try:
                cb(node_id, rel)
            except Exception:
                pass

    def clear(self) -> None:
        """Remove all nodes from the graph."""
        self._nodes.clear()
        self._children.clear()
        self._children[None] = []
        self._owned.clear()
        self._owned[None] = []

    @property
    def count(self) -> int:
        """Return the number of nodes in the graph."""
        return len(self._nodes)

    @property
    def all_windows(self) -> list[WindowNode]:
        """Return all window nodes."""
        return list(self._nodes.values())


# Singleton instance
window_graph = WindowRelationshipGraph()
