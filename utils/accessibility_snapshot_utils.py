"""
Accessibility snapshot utilities for macOS UI inspection.

This module provides utilities for capturing and analyzing
accessibility snapshots of UI elements.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Callable, Iterator


@dataclass(frozen=True)
class AccessibilityRole:
    """Represents an accessibility role with its identifier."""
    identifier: str
    description: str = ""

    def __str__(self) -> str:
        return self.identifier


# Common accessibility roles
ROLE_WINDOW = AccessibilityRole("AXWindow")
ROLE_BUTTON = AccessibilityRole("AXButton")
ROLE_TEXT_FIELD = AccessibilityRole("AXTextField")
ROLE_TEXT_AREA = AccessibilityRole("AXTextArea")
ROLE_CHECKBOX = AccessibilityRole("AXCheckBox")
ROLE_RADIO_BUTTON = AccessibilityRole("AXRadioButton")
ROLE_POP_UP_BUTTON = AccessibilityRole("AXPopUpButton")
ROLE_MENU_ITEM = AccessibilityRole("AXMenuItem")
ROLE_MENU = AccessibilityRole("AXMenu")
ROLE_TABLE = AccessibilityRole("AXTable")
ROLE_ROW = AccessibilityRole("AXRow")
ROLE_CELL = AccessibilityRole("AXCell")
ROLE_GROUP = AccessibilityRole("AXGroup")
ROLE_IMAGE = AccessibilityRole("AXImage")
ROLE_STATIC_TEXT = AccessibilityRole("AXStaticText")
ROLE_LINK = AccessibilityRole("AXLink")
ROLE_SCROLL_AREA = AccessibilityRole("AXScrollArea")
ROLE_SPLIT_GROUP = AccessibilityRole("AXSplitGroup")
ROLE_TAB_GROUP = AccessibilityRole("AXTabGroup")
ROLE_SLIDER = AccessibilityRole("AXSlider")
ROLE_INCREMENTOR = AccessibilityRole("AXIncrementor")
ROLE_COMBO_BOX = AccessibilityRole("AXComboBox")


@dataclass
class AccessibilityNode:
    """
    A node in the accessibility tree.

    Attributes:
        role: The element's accessibility role.
        role_description: Human-readable role description.
        title: Element title or label.
        value: Current value of the element.
        enabled: Whether the element is enabled.
        focused: Whether the element has focus.
        selected: Whether the element is selected.
        expanded: Whether expandable elements are expanded.
        label: Accessibility label.
        identifier: Unique identifier.
        bounds: Bounding box (x, y, width, height).
        children: Child accessibility nodes.
        attributes: Additional attributes dictionary.
    """
    role: str
    role_description: str = ""
    title: str = ""
    value: str = ""
    enabled: bool = True
    focused: bool = False
    selected: bool = False
    expanded: bool = True
    label: str = ""
    identifier: str = ""
    bounds: Optional[Dict[str, float]] = None
    children: List[AccessibilityNode] = field(default_factory=list)
    attributes: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_interactive(self) -> bool:
        """Check if element is interactive."""
        interactive_roles = {
            "AXButton", "AXTextField", "AXTextArea", "AXCheckBox",
            "AXRadioButton", "AXPopUpButton", "AXMenuItem", "AXLink",
            "AXSlider", "AXIncrementor", "AXComboBox",
        }
        return self.role in interactive_roles

    @property
    def is_container(self) -> bool:
        """Check if element can contain children."""
        container_roles = {
            "AXWindow", "AXGroup", "AXScrollArea", "AXSplitGroup",
            "AXTabGroup", "AXTable", "AXMenu",
        }
        return self.role in container_roles or len(self.children) > 0

    def find_child(
        self,
        predicate: Callable[[AccessibilityNode], bool],
    ) -> Optional[AccessibilityNode]:
        """Find first child matching predicate."""
        for child in self.children:
            if predicate(child):
                return child
            result = child.find_child(predicate)
            if result:
                return result
        return None

    def find_all(
        self,
        predicate: Callable[[AccessibilityNode], bool],
    ) -> List[AccessibilityNode]:
        """Find all descendants matching predicate."""
        results: List[AccessibilityNode] = []
        for child in self.children:
            if predicate(child):
                results.append(child)
            results.extend(child.find_all(predicate))
        return results

    def iter_nodes(self) -> Iterator[AccessibilityNode]:
        """Iterate over all nodes in tree (depth-first)."""
        yield self
        for child in self.children:
            yield from child.iter_nodes()

    def to_dict(self) -> Dict[str, Any]:
        """Convert node to dictionary representation."""
        return {
            "role": self.role,
            "roleDescription": self.role_description,
            "title": self.title,
            "value": self.value,
            "enabled": self.enabled,
            "focused": self.focused,
            "selected": self.selected,
            "expanded": self.expanded,
            "label": self.label,
            "identifier": self.identifier,
            "bounds": self.bounds,
            "children": [c.to_dict() for c in self.children],
            "attributes": self.attributes,
        }


@dataclass
class AccessibilitySnapshot:
    """
    Complete accessibility snapshot of an application.

    Attributes:
        app_name: Name of the application.
        window_title: Title of the window.
        root: Root accessibility node.
        timestamp: Snapshot capture timestamp.
    """
    app_name: str
    window_title: str = ""
    root: Optional[AccessibilityNode] = None
    timestamp: float = 0.0

    def find_by_role(self, role: str) -> List[AccessibilityNode]:
        """Find all nodes with given role."""
        if self.root:
            return self.root.find_all(lambda n: n.role == role)
        return []

    def find_by_title(self, title: str) -> List[AccessibilityNode]:
        """Find all nodes with given title."""
        if self.root:
            return self.root.find_all(lambda n: n.title == title)
        return []

    def find_by_label(self, label: str) -> List[AccessibilityNode]:
        """Find all nodes with given accessibility label."""
        if self.root:
            return self.root.find_all(lambda n: n.label == label)
        return []

    def find_focused(self) -> Optional[AccessibilityNode]:
        """Find the currently focused element."""
        if self.root:
            return self.root.find_child(lambda n: n.focused)
        return None

    def interactive_elements(self) -> List[AccessibilityNode]:
        """Get all interactive elements in snapshot."""
        if self.root:
            return self.root.find_all(lambda n: n.is_interactive)
        return []

    def to_dict(self) -> Dict[str, Any]:
        """Convert snapshot to dictionary."""
        return {
            "appName": self.app_name,
            "windowTitle": self.window_title,
            "timestamp": self.timestamp,
            "root": self.root.to_dict() if self.root else None,
        }


class AccessibilitySnapshotBuilder:
    """
    Builder for constructing accessibility snapshots.

    Provides a fluent API for building node hierarchies.
    """

    def __init__(self, app_name: str) -> None:
        self._app_name = app_name
        self._root: Optional[AccessibilityNode] = None
        self._current: Optional[AccessibilityNode] = None
        self._stack: List[AccessibilityNode] = []

    def root(self, role: str) -> AccessibilitySnapshotBuilder:
        """Set root node with role."""
        self._root = AccessibilityNode(role=role)
        self._current = self._root
        self._stack.clear()
        return self

    def child(self, role: str) -> AccessibilitySnapshotBuilder:
        """Add a child node and descend into it."""
        if not self._current:
            raise ValueError("Must call root() first")
        node = AccessibilityNode(role=role)
        self._current.children.append(node)
        self._stack.append(self._current)
        self._current = node
        return self

    def sibling(self, role: str) -> AccessibilitySnapshotBuilder:
        """Add a sibling node at current level."""
        if not self._current or not self._stack:
            raise ValueError("Must have a parent node")
        node = AccessibilityNode(role=role)
        self._current.children.append(node)
        self._current = node
        return self

    def up(self) -> AccessibilitySnapshotBuilder:
        """Move up to parent node."""
        if self._stack:
            self._current = self._stack.pop()
        return self

    def with_title(self, title: str) -> AccessibilitySnapshotBuilder:
        """Set title on current node."""
        if self._current:
            self._current.title = title
        return self

    def with_value(self, value: str) -> AccessibilitySnapshotBuilder:
        """Set value on current node."""
        if self._current:
            self._current.value = value
        return self

    def with_label(self, label: str) -> AccessibilitySnapshotBuilder:
        """Set accessibility label on current node."""
        if self._current:
            self._current.label = label
        return self

    def with_enabled(self, enabled: bool) -> AccessibilitySnapshotBuilder:
        """Set enabled state on current node."""
        if self._current:
            self._current.enabled = enabled
        return self

    def with_focused(self, focused: bool) -> AccessibilitySnapshotBuilder:
        """Set focused state on current node."""
        if self._current:
            self._current.focused = focused
        return self

    def with_bounds(
        self,
        x: float,
        y: float,
        width: float,
        height: float,
    ) -> AccessibilitySnapshotBuilder:
        """Set bounds on current node."""
        if self._current:
            self._current.bounds = {"x": x, "y": y, "width": width, "height": height}
        return self

    def with_attribute(self, key: str, value: Any) -> AccessibilitySnapshotBuilder:
        """Set custom attribute on current node."""
        if self._current:
            self._current.attributes[key] = value
        return self

    def build(self) -> AccessibilitySnapshot:
        """Build and return the accessibility snapshot."""
        import time
        return AccessibilitySnapshot(
            app_name=self._app_name,
            root=self._root,
            timestamp=time.time(),
        )
