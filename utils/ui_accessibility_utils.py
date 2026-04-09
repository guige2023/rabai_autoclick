"""UI Accessibility utilities for accessibility API operations.

This module provides utilities for working with UI accessibility APIs,
including tree traversal, role filtering, and property extraction.
"""

from typing import List, Optional, Dict, Any, Callable, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import deque


class AccessibilityRole(Enum):
    """Common accessibility roles."""
    BUTTON = "AXButton"
    CHECKBOX = "AXCheckBox"
    TEXT_FIELD = "AXTextField"
    TEXT_AREA = "AXTextArea"
    COMBO_BOX = "AXComboBox"
    LIST = "AXList"
    LIST_ITEM = "AXListItem"
    MENU = "AXMenu"
    MENU_ITEM = "AXMenuItem"
    WINDOW = "AXWindow"
    GROUP = "AXGroup"
    RADIO_GROUP = "AXRadioGroup"
    RADIO_BUTTON = "AXRadioButton"
    SLIDER = "AXSlider"
    TAB_GROUP = "AXTabGroup"
    TABLE = "AXTable"
    TABLE_ROW = "AXTableRow"
    TABLE_CELL = "AXTableCell"
    IMAGE = "AXImage"
    LINK = "AXLink"
    HEADING = "AXHeading"
    STATIC_TEXT = "AXStaticText"
    POPUP_BUTTON = "AXPopupButton"
    SPLIT_GROUP = "AXSplitGroup"
    NAVIGATOR = "AXNavigator"


@dataclass
class AccessibilityProperty:
    """Represents an accessibility property."""
    name: str
    value: Any
    type: str = "unknown"

    def is_valid(self) -> bool:
        """Check if property has a valid value."""
        return self.value is not None and self.value != ""


@dataclass
class AccessibilityNode:
    """Represents an accessibility tree node."""
    role: str
    name: str
    value: Any = None
    description: str = ""
    help: str = ""
    enabled: bool = True
    focused: bool = False
    selected: bool = False
    expanded: bool = False
    checked: bool = False
    children: List['AccessibilityNode'] = field(default_factory=list)
    properties: Dict[str, AccessibilityProperty] = field(default_factory=dict)
    bounds: Tuple[int, int, int, int] = (0, 0, 0, 0)  # x, y, width, height
    parent: Optional['AccessibilityNode'] = None

    def add_child(self, child: 'AccessibilityNode') -> None:
        """Add a child node."""
        child.parent = self
        self.children.append(child)

    def get_depth(self) -> int:
        """Get depth of this node in the tree."""
        depth = 0
        node = self.parent
        while node is not None:
            depth += 1
            node = node.parent
        return depth

    def get_path(self) -> List[str]:
        """Get path from root to this node."""
        path = []
        node: Optional[AccessibilityNode] = self
        while node is not None:
            path.insert(0, f"{node.role}:{node.name}")
            node = node.parent
        return path

    def find_by_role(self, role: str) -> List['AccessibilityNode']:
        """Find all descendants with given role."""
        results = []
        queue = deque([self])
        while queue:
            node = queue.popleft()
            if node.role == role:
                results.append(node)
            queue.extend(node.children)
        return results

    def find_by_name(self, name: str, exact: bool = True) -> List['AccessibilityNode']:
        """Find all descendants with given name."""
        results = []
        queue = deque([self])
        while queue:
            node = queue.popleft()
            if (exact and node.name == name) or (not exact and name.lower() in node.name.lower()):
                results.append(node)
            queue.extend(node.children)
        return results

    def find_by_predicate(self, predicate: Callable[['AccessibilityNode'], bool]) -> List['AccessibilityNode']:
        """Find all descendants matching a predicate."""
        results = []
        queue = deque([self])
        while queue:
            node = queue.popleft()
            if predicate(node):
                results.append(node)
            queue.extend(node.children)
        return results

    def get_first_ancestor(self, role: Optional[str] = None,
                          name: Optional[str] = None) -> Optional['AccessibilityNode']:
        """Get the first ancestor matching criteria."""
        node = self.parent
        while node is not None:
            if role is not None and node.role != role:
                node = node.parent
                continue
            if name is not None and node.name != name:
                node = node.parent
                continue
            return node
        return None

    def get_ancestors(self) -> List['AccessibilityNode']:
        """Get all ancestors from parent to root."""
        ancestors = []
        node = self.parent
        while node is not None:
            ancestors.append(node)
            node = node.parent
        return ancestors

    def is_visible(self) -> bool:
        """Check if node is visible based on bounds."""
        x, y, w, h = self.bounds
        return w > 0 and h > 0

    def is_interactive(self) -> bool:
        """Check if node represents an interactive element."""
        interactive_roles = {
            AccessibilityRole.BUTTON.value,
            AccessibilityRole.CHECKBOX.value,
            AccessibilityRole.COMBO_BOX.value,
            AccessibilityRole.LIST_ITEM.value,
            AccessibilityRole.MENU_ITEM.value,
            AccessibilityRole.RADIO_BUTTON.value,
            AccessibilityRole.SLIDER.value,
            AccessibilityRole.TAB_GROUP.value,
            AccessibilityRole.LINK.value,
            AccessibilityRole.POPUP_BUTTON.value,
        }
        return self.role in interactive_roles

    def get_text_content(self) -> str:
        """Get all text content from this node and descendants."""
        texts = []
        if self.role == AccessibilityRole.STATIC_TEXT.value:
            texts.append(str(self.value) if self.value else self.name)
        for child in self.children:
            texts.append(child.get_text_content())
        return " ".join(texts)

    def get_action_count(self) -> int:
        """Get number of available actions."""
        actions = self.properties.get("AXActions", AccessibilityProperty("AXActions", []))
        if isinstance(actions.value, list):
            return len(actions.value)
        return 0

    def has_action(self, action_name: str) -> bool:
        """Check if node has a specific action."""
        actions = self.properties.get("AXActions", AccessibilityProperty("AXActions", []))
        if isinstance(actions.value, list):
            return action_name in actions.value
        return False

    def __repr__(self) -> str:
        return f"AccessibilityNode(role={self.role}, name={self.name!r}, bounds={self.bounds})"


class AccessibilityTree:
    """Represents an accessibility tree."""

    def __init__(self, root: Optional[AccessibilityNode] = None):
        self.root = root

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AccessibilityTree':
        """Build tree from dictionary representation."""
        def build_node(d: Dict[str, Any]) -> AccessibilityNode:
            node = AccessibilityNode(
                role=d.get("role", ""),
                name=d.get("name", ""),
                value=d.get("value"),
                description=d.get("description", ""),
                help=d.get("help", ""),
                enabled=d.get("enabled", True),
                focused=d.get("focused", False),
                selected=d.get("selected", False),
                expanded=d.get("expanded", False),
                checked=d.get("checked", False),
                bounds=tuple(d.get("bounds", (0, 0, 0, 0)))
            )
            for child_data in d.get("children", []):
                node.add_child(build_node(child_data))
            return node

        root = build_node(data) if data else None
        return cls(root)

    def to_dict(self) -> Dict[str, Any]:
        """Convert tree to dictionary representation."""
        def node_to_dict(node: AccessibilityNode) -> Dict[str, Any]:
            result = {
                "role": node.role,
                "name": node.name,
                "value": node.value,
                "description": node.description,
                "help": node.help,
                "enabled": node.enabled,
                "focused": node.focused,
                "selected": node.selected,
                "expanded": node.expanded,
                "checked": node.checked,
                "bounds": list(node.bounds),
                "children": [node_to_dict(c) for c in node.children]
            }
            return result

        return node_to_dict(self.root) if self.root else {}

    def find_all(self, role: Optional[str] = None,
                 name: Optional[str] = None,
                 enabled: Optional[bool] = None,
                 interactive: Optional[bool] = None) -> List[AccessibilityNode]:
        """Find nodes matching criteria."""
        if self.root is None:
            return []

        results = []
        queue = deque([self.root])

        while queue:
            node = queue.popleft()

            if role is not None and node.role != role:
                queue.extend(node.children)
                continue
            if name is not None and node.name != name:
                queue.extend(node.children)
                continue
            if enabled is not None and node.enabled != enabled:
                queue.extend(node.children)
                continue
            if interactive is not None and node.is_interactive() != interactive:
                queue.extend(node.children)
                continue

            results.append(node)
            queue.extend(node.children)

        return results

    def find_focused(self) -> Optional[AccessibilityNode]:
        """Find the focused node."""
        return self.find_first(focused=True)

    def find_selected(self) -> List[AccessibilityNode]:
        """Find all selected nodes."""
        return self.find_all(selected=True)

    def get_interactive_elements(self) -> List[AccessibilityNode]:
        """Get all interactive elements."""
        return self.find_all(interactive=True)

    def get_text_elements(self) -> List[AccessibilityNode]:
        """Get all text elements."""
        return self.find_all(role=AccessibilityRole.STATIC_TEXT.value)

    def get_table_structure(self) -> List[List[AccessibilityNode]]:
        """Get table structure as 2D grid."""
        if self.root is None:
            return []

        tables = self.find_all(role=AccessibilityRole.TABLE.value)
        if not tables:
            return []

        table = tables[0]
        rows = []
        for child in table.children:
            if child.role == AccessibilityRole.TABLE_ROW.value:
                row = []
                for grandchild in child.children:
                    if grandchild.role == AccessibilityRole.TABLE_CELL.value:
                        row.append(grandchild)
                if row:
                    rows.append(row)
        return rows


def filter_by_role(nodes: List[AccessibilityNode], roles: Set[str]) -> List[AccessibilityNode]:
    """Filter nodes by accessibility roles."""
    return [n for n in nodes if n.role in roles]


def filter_by_bounds(nodes: List[AccessibilityNode],
                    x: int, y: int, width: int, height: int) -> List[AccessibilityNode]:
    """Filter nodes within given bounds."""
    results = []
    for node in nodes:
        nx, ny, nw, nh = node.bounds
        if (nx >= x and ny >= y and
            nx + nw <= x + width and ny + nh <= y + height):
            results.append(node)
    return results


def sort_by_bounds(nodes: List[AccessibilityNode],
                   order: str = "row_major") -> List[AccessibilityNode]:
    """Sort nodes by their bounds position."""
    if order == "row_major":
        return sorted(nodes, key=lambda n: (n.bounds[1], n.bounds[0]))
    elif order == "column_major":
        return sorted(nodes, key=lambda n: (n.bounds[0], n.bounds[1]))
    elif order == "depth_first":
        return sorted(nodes, key=lambda n: n.get_depth())
    else:
        return nodes


def get_element_path(node: AccessibilityNode) -> str:
    """Get a human-readable path string for a node."""
    return " > ".join(node.get_path())


def compare_trees(old: AccessibilityTree, new: AccessibilityTree) -> Tuple[List[AccessibilityNode], List[AccessibilityNode], List[Tuple[AccessibilityNode, AccessibilityNode]]]:
    """Compare two accessibility trees.

    Returns:
        Tuple of (removed_nodes, added_nodes, changed_nodes)
    """
    old_nodes = list(old.find_all()) if old.root else []
    new_nodes = list(new.find_all()) if new.root else []

    old_map = {(n.role, n.name, n.bounds): n for n in old_nodes}
    new_map = {(n.role, n.name, n.bounds): n for n in new_nodes}

    removed = []
    for key, node in old_map.items():
        if key not in new_map:
            removed.append(node)

    added = []
    for key, node in new_map.items():
        if key not in old_map:
            added.append(node)

    changed = []
    for key in set(old_map.keys()) & set(new_map.keys()):
        old_node = old_map[key]
        new_node = new_map[key]
        if old_node.value != new_node.value or old_node.enabled != new_node.enabled:
            changed.append((old_node, new_node))

    return removed, added, changed
