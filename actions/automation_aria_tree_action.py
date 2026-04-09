"""Automation ARIA Tree Action.

Accessibility tree navigation and interaction for UI automation.
Parses ARIA roles, properties, and states to build navigable tree.
"""
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum


class ARIARole(Enum):
    BUTTON = "button"
    LINK = "link"
    CHECKBOX = "checkbox"
    RADIO = "radio"
    TAB = "tab"
    TREEITEM = "treeitem"
    MENUITEM = "menuitem"
    TEXTBOX = "textbox"
    COMBOBOX = "combobox"
    SLIDER = "slider"
    SWITCH = "switch"
    DIALOG = "dialog"
    ALERT = "alert"
    PROGRESSBAR = "progressbar"
    SPINBUTTON = "spinbutton"
    CUSTOM = "custom"


@dataclass
class ARIANode:
    id: str
    role: str
    name: str
    label: Optional[str] = None
    description: Optional[str] = None
    level: int = 0
    expanded: Optional[bool] = None
    selected: bool = False
    checked: Optional[bool] = None
    disabled: bool = False
    readonly: bool = False
    required: bool = False
    hidden: bool = False
    children: List["ARIANode"] = field(default_factory=list)
    properties: Dict[str, Any] = field(default_factory=dict)
    actions: List[str] = field(default_factory=list)

    def is_focusable(self) -> bool:
        return not self.disabled and not self.hidden

    def is_expanded(self) -> bool:
        return bool(self.expanded)

    def get_display_name(self) -> str:
        return self.label or self.name or f"[{self.role}]"


@dataclass
class ARIATree:
    root: Optional[ARIANode] = None
    nodes_by_id: Dict[str, ARIANode] = field(default_factory=dict)
    focus_order: List[str] = field(default_factory=list)

    def get_node(self, node_id: str) -> Optional[ARIANode]:
        return self.nodes_by_id.get(node_id)

    def get_focusable_nodes(self) -> List[ARIANode]:
        return [n for n in self.nodes_by_id.values() if n.is_focusable()]

    def search_by_role(self, role: str) -> List[ARIANode]:
        return [n for n in self.nodes_by_id.values() if n.role == role]

    def search_by_name(self, name: str, exact: bool = False) -> List[ARIANode]:
        if exact:
            return [n for n in self.nodes_by_id.values() if n.name == name]
        name_lower = name.lower()
        return [n for n in self.nodes_by_id.values() if name_lower in n.name.lower()]

    def breadcrumb(self, node_id: str) -> List[str]:
        """Returns path of node names from root to node."""
        node = self.nodes_by_id.get(node_id)
        if not node:
            return []
        path = []
        current: Optional[ARIANode] = node
        while current:
            path.append(current.get_display_name())
            current = None
        return list(reversed(path))


class AutomationARIATreeAction:
    """Builds and navigates ARIA accessibility trees."""

    def __init__(self) -> None:
        self._trees: Dict[str, ARIATree] = {}

    def parse_accessibility_tree(
        self,
        raw_tree: Dict[str, Any],
        tree_id: str = "default",
    ) -> ARIATree:
        """Parse a raw accessibility tree dict into an ARIATree."""
        tree = ARIATree()
        self._trees[tree_id] = tree
        if not raw_tree:
            return tree
        root = self._build_node(raw_tree, 0)
        tree.root = root
        self._index_tree(root, tree)
        self._build_focus_order(tree)
        return tree

    def _build_node(self, data: Dict[str, Any], level: int) -> ARIANode:
        children = []
        for child_data in data.get("children", []):
            children.append(self._build_node(child_data, level + 1))
        return ARIANode(
            id=data.get("id", ""),
            role=data.get("role", "custom"),
            name=data.get("name", ""),
            label=data.get("label"),
            description=data.get("description"),
            level=level,
            expanded=data.get("expanded"),
            selected=data.get("selected", False),
            checked=data.get("checked"),
            disabled=data.get("disabled", False),
            readonly=data.get("readonly", False),
            required=data.get("required", False),
            hidden=data.get("hidden", False),
            children=children,
            properties=data.get("properties", {}),
            actions=data.get("actions", []),
        )

    def _index_tree(self, node: ARIANode, tree: ARIATree) -> None:
        tree.nodes_by_id[node.id] = node
        for child in node.children:
            self._index_tree(child, tree)

    def _build_focus_order(self, tree: ARIATree) -> None:
        tree.focus_order = [n.id for n in tree.get_focusable_nodes()]

    def find_interactive(
        self,
        tree_id: str = "default",
        role_filter: Optional[List[str]] = None,
        name_contains: Optional[str] = None,
    ) -> List[ARIANode]:
        tree = self._trees.get(tree_id)
        if not tree:
            return []
        results = tree.get_focusable_nodes()
        if role_filter:
            results = [n for n in results if n.role in role_filter]
        if name_contains:
            nc = name_contains.lower()
            results = [n for n in results if nc in n.name.lower()]
        return results

    def get_interaction_path(
        self,
        target_id: str,
        tree_id: str = "default",
    ) -> List[str]:
        tree = self._trees.get(tree_id)
        if not tree:
            return []
        return tree.breadcrumb(target_id)
