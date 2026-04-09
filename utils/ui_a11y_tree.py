"""
UI Accessibility Tree Module.

Provides utilities for building, traversing, and analyzing accessibility
trees from UI hierarchies for testing and automation purposes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Iterator


logger = logging.getLogger(__name__)


class AccessibilityRole(Enum):
    """Enumeration of accessibility roles."""
    WINDOW = auto()
    SHEET = auto()
    GROUP = auto()
    BUTTON = auto()
    CHECKBOX = auto()
    TEXT_FIELD = auto()
    TEXT_AREA = auto()
    LABEL = auto()
    LINK = auto()
    IMAGE = auto()
    TABLE = auto()
    TABLE_ROW = auto()
    TABLE_CELL = auto()
    MENU = auto()
    MENU_ITEM = auto()
    POP_UP_BUTTON = auto()
    COMBO_BOX = auto()
    LIST = auto()
    LIST_ITEM = auto()
    RADIO_GROUP = auto()
    RADIO_BUTTON = auto()
    SLIDER = auto()
    INCREMENTOR = auto()
    SCROLL_AREA = auto()
    SPLIT_GROUP = auto()
    SPLIT_VIEW = auto()
    TAB_GROUP = auto()
    TAB = auto()
    TOOLBAR = auto()
    UNKNOWN = auto()


@dataclass
class AccessibilityNode:
    """Represents a node in the accessibility tree."""
    id: str
    role: AccessibilityRole
    name: str
    value: str = ""
    description: str = ""
    enabled: bool = True
    focused: bool = False
    selected: bool = False
    expanded: bool = False
    checked: bool = False
    children: list[AccessibilityNode] = field(default_factory=list)
    parent: AccessibilityNode | None = None
    bounds: tuple[int, int, int, int] = (0, 0, 0, 0)
    attributes: dict[str, Any] = field(default_factory=dict)

    def add_child(self, child: AccessibilityNode) -> None:
        """Add a child node."""
        child.parent = self
        self.children.append(child)

    def remove_child(self, child: AccessibilityNode) -> None:
        """Remove a child node."""
        if child in self.children:
            child.parent = None
            self.children.remove(child)

    def depth(self) -> int:
        """Get the depth of this node in the tree."""
        depth = 0
        current = self.parent
        while current:
            depth += 1
            current = current.parent
        return depth

    def ancestors(self) -> list[AccessibilityNode]:
        """Get all ancestor nodes."""
        ancestors: list[AccessibilityNode] = []
        current = self.parent
        while current:
            ancestors.append(current)
            current = current.parent
        return ancestors

    def descendants(self) -> list[AccessibilityNode]:
        """Get all descendant nodes (breadth-first)."""
        result: list[AccessibilityNode] = []
        queue: list[AccessibilityNode] = list(self.children)

        while queue:
            node = queue.pop(0)
            result.append(node)
            queue.extend(node.children)

        return result


@dataclass
class AccessibilityTree:
    """Complete accessibility tree for a UI."""
    root: AccessibilityNode | None = None
    focus_node: AccessibilityNode | None = None
    selection: list[AccessibilityNode] = field(default_factory=list)
    timestamp: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def find_by_id(self, node_id: str) -> AccessibilityNode | None:
        """
        Find a node by its ID.

        Args:
            node_id: Node ID to search for.

        Returns:
            Node or None if not found.
        """
        if not self.root:
            return None

        if self.root.id == node_id:
            return self.root

        for descendant in self.root.descendants():
            if descendant.id == node_id:
                return descendant

        return None

    def find_by_role(
        self,
        role: AccessibilityRole
    ) -> list[AccessibilityNode]:
        """
        Find all nodes with a specific role.

        Args:
            role: Role to search for.

        Returns:
            List of matching nodes.
        """
        if not self.root:
            return []

        results: list[AccessibilityNode] = []
        if self.root.role == role:
            results.append(self.root)

        for descendant in self.root.descendants():
            if descendant.role == role:
                results.append(descendant)

        return results

    def find_by_name(
        self,
        name: str,
        exact: bool = False
    ) -> list[AccessibilityNode]:
        """
        Find nodes by name.

        Args:
            name: Name to search for.
            exact: If True, require exact match.

        Returns:
            List of matching nodes.
        """
        if not self.root:
            return []

        results: list[AccessibilityNode] = []

        def matches(node: AccessibilityNode) -> bool:
            if exact:
                return node.name == name
            return name.lower() in node.name.lower()

        if matches(self.root):
            results.append(self.root)

        for descendant in self.root.descendants():
            if matches(descendant):
                results.append(descendant)

        return results

    def find_by_predicate(
        self,
        predicate: Callable[[AccessibilityNode], bool]
    ) -> list[AccessibilityNode]:
        """
        Find nodes matching a predicate.

        Args:
            predicate: Function that returns True for matching nodes.

        Returns:
            List of matching nodes.
        """
        results: list[AccessibilityNode] = []

        if self.root and predicate(self.root):
            results.append(self.root)

        if self.root:
            for descendant in self.root.descendants():
                if predicate(descendant):
                    results.append(descendant)

        return results

    def traverse_breadth_first(self) -> Iterator[AccessibilityNode]:
        """Traverse tree breadth-first."""
        if self.root:
            yield self.root

            queue: list[AccessibilityNode] = list(self.root.children)
            while queue:
                node = queue.pop(0)
                yield node
                queue.extend(node.children)

    def traverse_depth_first(self) -> Iterator[AccessibilityNode]:
        """Traverse tree depth-first (pre-order)."""
        def traverse(node: AccessibilityNode) -> Iterator[AccessibilityNode]:
            yield node
            for child in node.children:
                yield from traverse(child)

        if self.root:
            yield from traverse(self.root)


class AccessibilityTreeBuilder:
    """
    Builds accessibility trees from UI hierarchies.

    Example:
        >>> builder = AccessibilityTreeBuilder()
        >>> builder.add_node("btn", AccessibilityRole.BUTTON, "Click Me")
        >>> builder.add_child("root", "btn")
        >>> tree = builder.build()
    """

    def __init__(self) -> None:
        """Initialize the tree builder."""
        self._nodes: dict[str, AccessibilityNode] = {}
        self._root_ids: list[str] = []

    def add_node(
        self,
        node_id: str,
        role: AccessibilityRole,
        name: str,
        **kwargs: Any
    ) -> AccessibilityNode:
        """
        Add a node to the tree.

        Args:
            node_id: Unique identifier for the node.
            role: Accessibility role.
            name: Display name.
            **kwargs: Additional node properties.

        Returns:
            Created AccessibilityNode.
        """
        node = AccessibilityNode(
            id=node_id,
            role=role,
            name=name,
            value=kwargs.get("value", ""),
            description=kwargs.get("description", ""),
            enabled=kwargs.get("enabled", True),
            focused=kwargs.get("focused", False),
            selected=kwargs.get("selected", False),
            expanded=kwargs.get("expanded", False),
            checked=kwargs.get("checked", False),
            bounds=kwargs.get("bounds", (0, 0, 0, 0)),
            attributes=kwargs.get("attributes", {})
        )

        self._nodes[node_id] = node
        return node

    def add_child(
        self,
        parent_id: str,
        child_id: str
    ) -> bool:
        """
        Add a child relationship.

        Args:
            parent_id: Parent node ID.
            child_id: Child node ID.

        Returns:
            True if relationship was created.
        """
        parent = self._nodes.get(parent_id)
        child = self._nodes.get(child_id)

        if not parent or not child:
            logger.warning(
                f"Cannot add child: parent={parent_id} or child={child_id} not found"
            )
            return False

        parent.add_child(child)
        return True

    def set_focus(self, node_id: str) -> bool:
        """
        Set the focus node.

        Args:
            node_id: Node ID to focus.

        Returns:
            True if node was found and focused.
        """
        node = self._nodes.get(node_id)
        if node:
            node.focused = True
            return True
        return False

    def set_as_root(self, node_id: str) -> None:
        """
        Mark a node as a root.

        Args:
            node_id: Node ID to mark as root.
        """
        if node_id not in self._root_ids:
            self._root_ids.append(node_id)

    def build(self) -> AccessibilityTree:
        """
        Build the accessibility tree.

        Returns:
            Complete AccessibilityTree.
        """
        import time

        root: AccessibilityNode | None = None

        if len(self._root_ids) == 1:
            root = self._nodes.get(self._root_ids[0])
        elif len(self._root_ids) > 1:
            root = AccessibilityNode(
                id="synthetic_root",
                role=AccessibilityRole.GROUP,
                name="Root"
            )
            for root_id in self._root_ids:
                node = self._nodes.get(root_id)
                if node:
                    root.add_child(node)

        focus_node = None
        for node in self._nodes.values():
            if node.focused:
                focus_node = node
                break

        return AccessibilityTree(
            root=root,
            focus_node=focus_node,
            timestamp=time.time()
        )


class AccessibilityTreeFilter:
    """
    Filters accessibility trees to extract relevant portions.
    """

    def __init__(self) -> None:
        """Initialize the filter."""
        self._role_filter: set[AccessibilityRole] | None = None
        self._name_filter: str | None = None
        self._enabled_only: bool = False
        self._min_depth: int | None = None
        self._max_depth: int | None = None

    def with_roles(
        self,
        roles: list[AccessibilityRole]
    ) -> AccessibilityTreeFilter:
        """Filter by roles (whitelist)."""
        self._role_filter = set(roles)
        return self

    def with_name_containing(self, name: str) -> AccessibilityTreeFilter:
        """Filter by name containing substring."""
        self._name_filter = name.lower()
        return self

    def enabled_only(self) -> AccessibilityTreeFilter:
        """Only include enabled nodes."""
        self._enabled_only = True
        return self

    def with_depth_range(
        self,
        min_depth: int | None = None,
        max_depth: int | None = None
    ) -> AccessibilityTreeFilter:
        """Filter by depth range."""
        self._min_depth = min_depth
        self._max_depth = max_depth
        return self

    def apply(self, tree: AccessibilityTree) -> list[AccessibilityNode]:
        """
        Apply filter to tree.

        Args:
            tree: AccessibilityTree to filter.

        Returns:
            List of matching nodes.
        """
        if not tree.root:
            return []

        results: list[AccessibilityNode] = []

        for node in tree.traverse_depth_first():
            if self._matches(node):
                results.append(node)

        return results

    def _matches(self, node: AccessibilityNode) -> bool:
        """Check if node matches all filters."""
        if self._role_filter and node.role not in self._role_filter:
            return False

        if self._name_filter:
            if self._name_filter not in node.name.lower():
                return False

        if self._enabled_only and not node.enabled:
            return False

        depth = node.depth()
        if self._min_depth is not None and depth < self._min_depth:
            return False

        if self._max_depth is not None and depth > self._max_depth:
            return False

        return True


class AccessibilityTreeComparator:
    """
    Compares accessibility trees to detect structural differences.
    """

    def __init__(self) -> None:
        """Initialize the comparator."""
        self._ignore_attributes: set[str] = {"focused", "timestamp"}

    def compare(
        self,
        expected: AccessibilityTree,
        actual: AccessibilityTree
    ) -> AccessibilityTreeDiff:
        """
        Compare two accessibility trees.

        Args:
            expected: Expected tree.
            actual: Actual tree.

        Returns:
            AccessibilityTreeDiff with differences.
        """
        added: list[AccessibilityNode] = []
        removed: list[AccessibilityNode] = []
        modified: list[tuple[AccessibilityNode, AccessibilityNode]] = []

        if not expected.root or not actual.root:
            return AccessibilityTreeDiff(
                is_equal=False,
                added=added,
                removed=removed,
                modified=modified
            )

        self._compare_nodes(expected.root, actual.root, added, removed, modified)

        return AccessibilityTreeDiff(
            is_equal=len(added) == 0 and len(removed) == 0 and len(modified) == 0,
            added=added,
            removed=removed,
            modified=modified
        )

    def _compare_nodes(
        self,
        expected: AccessibilityNode,
        actual: AccessibilityNode,
        added: list[AccessibilityNode],
        removed: list[AccessibilityNode],
        modified: list[tuple[AccessibilityNode, AccessibilityNode]]
    ) -> None:
        """Recursively compare nodes."""
        if expected.id != actual.id:
            removed.append(expected)
            added.append(actual)
            return

        if not self._nodes_equal(expected, actual):
            modified.append((expected, actual))

        expected_children = {c.id for c in expected.children}
        actual_children = {c.id for c in actual.children}

        for child_id in expected_children - actual_children:
            removed.append(self._find_by_id(expected, child_id))

        for child_id in actual_children - expected_children:
            added.append(self._find_by_id(actual, child_id))

        for exp_child in expected.children:
            act_child = self._find_by_id(actual, exp_child.id)
            if act_child:
                self._compare_nodes(exp_child, act_child, added, removed, modified)

    def _nodes_equal(
        self,
        expected: AccessibilityNode,
        actual: AccessibilityNode
    ) -> bool:
        """Check if two nodes are equal (excluding specified attributes)."""
        if expected.role != actual.role:
            return False

        if expected.name != actual.name:
            return False

        for key in expected.attributes:
            if key in self._ignore_attributes:
                continue

            if expected.attributes.get(key) != actual.attributes.get(key):
                return False

        return True

    def _find_by_id(
        self,
        node: AccessibilityNode,
        node_id: str
    ) -> AccessibilityNode | None:
        """Find node by ID in subtree."""
        if node.id == node_id:
            return node

        for child in node.children:
            found = self._find_by_id(child, node_id)
            if found:
                return found

        return None


@dataclass
class AccessibilityTreeDiff:
    """Result of comparing two accessibility trees."""
    is_equal: bool
    added: list[AccessibilityNode]
    removed: list[AccessibilityNode]
    modified: list[tuple[AccessibilityNode, AccessibilityNode]]

    def summary(self) -> str:
        """Get a human-readable summary."""
        return (
            f"Added: {len(self.added)}, "
            f"Removed: {len(self.removed)}, "
            f"Modified: {len(self.modified)}"
        )
