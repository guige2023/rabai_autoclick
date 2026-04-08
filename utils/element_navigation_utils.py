"""Element Navigation and Traversal Utilities.

Navigates UI element hierarchies and traverses element trees.
Supports path-based navigation, tree walking, and relationship traversal.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Iterator, Optional


class TraversalOrder(Enum):
    """Order of tree traversal."""

    DEPTH_FIRST = auto()
    BREADTH_FIRST = auto()
    PRE_ORDER = auto()
    POST_ORDER = auto()


@dataclass
class ElementNode:
    """Represents a node in the element tree.

    Attributes:
        element_id: Unique identifier.
        role: Element role.
        name: Element name.
        children: List of child element IDs.
        parent: Parent element ID.
    """

    element_id: str
    role: str = ""
    name: str = ""
    children: list[str] = field(default_factory=list)
    parent: Optional[str] = None


@dataclass
class NavigationPath:
    """A path through the element tree.

    Attributes:
        steps: List of element IDs in the path.
    """

    steps: list[str] = field(default_factory=list)

    def append(self, element_id: str) -> "NavigationPath":
        """Append a step to the path.

        Args:
            element_id: Element to add.

        Returns:
            New NavigationPath with the step appended.
        """
        return NavigationPath(steps=self.steps + [element_id])

    def __len__(self) -> int:
        """Get path length."""
        return len(self.steps)

    def __getitem__(self, index: int) -> str:
        """Get step at index."""
        return self.steps[index]


class ElementTree:
    """Tree structure for element hierarchy.

    Example:
        tree = ElementTree()
        tree.add_node("win1", role="window")
        tree.add_child("win1", "btn1", role="button")
        path = tree.find_path("btn1")
    """

    def __init__(self):
        """Initialize the element tree."""
        self._nodes: dict[str, ElementNode] = {}
        self._root_ids: list[str] = []

    def add_node(
        self,
        element_id: str,
        role: str = "",
        name: str = "",
    ) -> None:
        """Add a node to the tree.

        Args:
            element_id: Unique identifier.
            role: Element role.
            name: Element name.
        """
        if element_id not in self._nodes:
            self._nodes[element_id] = ElementNode(
                element_id=element_id,
                role=role,
                name=name,
            )

    def add_child(
        self,
        parent_id: str,
        child_id: str,
        role: str = "",
        name: str = "",
    ) -> None:
        """Add a child node.

        Args:
            parent_id: Parent element ID.
            child_id: Child element ID.
            role: Child role.
            name: Child name.
        """
        if parent_id not in self._nodes:
            self.add_node(parent_id)

        if child_id not in self._nodes:
            self._nodes[child_id] = ElementNode(
                element_id=child_id,
                role=role,
                name=name,
            )

        node = self._nodes[child_id]
        node.parent = parent_id

        parent = self._nodes[parent_id]
        if child_id not in parent.children:
            parent.children.append(child_id)

        if parent_id not in self._root_ids and node.parent is None:
            self._root_ids.append(parent_id)

    def remove_node(self, element_id: str) -> None:
        """Remove a node and its descendants.

        Args:
            element_id: Element to remove.
        """
        if element_id not in self._nodes:
            return

        node = self._nodes[element_id]
        for child_id in list(node.children):
            self.remove_node(child_id)

        if node.parent:
            parent = self._nodes.get(node.parent)
            if parent and element_id in parent.children:
                parent.children.remove(element_id)

        del self._nodes[element_id]
        if element_id in self._root_ids:
            self._root_ids.remove(element_id)

    def get_node(self, element_id: str) -> Optional[ElementNode]:
        """Get a node by ID.

        Args:
            element_id: Element identifier.

        Returns:
            ElementNode or None.
        """
        return self._nodes.get(element_id)

    def get_children(self, element_id: str) -> list[ElementNode]:
        """Get children of a node.

        Args:
            element_id: Element identifier.

        Returns:
            List of child ElementNodes.
        """
        node = self._nodes.get(element_id)
        if not node:
            return []
        return [self._nodes[c] for c in node.children if c in self._nodes]

    def get_parent(self, element_id: str) -> Optional[ElementNode]:
        """Get parent of a node.

        Args:
            element_id: Element identifier.

        Returns:
            Parent ElementNode or None.
        """
        node = self._nodes.get(element_id)
        if not node or not node.parent:
            return None
        return self._nodes.get(node.parent)

    def get_ancestors(self, element_id: str) -> list[ElementNode]:
        """Get all ancestors (parent, grandparent, etc.).

        Args:
            element_id: Element identifier.

        Returns:
            List of ancestors from immediate parent up.
        """
        ancestors = []
        current = self.get_parent(element_id)
        while current:
            ancestors.append(current)
            current = self.get_parent(current.element_id)
        return ancestors

    def get_descendants(self, element_id: str) -> list[ElementNode]:
        """Get all descendants (children, grandchildren, etc.).

        Args:
            element_id: Element identifier.

        Returns:
            List of all descendants.
        """
        descendants = []
        to_visit = list(self._nodes.get(element_id, ElementNode("")).children)

        while to_visit:
            child_id = to_visit.pop()
            if child_id in self._nodes:
                descendants.append(self._nodes[child_id])
                to_visit.extend(self._nodes[child_id].children)

        return descendants

    def find_path(self, element_id: str) -> Optional[NavigationPath]:
        """Find path from root to element.

        Args:
            element_id: Target element ID.

        Returns:
            NavigationPath or None if not found.
        """
        if element_id not in self._nodes:
            return None

        path = []
        current = element_id
        while current:
            path.insert(0, current)
            node = self._nodes.get(current)
            current = node.parent if node else None

        return NavigationPath(steps=path)

    def traverse(
        self,
        start_id: Optional[str] = None,
        order: TraversalOrder = TraversalOrder.DEPTH_FIRST,
    ) -> Iterator[ElementNode]:
        """Traverse the tree.

        Args:
            start_id: Starting element (uses roots if None).
            order: Traversal order.

        Yields:
            ElementNodes in traversal order.
        """
        if start_id:
            yield from self._traverse_node(start_id, order)
        else:
            for root_id in self._root_ids:
                yield from self._traverse_node(root_id, order)

    def _traverse_node(
        self,
        element_id: str,
        order: TraversalOrder,
    ) -> Iterator[ElementNode]:
        """Traverse a subtree.

        Args:
            element_id: Root of subtree.
            order: Traversal order.

        Yields:
            ElementNodes in traversal order.
        """
        node = self._nodes.get(element_id)
        if not node:
            return

        if order in (TraversalOrder.DEPTH_FIRST, TraversalOrder.PRE_ORDER):
            yield node

        for child_id in node.children:
            yield from self._traverse_node(child_id, order)

        if order == TraversalOrder.POST_ORDER:
            yield node


class ElementNavigator:
    """Navigates to elements using various strategies.

    Example:
        navigator = ElementNavigator(tree)
        button = navigator.navigate_to_role("button", max_depth=5)
    """

    def __init__(self, tree: ElementTree):
        """Initialize the navigator.

        Args:
            tree: ElementTree to navigate.
        """
        self._tree = tree

    def navigate_to_role(
        self,
        role: str,
        start_id: Optional[str] = None,
        max_depth: Optional[int] = None,
    ) -> Optional[ElementNode]:
        """Navigate to first element with a role.

        Args:
            role: Role to find.
            start_id: Starting element (uses roots if None).
            max_depth: Maximum depth to search.

        Returns:
            ElementNode or None if not found.
        """
        for node in self._tree.traverse(start_id=start_id):
            if node.role.lower() == role.lower():
                return node
        return None

    def navigate_to_name(
        self,
        name: str,
        start_id: Optional[str] = None,
        exact: bool = False,
    ) -> Optional[ElementNode]:
        """Navigate to element by name.

        Args:
            name: Name to find.
            start_id: Starting element.
            exact: Whether to match exactly.

        Returns:
            ElementNode or None.
        """
        for node in self._tree.traverse(start_id=start_id):
            if exact:
                if node.name == name:
                    return node
            else:
                if name.lower() in node.name.lower():
                    return node
        return None

    def navigate_first(
        self,
        predicate: Callable[[ElementNode], bool],
        start_id: Optional[str] = None,
    ) -> Optional[ElementNode]:
        """Navigate to first element matching predicate.

        Args:
            predicate: Function that returns True for target.
            start_id: Starting element.

        Returns:
            ElementNode or None.
        """
        for node in self._tree.traverse(start_id=start_id):
            if predicate(node):
                return node
        return None

    def navigate_all(
        self,
        predicate: Callable[[ElementNode], bool],
        start_id: Optional[str] = None,
    ) -> list[ElementNode]:
        """Navigate to all elements matching predicate.

        Args:
            predicate: Function that returns True for target.
            start_id: Starting element.

        Returns:
            List of matching ElementNodes.
        """
        return [
            node for node in self._tree.traverse(start_id=start_id)
            if predicate(node)
        ]

    def navigate_by_index(
        self,
        role: str,
        index: int,
        start_id: Optional[str] = None,
    ) -> Optional[ElementNode]:
        """Navigate to nth element with a role.

        Args:
            role: Role to find.
            index: 0-based index.
            start_id: Starting element.

        Returns:
            ElementNode or None.
        """
        count = 0
        for node in self._tree.traverse(start_id=start_id):
            if node.role.lower() == role.lower():
                if count == index:
                    return node
                count += 1
        return None
