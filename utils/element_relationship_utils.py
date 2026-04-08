"""Element Relationship Utilities.

Tracks and queries relationships between UI elements including parent-child,
sibling, ancestor-descendant, and ownership relationships.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Optional


class RelationshipType(Enum):
    """Types of relationships between UI elements."""

    PARENT = auto()
    CHILD = auto()
    SIBLING = auto()
    ANCESTOR = auto()
    DESCENDANT = auto()
    OWNER = auto()
    OWNED = auto()
    LABELLED_BY = auto()
    DESCRIBED_BY = auto()


@dataclass
class ElementNode:
    """Represents a UI element in a relationship graph.

    Attributes:
        element_id: Unique identifier for the element.
        role: Element's accessibility role.
        name: Element's accessible name.
        parent_id: Parent element's ID, if any.
        children_ids: List of direct child element IDs.
        attributes: Additional element attributes.
    """

    element_id: str
    role: str = ""
    name: str = ""
    parent_id: Optional[str] = None
    children_ids: list[str] = field(default_factory=list)
    attributes: dict = field(default_factory=dict)

    def add_child(self, child_id: str) -> None:
        """Add a child element ID.

        Args:
            child_id: ID of child element to add.
        """
        if child_id not in self.children_ids:
            self.children_ids.append(child_id)

    def remove_child(self, child_id: str) -> None:
        """Remove a child element ID.

        Args:
            child_id: ID of child element to remove.
        """
        if child_id in self.children_ids:
            self.children_ids.remove(child_id)


class ElementRelationshipGraph:
    """Graph of relationships between UI elements.

    Maintains parent-child and ancestor-descendant relationships.

    Example:
        graph = ElementRelationshipGraph()
        graph.add_element(ElementNode("window1", role="window"))
        graph.add_child("window1", "button1")
        parent = graph.get_parent("button1")
    """

    def __init__(self):
        """Initialize the relationship graph."""
        self._nodes: dict[str, ElementNode] = {}

    def add_element(self, node: ElementNode) -> None:
        """Add an element to the graph.

        Args:
            node: ElementNode to add.
        """
        self._nodes[node.element_id] = node

        # Update parent's children list
        if node.parent_id and node.parent_id in self._nodes:
            self._nodes[node.parent_id].add_child(node.element_id)

    def remove_element(self, element_id: str) -> None:
        """Remove an element and its descendants from the graph.

        Args:
            element_id: ID of element to remove.
        """
        if element_id not in self._nodes:
            return

        # Recursively remove descendants
        node = self._nodes[element_id]
        for child_id in list(node.children_ids):
            self.remove_element(child_id)

        # Remove from parent's children
        if node.parent_id and node.parent_id in self._nodes:
            self._nodes[node.parent_id].remove_child(element_id)

        del self._nodes[element_id]

    def get_node(self, element_id: str) -> Optional[ElementNode]:
        """Get an element node by ID.

        Args:
            element_id: Element identifier.

        Returns:
            ElementNode or None if not found.
        """
        return self._nodes.get(element_id)

    def get_parent(self, element_id: str) -> Optional[ElementNode]:
        """Get the parent of an element.

        Args:
            element_id: Element identifier.

        Returns:
            Parent ElementNode or None.
        """
        node = self._nodes.get(element_id)
        if not node or not node.parent_id:
            return None
        return self._nodes.get(node.parent_id)

    def get_children(self, element_id: str) -> list[ElementNode]:
        """Get direct children of an element.

        Args:
            element_id: Element identifier.

        Returns:
            List of child ElementNodes.
        """
        node = self._nodes.get(element_id)
        if not node:
            return []
        return [self._nodes[cid] for cid in node.children_ids if cid in self._nodes]

    def get_siblings(self, element_id: str) -> list[ElementNode]:
        """Get siblings of an element (same parent).

        Args:
            element_id: Element identifier.

        Returns:
            List of sibling ElementNodes (excluding self).
        """
        node = self._nodes.get(element_id)
        if not node or not node.parent_id:
            return []
        parent = self._nodes.get(node.parent_id)
        if not parent:
            return []
        return [
            self._nodes[cid]
            for cid in parent.children_ids
            if cid != element_id and cid in self._nodes
        ]

    def get_ancestors(self, element_id: str) -> list[ElementNode]:
        """Get all ancestors of an element (parent, grandparent, etc.).

        Args:
            element_id: Element identifier.

        Returns:
            List of ancestors from immediate parent upward.
        """
        ancestors = []
        current = self.get_parent(element_id)
        while current:
            ancestors.append(current)
            current = self.get_parent(current.element_id)
        return ancestors

    def get_descendants(self, element_id: str) -> list[ElementNode]:
        """Get all descendants of an element (children, grandchildren, etc.).

        Args:
            element_id: Element identifier.

        Returns:
            List of all descendant ElementNodes.
        """
        descendants = []
        to_visit = list(self._nodes.get(element_id, ElementNode("")).children_ids)

        while to_visit:
            child_id = to_visit.pop()
            if child_id in self._nodes:
                descendants.append(self._nodes[child_id])
                to_visit.extend(self._nodes[child_id].children_ids)

        return descendants

    def get_depth(self, element_id: str) -> int:
        """Get the depth of an element in the tree (root = 0).

        Args:
            element_id: Element identifier.

        Returns:
            Depth of the element.
        """
        depth = 0
        current = self.get_parent(element_id)
        while current:
            depth += 1
            current = self.get_parent(current.element_id)
        return depth

    def find_path_to(self, from_id: str, to_id: str) -> Optional[list[str]]:
        """Find the path between two elements.

        Args:
            from_id: Starting element ID.
            to_id: Target element ID.

        Returns:
            List of element IDs forming the path, or None if no path.
        """
        if from_id == to_id:
            return [from_id]

        # BFS to find shortest path
        visited = {from_id}
        queue = [(from_id, [from_id])]

        while queue:
            current, path = queue.pop(0)
            node = self._nodes.get(current)
            if not node:
                continue

            for neighbor_id in [node.parent_id] + node.children_ids:
                if neighbor_id and neighbor_id not in visited:
                    new_path = path + [neighbor_id]
                    if neighbor_id == to_id:
                        return new_path
                    visited.add(neighbor_id)
                    queue.append((neighbor_id, new_path))

        return None

    def add_child(self, parent_id: str, child_id: str) -> bool:
        """Add a parent-child relationship.

        Args:
            parent_id: Parent element ID.
            child_id: Child element ID.

        Returns:
            True if relationship was added.
        """
        parent = self._nodes.get(parent_id)
        child = self._nodes.get(child_id)
        if not parent or not child:
            return False

        if child.parent_id and child.parent_id != parent_id:
            self.remove_child_relation(child_id)

        child.parent_id = parent_id
        parent.add_child(child_id)
        return True

    def remove_child_relation(self, child_id: str) -> None:
        """Remove a parent-child relationship.

        Args:
            child_id: Child element ID to unlink from parent.
        """
        child = self._nodes.get(child_id)
        if not child or not child.parent_id:
            return

        parent = self._nodes.get(child.parent_id)
        if parent:
            parent.remove_child(child_id)
        child.parent_id = None

    def filter_elements(
        self,
        predicate: Callable[[ElementNode], bool],
    ) -> list[ElementNode]:
        """Filter elements by a predicate.

        Args:
            predicate: Function that returns True for elements to include.

        Returns:
            List of matching ElementNodes.
        """
        return [node for node in self._nodes.values() if predicate(node)]


class RelationshipQueryEngine:
    """Query engine for element relationships.

    Provides methods to query specific relationship patterns.

    Example:
        engine = RelationshipQueryEngine(graph)
        buttons = engine.find_by_role("button")
        labelled = engine.find_labelled_by("label1")
    """

    def __init__(self, graph: Optional[ElementRelationshipGraph] = None):
        """Initialize the query engine.

        Args:
            graph: ElementRelationshipGraph to query.
        """
        self.graph = graph or ElementRelationshipGraph()

    def find_by_role(self, role: str) -> list[ElementNode]:
        """Find all elements with a specific role.

        Args:
            role: Role to search for.

        Returns:
            List of matching ElementNodes.
        """
        return self.graph.filter_elements(lambda n: n.role.lower() == role.lower())

    def find_by_name(self, name: str, exact: bool = True) -> list[ElementNode]:
        """Find elements by accessible name.

        Args:
            name: Name to search for.
            exact: Whether to match exactly (vs. contains).

        Returns:
            List of matching ElementNodes.
        """
        def matches(node: ElementNode) -> bool:
            if exact:
                return node.name == name
            return name.lower() in node.name.lower()

        return self.graph.filter_elements(matches)

    def find_children_with_role(
        self,
        parent_id: str,
        role: str,
    ) -> list[ElementNode]:
        """Find children of an element with a specific role.

        Args:
            parent_id: Parent element ID.
            role: Role to search for.

        Returns:
            List of matching child ElementNodes.
        """
        children = self.graph.get_children(parent_id)
        return [c for c in children if c.role.lower() == role.lower()]

    def find_labelled_by(self, label_element_id: str) -> list[ElementNode]:
        """Find elements labelled by a specific label element.

        Args:
            label_element_id: ID of the label element.

        Returns:
            List of ElementNodes that reference this label.
        """
        return self.graph.filter_elements(
            lambda n: n.attributes.get("labelled_by") == label_element_id
        )

    def find_related(
        self,
        element_id: str,
        relationship: RelationshipType,
    ) -> list[ElementNode]:
        """Find elements related by a specific relationship type.

        Args:
            element_id: Starting element ID.
            relationship: Type of relationship to follow.

        Returns:
            List of related ElementNodes.
        """
        if relationship == RelationshipType.PARENT:
            parent = self.graph.get_parent(element_id)
            return [parent] if parent else []
        elif relationship == RelationshipType.CHILDREN:
            return self.graph.get_children(element_id)
        elif relationship == RelationshipType.SIBLING:
            return self.graph.get_siblings(element_id)
        elif relationship == RelationshipType.ANCESTOR:
            return self.graph.get_ancestors(element_id)
        elif relationship == RelationshipType.DESCENDANT:
            return self.graph.get_descendants(element_id)
        return []
