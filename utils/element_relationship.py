"""
Element Relationship Module.

Provides utilities for analyzing and managing relationships between UI
elements, including parent-child relationships, siblings, ownership,
and spatial relationships.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any


logger = logging.getLogger(__name__)


class RelationshipType(Enum):
    """Types of relationships between elements."""
    PARENT = auto()
    CHILD = auto()
    SIBLING = auto()
    ANCESTOR = auto()
    DESCENDANT = auto()
    OWNER = auto()
    OWNED = auto()
    LABEL_FOR = auto()
    LABELED_BY = auto()
    DESCRIBED_BY = auto()
    DESCRIPTION_FOR = auto()
    SPATIAL_NEAR = auto()
    SPATIAL_CONTAINS = auto()
    SPATIAL_OVERLAPS = auto()
    Z_ORDER_ABOVE = auto()
    Z_ORDER_BELOW = auto()


@dataclass
class ElementBounds:
    """Bounding box of an element."""
    x: int
    y: int
    width: int
    height: int

    @property
    def center_x(self) -> float:
        """Get center X coordinate."""
        return self.x + self.width / 2

    @property
    def center_y(self) -> float:
        """Get center Y coordinate."""
        return self.y + self.height / 2

    @property
    def left(self) -> int:
        """Get left edge."""
        return self.x

    @property
    def right(self) -> int:
        """Get right edge."""
        return self.x + self.width

    @property
    def top(self) -> int:
        """Get top edge."""
        return self.y

    @property
    def bottom(self) -> int:
        """Get bottom edge."""
        return self.y + self.height

    def distance_to(self, other: ElementBounds) -> float:
        """
        Calculate minimum distance to another bounds.

        Args:
            other: Other bounds.

        Returns:
            Minimum distance (0 if overlapping).
        """
        dx = max(self.left, other.left) - min(self.right, other.right)
        dy = max(self.top, other.top) - min(self.bottom, other.bottom)

        if dx <= 0 and dy <= 0:
            return 0.0

        return math.sqrt(max(dx, 0) ** 2 + max(dy, 0) ** 2)

    def contains_point(self, px: int, py: int) -> bool:
        """
        Check if a point is within bounds.

        Args:
            px: Point X coordinate.
            py: Point Y coordinate.

        Returns:
            True if point is inside bounds.
        """
        return self.left <= px <= self.right and self.top <= py <= self.bottom

    def contains_bounds(self, other: ElementBounds) -> bool:
        """
        Check if bounds fully contain another bounds.

        Args:
            other: Bounds to check.

        Returns:
            True if fully contains other.
        """
        return (
            self.left <= other.left and
            self.right >= other.right and
            self.top <= other.top and
            self.bottom >= other.bottom
        )

    def overlaps(self, other: ElementBounds) -> bool:
        """
        Check if bounds overlap with another.

        Args:
            other: Other bounds.

        Returns:
            True if any overlap exists.
        """
        return (
            self.left < other.right and
            self.right > other.left and
            self.top < other.bottom and
            self.bottom > other.top
        )

    def intersection(
        self,
        other: ElementBounds
    ) -> ElementBounds | None:
        """
        Calculate intersection with another bounds.

        Args:
            other: Other bounds.

        Returns:
            Intersection bounds or None if no overlap.
        """
        if not self.overlaps(other):
            return None

        return ElementBounds(
            x=max(self.left, other.left),
            y=max(self.top, other.top),
            width=min(self.right, other.right) - max(self.left, other.left),
            height=min(self.bottom, other.bottom) - max(self.top, other.top)
        )


@dataclass
class ElementRelationship:
    """Represents a relationship between two elements."""
    type: RelationshipType
    source_id: str
    target_id: str
    strength: float = 1.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class UIElement:
    """Represents a UI element with relationships."""
    id: str
    name: str
    role: str
    bounds: ElementBounds
    parent_id: str | None = None
    children_ids: list[str] = field(default_factory=list)
    relationships: list[ElementRelationship] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def add_child(self, child_id: str) -> None:
        """Add a child element ID."""
        if child_id not in self.children_ids:
            self.children_ids.append(child_id)

    def remove_child(self, child_id: str) -> None:
        """Remove a child element ID."""
        if child_id in self.children_ids:
            self.children_ids.remove(child_id)

    def get_relationship(
        self,
        rel_type: RelationshipType,
        target_id: str
    ) -> ElementRelationship | None:
        """
        Get a specific relationship.

        Args:
            rel_type: Relationship type.
            target_id: Target element ID.

        Returns:
            Relationship or None.
        """
        for rel in self.relationships:
            if rel.type == rel_type and rel.target_id == target_id:
                return rel
        return None


class ElementRelationshipGraph:
    """
    Manages relationships between UI elements.

    Example:
        >>> graph = ElementRelationshipGraph()
        >>> graph.add_element(element)
        >>> graph.add_relationship(rel)
        >>> children = graph.get_children("element_id")
    """

    def __init__(self) -> None:
        """Initialize the relationship graph."""
        self._elements: dict[str, UIElement] = {}
        self._relationships: dict[str, list[ElementRelationship]] = {}

    def add_element(self, element: UIElement) -> None:
        """
        Add an element to the graph.

        Args:
            element: UIElement to add.
        """
        self._elements[element.id] = element

        if element.id not in self._relationships:
            self._relationships[element.id] = []

        if element.parent_id:
            parent = self._elements.get(element.parent_id)
            if parent:
                parent.add_child(element.id)

    def remove_element(self, element_id: str) -> None:
        """
        Remove an element and its relationships.

        Args:
            element_id: ID of element to remove.
        """
        element = self._elements.get(element_id)
        if not element:
            return

        if element.parent_id:
            parent = self._elements.get(element.parent_id)
            if parent:
                parent.remove_child(element_id)

        for child_id in element.children_ids:
            child = self._elements.get(child_id)
            if child:
                child.parent_id = None

        del self._elements[element_id]
        del self._relationships[element_id]

        for relationships in self._relationships.values():
            relationships[:] = [
                r for r in relationships
                if r.source_id != element_id and r.target_id != element_id
            ]

    def get_element(self, element_id: str) -> UIElement | None:
        """
        Get an element by ID.

        Args:
            element_id: Element ID.

        Returns:
            UIElement or None.
        """
        return self._elements.get(element_id)

    def add_relationship(self, relationship: ElementRelationship) -> None:
        """
        Add a relationship to the graph.

        Args:
            relationship: ElementRelationship to add.
        """
        if relationship.source_id not in self._relationships:
            self._relationships[relationship.source_id] = []

        existing = self.get_relationship(
            relationship.source_id,
            relationship.type,
            relationship.target_id
        )

        if existing:
            existing.strength = relationship.strength
            existing.metadata.update(relationship.metadata)
        else:
            self._relationships[relationship.source_id].append(relationship)

    def get_relationship(
        self,
        source_id: str,
        rel_type: RelationshipType,
        target_id: str
    ) -> ElementRelationship | None:
        """
        Get a specific relationship.

        Args:
            source_id: Source element ID.
            rel_type: Relationship type.
            target_id: Target element ID.

        Returns:
            ElementRelationship or None.
        """
        relationships = self._relationships.get(source_id, [])
        for rel in relationships:
            if rel.type == rel_type and rel.target_id == target_id:
                return rel
        return None

    def get_children(self, element_id: str) -> list[UIElement]:
        """
        Get direct children of an element.

        Args:
            element_id: Parent element ID.

        Returns:
            List of child UIElement objects.
        """
        element = self._elements.get(element_id)
        if not element:
            return []

        return [
            self._elements[cid]
            for cid in element.children_ids
            if cid in self._elements
        ]

    def get_parent(self, element_id: str) -> UIElement | None:
        """
        Get parent of an element.

        Args:
            element_id: Element ID.

        Returns:
            Parent UIElement or None.
        """
        element = self._elements.get(element_id)
        if not element or not element.parent_id:
            return None

        return self._elements.get(element.parent_id)

    def get_siblings(self, element_id: str) -> list[UIElement]:
        """
        Get siblings of an element.

        Args:
            element_id: Element ID.

        Returns:
            List of sibling UIElement objects.
        """
        element = self._elements.get(element_id)
        if not element or not element.parent_id:
            return []

        parent = self._elements.get(element.parent_id)
        if not parent:
            return []

        return [
            self._elements[cid]
            for cid in parent.children_ids
            if cid != element_id and cid in self._elements
        ]

    def get_ancestors(self, element_id: str) -> list[UIElement]:
        """
        Get all ancestors of an element.

        Args:
            element_id: Starting element ID.

        Returns:
            List of ancestor UIElement objects (root first).
        """
        ancestors: list[UIElement] = []
        current = self.get_parent(element_id)

        while current:
            ancestors.append(current)
            current = self.get_parent(current.id)

        return ancestors

    def get_descendants(self, element_id: str) -> list[UIElement]:
        """
        Get all descendants of an element.

        Args:
            element_id: Starting element ID.

        Returns:
            List of descendant UIElement objects.
        """
        descendants: list[UIElement] = []
        queue = list(self.get_children(element_id))

        while queue:
            current = queue.pop(0)
            descendants.append(current)
            queue.extend(self.get_children(current.id))

        return descendants

    def get_related(
        self,
        element_id: str,
        rel_type: RelationshipType
    ) -> list[UIElement]:
        """
        Get elements related by a specific relationship type.

        Args:
            element_id: Source element ID.
            rel_type: Relationship type.

        Returns:
            List of related UIElement objects.
        """
        relationships = self._relationships.get(element_id, [])
        target_ids = [r.target_id for r in relationships if r.type == rel_type]

        return [
            self._elements[tid]
            for tid in target_ids
            if tid in self._elements
        ]


class SpatialRelationshipAnalyzer:
    """
    Analyzes spatial relationships between elements.

    Example:
        >>> analyzer = SpatialRelationshipAnalyzer()
        >>> nearby = analyzer.find_nearby(element, max_distance=50)
    """

    def __init__(self, graph: ElementRelationshipGraph) -> None:
        """
        Initialize the analyzer.

        Args:
            graph: ElementRelationshipGraph to analyze.
        """
        self.graph = graph

    def find_nearby(
        self,
        element_id: str,
        max_distance: float = 50.0
    ) -> list[tuple[UIElement, float]]:
        """
        Find elements within a certain distance.

        Args:
            element_id: Source element ID.
            max_distance: Maximum distance in pixels.

        Returns:
            List of (element, distance) tuples.
        """
        element = self.graph.get_element(element_id)
        if not element:
            return []

        nearby: list[tuple[UIElement, float]] = []

        for other in self.graph._elements.values():
            if other.id == element_id:
                continue

            distance = element.bounds.distance_to(other.bounds)
            if distance <= max_distance:
                nearby.append((other, distance))

        nearby.sort(key=lambda x: x[1])
        return nearby

    def find_contained(self, element_id: str) -> list[UIElement]:
        """
        Find elements contained within an element.

        Args:
            element_id: Container element ID.

        Returns:
            List of contained UIElement objects.
        """
        element = self.graph.get_element(element_id)
        if not element:
            return []

        contained: list[UIElement] = []

        for other in self.graph._elements.values():
            if other.id == element_id:
                continue

            if element.bounds.contains_bounds(other.bounds):
                contained.append(other)

        return contained

    def find_overlapping(self, element_id: str) -> list[UIElement]:
        """
        Find elements that overlap with an element.

        Args:
            element_id: Source element ID.

        Returns:
            List of overlapping UIElement objects.
        """
        element = self.graph.get_element(element_id)
        if not element:
            return []

        overlapping: list[UIElement] = []

        for other in self.graph._elements.values():
            if other.id == element_id:
                continue

            if element.bounds.overlaps(other.bounds):
                overlapping.append(other)

        return overlapping

    def infer_spatial_relationships(
        self,
        distance_threshold: float = 20.0
    ) -> None:
        """
        Infer spatial relationships for all elements.

        Args:
            distance_threshold: Distance for "near" relationship.
        """
        for element in self.graph._elements.values():
            nearby = self.find_nearby(element.id, distance_threshold)

            for nearby_element, distance in nearby:
                relationship = ElementRelationship(
                    type=RelationshipType.SPATIAL_NEAR,
                    source_id=element.id,
                    target_id=nearby_element.id,
                    strength=max(0.0, 1.0 - distance / distance_threshold),
                    metadata={"distance": distance}
                )

                self.graph.add_relationship(relationship)


class RelationshipNavigator:
    """
    Provides navigation through element relationships.

    Example:
        >>> nav = RelationshipNavigator(graph)
        >>> siblings = nav.navigate(element_id).siblings().execute()
    """

    def __init__(self, graph: ElementRelationshipGraph) -> None:
        """
        Initialize the navigator.

        Args:
            graph: ElementRelationshipGraph to navigate.
        """
        self.graph = graph
        self._current_ids: list[str] = []

    def from_element(self, element_id: str) -> RelationshipNavigator:
        """Start navigation from an element."""
        self._current_ids = [element_id]
        return self

    def from_elements(self, element_ids: list[str]) -> RelationshipNavigator:
        """Start navigation from multiple elements."""
        self._current_ids = list(element_ids)
        return self

    def children(self) -> RelationshipNavigator:
        """Navigate to children."""
        new_ids: list[str] = []

        for eid in self._current_ids:
            for child in self.graph.get_children(eid):
                if child.id not in new_ids:
                    new_ids.append(child.id)

        self._current_ids = new_ids
        return self

    def parents(self) -> RelationshipNavigator:
        """Navigate to parents."""
        new_ids: list[str] = []

        for eid in self._current_ids:
            parent = self.graph.get_parent(eid)
            if parent and parent.id not in new_ids:
                new_ids.append(parent.id)

        self._current_ids = new_ids
        return self

    def siblings(self) -> RelationshipNavigator:
        """Navigate to siblings."""
        new_ids: list[str] = []

        for eid in self._current_ids:
            for sib in self.graph.get_siblings(eid):
                if sib.id not in new_ids:
                    new_ids.append(sib.id)

        self._current_ids = new_ids
        return self

    def descendants(self) -> RelationshipNavigator:
        """Navigate to all descendants."""
        new_ids: list[str] = []

        for eid in self._current_ids:
            for desc in self.graph.get_descendants(eid):
                if desc.id not in new_ids:
                    new_ids.append(desc.id)

        self._current_ids = new_ids
        return self

    def ancestors(self) -> RelationshipNavigator:
        """Navigate to all ancestors."""
        new_ids: list[str] = []

        for eid in self._current_ids:
            for anc in self.graph.get_ancestors(eid):
                if anc.id not in new_ids:
                    new_ids.append(anc.id)

        self._current_ids = new_ids
        return self

    def filter_by_role(self, role: str) -> RelationshipNavigator:
        """Filter current elements by role."""
        self._current_ids = [
            eid for eid in self._current_ids
            if self.graph.get_element(eid) and
            self.graph.get_element(eid).role == role
        ]
        return self

    def execute(self) -> list[UIElement]:
        """Execute navigation and return elements."""
        return [
            self.graph.get_element(eid)
            for eid in self._current_ids
            if self.graph.get_element(eid)
        ]

    def ids(self) -> list[str]:
        """Get current element IDs without fetching elements."""
        return list(self._current_ids)
