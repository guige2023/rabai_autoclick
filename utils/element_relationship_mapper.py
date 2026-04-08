"""
Element Relationship Mapper.

Map and query relationships between UI elements including
parent-child, sibling, owner-owned, and labelled-by relationships.

Usage:
    from utils.element_relationship_mapper import ElementRelationshipMapper, get_siblings

    mapper = ElementRelationshipMapper(tree)
    children = mapper.get_children(element)
    siblings = mapper.get_siblings(element)
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, Set, Iterator, TYPE_CHECKING
from dataclasses import dataclass, field

if TYPE_CHECKING:
    pass


@dataclass
class Relationship:
    """Represents a relationship between two elements."""
    source_id: int
    target_id: int
    relation_type: str  # "child", "parent", "sibling", "labelled_by", etc.
    source_element: Optional[Dict[str, Any]] = None
    target_element: Optional[Dict[str, Any]] = None


@dataclass
class ElementRelationships:
    """All relationships for a single element."""
    element_id: int
    element: Dict[str, Any]
    parent: Optional[Dict[str, Any]] = None
    children: List[Dict[str, Any]] = field(default_factory=list)
    siblings: List[Dict[str, Any]] = field(default_factory=list)
    labelled_by: List[Dict[str, Any]] = field(default_factory=list)
    label_for: List[Dict[str, Any]] = field(default_factory=list)
    described_by: List[Dict[str, Any]] = field(default_factory=list)
    description_for: List[Dict[str, Any]] = field(default_factory=list)


class ElementRelationshipMapper:
    """
    Map and query relationships between UI elements.

    Builds an internal relationship graph for efficient querying
    of parent-child, sibling, and other relationships.

    Example:
        mapper = ElementRelationshipMapper(tree)
        siblings = mapper.get_siblings(element)
        parent = mapper.get_parent(element)
    """

    def __init__(
        self,
        root: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Initialize the relationship mapper.

        Args:
            root: Optional root element of the tree to map.
        """
        self._root = root
        self._id_map: Dict[int, Dict[str, Any]] = {}
        self._parent_map: Dict[int, Optional[int]] = {}
        self._children_map: Dict[int, List[int]] = {}
        self._sibling_cache: Dict[int, List[int]] = {}
        self._initialized = False

        if root:
            self.build_map(root)

    def build_map(self, root: Dict[str, Any]) -> None:
        """
        Build the relationship map from a tree.

        Args:
            root: Root element of the tree.
        """
        self._root = root
        self._id_map.clear()
        self._parent_map.clear()
        self._children_map.clear()
        self._sibling_cache.clear()

        self._walk_and_index(root, None)

        self._initialized = True

    def _walk_and_index(
        self,
        element: Dict[str, Any],
        parent: Optional[Dict[str, Any]],
    ) -> None:
        """Recursively walk tree and index elements."""
        elem_id = id(element)
        parent_id = id(parent) if parent else 0

        self._id_map[elem_id] = element
        self._parent_map[elem_id] = parent_id if parent else None

        if parent:
            if parent_id not in self._children_map:
                self._children_map[parent_id] = []
            self._children_map[parent_id].append(elem_id)

        for child in element.get("children", []):
            if isinstance(child, dict):
                self._walk_and_index(child, element)

    def get_parent(
        self,
        element: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        Get the parent of an element.

        Args:
            element: Element dictionary.

        Returns:
            Parent element or None.
        """
        elem_id = id(element)
        parent_id = self._parent_map.get(elem_id)
        if parent_id and parent_id in self._id_map:
            return self._id_map[parent_id]
        return None

    def get_children(
        self,
        element: Dict[str, Any],
        role_filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get children of an element.

        Args:
            element: Parent element.
            role_filter: Optional role name to filter children.

        Returns:
            List of child elements.
        """
        elem_id = id(element)
        child_ids = self._children_map.get(elem_id, [])

        children = []
        for cid in child_ids:
            if cid in self._id_map:
                child = self._id_map[cid]
                if role_filter is None or child.get("role") == role_filter:
                    children.append(child)

        return children

    def get_siblings(
        self,
        element: Dict[str, Any],
        include_self: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Get siblings of an element.

        Args:
            element: Element dictionary.
            include_self: Include the element itself in results.

        Returns:
            List of sibling elements.
        """
        elem_id = id(element)
        parent_id = self._parent_map.get(elem_id)

        if parent_id and parent_id in self._children_map:
            sibling_ids = list(self._children_map[parent_id])
            if not include_self and elem_id in sibling_ids:
                sibling_ids.remove(elem_id)
            return [self._id_map[cid] for cid in sibling_ids if cid in self._id_map]

        return []

    def get_ancestors(
        self,
        element: Dict[str, Any],
        max_depth: int = 20,
    ) -> List[Dict[str, Any]]:
        """
        Get all ancestors of an element.

        Args:
            element: Element dictionary.
            max_depth: Maximum traversal depth.

        Returns:
            List of ancestors from parent to root.
        """
        ancestors = []
        current = element
        depth = 0

        while depth < max_depth:
            parent = self.get_parent(current)
            if parent is None:
                break
            ancestors.append(parent)
            current = parent
            depth += 1

        return ancestors

    def get_descendants(
        self,
        element: Dict[str, Any],
        max_depth: int = 20,
    ) -> List[Dict[str, Any]]:
        """
        Get all descendants of an element.

        Args:
            element: Root element.
            max_depth: Maximum traversal depth.

        Returns:
            List of all descendant elements.
        """
        descendants = []
        self._collect_descendants(element, descendants, 0, max_depth)
        return descendants

    def _collect_descendants(
        self,
        element: Dict[str, Any],
        results: List[Dict[str, Any]],
        depth: int,
        max_depth: int,
    ) -> None:
        """Recursively collect descendants."""
        if depth >= max_depth:
            return

        for child in self.get_children(element):
            results.append(child)
            self._collect_descendants(child, results, depth + 1, max_depth)

    def get_element_at_index(
        self,
        element: Dict[str, Any],
        index: int,
    ) -> Optional[Dict[str, Any]]:
        """
        Get the Nth child of an element.

        Args:
            element: Parent element.
            index: Zero-based child index.

        Returns:
            Child element or None.
        """
        children = self.get_children(element)
        if 0 <= index < len(children):
            return children[index]
        return None

    def get_all_of_role(
        self,
        root: Optional[Dict[str, Any]] = None,
        role: str = "button",
    ) -> List[Dict[str, Any]]:
        """
        Get all elements of a specific role in the tree.

        Args:
            root: Root element to search (defaults to mapped root).
            role: Role name to filter by.

        Returns:
            List of matching elements.
        """
        search_root = root or self._root
        if search_root is None:
            return []

        results = []
        self._collect_by_role(search_root, role, results)
        return results

    def _collect_by_role(
        self,
        element: Dict[str, Any],
        role: str,
        results: List[Dict[str, Any]],
    ) -> None:
        """Recursively collect elements of a specific role."""
        if element.get("role") == role:
            results.append(element)
        for child in element.get("children", []):
            if isinstance(child, dict):
                self._collect_by_role(child, role, results)

    def get_relationships(
        self,
        element: Dict[str, Any],
    ) -> ElementRelationships:
        """
        Get all relationships for an element.

        Args:
            element: Element dictionary.

        Returns:
            ElementRelationships object with all related elements.
        """
        return ElementRelationships(
            element_id=id(element),
            element=element,
            parent=self.get_parent(element),
            children=self.get_children(element),
            siblings=self.get_siblings(element),
        )

    def find_common_parent(
        self,
        element_a: Dict[str, Any],
        element_b: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        Find the common parent of two elements.

        Args:
            element_a: First element.
            element_b: Second element.

        Returns:
            Common parent element or None.
        """
        ancestors_a = set(id(e) for e in self.get_ancestors(element_a))
        ancestors_a.add(id(element_a))

        current = element_b
        while current:
            if id(current) in ancestors_a:
                return current
            current = self.get_parent(current)

        return None
