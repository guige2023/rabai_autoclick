"""
Element relationship utilities for parent-child and sibling relationships.

Analyzes and manages relationships between UI elements including
parent-child, sibling, and owner-owned relationships.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ElementRelation:
    """Represents a relationship between two elements."""
    from_id: str
    to_id: str
    relation_type: str  # "child", "parent", "sibling", "owner", "member"
    metadata: dict = field(default_factory=dict)


class ElementRelationshipGraph:
    """Manages relationships between UI elements."""

    def __init__(self):
        self._parents: dict[str, str] = {}
        self._children: dict[str, list[str]] = {}
        self._siblings: dict[str, list[str]] = {}
        self._owners: dict[str, str] = {}
        self._members: dict[str, list[str]] = {}

    def add_child(self, parent_id: str, child_id: str) -> None:
        """Add a parent-child relationship."""
        self._parents[child_id] = parent_id
        if parent_id not in self._children:
            self._children[parent_id] = []
        if child_id not in self._children[parent_id]:
            self._children[parent_id].append(child_id)

    def add_sibling(self, a_id: str, b_id: str) -> None:
        """Add a sibling relationship between two elements."""
        if a_id not in self._siblings:
            self._siblings[a_id] = []
        if b_id not in self._siblings[a_id]:
            self._siblings[a_id].append(b_id)

        if b_id not in self._siblings:
            self._siblings[b_id] = []
        if a_id not in self._siblings[b_id]:
            self._siblings[b_id].append(a_id)

    def add_owner(self, owner_id: str, owned_id: str) -> None:
        """Add an owner-owned relationship."""
        self._owners[owned_id] = owner_id
        if owner_id not in self._members:
            self._members[owner_id] = []
        if owned_id not in self._members[owner_id]:
            self._members[owner_id].append(owned_id)

    def get_parent(self, element_id: str) -> Optional[str]:
        """Get parent element ID."""
        return self._parents.get(element_id)

    def get_children(self, element_id: str) -> list[str]:
        """Get child element IDs."""
        return list(self._children.get(element_id, []))

    def get_siblings(self, element_id: str) -> list[str]:
        """Get sibling element IDs."""
        return list(self._siblings.get(element_id, []))

    def get_owner(self, element_id: str) -> Optional[str]:
        """Get owner element ID."""
        return self._owners.get(element_id)

    def get_members(self, element_id: str) -> list[str]:
        """Get members of an element (inverse of owner)."""
        return list(self._members.get(element_id, []))

    def get_ancestors(self, element_id: str) -> list[str]:
        """Get all ancestors (parent chain)."""
        ancestors = []
        current = self._parents.get(element_id)
        visited = set()
        while current:
            if current in visited:
                break
            visited.add(current)
            ancestors.append(current)
            current = self._parents.get(current)
        return ancestors

    def get_descendants(self, element_id: str) -> list[str]:
        """Get all descendants (children chain)."""
        descendants = []
        stack = list(self._children.get(element_id, []))
        visited = set()
        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            descendants.append(current)
            stack.extend(self._children.get(current, []))
        return descendants

    def get_root(self, element_id: str) -> str:
        """Get the root ancestor of an element."""
        ancestors = self.get_ancestors(element_id)
        return ancestors[-1] if ancestors else element_id

    def get_path_to_root(self, element_id: str) -> list[str]:
        """Get the full path from element to root."""
        path = [element_id]
        path.extend(self.get_ancestors(element_id))
        return path

    def is_ancestor_of(self, ancestor_id: str, descendant_id: str) -> bool:
        """Check if ancestor_id is an ancestor of descendant_id."""
        return ancestor_id in self.get_ancestors(descendant_id)

    def is_descendant_of(self, descendant_id: str, ancestor_id: str) -> bool:
        """Check if descendant_id is a descendant of ancestor_id."""
        return ancestor_id in self.get_ancestors(descendant_id)

    def remove_element(self, element_id: str) -> None:
        """Remove an element and its relationships."""
        # Remove from parent's children list
        parent = self._parents.pop(element_id, None)
        if parent and parent in self._children:
            self._children[parent] = [c for c in self._children[parent] if c != element_id]

        # Remove from siblings
        for sib_list in self._siblings.values():
            if element_id in sib_list:
                sib_list.remove(element_id)

        # Remove owner relationship
        owner = self._owners.pop(element_id, None)
        if owner and owner in self._members:
            self._members[owner] = [m for m in self._members[owner] if m != element_id]

        # Remove children relationships
        for child in self._children.pop(element_id, []):
            if self._parents.get(child) == element_id:
                del self._parents[child]

        # Remove from members
        for member_list in self._members.values():
            if element_id in member_list:
                member_list.remove(element_id)


__all__ = ["ElementRelationshipGraph", "ElementRelation"]
