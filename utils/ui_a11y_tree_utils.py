"""
UI A11y Tree Utilities - Accessibility tree traversal and manipulation.

This module provides utilities for working with accessibility trees,
enabling navigation, search, and manipulation of UI elements through
their accessibility representations.

Author: rabai_autoclick team
License: MIT
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Callable, Iterator, Optional, Sequence


@dataclass
class A11yNode:
    """Represents a node in an accessibility tree.
    
    Attributes:
        id: Unique identifier for this node.
        role: Accessibility role (button, input, etc.).
        name: Accessible name of the element.
        description: Optional description.
        value: Current value if applicable.
        states: Set of accessibility states.
        actions: Available actions on this node.
        bounds: Optional (x, y, width, height) bounds.
        parent: ID of parent node.
        children: List of child node IDs.
        index: Position among siblings.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    role: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    value: Optional[str] = None
    states: set[str] = field(default_factory=set)
    actions: set[str] = field(default_factory=set)
    bounds: Optional[tuple[int, int, int, int]] = None
    parent: Optional[str] = None
    children: list[str] = field(default_factory=list)
    index: int = 0
    
    def has_state(self, state: str) -> bool:
        """Check if node has a specific state.
        
        Args:
            state: State to check.
            
        Returns:
            True if node has the state.
        """
        return state in self.states
    
    def is_focusable(self) -> bool:
        """Check if node can receive focus.
        
        Returns:
            True if node is focusable.
        """
        return "focusable" in self.states or "focused" in self.states
    
    def is_visible(self) -> bool:
        """Check if node is visible.
        
        Returns:
            True if node is visible.
        """
        return "invisible" not in self.states and "hidden" not in self.states
    
    def supports_action(self, action: str) -> bool:
        """Check if node supports a specific action.
        
        Args:
            action: Action to check.
            
        Returns:
            True if action is supported.
        """
        return action in self.actions


class A11yTree:
    """Manages an accessibility tree structure.
    
    Provides methods for building, traversing, and querying
    accessibility trees for UI automation.
    
    Example:
        >>> tree = A11yTree()
        >>> tree.add_node("root", role="window")
        >>> tree.add_node("btn", role="button", parent="root")
        >>> button = tree.find_node_by_role("button")
    """
    
    def __init__(self) -> None:
        """Initialize an empty accessibility tree."""
        self._nodes: dict[str, A11yNode] = {}
        self._root_id: Optional[str] = None
    
    def add_node(
        self,
        node_id: str,
        role: Optional[str] = None,
        name: Optional[str] = None,
        parent: Optional[str] = None,
        **kwargs
    ) -> A11yNode:
        """Add a node to the tree.
        
        Args:
            node_id: Unique identifier for the node.
            role: Accessibility role.
            name: Accessible name.
            parent: Parent node ID.
            **kwargs: Additional node attributes.
            
        Returns:
            The created A11yNode.
        """
        if node_id in self._nodes:
            node = self._nodes[node_id]
            if role is not None:
                node.role = role
            if name is not None:
                node.name = name
            return node
        
        if parent and parent in self._nodes:
            parent_node = self._nodes[parent]
            index = len(parent_node.children)
        else:
            index = 0
        
        node = A11yNode(
            id=node_id,
            role=role,
            name=name,
            parent=parent,
            index=index
        )
        
        for key, value in kwargs.items():
            if hasattr(node, key):
                setattr(node, key, value)
        
        self._nodes[node_id] = node
        
        if parent and parent in self._nodes:
            self._nodes[parent].children.append(node_id)
        
        if parent is None and self._root_id is None:
            self._root_id = node_id
        
        return node
    
    def remove_node(self, node_id: str) -> bool:
        """Remove a node and its children from the tree.
        
        Args:
            node_id: ID of node to remove.
            
        Returns:
            True if node was removed.
        """
        if node_id not in self._nodes:
            return False
        
        node = self._nodes[node_id]
        
        for child_id in node.children:
            self.remove_node(child_id)
        
        if node.parent and node.parent in self._nodes:
            parent = self._nodes[node.parent]
            if node_id in parent.children:
                parent.children.remove(node_id)
        
        del self._nodes[node_id]
        
        if self._root_id == node_id:
            self._root_id = None
        
        return True
    
    def get_node(self, node_id: str) -> Optional[A11yNode]:
        """Get a node by ID.
        
        Args:
            node_id: Node identifier.
            
        Returns:
            A11yNode if found.
        """
        return self._nodes.get(node_id)
    
    def get_root(self) -> Optional[A11yNode]:
        """Get the root node of the tree.
        
        Returns:
            Root A11yNode, or None.
        """
        if self._root_id:
            return self._nodes.get(self._root_id)
        return None
    
    def get_parent(self, node_id: str) -> Optional[A11yNode]:
        """Get the parent of a node.
        
        Args:
            node_id: Node identifier.
            
        Returns:
            Parent A11yNode, or None.
        """
        node = self._nodes.get(node_id)
        if node and node.parent:
            return self._nodes.get(node.parent)
        return None
    
    def get_children(self, node_id: str) -> list[A11yNode]:
        """Get direct children of a node.
        
        Args:
            node_id: Parent node identifier.
            
        Returns:
            List of child A11yNodes.
        """
        node = self._nodes.get(node_id)
        if not node:
            return []
        return [self._nodes[cid] for cid in node.children if cid in self._nodes]
    
    def get_siblings(self, node_id: str) -> list[A11yNode]:
        """Get siblings of a node (same parent).
        
        Args:
            node_id: Node identifier.
            
        Returns:
            List of sibling A11yNodes.
        """
        node = self._nodes.get(node_id)
        if not node or not node.parent:
            return []
        
        parent = self._nodes.get(node.parent)
        if not parent:
            return []
        
        return [
            self._nodes[cid]
            for cid in parent.children
            if cid in self._nodes and cid != node_id
        ]
    
    def find_node_by_role(
        self,
        role: str,
        root_id: Optional[str] = None
    ) -> Optional[A11yNode]:
        """Find the first node with a specific role.
        
        Args:
            role: Role to search for.
            root_id: Optional root to search from.
            
        Returns:
            First matching A11yNode, or None.
        """
        results = self.find_nodes_by_role(role, root_id)
        return results[0] if results else None
    
    def find_nodes_by_role(
        self,
        role: str,
        root_id: Optional[str] = None
    ) -> list[A11yNode]:
        """Find all nodes with a specific role.
        
        Args:
            role: Role to search for.
            root_id: Optional root to search from.
            
        Returns:
            List of matching A11yNodes.
        """
        results: list[A11yNode] = []
        root = root_id or self._root_id
        
        if not root:
            return results
        
        def search(node_id: str) -> None:
            node = self._nodes.get(node_id)
            if node and node.role == role:
                results.append(node)
            if node:
                for child_id in node.children:
                    search(child_id)
        
        search(root)
        return results
    
    def find_node_by_name(
        self,
        name: str,
        root_id: Optional[str] = None
    ) -> Optional[A11yNode]:
        """Find a node by its accessible name.
        
        Args:
            name: Name to search for.
            root_id: Optional root to search from.
            
        Returns:
            First matching A11yNode, or None.
        """
        results = self.find_nodes_by_name(name, root_id)
        return results[0] if results else None
    
    def find_nodes_by_name(
        self,
        name: str,
        root_id: Optional[str] = None
    ) -> list[A11yNode]:
        """Find all nodes with a specific name.
        
        Args:
            name: Name to search for.
            root_id: Optional root to search from.
            
        Returns:
            List of matching A11yNodes.
        """
        results: list[A11yNode] = []
        root = root_id or self._root_id
        name_lower = name.lower()
        
        if not root:
            return results
        
        def search(node_id: str) -> None:
            node = self._nodes.get(node_id)
            if node and node.name and node.name.lower() == name_lower:
                results.append(node)
            if node:
                for child_id in node.children:
                    search(child_id)
        
        search(root)
        return results
    
    def find_nodes(
        self,
        predicate: Callable[[A11yNode], bool],
        root_id: Optional[str] = None
    ) -> list[A11yNode]:
        """Find nodes matching a predicate.
        
        Args:
            predicate: Function returning True for matches.
            root_id: Optional root to search from.
            
        Returns:
            List of matching A11yNodes.
        """
        results: list[A11yNode] = []
        root = root_id or self._root_id
        
        if not root:
            return results
        
        def search(node_id: str) -> None:
            node = self._nodes.get(node_id)
            if node and predicate(node):
                results.append(node)
            if node:
                for child_id in node.children:
                    search(child_id)
        
        search(root)
        return results
    
    def traverse(
        self,
        root_id: Optional[str] = None,
        order: str = "depth-first"
    ) -> Iterator[A11yNode]:
        """Traverse the tree.
        
        Args:
            root_id: Starting node (default: root).
            order: Traversal order ("depth-first" or "breadth-first").
            
        Yields:
            A11yNodes in traversal order.
        """
        root = root_id or self._root_id
        if not root:
            return
        
        if order == "breadth-first":
            queue = [root]
            while queue:
                node_id = queue.pop(0)
                node = self._nodes.get(node_id)
                if node:
                    yield node
                    queue.extend(node.children)
        else:
            stack = [root]
            while stack:
                node_id = stack.pop()
                node = self._nodes.get(node_id)
                if node:
                    yield node
                    stack.extend(reversed(node.children))
    
    def get_path_to(self, node_id: str) -> list[A11yNode]:
        """Get the path from root to a node.
        
        Args:
            node_id: Target node identifier.
            
        Returns:
            List of A11yNodes from root to target.
        """
        path: list[A11yNode] = []
        current = self._nodes.get(node_id)
        
        while current:
            path.append(current)
            current = self.get_parent(current.id) if current else None
        
        return list(reversed(path))
    
    def get_depth(self, node_id: str) -> int:
        """Get the depth of a node from root.
        
        Args:
            node_id: Node identifier.
            
        Returns:
            Depth (root = 0).
        """
        return len(self.get_path_to(node_id)) - 1
    
    def clear(self) -> None:
        """Clear all nodes from the tree."""
        self._nodes.clear()
        self._root_id = None
    
    def __len__(self) -> int:
        """Get number of nodes in tree."""
        return len(self._nodes)
    
    def __contains__(self, node_id: str) -> bool:
        """Check if node exists in tree."""
        return node_id in self._nodes


def build_a11y_tree_from_snapshot(
    snapshot: dict
) -> A11yTree:
    """Build an accessibility tree from a snapshot.
    
    Args:
        snapshot: Accessibility snapshot data.
        
    Returns:
        Constructed A11yTree.
    """
    tree = A11yTree()
    
    def add_recursive(node_data: dict, parent_id: Optional[str] = None) -> str:
        node_id = node_data.get("id", str(uuid.uuid4()))
        tree.add_node(
            node_id=node_id,
            role=node_data.get("role"),
            name=node_data.get("name"),
            description=node_data.get("description"),
            value=node_data.get("value"),
            parent=parent_id
        )
        
        if "states" in node_data:
            tree._nodes[node_id].states = set(node_data["states"])
        if "actions" in node_data:
            tree._nodes[node_id].actions = set(node_data["actions"])
        if "bounds" in node_data:
            tree._nodes[node_id].bounds = tuple(node_data["bounds"])
        
        for child_data in node_data.get("children", []):
            add_recursive(child_data, node_id)
        
        return node_id
    
    if "children" in snapshot:
        for child in snapshot["children"]:
            add_recursive(child)
    
    return tree
