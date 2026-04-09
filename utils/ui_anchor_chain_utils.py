"""
UI Anchor Chain Utilities - Anchor-based element chaining and traversal.

This module provides utilities for creating and traversing chains of
UI elements using anchors. Each element in the chain references the
next element through anchor relationships, enabling reliable navigation
through complex UI hierarchies.

Author: rabai_autoclick team
License: MIT
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Callable, Iterator, Optional, Sequence


@dataclass
class AnchorLink:
    """Represents a link in an anchor chain.
    
    Attributes:
        id: Unique identifier for this link.
        element_id: ID of the UI element this link points to.
        anchor_type: Type of anchor (e.g., 'child', 'sibling', 'parent').
        anchor_label: Optional label describing the relationship.
        weight: Weight for pathfinding algorithms.
        metadata: Additional link metadata.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    element_id: str = ""
    anchor_type: str = "child"
    anchor_label: Optional[str] = None
    weight: float = 1.0
    metadata: dict = field(default_factory=dict)


@dataclass
class AnchorChainNode:
    """A node in the anchor chain containing element reference and outgoing links.
    
    Attributes:
        id: Unique identifier for this node.
        element: The UI element at this node.
        links: List of outgoing AnchorLinks.
        incoming: List of incoming anchor references.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    element: Optional[object] = None
    links: list[AnchorLink] = field(default_factory=list)
    incoming: list[str] = field(default_factory=list)


class AnchorChain:
    """Manages a chain of anchors between UI elements.
    
    Provides methods for building chains, traversing relationships,
    and finding paths between elements through anchor links.
    
    Example:
        >>> chain = AnchorChain()
        >>> chain.add_link("btn_start", "label_status", anchor_type="controls")
        >>> path = chain.find_path("btn_start", "btn_save")
        >>> print(f"Path: {[n.id for n in path]}")
    """
    
    def __init__(self) -> None:
        """Initialize an empty anchor chain."""
        self._nodes: dict[str, AnchorChainNode] = {}
        self._head: Optional[str] = None
        self._tail: Optional[str] = None
    
    def add_node(
        self,
        node_id: str,
        element: Optional[object] = None
    ) -> AnchorChainNode:
        """Add a node to the chain.
        
        Args:
            node_id: Unique identifier for the node.
            element: Optional UI element to associate.
            
        Returns:
            The created or existing AnchorChainNode.
        """
        if node_id in self._nodes:
            node = self._nodes[node_id]
            if element is not None:
                node.element = element
            return node
        
        node = AnchorChainNode(id=node_id, element=element)
        self._nodes[node_id] = node
        
        if self._head is None:
            self._head = node_id
        self._tail = node_id
        
        return node
    
    def add_link(
        self,
        from_id: str,
        to_id: str,
        anchor_type: str = "child",
        anchor_label: Optional[str] = None,
        weight: float = 1.0
    ) -> Optional[AnchorLink]:
        """Add a link between two nodes.
        
        Args:
            from_id: Source node ID.
            to_id: Target node ID.
            anchor_type: Type of anchor relationship.
            anchor_label: Optional descriptive label.
            weight: Weight for pathfinding.
            
        Returns:
            The created AnchorLink, or None if nodes don't exist.
        """
        if from_id not in self._nodes:
            self.add_node(from_id)
        if to_id not in self._nodes:
            self.add_node(to_id)
        
        from_node = self._nodes[from_id]
        to_node = self._nodes[to_id]
        
        link = AnchorLink(
            element_id=to_id,
            anchor_type=anchor_type,
            anchor_label=anchor_label,
            weight=weight
        )
        from_node.links.append(link)
        to_node.incoming.append(from_id)
        
        self._tail = to_id
        
        return link
    
    def get_node(self, node_id: str) -> Optional[AnchorChainNode]:
        """Get a node by ID.
        
        Args:
            node_id: Node identifier.
            
        Returns:
            AnchorChainNode if found, None otherwise.
        """
        return self._nodes.get(node_id)
    
    def get_links_from(
        self,
        node_id: str,
        anchor_type: Optional[str] = None
    ) -> list[AnchorLink]:
        """Get outgoing links from a node.
        
        Args:
            node_id: Source node ID.
            anchor_type: Optional filter by anchor type.
            
        Returns:
            List of outgoing links.
        """
        node = self._nodes.get(node_id)
        if not node:
            return []
        
        if anchor_type:
            return [l for l in node.links if l.anchor_type == anchor_type]
        return node.links.copy()
    
    def get_links_to(
        self,
        node_id: str,
        anchor_type: Optional[str] = None
    ) -> list[tuple[str, AnchorLink]]:
        """Get incoming links to a node.
        
        Args:
            node_id: Target node ID.
            anchor_type: Optional filter by anchor type.
            
        Returns:
            List of (source_id, link) tuples.
        """
        node = self._nodes.get(node_id)
        if not node:
            return []
        
        results = []
        for src_id in node.incoming:
            src_node = self._nodes.get(src_id)
            if src_node:
                for link in src_node.links:
                    if link.element_id == node_id:
                        if anchor_type is None or link.anchor_type == anchor_type:
                            results.append((src_id, link))
        return results
    
    def traverse(
        self,
        start_id: str,
        anchor_type: Optional[str] = None,
        max_depth: int = -1
    ) -> Iterator[tuple[str, AnchorLink, int]]:
        """Traverse the chain from a starting node.
        
        Args:
            start_id: Starting node ID.
            anchor_type: Optional filter by anchor type.
            max_depth: Maximum traversal depth (-1 for unlimited).
            
        Yields:
            Tuples of (node_id, link, depth).
        """
        visited: set[str] = set()
        stack: list[tuple[str, int]] = [(start_id, 0)]
        
        while stack:
            current_id, depth = stack.pop()
            
            if current_id in visited:
                continue
            if max_depth >= 0 and depth > max_depth:
                continue
            
            visited.add(current_id)
            
            node = self._nodes.get(current_id)
            if not node:
                continue
            
            for link in node.links:
                if anchor_type is None or link.anchor_type == anchor_type:
                    yield (link.element_id, link, depth + 1)
                    if link.element_id not in visited:
                        stack.append((link.element_id, depth + 1))
    
    def find_path(
        self,
        from_id: str,
        to_id: str,
        anchor_type: Optional[str] = None
    ) -> Optional[list[str]]:
        """Find a path between two nodes using BFS.
        
        Args:
            from_id: Start node ID.
            to_id: Target node ID.
            anchor_type: Optional anchor type filter.
            
        Returns:
            List of node IDs forming the path, or None if no path found.
        """
        if from_id == to_id:
            return [from_id]
        
        visited: set[str] = {from_id}
        queue: list[tuple[str, list[str]]] = [(from_id, [from_id])]
        
        while queue:
            current, path = queue.pop(0)
            
            for link in self.get_links_from(current, anchor_type):
                if link.element_id == to_id:
                    return path + [link.element_id]
                
                if link.element_id not in visited:
                    visited.add(link.element_id)
                    queue.append((link.element_id, path + [link.element_id]))
        
        return None
    
    def find_all_paths(
        self,
        from_id: str,
        to_id: str,
        anchor_type: Optional[str] = None,
        max_length: int = 10
    ) -> list[list[str]]:
        """Find all paths between two nodes.
        
        Args:
            from_id: Start node ID.
            to_id: Target node ID.
            anchor_type: Optional anchor type filter.
            max_length: Maximum path length.
            
        Returns:
            List of paths, each path is a list of node IDs.
        """
        results: list[list[str]] = []
        
        def dfs(current: str, path: list[str], visited: set[str]) -> None:
            if len(path) > max_length:
                return
            
            if current == to_id:
                results.append(path.copy())
                return
            
            for link in self.get_links_from(current, anchor_type):
                if link.element_id not in visited:
                    visited.add(link.element_id)
                    path.append(link.element_id)
                    dfs(link.element_id, path, visited)
                    path.pop()
                    visited.remove(link.element_id)
        
        visited = {from_id}
        dfs(from_id, [from_id], visited)
        return results
    
    def find_shortest_path(
        self,
        from_id: str,
        to_id: str,
        anchor_type: Optional[str] = None
    ) -> Optional[list[str]]:
        """Find shortest weighted path between nodes.
        
        Args:
            from_id: Start node ID.
            to_id: Target node ID.
            anchor_type: Optional anchor type filter.
            
        Returns:
            List of node IDs in the path, or None.
        """
        import heapq
        
        if from_id == to_id:
            return [from_id]
        
        distances: dict[str, float] = {from_id: 0}
        previous: dict[str, tuple[str, AnchorLink]] = {}
        visited: set[str] = set()
        heap: list[tuple[float, str]] = [(0, from_id)]
        
        while heap:
            current_dist, current = heapq.heappop(heap)
            
            if current in visited:
                continue
            visited.add(current)
            
            if current == to_id:
                break
            
            for link in self.get_links_from(current, anchor_type):
                neighbor = link.element_id
                weight = link.weight
                new_dist = current_dist + weight
                
                if neighbor not in distances or new_dist < distances[neighbor]:
                    distances[neighbor] = new_dist
                    previous[neighbor] = (current, link)
                    heapq.heappush(heap, (new_dist, neighbor))
        
        if to_id not in previous:
            return None
        
        path = []
        current = to_id
        while current != from_id:
            path.append(current)
            current, _ = previous[current]
        path.append(from_id)
        
        return list(reversed(path))
    
    def get_head(self) -> Optional[AnchorChainNode]:
        """Get the head (first) node of the chain.
        
        Returns:
            First node, or None if chain is empty.
        """
        if self._head:
            return self._nodes.get(self._head)
        return None
    
    def get_tail(self) -> Optional[AnchorChainNode]:
        """Get the tail (last) node of the chain.
        
        Returns:
            Last node, or None if chain is empty.
        """
        if self._tail:
            return self._nodes.get(self._tail)
        return None
    
    def iterate_nodes(self) -> Iterator[AnchorChainNode]:
        """Iterate over all nodes in the chain.
        
        Yields:
            Each AnchorChainNode.
        """
        yield from self._nodes.values()
    
    def remove_node(self, node_id: str) -> bool:
        """Remove a node and all its connections.
        
        Args:
            node_id: Node to remove.
            
        Returns:
            True if node was removed.
        """
        if node_id not in self._nodes:
            return False
        
        node = self._nodes[node_id]
        
        for link in node.links:
            target = self._nodes.get(link.element_id)
            if target and node_id in target.incoming:
                target.incoming.remove(node_id)
        
        for src_id in node.incoming:
            src_node = self._nodes.get(src_id)
            if src_node:
                src_node.links = [
                    l for l in src_node.links if l.element_id != node_id
                ]
        
        del self._nodes[node_id]
        
        if self._head == node_id:
            self._head = node.incoming[0] if node.incoming else None
        if self._tail == node_id:
            self._tail = node.links[0].element_id if node.links else None
        
        return True
    
    def clear(self) -> None:
        """Remove all nodes and links from the chain."""
        self._nodes.clear()
        self._head = None
        self._tail = None


class AnchorChainBuilder:
    """Builder for constructing anchor chains with fluent API.
    
    Example:
        >>> builder = AnchorChainBuilder()
        >>> chain = (builder
        ...     .add_node("home", element)
        ...     .add_link("home", "menu", "sibling")
        ...     .add_link("menu", "settings", "child")
        ...     .build())
    """
    
    def __init__(self) -> None:
        """Initialize the builder."""
        self._chain = AnchorChain()
    
    def add_node(
        self,
        node_id: str,
        element: Optional[object] = None
    ) -> AnchorChainBuilder:
        """Add a node to the chain.
        
        Args:
            node_id: Node identifier.
            element: Optional element.
            
        Returns:
            Self for chaining.
        """
        self._chain.add_node(node_id, element)
        return self
    
    def add_link(
        self,
        from_id: str,
        to_id: str,
        anchor_type: str = "child",
        anchor_label: Optional[str] = None,
        weight: float = 1.0
    ) -> AnchorChainBuilder:
        """Add a link between nodes.
        
        Args:
            from_id: Source node.
            to_id: Target node.
            anchor_type: Anchor type.
            anchor_label: Optional label.
            weight: Link weight.
            
        Returns:
            Self for chaining.
        """
        self._chain.add_link(from_id, to_id, anchor_type, anchor_label, weight)
        return self
    
    def build(self) -> AnchorChain:
        """Build and return the anchor chain.
        
        Returns:
            The constructed AnchorChain.
        """
        return self._chain
