"""Graph traversal and tree data structure utilities.

Provides graph algorithms (BFS, DFS, Dijkstra, A*),
tree operations, and hierarchical data processing.
"""

from __future__ import annotations

from typing import (
    TypeVar, Generic, Dict, List, Set, Optional, Tuple,
    Callable, Iterator, Any
)
from dataclasses import dataclass, field
from enum import Enum, auto
import heapq
import math


T = TypeVar('T')


class TraversalOrder(Enum):
    """Graph/tree traversal orders."""
    BREADTH_FIRST = auto()
    DEPTH_FIRST_PRE = auto()
    DEPTH_FIRST_POST = auto()
    DEPTH_FIRST_IN = auto()
    LEVEL_ORDER = auto()


@dataclass
class Node(Generic[T]):
    """Tree node with value and optional children."""
    value: T
    children: List[Node[T]] = field(default_factory=list)
    parent: Optional[Node[T]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def add_child(self, value: T) -> Node[T]:
        """Add a child node and return it."""
        child = Node(value=value, parent=self)
        self.children.append(child)
        return child

    def is_leaf(self) -> bool:
        """Check if node is a leaf (no children)."""
        return len(self.children) == 0

    def depth(self) -> int:
        """Get depth of this node from root."""
        d = 0
        node: Optional[Node[T]] = self
        while node is not None and node.parent is not None:
            d += 1
            node = node.parent
        return d

    def height(self) -> int:
        """Get height of subtree rooted at this node."""
        if self.is_leaf():
            return 1
        return 1 + max(child.height() for child in self.children)

    def ancestors(self) -> List[Node[T]]:
        """Get list of ancestor nodes (parent, grandparent, etc.)."""
        result = []
        node = self.parent
        while node is not None:
            result.append(node)
            node = node.parent
        return result

    def descendants(self) -> List[Node[T]]:
        """Get all descendant nodes (children, grandchildren, etc.)."""
        result = []
        stack = list(self.children)
        while stack:
            node = stack.pop()
            result.append(node)
            stack.extend(node.children)
        return result

    def siblings(self) -> List[Node[T]]:
        """Get sibling nodes (excluding self)."""
        if self.parent is None:
            return []
        return [c for c in self.parent.children if c is not self]

    def path_to_root(self) -> List[Node[T]]:
        """Get path from this node to root (inclusive)."""
        result = [self]
        node = self.parent
        while node is not None:
            result.append(node)
            node = node.parent
        return result

    def __repr__(self) -> str:
        return f"Node({self.value!r}, children={len(self.children)})"


@dataclass
class Tree(Generic[T]):
    """Generic tree data structure."""
    root: Node[T]

    def traverse(
        self,
        order: TraversalOrder = TraversalOrder.DEPTH_FIRST_PRE
    ) -> Iterator[Node[T]]:
        """Traverse tree in specified order."""
        if order == TraversalOrder.DEPTH_FIRST_PRE:
            return self._dfs_pre(self.root)
        elif order == TraversalOrder.DEPTH_FIRST_POST:
            return self._dfs_post(self.root)
        elif order == TraversalOrder.BREADTH_FIRST:
            return self._bfs(self.root)
        elif order == TraversalOrder.LEVEL_ORDER:
            return self._bfs(self.root)
        return iter([])

    def _dfs_pre(self, node: Node[T]) -> Iterator[Node[T]]:
        """Depth-first pre-order traversal."""
        yield node
        for child in node.children:
            yield from self._dfs_pre(child)

    def _dfs_post(self, node: Node[T]) -> Iterator[Node[T]]:
        """Depth-first post-order traversal."""
        for child in node.children:
            yield from self._dfs_post(child)
        yield node

    def _bfs(self, node: Node[T]) -> Iterator[Node[T]]:
        """Breadth-first (level order) traversal."""
        queue: List[Node[T]] = [node]
        while queue:
            current = queue.pop(0)
            yield current
            queue.extend(current.children)

    def find(self, value: T) -> Optional[Node[T]]:
        """Find first node with matching value."""
        for node in self.traverse():
            if node.value == value:
                return node
        return None

    def find_all(self, value: T) -> List[Node[T]]:
        """Find all nodes with matching value."""
        return [node for node in self.traverse() if node.value == value]

    def find_matching(
        self, predicate: Callable[[Node[T]], bool]
    ) -> Optional[Node[T]]:
        """Find first node matching predicate."""
        for node in self.traverse():
            if predicate(node):
                return node
        return None

    def height(self) -> int:
        """Get height of tree."""
        return self.root.height()

    def node_count(self) -> int:
        """Count total nodes in tree."""
        return sum(1 for _ in self.traverse())

    def leaf_count(self) -> int:
        """Count leaf nodes in tree."""
        return sum(1 for node in self.traverse() if node.is_leaf())


@dataclass
class GraphNode(Generic[T]):
    """Node in a weighted graph."""
    id: str
    value: T
    neighbors: Dict[str, float] = field(default_factory=dict)

    def add_edge(self, neighbor_id: str, weight: float = 1.0) -> None:
        """Add weighted edge to neighbor."""
        self.neighbors[neighbor_id] = weight

    def get_edge_weight(self, neighbor_id: str) -> Optional[float]:
        """Get weight of edge to neighbor."""
        return self.neighbors.get(neighbor_id)


@dataclass
class Graph(Generic[T]):
    """Weighted graph data structure."""
    nodes: Dict[str, GraphNode[T]] = field(default_factory=dict)

    def add_node(self, id: str, value: T) -> GraphNode[T]:
        """Add a node to the graph."""
        node = GraphNode(id=id, value=value)
        self.nodes[id] = node
        return node

    def add_edge(self, from_id: str, to_id: str, weight: float = 1.0) -> None:
        """Add weighted edge between nodes."""
        if from_id not in self.nodes:
            raise KeyError(f"Node {from_id} not found")
        if to_id not in self.nodes:
            raise KeyError(f"Node {to_id} not found")
        self.nodes[from_id].add_edge(to_id, weight)
        self.nodes[to_id].add_edge(from_id, weight)

    def get_node(self, id: str) -> Optional[GraphNode[T]]:
        """Get node by ID."""
        return self.nodes.get(id)

    def bfs(self, start_id: str) -> Iterator[str]:
        """Breadth-first search from start node."""
        if start_id not in self.nodes:
            return iter([])
        visited: Set[str] = set()
        queue: List[str] = [start_id]
        while queue:
            node_id = queue.pop(0)
            if node_id in visited:
                continue
            visited.add(node_id)
            yield node_id
            node = self.nodes[node_id]
            for neighbor_id in node.neighbors:
                if neighbor_id not in visited:
                    queue.append(neighbor_id)

    def dfs(self, start_id: str) -> Iterator[str]:
        """Depth-first search from start node."""
        if start_id not in self.nodes:
            return iter([])
        visited: Set[str] = set()
        stack: List[str] = [start_id]
        while stack:
            node_id = stack.pop()
            if node_id in visited:
                continue
            visited.add(node_id)
            yield node_id
            node = self.nodes[node_id]
            for neighbor_id in node.neighbors:
                if neighbor_id not in visited:
                    stack.append(neighbor_id)

    def dijkstra(
        self, start_id: str, end_id: Optional[str] = None
    ) -> Tuple[Dict[str, float], Dict[str, Optional[str]]]:
        """Dijkstra's shortest path algorithm.

        Returns:
            Tuple of (distances dict, predecessors dict)
        """
        if start_id not in self.nodes:
            raise KeyError(f"Start node {start_id} not found")
        distances: Dict[str, float] = {n: math.inf for n in self.nodes}
        distances[start_id] = 0.0
        predecessors: Dict[str, Optional[str]] = {n: None for n in self.nodes}
        visited: Set[str] = set()
        pq: List[Tuple[float, str]] = [(0.0, start_id)]
        while pq:
            current_dist, current = heapq.heappop(pq)
            if current in visited:
                continue
            visited.add(current)
            if end_id and current == end_id:
                break
            node = self.nodes[current]
            for neighbor_id, weight in node.neighbors.items():
                if neighbor_id in visited:
                    continue
                new_dist = current_dist + weight
                if new_dist < distances[neighbor_id]:
                    distances[neighbor_id] = new_dist
                    predecessors[neighbor_id] = current
                    heapq.heappush(pq, (new_dist, neighbor_id))
        return distances, predecessors

    def shortest_path(self, start_id: str, end_id: str) -> Optional[List[str]]:
        """Get shortest path between two nodes."""
        _, predecessors = self.dijkstra(start_id, end_id)
        if predecessors.get(end_id) is None and start_id != end_id:
            return None
        path = []
        current: Optional[str] = end_id
        while current is not None:
            path.append(current)
            current = predecessors[current]
        return list(reversed(path))

    def is_connected(self) -> bool:
        """Check if graph is connected (all nodes reachable from any node)."""
        if not self.nodes:
            return True
        start = next(iter(self.nodes))
        visited = set(self.bfs(start))
        return len(visited) == len(self.nodes)

    def connected_components(self) -> List[Set[str]]:
        """Find all connected components."""
        visited: Set[str] = set()
        components: List[Set[str]] = []
        for node_id in self.nodes:
            if node_id not in visited:
                component = set(self.bfs(node_id))
                visited.update(component)
                components.append(component)
        return components


def build_tree_from_dict(
    data: dict, children_key: str = "children"
) -> Optional[Node[Any]]:
    """Build a tree from a nested dictionary.

    Example:
        data = {
            "id": 1,
            "children": [{"id": 2}, {"id": 3, "children": [{"id": 4}]}]
        }
        tree = build_tree_from_dict(data)
    """
    if not data:
        return None
    node = Node(value=data.get("id", data.get("name", data)))
    children_data = data.get(children_key, [])
    if isinstance(children_data, list):
        for child_data in children_data:
            child_node = build_tree_from_dict(child_data, children_key)
            if child_node:
                child_node.parent = node
                node.children.append(child_node)
    return node


def tree_to_dict(node: Node[T], children_key: str = "children") -> dict:
    """Convert a tree node to dictionary representation."""
    result: Dict[str, Any] = {"value": node.value}
    if node.children:
        result[children_key] = [
            tree_to_dict(child, children_key) for child in node.children
        ]
    return result
