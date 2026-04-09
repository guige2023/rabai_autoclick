"""
Graph Action Module

Provides graph data structure and algorithm implementations for UI automation workflows.
Supports directed/undirected graphs, traversal algorithms, and pathfinding.

Author: AI Agent
Version: 1.0.0
"""

from __future__ import annotations

import heapq
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Iterator, Optional


class GraphType(Enum):
    """Graph type enumeration."""
    DIRECTED = auto()
    UNDIRECTED = auto()
    WEIGHTED_DIRECTED = auto()
    WEIGHTED_UNDIRECTED = auto()


class TraversalOrder(Enum):
    """Graph traversal order."""
    BREADTH_FIRST = auto()
    DEPTH_FIRST_PRE = auto()
    DEPTH_FIRST_IN = auto()
    DEPTH_FIRST_POST = auto()


@dataclass
class GraphNode:
    """Represents a node in the graph."""
    id: str
    data: Any = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GraphNode):
            return False
        return self.id == other.id


@dataclass
class GraphEdge:
    """Represents an edge between nodes."""
    source: str
    target: str
    weight: float = 1.0
    data: Any = None
    metadata: dict[str, Any] = field(default_factory=dict)


class Graph:
    """
    Graph data structure supporting multiple traversal algorithms.

    Example:
        >>> g = Graph(graph_type=GraphType.WEIGHTED_DIRECTED)
        >>> g.add_node("A", data={"label": "Start"})
        >>> g.add_node("B")
        >>> g.add_edge("A", "B", weight=5.0)
        >>> paths = g.find_shortest_path("A", "B")
    """

    def __init__(
        self,
        graph_type: GraphType = GraphType.DIRECTED,
        allow_parallel_edges: bool = True,
    ) -> None:
        self.graph_type = graph_type
        self.allow_parallel_edges = allow_parallel_edges
        self._nodes: dict[str, GraphNode] = {}
        self._adjacency: dict[str, list[tuple[str, float]]] = defaultdict(list)
        self._edges: dict[tuple[str, str], GraphEdge] = {}

    def add_node(self, node_id: str, data: Any = None, **metadata: Any) -> GraphNode:
        """Add a node to the graph."""
        if node_id in self._nodes:
            raise ValueError(f"Node {node_id} already exists")
        node = GraphNode(id=node_id, data=data, metadata=metadata)
        self._nodes[node_id] = node
        return node

    def add_edge(
        self,
        source: str,
        target: str,
        weight: float = 1.0,
        data: Any = None,
        **metadata: Any,
    ) -> GraphEdge:
        """Add an edge between two nodes."""
        if source not in self._nodes:
            raise ValueError(f"Source node {source} not found")
        if target not in self._nodes:
            raise ValueError(f"Target node {target} not found")

        edge_key = (source, target)
        if not self.allow_parallel_edges and edge_key in self._edges:
            raise ValueError(f"Edge {source} -> {target} already exists")

        edge = GraphEdge(
            source=source,
            target=target,
            weight=weight,
            data=data,
            metadata=metadata,
        )
        self._edges[edge_key] = edge
        self._adjacency[source].append((target, weight))

        if self.graph_type in (
            GraphType.UNDIRECTED,
            GraphType.WEIGHTED_UNDIRECTED,
        ):
            self._adjacency[target].append((source, weight))

        return edge

    def get_node(self, node_id: str) -> Optional[GraphNode]:
        """Get a node by ID."""
        return self._nodes.get(node_id)

    def get_neighbors(self, node_id: str) -> list[str]:
        """Get all neighboring node IDs."""
        return [target for target, _ in self._adjacency.get(node_id, [])]

    def has_node(self, node_id: str) -> bool:
        """Check if node exists."""
        return node_id in self._nodes

    def has_edge(self, source: str, target: str) -> bool:
        """Check if edge exists."""
        return (source, target) in self._edges

    def remove_node(self, node_id: str) -> None:
        """Remove a node and all its edges."""
        if node_id not in self._nodes:
            return

        del self._nodes[node_id]
        edges_to_remove = [
            (s, t) for (s, t) in self._edges if s == node_id or t == node_id
        ]
        for edge in edges_to_remove:
            del self._edges[edge]

        if node_id in self._adjacency:
            del self._adjacency[node_id]

        for source in self._adjacency:
            self._adjacency[source] = [
                (t, w) for t, w in self._adjacency[source] if t != node_id
            ]

    def node_count(self) -> int:
        """Get total number of nodes."""
        return len(self._nodes)

    def edge_count(self) -> int:
        """Get total number of edges."""
        return len(self._edges)

    def bfs(self, start: str) -> Iterator[str]:
        """Breadth-first traversal."""
        if start not in self._nodes:
            raise ValueError(f"Start node {start} not found")

        visited: set[str] = set()
        queue: deque[str] = deque([start])

        while queue:
            node_id = queue.popleft()
            if node_id in visited:
                continue
            visited.add(node_id)
            yield node_id
            for neighbor, _ in self._adjacency.get(node_id, []):
                if neighbor not in visited:
                    queue.append(neighbor)

    def dfs(
        self,
        start: str,
        pre_visit: Optional[Callable[[str], None]] = None,
        post_visit: Optional[Callable[[str], None]] = None,
    ) -> Iterator[str]:
        """Depth-first traversal with pre/post visit hooks."""
        if start not in self._nodes:
            raise ValueError(f"Start node {start} not found")

        visited: set[str] = set()
        stack: list[str] = [start]

        while stack:
            node_id = stack.pop()
            if node_id in visited:
                continue
            visited.add(node_id)

            if pre_visit:
                pre_visit(node_id)
            yield node_id
            if post_visit:
                post_visit(node_id)

            for neighbor, _ in reversed(self._adjacency.get(node_id, [])):
                if neighbor not in visited:
                    stack.append(neighbor)

    def find_shortest_path(
        self,
        start: str,
        end: str,
        weight_fn: Optional[Callable[[str, str], float]] = None,
    ) -> Optional[list[str]]:
        """Find shortest path using Dijkstra's algorithm."""
        if start not in self._nodes or end not in self._nodes:
            return None

        distances: dict[str, float] = {start: 0.0}
        previous: dict[str, Optional[str]] = {start: None}
        priority_queue: list[tuple[float, str]] = [(0.0, start)]
        visited: set[str] = set()

        while priority_queue:
            current_dist, current = heapq.heappop(priority_queue)

            if current in visited:
                continue
            visited.add(current)

            if current == end:
                break

            for neighbor, default_weight in self._adjacency.get(current, []):
                weight = weight_fn(current, neighbor) if weight_fn else default_weight
                distance = current_dist + weight

                if neighbor not in distances or distance < distances[neighbor]:
                    distances[neighbor] = distance
                    previous[neighbor] = current
                    heapq.heappush(priority_queue, (distance, neighbor))

        if end not in previous or previous[end] is None:
            return None

        path: list[str] = []
        current: Optional[str] = end
        while current is not None:
            path.append(current)
            current = previous[current]
        return list(reversed(path))

    def find_all_paths(
        self,
        start: str,
        end: str,
        max_depth: int = 100,
    ) -> list[list[str]]:
        """Find all paths between two nodes."""
        if start not in self._nodes or end not in self._nodes:
            return []

        paths: list[list[str]] = []

        def dfs_recursive(
            current: str,
            target: str,
            path: list[str],
            visited: set[str],
            depth: int,
        ) -> None:
            if depth > max_depth:
                return
            if current == target:
                paths.append(list(path))
                return

            for neighbor, _ in self._adjacency.get(current, []):
                if neighbor not in visited:
                    visited.add(neighbor)
                    path.append(neighbor)
                    dfs_recursive(neighbor, target, path, visited, depth + 1)
                    path.pop()
                    visited.remove(neighbor)

        visited = {start}
        dfs_recursive(start, end, [start], visited, 0)
        return paths

    def topological_sort(self) -> list[str]:
        """Return nodes in topological order (for DAGs)."""
        if self.graph_type not in (
            GraphType.DIRECTED,
            GraphType.WEIGHTED_DIRECTED,
        ):
            raise ValueError("Topological sort requires directed graph")

        in_degree: dict[str, int] = defaultdict(int)
        for node_id in self._nodes:
            in_degree[node_id] = 0

        for source, target in self._edges:
            in_degree[target] += 1

        queue: deque[str] = deque(
            [node_id for node_id, degree in in_degree.items() if degree == 0]
        )
        result: list[str] = []

        while queue:
            node_id = queue.popleft()
            result.append(node_id)

            for neighbor, _ in self._adjacency.get(node_id, []):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(result) != len(self._nodes):
            raise ValueError("Graph contains a cycle - topological sort not possible")

        return result

    def find_connected_components(self) -> list[set[str]]:
        """Find all connected components."""
        visited: set[str] = set()
        components: list[set[str]] = []

        for node_id in self._nodes:
            if node_id in visited:
                continue

            component: set[str] = set()
            queue: deque[str] = deque([node_id])

            while queue:
                current = queue.popleft()
                if current in visited:
                    continue
                visited.add(current)
                component.add(current)

                for neighbor, _ in self._adjacency.get(current, []):
                    if neighbor not in visited:
                        queue.append(neighbor)
                    if self.graph_type in (
                        GraphType.UNDIRECTED,
                        GraphType.WEIGHTED_UNDIRECTED,
                    ):
                        for rev_neighbor, _ in self._adjacency.get(neighbor, []):
                            if rev_neighbor not in visited:
                                queue.append(rev_neighbor)

            components.append(component)

        return components

    def __repr__(self) -> str:
        return (
            f"Graph(nodes={self.node_count()}, edges={self.edge_count()}, "
            f"type={self.graph_type.name})"
        )


class GraphBuilder:
    """Builder for creating graphs from various data formats."""

    @staticmethod
    def from_adjacency_list(
        adjacency: dict[str, list[str]],
        graph_type: GraphType = GraphType.DIRECTED,
    ) -> Graph:
        """Create graph from adjacency list."""
        graph = Graph(graph_type=graph_type)
        for node_id in adjacency:
            if not graph.has_node(node_id):
                graph.add_node(node_id)
            for neighbor in adjacency[node_id]:
                if not graph.has_node(neighbor):
                    graph.add_node(neighbor)
                graph.add_edge(node_id, neighbor)
        return graph

    @staticmethod
    def from_edge_list(
        edges: list[tuple[str, str]],
        nodes: Optional[list[str]] = None,
        graph_type: GraphType = GraphType.DIRECTED,
        weighted: bool = False,
        weights: Optional[dict[tuple[str, str], float]] = None,
    ) -> Graph:
        """Create graph from edge list."""
        graph = Graph(graph_type=graph_type)
        if nodes:
            for node_id in nodes:
                graph.add_node(node_id)
        for source, target in edges:
            if not graph.has_node(source):
                graph.add_node(source)
            if not graph.has_node(target):
                graph.add_node(target)
            weight = weights.get((source, target), 1.0) if weighted else 1.0
            graph.add_edge(source, target, weight=weight)
        return graph
