"""
Graph network action for network analysis and graph algorithms.

This module provides actions for graph operations including traversal,
shortest path, centrality measures, and community detection.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import collections
import heapq
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union


@dataclass
class Node:
    """Represents a node in the graph."""
    id: str
    label: Optional[str] = None
    weight: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        if isinstance(other, Node):
            return self.id == other.id
        return False


@dataclass
class Edge:
    """Represents an edge in the graph."""
    source: str
    target: str
    weight: float = 1.0
    directed: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __hash__(self):
        return hash((self.source, self.target))


@dataclass
class Path:
    """Represents a path in the graph."""
    nodes: List[str]
    edges: List[Edge]
    total_weight: float

    def to_list(self) -> List[str]:
        """Get path as list of node IDs."""
        return self.nodes

    def to_dict(self) -> Dict[str, Any]:
        """Convert path to dictionary."""
        return {
            "nodes": self.nodes,
            "total_weight": self.total_weight,
            "length": len(self.nodes),
        }


class GraphType(Enum):
    """Types of graphs."""
    UNDIRECTED = "undirected"
    DIRECTED = "directed"
    WEIGHTED = "weighted"
    MIXED = "mixed"


class Graph:
    """
    Graph data structure with various algorithms.

    Supports directed and undirected graphs with weighted edges.
    """

    def __init__(self, graph_type: GraphType = GraphType.UNDIRECTED):
        """
        Initialize the graph.

        Args:
            graph_type: Type of graph.
        """
        self.graph_type = graph_type
        self._nodes: Dict[str, Node] = {}
        self._edges: Dict[str, Dict[str, Edge]] = collections.defaultdict(dict)
        self._adjacency: Dict[str, Set[str]] = collections.defaultdict(set)
        self._lock = threading.RLock()

    def add_node(
        self,
        node_id: str,
        label: Optional[str] = None,
        weight: float = 1.0,
        **metadata,
    ) -> Node:
        """
        Add a node to the graph.

        Args:
            node_id: Unique identifier for the node.
            label: Optional human-readable label.
            weight: Node weight.
            **metadata: Additional node metadata.

        Returns:
            The created Node.
        """
        with self._lock:
            if node_id in self._nodes:
                node = self._nodes[node_id]
                node.label = label or node.label
                node.weight = weight
                node.metadata.update(metadata)
                return node

            node = Node(id=node_id, label=label, weight=weight, metadata=metadata)
            self._nodes[node_id] = node
            return node

    def add_edge(
        self,
        source: str,
        target: str,
        weight: float = 1.0,
        directed: Optional[bool] = None,
        **metadata,
    ) -> Edge:
        """
        Add an edge to the graph.

        Args:
            source: Source node ID.
            target: Target node ID.
            weight: Edge weight.
            directed: Whether the edge is directed.
            **metadata: Additional edge metadata.

        Returns:
            The created Edge.
        """
        with self._lock:
            if source not in self._nodes:
                self.add_node(source)
            if target not in self._nodes:
                self.add_node(target)

            is_directed = directed if directed is not None else (
                self.graph_type in (GraphType.DIRECTED, GraphType.WEIGHTED)
            )

            edge = Edge(
                source=source,
                target=target,
                weight=weight,
                directed=is_directed,
                metadata=metadata,
            )

            self._edges[source][target] = edge
            self._adjacency[source].add(target)

            if not is_directed:
                reverse_edge = Edge(
                    source=target,
                    target=source,
                    weight=weight,
                    directed=False,
                    metadata=metadata,
                )
                self._edges[target][source] = reverse_edge
                self._adjacency[target].add(source)

            return edge

    def get_node(self, node_id: str) -> Optional[Node]:
        """Get a node by ID."""
        return self._nodes.get(node_id)

    def get_edge(self, source: str, target: str) -> Optional[Edge]:
        """Get an edge between two nodes."""
        return self._edges.get(source, {}).get(target)

    def remove_node(self, node_id: str) -> bool:
        """Remove a node and all its edges."""
        with self._lock:
            if node_id not in self._nodes:
                return False

            del self._nodes[node_id]

            for target in list(self._adjacency.get(node_id, set())):
                self._edges[node_id].pop(target, None)
            del self._adjacency[node_id]

            for source in self._edges:
                if node_id in self._edges[source]:
                    del self._edges[source][node_id]
                self._adjacency[source].discard(node_id)

            return True

    def remove_edge(self, source: str, target: str) -> bool:
        """Remove an edge between two nodes."""
        with self._lock:
            if source in self._edges and target in self._edges[source]:
                del self._edges[source][target]
                self._adjacency[source].discard(target)
                return True
            return False

    def get_neighbors(self, node_id: str) -> Set[str]:
        """Get all neighboring node IDs."""
        return self._adjacency.get(node_id, set()).copy()

    def get_all_nodes(self) -> List[Node]:
        """Get all nodes in the graph."""
        return list(self._nodes.values())

    def get_all_edges(self) -> List[Edge]:
        """Get all edges in the graph."""
        edges = []
        seen = set()
        for source in self._edges:
            for target, edge in self._edges[source].items():
                if edge.directed or (source, target) not in seen:
                    edges.append(edge)
                    seen.add((source, target))
        return edges

    def node_count(self) -> int:
        """Get the number of nodes."""
        return len(self._nodes)

    def edge_count(self) -> int:
        """Get the number of edges."""
        return sum(len(edges) for edges in self._edges.values()) // (
            2 if self.graph_type == GraphType.UNDIRECTED else 1
        )

    def bfs(self, start: str) -> List[str]:
        """
        Breadth-first search from a starting node.

        Args:
            start: Starting node ID.

        Returns:
            List of node IDs in BFS order.
        """
        if start not in self._nodes:
            return []

        visited = set()
        queue = deque([start])
        result = []

        while queue:
            node = queue.popleft()
            if node in visited:
                continue
            visited.add(node)
            result.append(node)
            for neighbor in self._adjacency.get(node, set()):
                if neighbor not in visited:
                    queue.append(neighbor)

        return result

    def dfs(self, start: str) -> List[str]:
        """
        Depth-first search from a starting node.

        Args:
            start: Starting node ID.

        Returns:
            List of node IDs in DFS order.
        """
        if start not in self._nodes:
            return []

        visited = set()
        result = []

        def dfs_recursive(node: str):
            visited.add(node)
            result.append(node)
            for neighbor in self._adjacency.get(node, set()):
                if neighbor not in visited:
                    dfs_recursive(neighbor)

        dfs_recursive(start)
        return result

    def dijkstra(
        self,
        start: str,
        end: Optional[str] = None,
    ) -> Tuple[Optional[Path], Dict[str, float]]:
        """
        Find shortest path using Dijkstra's algorithm.

        Args:
            start: Starting node ID.
            end: Optional ending node ID.

        Returns:
            Tuple of (Path if end specified, else None, distance dict).
        """
        if start not in self._nodes:
            return None, {}

        distances = {start: 0}
        previous: Dict[str, Optional[Tuple[str, Edge]]] = {start: None}
        heap = [(0, start)]
        visited = set()

        while heap:
            current_dist, current = heapq.heappop(heap)

            if current in visited:
                continue
            visited.add(current)

            if end and current == end:
                break

            for neighbor in self._adjacency.get(current, set()):
                edge = self._edges[current][neighbor]
                distance = current_dist + edge.weight

                if neighbor not in distances or distance < distances[neighbor]:
                    distances[neighbor] = distance
                    previous[neighbor] = (current, edge)
                    heapq.heappush(heap, (distance, neighbor))

        if end:
            if end not in previous:
                return None, distances

            path_nodes = []
            path_edges = []
            node = end
            while node is not None:
                path_nodes.append(node)
                if previous[node] and len(path_nodes) > 1:
                    path_edges.append(previous[node][1])
                node = previous[node][0] if previous[node] else None

            path_nodes.reverse()
            path_edges.reverse()

            return Path(
                nodes=path_nodes,
                edges=path_edges,
                total_weight=distances.get(end, 0),
            ), distances

        return None, distances

    def shortest_path(self, start: str, end: str) -> Optional[Path]:
        """Find shortest path between two nodes."""
        path, _ = self.dijkstra(start, end)
        return path

    def find_all_paths(
        self,
        start: str,
        end: str,
        max_depth: int = 100,
    ) -> List[Path]:
        """Find all paths between two nodes."""
        if start not in self._nodes or end not in self._nodes:
            return []

        paths = []

        def dfs_paths(
            current: str,
            visited: Set[str],
            path_nodes: List[str],
            path_edges: List[Edge],
            depth: int,
        ):
            if depth > max_depth:
                return
            if current == end:
                paths.append(Path(
                    nodes=path_nodes.copy(),
                    edges=path_edges.copy(),
                    total_weight=sum(e.weight for e in path_edges),
                ))
                return

            for neighbor in self._adjacency.get(current, set()):
                if neighbor not in visited:
                    visited.add(neighbor)
                    edge = self._edges[current][neighbor]
                    path_nodes.append(neighbor)
                    path_edges.append(edge)
                    dfs_paths(neighbor, visited, path_nodes, path_edges, depth + 1)
                    path_nodes.pop()
                    path_edges.pop()
                    visited.remove(neighbor)

        visited = {start}
        dfs_paths(start, visited, [start], [], 0)

        return sorted(paths, key=lambda p: p.total_weight)

    def degree_centrality(self) -> Dict[str, float]:
        """Calculate degree centrality for all nodes."""
        n = len(self._nodes)
        if n <= 1:
            return {node_id: 0 for node_id in self._nodes}

        centrality = {}
        for node_id in self._nodes:
            degree = len(self._adjacency.get(node_id, set()))
            centrality[node_id] = degree / (n - 1)

        return centrality

    def betweenness_centrality(self) -> Dict[str, float]:
        """Calculate betweenness centrality for all nodes."""
        n = len(self._nodes)
        if n <= 2:
            return {node_id: 0.0 for node_id in self._nodes}

        centrality: Dict[str, float] = {node_id: 0.0 for node_id in self._nodes}

        for source in self._nodes:
            paths, _ = self.dijkstra(source)
            if paths is None:
                continue

            for node_id in self._nodes:
                if node_id != source:
                    count = sum(1 for p in self.find_all_paths(source, node_id))
                    centrality[node_id] += count / (n * (n - 1))

        return centrality

    def page_rank(
        self,
        damping: float = 0.85,
        iterations: int = 100,
        tolerance: float = 1e-6,
    ) -> Dict[str, float]:
        """
        Calculate PageRank for all nodes.

        Args:
            damping: Damping factor (typically 0.85).
            iterations: Maximum iterations.
            tolerance: Convergence tolerance.

        Returns:
            Dictionary mapping node IDs to their PageRank scores.
        """
        n = len(self._nodes)
        if n == 0:
            return {}

        nodes = list(self._nodes.keys())
        ranks = {node: 1.0 / n for node in nodes}

        for _ in range(iterations):
            new_ranks = {}
            diff = 0.0

            for node in nodes:
                rank_sum = 0.0
                for other in nodes:
                    if other == node:
                        continue
                    edge = self.get_edge(other, node)
                    if edge:
                        out_degree = len(self._adjacency.get(other, set()))
                        if out_degree > 0:
                            rank_sum += ranks[other] * edge.weight / out_degree

                new_rank = (1 - damping) / n + damping * rank_sum
                new_ranks[node] = new_rank
                diff += abs(new_rank - ranks[node])

            ranks = new_ranks
            if diff < tolerance:
                break

        return ranks

    def connected_components(self) -> List[Set[str]]:
        """Find all connected components."""
        visited = set()
        components = []

        for node_id in self._nodes:
            if node_id not in visited:
                component = set(self.bfs(node_id))
                visited.update(component)
                components.append(component)

        return components

    def is_connected(self) -> bool:
        """Check if the graph is connected."""
        if not self._nodes:
            return True
        components = self.connected_components()
        return len(components) == 1


def graph_create_action(
    edges: List[Dict[str, Any]],
    nodes: Optional[List[Dict[str, Any]]] = None,
    graph_type: str = "undirected",
) -> Dict[str, Any]:
    """
    Action function to create a graph from edges.

    Args:
        edges: List of edge dictionaries with source, target, weight.
        nodes: Optional list of node dictionaries.
        graph_type: Type of graph (undirected, directed, weighted).

    Returns:
        Dictionary with graph statistics.
    """
    graph_type_map = {
        "undirected": GraphType.UNDIRECTED,
        "directed": GraphType.DIRECTED,
        "weighted": GraphType.WEIGHTED,
    }

    gt = graph_type_map.get(graph_type.lower(), GraphType.UNDIRECTED)
    graph = Graph(gt)

    if nodes:
        for node_data in nodes:
            graph.add_node(
                node_data["id"],
                label=node_data.get("label"),
                weight=node_data.get("weight", 1.0),
            )

    for edge_data in edges:
        graph.add_edge(
            edge_data["source"],
            edge_data["target"],
            weight=edge_data.get("weight", 1.0),
        )

    return {
        "node_count": graph.node_count(),
        "edge_count": graph.edge_count(),
        "is_connected": graph.is_connected(),
        "components": len(graph.connected_components()),
    }


def graph_shortest_path_action(
    edges: List[Dict[str, Any]],
    start: str,
    end: str,
    graph_type: str = "undirected",
) -> Optional[Dict[str, Any]]:
    """Find shortest path between two nodes."""
    result = graph_create_action(edges, graph_type=graph_type)
    if result["edge_count"] == 0 and len(edges) > 0:
        pass

    gt = GraphType.DIRECTED if graph_type == "directed" else GraphType.UNDIRECTED
    graph = Graph(gt)

    for edge_data in edges:
        graph.add_edge(
            edge_data["source"],
            edge_data["target"],
            weight=edge_data.get("weight", 1.0),
        )

    path = graph.shortest_path(start, end)
    if path:
        return path.to_dict()
    return None


def graph_centrality_action(
    edges: List[Dict[str, Any]],
    metric: str = "degree",
) -> Dict[str, float]:
    """Calculate centrality metrics for graph nodes."""
    graph = Graph()
    for edge_data in edges:
        graph.add_edge(
            edge_data["source"],
            edge_data["target"],
            weight=edge_data.get("weight", 1.0),
        )

    if metric == "degree":
        return graph.degree_centrality()
    elif metric == "betweenness":
        return graph.betweenness_centrality()
    elif metric == "pagerank":
        return graph.page_rank()
    else:
        raise ValueError(f"Unknown centrality metric: {metric}")
