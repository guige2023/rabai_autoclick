"""
Graph data structure module.

Provides graph implementation with various traversal algorithms,
path finding, and graph analysis utilities.

Author: Aito Auto Agent
"""

from __future__ import annotations

import heapq
import threading
from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Callable,
    Generic,
    Hashable,
    Iterator,
    Optional,
    TypeVar,
)


T = TypeVar('T', bound=Hashable)


class EdgeType(Enum):
    """Graph edge type."""
    UNDIRECTED = auto()
    DIRECTED = auto()


@dataclass
class Edge(Generic[T]):
    """Graph edge representation."""
    source: T
    target: T
    weight: float = 1.0
    metadata: dict = field(default_factory=dict)


@dataclass
class Vertex(Generic[T]):
    """Graph vertex with associated data."""
    key: T
    data: Optional[any] = None
    metadata: dict = field(default_factory=dict)
    edges: list[Edge[T]] = field(default_factory=list)


class Graph(Generic[T]):
    """
    Generic graph data structure.

    Supports directed and undirected graphs with weighted edges.

    Example:
        graph = Graph[int](directed=False)
        graph.add_vertex(1)
        graph.add_vertex(2)
        graph.add_edge(1, 2, weight=5.0)

        for path in graph.bfs(1):
            print(path)
    """

    def __init__(
        self,
        directed: bool = False,
        weighted: bool = False,
        thread_safe: bool = False
    ):
        self._directed = directed
        self._weighted = weighted
        self._vertices: dict[T, Vertex[T]] = {}
        self._lock = threading.RLock() if thread_safe else None

    def add_vertex(self, key: T, data: Optional[any] = None) -> Vertex[T]:
        """Add a vertex to the graph."""
        with self._lock:
            if key not in self._vertices:
                self._vertices[key] = Vertex(key=key, data=data)
            return self._vertices[key]

    def add_edge(
        self,
        source: T,
        target: T,
        weight: float = 1.0,
        **metadata
    ) -> Edge[T]:
        """Add an edge between two vertices."""
        with self._lock:
            if source not in self._vertices:
                self.add_vertex(source)
            if target not in self._vertices:
                self.add_vertex(target)

            edge = Edge(source=source, target=target, weight=weight, metadata=metadata)
            self._vertices[source].edges.append(edge)

            if not self._directed:
                reverse_edge = Edge(source=target, target=source, weight=weight, metadata=metadata)
                self._vertices[target].edges.append(reverse_edge)

            return edge

    def get_vertex(self, key: T) -> Optional[Vertex[T]]:
        """Get vertex by key."""
        return self._vertices.get(key)

    def get_neighbors(self, key: T) -> list[T]:
        """Get all neighbors of a vertex."""
        vertex = self._vertices.get(key)
        if not vertex:
            return []
        return [edge.target for edge in vertex.edges]

    def get_edges(self, key: T) -> list[Edge[T]]:
        """Get all edges from a vertex."""
        vertex = self._vertices.get(key)
        if not vertex:
            return []
        return vertex.edges.copy()

    def has_vertex(self, key: T) -> bool:
        """Check if vertex exists."""
        return key in self._vertices

    def has_edge(self, source: T, target: T) -> bool:
        """Check if edge exists."""
        return target in self.get_neighbors(source)

    def remove_vertex(self, key: T) -> bool:
        """Remove vertex and all its edges."""
        with self._lock:
            if key not in self._vertices:
                return False

            del self._vertices[key]

            for vertex in self._vertices.values():
                vertex.edges = [
                    e for e in vertex.edges
                    if e.target != key and e.source != key
                ]

            return True

    def remove_edge(self, source: T, target: T) -> bool:
        """Remove edge between vertices."""
        with self._lock:
            if source not in self._vertices:
                return False

            original_len = len(self._vertices[source].edges)
            self._vertices[source].edges = [
                e for e in self._vertices[source].edges
                if e.target != target
            ]

            if not self._directed and target in self._vertices:
                self._vertices[target].edges = [
                    e for e in self._vertices[target].edges
                    if e.target != source
                ]

            return len(self._vertices[source].edges) < original_len

    def bfs(self, start: T) -> Iterator[list[T]]:
        """
        Breadth-first search generator.

        Yields paths from start to each reachable vertex.
        """
        if start not in self._vertices:
            return

        visited = {start}
        queue = deque([(start, [start])])

        while queue:
            current, path = queue.popleft()
            yield path

            for neighbor in self.get_neighbors(current):
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, path + [neighbor]))

    def dfs(self, start: T) -> Iterator[list[T]]:
        """
        Depth-first search generator.

        Yields paths from start to each reachable vertex.
        """
        if start not in self._vertices:
            return

        visited = set()

        def dfs_recursive(current: T, path: list[T]):
            visited.add(current)
            yield path

            for neighbor in self.get_neighbors(current):
                if neighbor not in visited:
                    yield from dfs_recursive(neighbor, path + [neighbor])

        yield from dfs_recursive(start, [start])

    def dijkstra(
        self,
        start: T,
        end: Optional[T] = None
    ) -> tuple[dict[T, float], dict[T, Optional[T]]]:
        """
        Dijkstra's shortest path algorithm.

        Args:
            start: Starting vertex
            end: Optional target vertex

        Returns:
            Tuple of (distances, predecessors)
        """
        if start not in self._vertices:
            return {}, {}

        distances: dict[T, float] = {v: float('inf') for v in self._vertices}
        distances[start] = 0
        predecessors: dict[T, Optional[T]] = {v: None for v in self._vertices}

        pq = [(0, start)]
        visited = set()

        while pq:
            current_dist, current = heapq.heappop(pq)

            if current in visited:
                continue

            visited.add(current)

            if current == end:
                break

            for edge in self._vertices[current].edges:
                neighbor = edge.target
                weight = edge.weight if self._weighted else 1.0
                new_dist = current_dist + weight

                if new_dist < distances[neighbor]:
                    distances[neighbor] = new_dist
                    predecessors[neighbor] = current
                    heapq.heappush(pq, (new_dist, neighbor))

        return distances, predecessors

    def shortest_path(self, start: T, end: T) -> Optional[list[T]]:
        """Get shortest path between two vertices."""
        distances, predecessors = self.dijkstra(start, end)

        if distances.get(end, float('inf')) == float('inf'):
            return None

        path = []
        current = end

        while current is not None:
            path.append(current)
            current = predecessors.get(current)

        return list(reversed(path))

    def all_paths(self, start: T, end: T) -> list[list[T]]:
        """Find all paths between two vertices."""
        paths = []

        def dfs_paths(current: T, path: list[T]):
            if current == end:
                paths.append(path.copy())
                return

            for neighbor in self.get_neighbors(current):
                if neighbor not in path:
                    path.append(neighbor)
                    dfs_paths(neighbor, path)
                    path.pop()

        dfs_paths(start, [start])
        return paths

    def has_cycle(self) -> bool:
        """Check if graph contains a cycle."""
        visited = set()
        rec_stack = set()

        def dfs(vertex: T) -> bool:
            visited.add(vertex)
            rec_stack.add(vertex)

            for neighbor in self.get_neighbors(vertex):
                if neighbor not in visited:
                    if dfs(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True

            rec_stack.remove(vertex)
            return False

        for vertex in self._vertices:
            if vertex not in visited:
                if dfs(vertex):
                    return True

        return False

    def topological_sort(self) -> Optional[list[T]]:
        """Return topologically sorted vertices for DAG."""
        if not self._directed:
            return None

        if self.has_cycle():
            return None

        in_degree = {v: 0 for v in self._vertices}
        for vertex in self._vertices:
            for neighbor in self.get_neighbors(vertex):
                in_degree[neighbor] += 1

        queue = deque([v for v, degree in in_degree.items() if degree == 0])
        result = []

        while queue:
            vertex = queue.popleft()
            result.append(vertex)

            for neighbor in self.get_neighbors(vertex):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        return result if len(result) == len(self._vertices) else None

    def connected_components(self) -> list[set[T]]:
        """Find all connected components."""
        visited = set()
        components = []

        def bfs(start: T) -> set[T]:
            component = set()
            queue = deque([start])

            while queue:
                vertex = queue.popleft()
                if vertex in visited:
                    continue
                visited.add(vertex)
                component.add(vertex)

                for neighbor in self.get_neighbors(vertex):
                    if neighbor not in visited:
                        queue.append(neighbor)

            return component

        for vertex in self._vertices:
            if vertex not in visited:
                components.append(bfs(vertex))

        return components

    def __len__(self) -> int:
        """Return number of vertices."""
        return len(self._vertices)

    def __contains__(self, key: T) -> bool:
        """Check if vertex exists."""
        return key in self._vertices


def create_graph(
    directed: bool = False,
    weighted: bool = False
) -> Graph:
    """Factory to create a Graph."""
    return Graph(directed=directed, weighted=weighted)
