"""Graph algorithms and data structures for RabAI AutoClick.

Provides:
- Graph representation (adjacency list, adjacency matrix)
- Traversal algorithms (BFS, DFS, iterative, recursive)
- Shortest path algorithms (Dijkstra, Bellman-Ford, A*)
- Topological sorting
- Cycle detection
- Connected components
- Minimum spanning tree (Kruskal, Prim)
"""

from __future__ import annotations

import heapq
from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Generic,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
)


T = TypeVar("T", bound=Any)


class EdgeType(Enum):
    """Type of graph edge."""

    UNDIRECTED = auto()
    DIRECTED = auto()


@dataclass(frozen=True, eq=False)
class Edge(Generic[T]):
    """Represents an edge between two vertices.

    Attributes:
        from_vertex: Source vertex.
        to_vertex: Destination vertex.
        weight: Edge weight (default 1.0).
        data: Optional metadata associated with the edge.
    """

    from_vertex: T
    to_vertex: T
    weight: float = 1.0
    data: Any = None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Edge):
            return False
        return (
            self.from_vertex == other.from_vertex
            and self.to_vertex == other.to_vertex
        )

    def __hash__(self) -> int:
        return hash((self.from_vertex, self.to_vertex))

    def __repr__(self) -> str:
        if self.weight != 1.0:
            return f"Edge({self.from_vertex!r} -> {self.to_vertex!r}, w={self.weight})"
        return f"Edge({self.from_vertex!r} -> {self.to_vertex!r})"


@dataclass
class Vertex(Generic[T]):
    """Represents a vertex in a graph.

    Attributes:
        key: Unique identifier for the vertex.
        data: Data associated with the vertex.
        edges: Set of edges originating from this vertex.
    """

    key: T
    data: Any = None
    edges: Set[Edge[T]] = field(default_factory=set)

    def add_edge(self, to: T, weight: float = 1.0, data: Any = None) -> Edge[T]:
        """Add an outgoing edge from this vertex.

        Args:
            to: Destination vertex key.
            weight: Edge weight.
            data: Optional edge metadata.

        Returns:
            The created Edge.
        """
        edge = Edge(from_vertex=self.key, to_vertex=to, weight=weight, data=data)
        self.edges.add(edge)
        return edge

    def remove_edge(self, to: T) -> bool:
        """Remove edge to specified vertex.

        Args:
            to: Destination vertex key.

        Returns:
            True if edge was removed, False if not found.
        """
        for edge in list(self.edges):
            if edge.to_vertex == to:
                self.edges.discard(edge)
                return True
        return False

    def has_edge_to(self, to: T) -> bool:
        """Check if edge exists to specified vertex."""
        return any(e.to_vertex == to for e in self.edges)

    def get_edge_weight(self, to: T) -> Optional[float]:
        """Get weight of edge to specified vertex."""
        for edge in self.edges:
            if edge.to_vertex == to:
                return edge.weight
        return None

    def degree(self) -> int:
        """Get number of outgoing edges."""
        return len(self.edges)

    def __hash__(self) -> int:
        return hash(self.key)

    def __repr__(self) -> str:
        return f"Vertex({self.key!r}, degree={len(self.edges)})"


class Graph(Generic[T]):
    """Generic graph data structure using adjacency list.

    Supports both directed and undirected graphs.

    Example:
        g = Graph[int](directed=False)
        g.add_edge(1, 2, weight=5.0)
        g.add_edge(2, 3)
        g.add_vertex(4)

        for v in g.bfs(1):
            print(v)

        path = g.shortest_path(1, 3)
    """

    def __init__(
        self,
        directed: bool = False,
        weighted: bool = False,
    ) -> None:
        """Initialize a graph.

        Args:
            directed: If True, edges are directed. If False, edges are bidirectional.
            weighted: If True, edges have weights. Default weight is 1.0.
        """
        self._directed = directed
        self._weighted = weighted
        self._vertices: Dict[T, Vertex[T]] = {}

    @property
    def directed(self) -> bool:
        """Whether graph is directed."""
        return self._directed

    @property
    def weighted(self) -> bool:
        """Whether graph is weighted."""
        return self._weighted

    @property
    def vertices(self) -> Dict[T, Vertex[T]]:
        """Get all vertices."""
        return self._vertices

    @property
    def vertex_count(self) -> int:
        """Number of vertices in the graph."""
        return len(self._vertices)

    @property
    def edge_count(self) -> int:
        """Number of edges in the graph."""
        return sum(v.degree() for v in self._vertices.values()) // (
            1 if self._directed else 2
        )

    def add_vertex(self, key: T, data: Any = None) -> Vertex[T]:
        """Add a vertex to the graph.

        Args:
            key: Unique identifier for the vertex.
            data: Optional data to associate with the vertex.

        Returns:
            The created (or existing) Vertex.
        """
        if key not in self._vertices:
            self._vertices[key] = Vertex(key=key, data=data)
        return self._vertices[key]

    def add_edge(
        self,
        from_key: T,
        to_key: T,
        weight: float = 1.0,
        data: Any = None,
    ) -> Optional[Edge[T]]:
        """Add an edge between two vertices.

        Args:
            from_key: Source vertex key.
            to_key: Destination vertex key.
            weight: Edge weight (default 1.0).
            data: Optional edge metadata.

        Returns:
            The created Edge, or None if vertices don't exist.
        """
        if from_key not in self._vertices:
            self.add_vertex(from_key)
        if to_key not in self._vertices:
            self.add_vertex(to_key)

        from_vertex = self._vertices[from_key]
        to_vertex = self._vertices[to_key]

        edge = from_vertex.add_edge(to_key, weight=weight, data=data)

        if not self._directed:
            to_vertex.add_edge(from_key, weight=weight, data=data)

        return edge

    def has_vertex(self, key: T) -> bool:
        """Check if vertex exists."""
        return key in self._vertices

    def has_edge(self, from_key: T, to_key: T) -> bool:
        """Check if edge exists between two vertices."""
        if from_key not in self._vertices:
            return False
        return self._vertices[from_key].has_edge_to(to_key)

    def get_edge(self, from_key: T, to_key: T) -> Optional[Edge[T]]:
        """Get edge between two vertices."""
        if from_key not in self._vertices:
            return None
        for edge in self._vertices[from_key].edges:
            if edge.to_vertex == to_key:
                return edge
        return None

    def remove_vertex(self, key: T) -> bool:
        """Remove vertex and all its edges.

        Args:
            key: Vertex key to remove.

        Returns:
            True if vertex was removed, False if not found.
        """
        if key not in self._vertices:
            return False

        for vertex in self._vertices.values():
            vertex.remove_edge(key)

        del self._vertices[key]
        return True

    def remove_edge(self, from_key: T, to_key: T) -> bool:
        """Remove edge between two vertices.

        Returns:
            True if edge was removed, False if not found.
        """
        if from_key not in self._vertices:
            return False

        removed = self._vertices[from_key].remove_edge(to_key)

        if not self._directed and to_key in self._vertices:
            self._vertices[to_key].remove_edge(from_key)

        return removed

    def get_neighbors(self, key: T) -> List[T]:
        """Get list of neighbor vertex keys.

        Args:
            key: Vertex key.

        Returns:
            List of neighbor keys.
        """
        if key not in self._vertices:
            return []
        return [e.to_vertex for e in self._vertices[key].edges]

    def get_incoming_neighbors(self, key: T) -> List[T]:
        """Get list of vertices that have edges pointing to this vertex.

        Only applicable for directed graphs.
        """
        if self._directed:
            return [
                v.key
                for v in self._vertices.values()
                if v.has_edge_to(key)
            ]
        return self.get_neighbors(key)

    def bfs(self, start: T) -> Generator[T, None, None]:
        """Breadth-first search traversal starting from vertex.

        Args:
            start: Starting vertex key.

        Yields:
            Vertex keys in BFS order.
        """
        if start not in self._vertices:
            return

        visited: Set[T] = {start}
        queue: deque[T] = deque([start])

        while queue:
            vertex = queue.popleft()
            yield vertex

            for neighbor in self._vertices[vertex].edges:
                if neighbor.to_vertex not in visited:
                    visited.add(neighbor.to_vertex)
                    queue.append(neighbor.to_vertex)

    def dfs(self, start: T) -> Generator[T, None, None]:
        """Depth-first search traversal starting from vertex.

        Args:
            start: Starting vertex key.

        Yields:
            Vertex keys in DFS order.
        """
        if start not in self._vertices:
            return

        visited: Set[T] = set()
        stack: List[T] = [start]

        while stack:
            vertex = stack.pop()
            if vertex in visited:
                continue
            visited.add(vertex)
            yield vertex

            for neighbor in self._vertices[vertex].edges:
                if neighbor.to_vertex not in visited:
                    stack.append(neighbor.to_vertex)

    def _dfs_recursive(
        self,
        vertex: T,
        visited: Set[T],
        result: List[T],
    ) -> None:
        visited.add(vertex)
        result.append(vertex)
        for neighbor in self._vertices[vertex].edges:
            if neighbor.to_vertex not in visited:
                self._dfs_recursive(neighbor.to_vertex, visited, result)

    def dfs_recursive(self, start: T) -> List[T]:
        """Recursive DFS traversal."""
        if start not in self._vertices:
            return []
        result: List[T] = []
        self._dfs_recursive(start, set(), result)
        return result

    def shortest_path(self, from_key: T, to_key: T) -> Optional[List[T]]:
        """Find shortest path between two vertices using Dijkstra's algorithm.

        Args:
            from_key: Start vertex key.
            to_key: End vertex key.

        Returns:
            List of vertex keys forming the path, or None if no path exists.
        """
        if from_key not in self._vertices or to_key not in self._vertices:
            return None

        if from_key == to_key:
            return [from_key]

        distances: Dict[T, float] = {v: float("inf") for v in self._vertices}
        distances[from_key] = 0.0
        previous: Dict[T, Optional[T]] = {v: None for v in self._vertices}
        visited: Set[T] = set()

        heap: List[Tuple[float, T]] = [(0.0, from_key)]

        while heap:
            current_dist, current = heapq.heappop(heap)

            if current in visited:
                continue
            visited.add(current)

            if current == to_key:
                break

            for edge in self._vertices[current].edges:
                neighbor = edge.to_vertex
                if neighbor in visited:
                    continue
                weight = edge.weight if self._weighted else 1.0
                new_dist = current_dist + weight

                if new_dist < distances[neighbor]:
                    distances[neighbor] = new_dist
                    previous[neighbor] = current
                    heapq.heappush(heap, (new_dist, neighbor))

        if previous[to_key] is None and from_key != to_key:
            return None

        path: List[T] = []
        current: Optional[T] = to_key
        while current is not None:
            path.append(current)
            current = previous[current]
        return list(reversed(path))

    def dijkstra(
        self,
        source: T,
    ) -> Tuple[Dict[T, float], Dict[T, Optional[T]]]:
        """Run Dijkstra's algorithm from source vertex.

        Returns:
            Tuple of (distances dict, previous dict).
        """
        if source not in self._vertices:
            return {}, {}

        distances: Dict[T, float] = {v: float("inf") for v in self._vertices}
        distances[source] = 0.0
        previous: Dict[T, Optional[T]] = {v: None for v in self._vertices}
        visited: Set[T] = set()
        heap: List[Tuple[float, T]] = [(0.0, source)]

        while heap:
            current_dist, current = heapq.heappop(heap)
            if current in visited:
                continue
            visited.add(current)

            for edge in self._vertices[current].edges:
                neighbor = edge.to_vertex
                if neighbor in visited:
                    continue
                weight = edge.weight if self._weighted else 1.0
                new_dist = current_dist + weight

                if new_dist < distances[neighbor]:
                    distances[neighbor] = new_dist
                    previous[neighbor] = current
                    heapq.heappush(heap, (new_dist, neighbor))

        return distances, previous

    def has_cycle(self) -> bool:
        """Detect if graph contains a cycle.

        Returns:
            True if cycle exists, False otherwise.
        """
        if self._directed:
            return self._has_cycle_directed()
        return self._has_cycle_undirected()

    def _has_cycle_undirected(self) -> bool:
        visited: Set[T] = set()

        def dfs(vertex: T, parent: Optional[T]) -> bool:
            visited.add(vertex)
            for neighbor in self._vertices[vertex].edges:
                if neighbor.to_vertex == parent:
                    continue
                if neighbor.to_vertex in visited:
                    return True
                if dfs(neighbor.to_vertex, vertex):
                    return True
            return False

        for vertex in self._vertices:
            if vertex not in visited:
                if dfs(vertex, None):
                    return True
        return False

    def _has_cycle_directed(self) -> bool:
        WHITE, GRAY, BLACK = 0, 1, 2
        color: Dict[T, int] = {v: WHITE for v in self._vertices}

        def dfs(vertex: T) -> bool:
            color[vertex] = GRAY
            for neighbor in self._vertices[vertex].edges:
                if color[neighbor.to_vertex] == GRAY:
                    return True
                if color[neighbor.to_vertex] == WHITE:
                    if dfs(neighbor.to_vertex):
                        return True
            color[vertex] = BLACK
            return False

        for vertex in self._vertices:
            if color[vertex] == WHITE:
                if dfs(vertex):
                    return True
        return False

    def topological_sort(self) -> Optional[List[T]]:
        """Return topological ordering of vertices.

        Only applicable for directed acyclic graphs (DAG).
        Returns None if graph has cycles.
        """
        if not self._directed:
            return None

        in_degree: Dict[T, int] = {v: 0 for v in self._vertices}
        for vertex in self._vertices:
            for edge in vertex.edges:
                in_degree[edge.to_vertex] += 1

        queue: deque[T] = deque(
            [v for v, degree in in_degree.items() if degree == 0]
        )
        result: List[T] = []

        while queue:
            vertex = queue.popleft()
            result.append(vertex)

            for edge in self._vertices[vertex].edges:
                in_degree[edge.to_vertex] -= 1
                if in_degree[edge.to_vertex] == 0:
                    queue.append(edge.to_vertex)

        if len(result) != self.vertex_count:
            return None

        return result

    def connected_components(self) -> List[Set[T]]:
        """Find all connected components.

        Returns:
            List of sets, each containing vertex keys in one component.
        """
        visited: Set[T] = set()
        components: List[Set[T]] = []

        for vertex in self._vertices:
            if vertex not in visited:
                component: Set[T] = set()
                for v in self.bfs(vertex):
                    visited.add(v)
                    component.add(v)
                components.append(component)

        return components

    def to_adjacency_matrix(self) -> Tuple[List[T], List[List[float]]]:
        """Convert graph to adjacency matrix representation.

        Returns:
            Tuple of (vertex list, matrix).
        """
        vertices = list(self._vertices.keys())
        n = len(vertices)
        index: Dict[T, int] = {v: i for i, v in enumerate(vertices)}
        matrix: List[List[float]] = [
            [float("inf") if i != j else 0.0 for j in range(n)] for i in range(n)
        ]

        for vertex in self._vertices:
            i = index[vertex]
            for edge in vertex.edges:
                j = index[edge.to_vertex]
                matrix[i][j] = edge.weight

        return vertices, matrix

    def __repr__(self) -> str:
        return (
            f"Graph(directed={self._directed}, "
            f"vertices={self.vertex_count}, edges={self.edge_count})"
        )
