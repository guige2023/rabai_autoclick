"""Graph traversal utilities for RabAI AutoClick.

Provides:
- BFS/DFS graph traversal
- Dijkstra and A* pathfinding
- Topological sorting
- Connected components
"""

from typing import List, Set, Optional, Tuple, Callable, Dict, Any
from dataclasses import dataclass
import heapq
import math


@dataclass
class GraphNode:
    """A node in a graph."""
    id: Any
    data: Any = None


class Graph:
    """Simple weighted graph."""

    def __init__(self):
        """Initialize empty graph."""
        self.nodes: Dict[Any, GraphNode] = {}
        self.edges: Dict[Any, Dict[Any, float]] = {}

    def add_node(self, id: Any, data: Any = None) -> None:
        """Add a node."""
        self.nodes[id] = GraphNode(id=id, data=data)
        if id not in self.edges:
            self.edges[id] = {}

    def add_edge(self, from_id: Any, to_id: Any, weight: float = 1.0) -> None:
        """Add directed edge."""
        if from_id not in self.nodes:
            self.add_node(from_id)
        if to_id not in self.nodes:
            self.add_node(to_id)
        self.edges[from_id][to_id] = weight

    def add_undirected_edge(self, a: Any, b: Any, weight: float = 1.0) -> None:
        """Add undirected edge."""
        self.add_edge(a, b, weight)
        self.add_edge(b, a, weight)

    def neighbors(self, node_id: Any) -> List[Tuple[Any, float]]:
        """Get (neighbor_id, weight) for all neighbors."""
        return list(self.edges.get(node_id, {}).items())


def bfs(
    graph: Graph,
    start: Any,
    goal: Optional[Any] = None,
    visit: Optional[Callable[[Any], bool]] = None,
) -> List[Any]:
    """Breadth-first search traversal.

    Args:
        graph: Graph to traverse.
        start: Starting node.
        goal: Optional target node.
        visit: Optional function(node) -> bool to continue.

    Returns:
        List of visited nodes.
    """
    visited: Set[Any] = set()
    queue: List[Any] = [start]
    order: List[Any] = []

    while queue:
        node = queue.pop(0)
        if node in visited:
            continue
        visited.add(node)
        order.append(node)
        if goal is not None and node == goal:
            break
        for neighbor, _ in graph.neighbors(node):
            if neighbor not in visited:
                queue.append(neighbor)
        if visit and not visit(node):
            break

    return order


def dfs(
    graph: Graph,
    start: Any,
    goal: Optional[Any] = None,
) -> List[Any]:
    """Depth-first search traversal.

    Args:
        graph: Graph to traverse.
        start: Starting node.
        goal: Optional target node.

    Returns:
        List of visited nodes in DFS order.
    """
    visited: Set[Any] = set()
    order: List[Any] = []

    def dfs_rec(node: Any) -> None:
        if node in visited:
            return
        visited.add(node)
        order.append(node)
        if goal is not None and node == goal:
            return
        for neighbor, _ in graph.neighbors(node):
            dfs_rec(neighbor)

    dfs_rec(start)
    return order


def dijkstra(
    graph: Graph,
    start: Any,
    end: Optional[Any] = None,
) -> Tuple[Dict[Any, float], Dict[Any, Optional[Any]]]:
    """Dijkstra's shortest path algorithm.

    Args:
        graph: Weighted graph.
        start: Start node.
        end: Optional end node.

    Returns:
        (distances, predecessors) where distances[node] = shortest distance
        and predecessors[node] = previous node on path.
    """
    dist: Dict[Any, float] = {n: math.inf for n in graph.nodes}
    prev: Dict[Any, Optional[Any]] = {n: None for n in graph.nodes}
    dist[start] = 0.0
    pq: List[Tuple[float, Any]] = [(0.0, start)]
    visited: Set[Any] = set()

    while pq:
        d, u = heapq.heappop(pq)
        if u in visited:
            continue
        visited.add(u)
        if end is not None and u == end:
            break
        for v, weight in graph.neighbors(u):
            if v in visited:
                continue
            alt = dist[u] + weight
            if alt < dist[v]:
                dist[v] = alt
                prev[v] = u
                heapq.heappush(pq, (alt, v))

    return dist, prev


def reconstruct_path(
    prev: Dict[Any, Optional[Any]],
    start: Any,
    end: Any,
) -> List[Any]:
    """Reconstruct path from predecessors dict."""
    path: List[Any] = []
    current: Optional[Any] = end
    while current is not None:
        path.append(current)
        current = prev[current]
    path.reverse()
    return path if path and path[0] == start else []


def astar(
    graph: Graph,
    start: Any,
    goal: Any,
    heuristic: Callable[[Any], float],
) -> Optional[List[Any]]:
    """A* pathfinding algorithm.

    Args:
        graph: Graph to search.
        start: Start node.
        goal: Goal node.
        heuristic: Function(node) -> estimated distance to goal.

    Returns:
        Path as list of nodes, or None if no path found.
    """
    g_score: Dict[Any, float] = {n: math.inf for n in graph.nodes}
    g_score[start] = 0.0
    f_score: Dict[Any, float] = {n: math.inf for n in graph.nodes}
    f_score[start] = heuristic(start)
    prev: Dict[Any, Optional[Any]] = {n: None for n in graph.nodes}
    open_set: List[Tuple[float, Any]] = [(f_score[start], start)]
    visited: Set[Any] = set()

    while open_set:
        _, current = heapq.heappop(open_set)
        if current == goal:
            return reconstruct_path(prev, start, goal)
        if current in visited:
            continue
        visited.add(current)

        for neighbor, weight in graph.neighbors(current):
            if neighbor in visited:
                continue
            tentative_g = g_score[current] + weight
            if tentative_g < g_score[neighbor]:
                prev[neighbor] = current
                g_score[neighbor] = tentative_g
                f = tentative_g + heuristic(neighbor)
                f_score[neighbor] = f
                heapq.heappush(open_set, (f, neighbor))

    return None


def topological_sort(graph: Graph) -> List[Any]:
    """Topological sort of directed acyclic graph.

    Args:
        graph: DAG to sort.

    Returns:
        Nodes in topological order.
    """
    visited: Set[Any] = set()
    order: List[Any] = []

    def dfs(node: Any) -> None:
        if node in visited:
            return
        visited.add(node)
        for neighbor, _ in graph.neighbors(node):
            dfs(neighbor)
        order.append(node)

    for node in graph.nodes:
        dfs(node)

    order.reverse()
    return order


def connected_components(graph: Graph) -> List[List[Any]]:
    """Find connected components using BFS.

    Args:
        graph: Graph to analyze.

    Returns:
        List of component node lists.
    """
    visited: Set[Any] = set()
    components: List[List[Any]] = []

    for node in graph.nodes:
        if node in visited:
            continue
        component = bfs(graph, node)
        for c in component:
            visited.add(c)
        components.append(component)

    return components


def shortest_path(
    graph: Graph,
    start: Any,
    end: Any,
    algorithm: str = "dijkstra",
) -> Optional[List[Any]]:
    """Find shortest path between two nodes.

    Args:
        graph: Graph to search.
        start: Start node.
        end: End node.
        algorithm: 'dijkstra' or 'astar'.

    Returns:
        Path as list of nodes, or None.
    """
    if algorithm == "astar":
        dist, prev = dijkstra(graph, start, end)
        if dist.get(end, math.inf) == math.inf:
            return None
        return reconstruct_path(prev, start, end)

    dist, prev = dijkstra(graph, start, end)
    if dist.get(end, math.inf) == math.inf:
        return None
    return reconstruct_path(prev, start, end)
