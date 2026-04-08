"""
Graph traversal algorithms implementation.

Provides BFS, DFS, Dijkstra, and A* algorithms
for graph traversal and pathfinding.
"""

from __future__ import annotations

import heapq
from collections import deque
from typing import Callable


Node = str | int
Cost = float


class Graph:
    """Simple weighted graph representation."""

    def __init__(self) -> None:
        self._adj: dict[Node, list[tuple[Node, Cost]]] = {}

    def add_node(self, node: Node) -> None:
        if node not in self._adj:
            self._adj[node] = []

    def add_edge(
        self,
        from_node: Node,
        to_node: Node,
        cost: Cost = 1.0,
        bidirectional: bool = False,
    ) -> None:
        self.add_node(from_node)
        self.add_node(to_node)
        self._adj[from_node].append((to_node, cost))
        if bidirectional:
            self._adj[to_node].append((from_node, cost))

    def neighbors(self, node: Node) -> list[tuple[Node, Cost]]:
        return self._adj.get(node, [])

    def nodes(self) -> list[Node]:
        return list(self._adj.keys())


def bfs(
    graph: Graph,
    start: Node,
    goal: Node | None = None,
    visit: Callable[[Node], bool] | None = None,
) -> list[Node]:
    """
    Breadth-first search.

    Args:
        graph: Graph to traverse
        start: Starting node
        goal: Optional target node (stops when found)
        visit: Optional early exit predicate

    Returns:
        Path from start to goal (or all reachable if no goal)
    """
    visited = {start}
    queue = deque([(start, [start])])

    while queue:
        node, path = queue.popleft()
        if visit and visit(node):
            return path
        if goal and node == goal:
            return path
        for neighbor, _ in graph.neighbors(node):
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append((neighbor, path + [neighbor]))
    return []


def dfs(
    graph: Graph,
    start: Node,
    goal: Node | None = None,
    visit: Callable[[Node], bool] | None = None,
) -> list[Node]:
    """
    Depth-first search.

    Args:
        graph: Graph to traverse
        start: Starting node
        goal: Optional target node
        visit: Optional early exit predicate

    Returns:
        Path from start to goal
    """
    visited: set[Node] = set()

    def _dfs_rec(node: Node, path: list[Node]) -> list[Node] | None:
        if visit and visit(node):
            return path
        if goal and node == goal:
            return path
        visited.add(node)
        for neighbor, _ in graph.neighbors(node):
            if neighbor not in visited:
                result = _dfs_rec(neighbor, path + [neighbor])
                if result is not None:
                    return result
        return None

    result = _dfs_rec(start, [start])
    return result if result else []


def dijkstra(
    graph: Graph,
    start: Node,
    goal: Node | None = None,
) -> tuple[dict[Node, Cost], dict[Node, Node | None]]:
    """
    Dijkstra's shortest path algorithm.

    Args:
        graph: Weighted graph
        start: Starting node
        goal: Optional target node

    Returns:
        Tuple of (distances dict, predecessors dict)
    """
    distances: dict[Node, Cost] = {n: float("inf") for n in graph.nodes()}
    predecessors: dict[Node, Node | None] = {n: None for n in graph.nodes()}
    distances[start] = 0.0

    pq = [(0.0, start)]
    while pq:
        current_dist, node = heapq.heappop(pq)
        if current_dist > distances[node]:
            continue
        if goal and node == goal:
            break
        for neighbor, cost in graph.neighbors(node):
            new_dist = current_dist + cost
            if new_dist < distances[neighbor]:
                distances[neighbor] = new_dist
                predecessors[neighbor] = node
                heapq.heappush(pq, (new_dist, neighbor))

    return distances, predecessors


def reconstruct_path(
    predecessors: dict[Node, Node | None],
    start: Node,
    goal: Node,
) -> list[Node]:
    """Reconstruct path from predecessors dict."""
    path = [goal]
    current = goal
    while predecessors.get(current) is not None:
        current = predecessors[current]  # type: ignore
        path.append(current)
    path.reverse()
    return path if path[0] == start else []


def astar(
    graph: Graph,
    start: Node,
    goal: Node,
    heuristic: Callable[[Node], Cost] | None = None,
) -> list[Node]:
    """
    A* pathfinding algorithm.

    Args:
        graph: Weighted graph
        start: Starting node
        goal: Target node
        heuristic: Heuristic function (node -> estimated cost to goal)

    Returns:
        Path from start to goal, or empty list if no path
    """
    if heuristic is None:
        heuristic = lambda _: 0.0

    g_score: dict[Node, Cost] = {n: float("inf") for n in graph.nodes()}
    f_score: dict[Node, Cost] = {n: float("inf") for n in graph.nodes()}
    predecessors: dict[Node, Node | None] = {n: None for n in graph.nodes()}

    g_score[start] = 0.0
    f_score[start] = heuristic(start)

    open_set = [(f_score[start], start)]
    open_set_nodes = {start}

    while open_set:
        _, current = heapq.heappop(open_set)
        open_set_nodes.discard(current)

        if current == goal:
            return reconstruct_path(predecessors, start, goal)

        for neighbor, cost in graph.neighbors(current):
            tentative_g = g_score[current] + cost
            if tentative_g < g_score[neighbor]:
                predecessors[neighbor] = current
                g_score[neighbor] = tentative_g
                f = tentative_g + heuristic(neighbor)
                f_score[neighbor] = f
                if neighbor not in open_set_nodes:
                    heapq.heappush(open_set, (f, neighbor))
                    open_set_nodes.add(neighbor)

    return []


def topological_sort(graph: Graph) -> list[Node]:
    """
    Topological sort of a DAG.

    Args:
        graph: Directed acyclic graph

    Returns:
        Nodes in topological order
    """
    visited: set[Node] = set()
    result: list[Node] = []

    def _dfs(node: Node) -> None:
        if node in visited:
            return
        visited.add(node)
        for neighbor, _ in graph.neighbors(node):
            _dfs(neighbor)
        result.append(node)

    for node in graph.nodes():
        _dfs(node)

    result.reverse()
    return result
