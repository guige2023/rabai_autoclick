"""
Graph algorithms and data structures.

Provides adjacency list graphs, BFS, DFS, Dijkstra, Bellman-Ford,
topological sort, and cycle detection.
"""

from __future__ import annotations

import heapq
from typing import Callable


class Graph:
    """Undirected graph using adjacency lists."""

    def __init__(self):
        self.adj: dict[int, set[int]] = {}
        self.edges: list[tuple[int, int, float]] = []

    def add_node(self, u: int) -> None:
        if u not in self.adj:
            self.adj[u] = set()

    def add_edge(self, u: int, v: int, weight: float = 1.0) -> None:
        self.add_node(u)
        self.add_node(v)
        self.adj[u].add(v)
        self.adj[v].add(u)
        self.edges.append((u, v, weight))

    def nodes(self) -> list[int]:
        return list(self.adj.keys())

    def bfs(self, start: int) -> list[int]:
        """Breadth-first search traversal."""
        if start not in self.adj:
            return []
        visited: set[int] = set()
        queue = [start]
        order: list[int] = []
        while queue:
            u = queue.pop(0)
            if u in visited:
                continue
            visited.add(u)
            order.append(u)
            for v in sorted(self.adj.get(u, [])):
                if v not in visited:
                    queue.append(v)
        return order

    def dfs(self, start: int) -> list[int]:
        """Depth-first search traversal (iterative)."""
        if start not in self.adj:
            return []
        visited: set[int] = set()
        stack = [start]
        order: list[int] = []
        while stack:
            u = stack.pop()
            if u in visited:
                continue
            visited.add(u)
            order.append(u)
            for v in sorted(self.adj.get(u, []), reverse=True):
                if v not in visited:
                    stack.append(v)
        return order

    def dfs_recursive(self, u: int, visited: set[int] | None = None) -> list[int]:
        """DFS starting from node u."""
        if visited is None:
            visited = set()
        if u in visited or u not in self.adj:
            return []
        visited.add(u)
        order = [u]
        for v in sorted(self.adj[u]):
            order.extend(self.dfs_recursive(v, visited))
        return order

    def has_cycle(self) -> bool:
        """Detect if the graph has a cycle (DFS-based)."""
        visited: set[int] = set()
        rec_stack: set[int] = set()

        def dfs(u: int) -> bool:
            visited.add(u)
            rec_stack.add(u)
            for v in self.adj.get(u, []):
                if v not in visited:
                    if dfs(v):
                        return True
                elif v in rec_stack:
                    return True
            rec_stack.remove(u)
            return False

        for u in self.adj:
            if u not in visited:
                if dfs(u):
                    return True
        return False

    def topological_sort(self) -> list[int] | None:
        """
        Kahn's algorithm for topological sort.
        Returns None if graph has a cycle.
        """
        in_degree: dict[int, int] = {u: 0 for u in self.adj}
        for u in self.adj:
            for v in self.adj[u]:
                in_degree[v] = in_degree.get(v, 0) + 1

        queue = [u for u, d in in_degree.items() if d == 0]
        order: list[int] = []
        while queue:
            u = queue.pop(0)
            order.append(u)
            for v in self.adj.get(u, []):
                in_degree[v] -= 1
                if in_degree[v] == 0:
                    queue.append(v)

        if len(order) != len(self.adj):
            return None  # cycle detected
        return order


class DirectedGraph:
    """Directed graph."""

    def __init__(self):
        self.adj: dict[int, set[int]] = {}
        self.edges: list[tuple[int, int, float]] = []

    def add_node(self, u: int) -> None:
        if u not in self.adj:
            self.adj[u] = set()

    def add_edge(self, u: int, v: int, weight: float = 1.0) -> None:
        self.add_node(u)
        self.add_node(v)
        self.adj[u].add(v)
        self.edges.append((u, v, weight))

    def dijkstra(self, source: int) -> dict[int, float]:
        """
        Dijkstra's shortest path algorithm.

        Returns:
            Dictionary of shortest distance from source to each node.
        """
        dist: dict[int, float] = {u: float("inf") for u in self.adj}
        dist[source] = 0.0
        pq: list[tuple[float, int]] = [(0.0, source)]
        visited: set[int] = set()

        while pq:
            d, u = heapq.heappop(pq)
            if u in visited:
                continue
            visited.add(u)
            for v in self.adj.get(u, []):
                edge_weight = next(w for (a, b, w) in self.edges if a == u and b == v)
                if dist[u] + edge_weight < dist[v]:
                    dist[v] = dist[u] + edge_weight
                    heapq.heappush(pq, (dist[v], v))

        return dist

    def bellman_ford(self, source: int) -> tuple[dict[int, float], bool]:
        """
        Bellman-Ford algorithm. Handles negative weights.

        Returns:
            Tuple of (distances, has_negative_cycle).
        """
        dist: dict[int, float] = {u: float("inf") for u in self.adj}
        dist[source] = 0.0
        n = len(self.adj)

        for _ in range(n - 1):
            for u, v, w in self.edges:
                if dist[u] != float("inf") and dist[u] + w < dist[v]:
                    dist[v] = dist[u] + w

        # Check for negative cycles
        has_neg_cycle = False
        for u, v, w in self.edges:
            if dist[u] != float("inf") and dist[u] + w < dist[v]:
                has_neg_cycle = True
                break

        return dist, has_neg_cycle

    def has_cycle(self) -> bool:
        """Detect cycle in directed graph using DFS with three-color marking."""
        WHITE, GRAY, BLACK = 0, 1, 2
        color: dict[int, int] = {u: WHITE for u in self.adj}

        def dfs(u: int) -> bool:
            color[u] = GRAY
            for v in self.adj.get(u, []):
                if color.get(v, WHITE) == GRAY:
                    return True
                if color.get(v, WHITE) == WHITE and dfs(v):
                    return True
            color[u] = BLACK
            return False

        for u in self.adj:
            if color.get(u, WHITE) == WHITE and dfs(u):
                return True
        return False

    def topological_sort(self) -> list[int] | None:
        """Topological sort for DAG."""
        if self.has_cycle():
            return None
        in_degree: dict[int, int] = {u: 0 for u in self.adj}
        for u in self.adj:
            for v in self.adj[u]:
                in_degree[v] += 1
        queue = [u for u, d in in_degree.items() if d == 0]
        order: list[int] = []
        while queue:
            u = queue.pop(0)
            order.append(u)
            for v in self.adj.get(u, []):
                in_degree[v] -= 1
                if in_degree[v] == 0:
                    queue.append(v)
        return order if len(order) == len(self.adj) else None


def page_rank(
    adj: dict[int, list[int]],
    damping: float = 0.85,
    iterations: int = 100,
    tol: float = 1e-6,
) -> dict[int, float]:
    """
    PageRank algorithm.

    Args:
        adj: Adjacency dict {page: [outlinks]}
        damping: Damping factor (default 0.85)
        iterations: Maximum iterations
        tol: Convergence tolerance

    Returns:
        Dictionary of PageRank scores.
    """
    nodes = list(adj.keys())
    n = len(nodes)
    if n == 0:
        return {}
    node_idx = {u: i for i, u in enumerate(nodes)}
    ranks = [1.0 / n] * n

    for _ in range(iterations):
        new_ranks = [(1.0 - damping) / n] * n
        for i, u in enumerate(nodes):
            rank_sum = 0.0
            for v in adj.get(u, []):
                out_degree = len(adj.get(v, []))
                if out_degree > 0:
                    rank_sum += ranks[node_idx[v]] / out_degree
            new_ranks[i] += damping * rank_sum

        if all(abs(new_ranks[i] - ranks[i]) < tol for i in range(n)):
            break
        ranks = new_ranks

    return {u: ranks[i] for i, u in enumerate(nodes)}


def connected_components(adj: dict[int, set[int]]) -> list[set[int]]:
    """Find all connected components."""
    visited: set[int] = set()
    components: list[set[int]] = []

    def dfs(u: int, comp: set[int]) -> None:
        visited.add(u)
        comp.add(u)
        for v in adj.get(u, []):
            if v not in visited:
                dfs(v, comp)

    for u in adj:
        if u not in visited:
            comp: set[int] = set()
            dfs(u, comp)
            components.append(comp)

    return components
