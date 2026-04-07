"""
Graph utilities for graph algorithms and data structures.

Provides graph representations, traversal, shortest path,
and topological sort algorithms.
"""

from __future__ import annotations

import heapq
from collections import deque
from typing import Callable


class Graph:
    """Adjacency list representation of a weighted graph."""

    def __init__(self, directed: bool = False) -> None:
        self.directed = directed
        self.adj: dict[str, list[tuple[str, float]]] = {}
        self.edges: list[tuple[str, str, float]] = []

    def add_node(self, node: str) -> None:
        if node not in self.adj:
            self.adj[node] = []

    def add_edge(self, u: str, v: str, weight: float = 1.0) -> None:
        self.add_node(u)
        self.add_node(v)
        self.adj[u].append((v, weight))
        if not self.directed:
            self.adj[v].append((u, weight))
        self.edges.append((u, v, weight))

    def bfs(self, start: str) -> list[str]:
        """Breadth-first search traversal."""
        visited = {start}
        queue = deque([start])
        result = []
        while queue:
            node = queue.popleft()
            result.append(node)
            for neighbor, _ in self.adj[node]:
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append(neighbor)
        return result

    def dfs(self, start: str) -> list[str]:
        """Depth-first search traversal."""
        visited: set[str] = set()
        result = []

        def dfs_rec(node: str) -> None:
            visited.add(node)
            result.append(node)
            for neighbor, _ in self.adj[node]:
                if neighbor not in visited:
                    dfs_rec(neighbor)

        dfs_rec(start)
        return result

    def dijkstra(self, start: str, end: str | None = None) -> tuple[dict[str, float], dict[str, str | None]]:
        """
        Dijkstra's shortest path algorithm.

        Returns:
            Tuple of (distances, predecessors)
        """
        dist: dict[str, float] = {n: float("inf") for n in self.adj}
        prev: dict[str, str | None] = {n: None for n in self.adj}
        dist[start] = 0.0
        pq = [(0.0, start)]
        visited: set[str] = set()

        while pq:
            d, u = heapq.heappop(pq)
            if u in visited:
                continue
            visited.add(u)
            if end is not None and u == end:
                break
            for v, weight in self.adj[u]:
                if v not in visited:
                    alt = d + weight
                    if alt < dist[v]:
                        dist[v] = alt
                        prev[v] = u
                        heapq.heappush(pq, (alt, v))

        return dist, prev

    def shortest_path(self, start: str, end: str) -> list[str]:
        """Get shortest path between two nodes."""
        _, prev = self.dijkstra(start, end)
        path = []
        current: str | None = end
        while current is not None:
            path.append(current)
            current = prev[current]
        path.reverse()
        return path if path and path[0] == start else []

    def bellman_ford(self, start: str) -> tuple[dict[str, float], dict[str, str | None]] | None:
        """Bellman-Ford algorithm for negative weights."""
        dist: dict[str, float] = {n: float("inf") for n in self.adj}
        prev: dict[str, str | None] = {n: None for n in self.adj}
        dist[start] = 0.0
        n = len(self.adj)
        for _ in range(n - 1):
            for u, v, w in self.edges:
                if dist[u] + w < dist[v]:
                    dist[v] = dist[u] + w
                    prev[v] = u
                if not self.directed and dist[v] + w < dist[u]:
                    dist[u] = dist[v] + w
                    prev[u] = v
        for u, v, w in self.edges:
            if dist[u] + w < dist[v]:
                return None
        return dist, prev

    def has_cycle(self) -> bool:
        """Check if graph has a cycle."""
        visited: set[str] = set()
        rec_stack: set[str] = set()

        def dfs(node: str) -> bool:
            visited.add(node)
            rec_stack.add(node)
            for neighbor, _ in self.adj[node]:
                if neighbor not in visited:
                    if dfs(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True
            rec_stack.remove(node)
            return False

        for node in self.adj:
            if node not in visited:
                if dfs(node):
                    return True
        return False

    def topological_sort(self) -> list[str] | None:
        """Kahn's algorithm for topological sort."""
        if self.directed and self.has_cycle():
            return None
        in_degree = {n: 0 for n in self.adj}
        for u in self.adj:
            for v, _ in self.adj[u]:
                in_degree[v] += 1
        queue = deque([n for n in self.adj if in_degree[n] == 0])
        result = []
        while queue:
            node = queue.popleft()
            result.append(node)
            for neighbor, _ in self.adj[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        return result if len(result) == len(self.adj) else None


def connected_components(graph: Graph) -> list[set[str]]:
    """Find all connected components."""
    visited: set[str] = set()
    components: list[set[str]] = []

    def dfs(node: str, comp: set[str]) -> None:
        visited.add(node)
        comp.add(node)
        for neighbor, _ in graph.adj[node]:
            if neighbor not in visited:
                dfs(neighbor, comp)

    for node in graph.adj:
        if node not in visited:
            comp: set[str] = set()
            dfs(node, comp)
            components.append(comp)
    return components


def pagerank(graph: Graph, damping: float = 0.85, iterations: int = 100) -> dict[str, float]:
    """
    Compute PageRank scores for graph nodes.

    Args:
        graph: Input graph
        damping: Damping factor (typically 0.85)
        iterations: Number of iterations

    Returns:
        Dict mapping node to PageRank score
    """
    n = len(graph.adj)
    if n == 0:
        return {}
    pr = {node: 1.0 / n for node in graph.adj}
    for _ in range(iterations):
        new_pr: dict[str, float] = {}
        for node in graph.adj:
            rank_sum = 0.0
            for neighbor in graph.adj:
                for v, _ in graph.adj[neighbor]:
                    if v == node:
                        rank_sum += pr[neighbor] / len(graph.adj[neighbor])
                        break
            new_pr[node] = (1 - damping) / n + damping * rank_sum
        pr = new_pr
    return pr
