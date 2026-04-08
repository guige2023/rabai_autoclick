"""Graph and traversal utilities.

Provides graph data structures and traversal algorithms
for dependency resolution and path finding.
"""

from collections import deque
from typing import Any, Callable, Dict, List, Optional, Set


class Graph:
    """Generic directed graph with adjacency list representation.

    Example:
        g = Graph()
        g.add_edge("a", "b")
        g.add_edge("b", "c")
        for node in g.topological_sort():
            print(node)
    """

    def __init__(self) -> None:
        self._adj: Dict[str, Set[str]] = {}
        self._nodes: Set[str] = set()

    def add_node(self, node: str) -> None:
        """Add a node to the graph."""
        self._nodes.add(node)
        if node not in self._adj:
            self._adj[node] = set()

    def add_edge(self, from_node: str, to_node: str) -> None:
        """Add a directed edge from one node to another."""
        self.add_node(from_node)
        self.add_node(to_node)
        self._adj[from_node].add(to_node)

    def remove_edge(self, from_node: str, to_node: str) -> None:
        """Remove a directed edge."""
        if from_node in self._adj:
            self._adj[from_node].discard(to_node)

    def nodes(self) -> List[str]:
        """Get all nodes."""
        return list(self._nodes)

    def edges(self, node: str) -> List[str]:
        """Get outgoing edges from a node."""
        return list(self._adj.get(node, set()))

    def has_node(self, node: str) -> bool:
        """Check if node exists."""
        return node in self._nodes

    def bfs(self, start: str) -> List[str]:
        """Breadth-first traversal from start node."""
        if start not in self._nodes:
            return []
        visited: Set[str] = set()
        queue = deque([start])
        order: List[str] = []
        while queue:
            node = queue.popleft()
            if node in visited:
                continue
            visited.add(node)
            order.append(node)
            for neighbor in self._adj.get(node, set()):
                if neighbor not in visited:
                    queue.append(neighbor)
        return order

    def dfs(self, start: str) -> List[str]:
        """Depth-first traversal from start node."""
        if start not in self._nodes:
            return []
        visited: Set[str] = set()
        order: List[str] = []

        def dfs_recursive(node: str) -> None:
            visited.add(node)
            order.append(node)
            for neighbor in self._adj.get(node, set()):
                if neighbor not in visited:
                    dfs_recursive(neighbor)

        dfs_recursive(start)
        return order

    def topological_sort(self) -> List[str]:
        """Return nodes in topological order (Kahn's algorithm).

        Raises:
            ValueError: If graph contains a cycle.
        """
        in_degree: Dict[str, int] = {n: 0 for n in self._nodes}
        for node in self._nodes:
            for neighbor in self._adj.get(node, set()):
                in_degree[neighbor] += 1

        queue = deque([n for n, d in in_degree.items() if d == 0])
        order: List[str] = []
        while queue:
            node = queue.popleft()
            order.append(node)
            for neighbor in self._adj.get(node, set()):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(order) != len(self._nodes):
            raise ValueError("Graph contains a cycle")
        return order

    def find_path(self, start: str, end: str) -> Optional[List[str]]:
        """Find path from start to end using BFS.

        Returns:
            Path as list of nodes, or None if no path.
        """
        if start not in self._nodes or end not in self._nodes:
            return None
        if start == end:
            return [start]

        visited: Set[str] = {start}
        queue = deque([(start, [start])])
        while queue:
            node, path = queue.popleft()
            for neighbor in self._adj.get(node, set()):
                if neighbor == end:
                    return path + [neighbor]
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, path + [neighbor]))
        return None

    def detect_cycle(self) -> Optional[List[str]]:
        """Detect if graph contains a cycle.

        Returns:
            Cycle path if found, None otherwise.
        """
        WHITE, GRAY, BLACK = 0, 1, 2
        color: Dict[str, int] = {n: WHITE for n in self._nodes}
        parent: Dict[str, Optional[str]] = {n: None for n in self._nodes}

        def dfs_cycle(node: str) -> Optional[List[str]]:
            color[node] = GRAY
            for neighbor in self._adj.get(node, set()):
                if color[neighbor] == GRAY:
                    path = [neighbor, node]
                    while parent[node] is not None:
                        node = parent[node]
                        path.append(node)
                    return list(reversed(path))
                if color[neighbor] == WHITE:
                    parent[neighbor] = node
                    result = dfs_cycle(neighbor)
                    if result:
                        return result
            color[node] = BLACK
            return None

        for node in self._nodes:
            if color[node] == WHITE:
                result = dfs_cycle(node)
                if result:
                    return result
        return None
