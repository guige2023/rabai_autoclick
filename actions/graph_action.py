"""
Graph and network utilities - graph traversal, shortest path, topology, network analysis.
"""
from typing import Any, Dict, List, Optional, Set, Tuple, Iterator
from collections import defaultdict, deque
import heapq
import logging

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


class Graph:
    """Undirected graph with adjacency list representation."""

    def __init__(self) -> None:
        self.adj: Dict[str, List[Tuple[str, float]]] = defaultdict(list)

    def add_node(self, node: str) -> None:
        if node not in self.adj:
            self.adj[node] = []

    def add_edge(self, u: str, v: str, weight: float = 1.0) -> None:
        self.add_node(u)
        self.add_node(v)
        self.adj[u].append((v, weight))
        self.adj[v].append((u, weight))

    def add_directed_edge(self, u: str, v: str, weight: float = 1.0) -> None:
        self.add_node(u)
        self.add_node(v)
        self.adj[u].append((v, weight))

    def nodes(self) -> List[str]:
        return list(self.adj.keys())

    def edges(self) -> List[Tuple[str, str, float]]:
        seen = set()
        result = []
        for u in self.adj:
            for v, w in self.adj[u]:
                if (v, u) not in seen:
                    seen.add((u, v))
                    result.append((u, v, w))
        return result

    def bfs(self, start: str) -> List[str]:
        visited: Set[str] = set()
        queue = deque([start])
        order = []
        while queue:
            node = queue.popleft()
            if node in visited:
                continue
            visited.add(node)
            order.append(node)
            for neighbor, _ in self.adj[node]:
                if neighbor not in visited:
                    queue.append(neighbor)
        return order

    def dfs(self, start: str) -> List[str]:
        visited: Set[str] = set()
        order = []

        def _dfs(node: str) -> None:
            if node in visited:
                return
            visited.add(node)
            order.append(node)
            for neighbor, _ in self.adj[node]:
                if neighbor not in visited:
                    _dfs(neighbor)

        _dfs(start)
        return order

    def dijkstra(self, start: str, end: Optional[str] = None) -> Tuple[Dict[str, float], Dict[str, Optional[str]]]:
        dist: Dict[str, float] = {start: 0.0}
        prev: Dict[str, Optional[str]] = {start: None}
        heap = [(0.0, start)]

        while heap:
            d, u = heapq.heappop(heap)
            if d > dist.get(u, float("inf")):
                continue
            if end and u == end:
                break
            for v, w in self.adj.get(u, []):
                alt = d + w
                if alt < dist.get(v, float("inf")):
                    dist[v] = alt
                    prev[v] = u
                    heapq.heappush(heap, (alt, v))

        return dist, prev

    def shortest_path(self, start: str, end: str) -> Optional[List[str]]:
        dist, prev = self.dijkstra(start, end)
        if end not in dist or dist[end] == float("inf"):
            return None
        path = []
        node: Optional[str] = end
        while node is not None:
            path.append(node)
            node = prev[node]
        return path[::-1]

    def has_cycle(self) -> bool:
        visited: Set[str] = set()
        rec_stack: Set[str] = set()

        def _has_cycle_from(node: str) -> bool:
            visited.add(node)
            rec_stack.add(node)
            for neighbor, _ in self.adj[node]:
                if neighbor not in visited:
                    if _has_cycle_from(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True
            rec_stack.remove(node)
            return False

        for node in self.adj:
            if node not in visited:
                if _has_cycle_from(node):
                    return True
        return False

    def topological_sort(self) -> Optional[List[str]]:
        in_degree: Dict[str, int] = defaultdict(int)
        for u in self.adj:
            for v, _ in self.adj[u]:
                in_degree[v] += 1
        queue = deque([n for n in self.adj if in_degree[n] == 0])
        order = []
        while queue:
            node = queue.popleft()
            order.append(node)
            for neighbor, _ in self.adj[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        if len(order) != len(self.adj):
            return None
        return order

    def pagerank(self, damping: float = 0.85, iterations: int = 100, tol: float = 1e-6) -> Dict[str, float]:
        nodes = list(self.adj.keys())
        n = len(nodes)
        if n == 0:
            return {}
        rank = {node: 1.0 / n for node in nodes}
        for _ in range(iterations):
            new_rank: Dict[str, float] = {}
            for node in nodes:
                score = 0.0
                for other in self.adj:
                    for neighbor, _ in self.adj[other]:
                        if neighbor == node:
                            score += damping * rank[other] / len(self.adj[other])
                new_rank[node] = (1 - damping) / n + score
            max_diff = max(abs(new_rank[n] - rank[n]) for n in nodes)
            rank = new_rank
            if max_diff < tol:
                break
        return rank


class GraphAction(BaseAction):
    """Graph and network operations.

    Supports BFS, DFS, Dijkstra's shortest path, cycle detection, topological sort, PageRank.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "build")
        edges = params.get("edges", [])
        nodes = params.get("nodes", [])
        directed = params.get("directed", False)
        start = params.get("start", "")
        end = params.get("end", "")

        try:
            graph = Graph()
            for n in nodes:
                graph.add_node(str(n))
            for edge in edges:
                if isinstance(edge, (list, tuple)) and len(edge) >= 2:
                    u, v = str(edge[0]), str(edge[1])
                    w = float(edge[2]) if len(edge) > 2 else 1.0
                    if directed:
                        graph.add_directed_edge(u, v, w)
                    else:
                        graph.add_edge(u, v, w)

            if operation == "build":
                return {
                    "success": True,
                    "nodes": graph.nodes(),
                    "edges": [(u, v, w) for u, v, w in graph.edges()],
                }

            elif operation == "bfs":
                if not start:
                    return {"success": False, "error": "start node required"}
                return {"success": True, "order": graph.bfs(start), "start": start}

            elif operation == "dfs":
                if not start:
                    return {"success": False, "error": "start node required"}
                return {"success": True, "order": graph.dfs(start), "start": start}

            elif operation == "shortest_path":
                if not start or not end:
                    return {"success": False, "error": "start and end required"}
                path = graph.shortest_path(start, end)
                if path:
                    dist, _ = graph.dijkstra(start, end)
                    return {"success": True, "path": path, "distance": dist.get(end, 0)}
                return {"success": False, "error": "No path found"}

            elif operation == "dijkstra":
                if not start:
                    return {"success": False, "error": "start node required"}
                dist, prev = graph.dijkstra(start)
                return {"success": True, "distances": dist, "target": end or None}

            elif operation == "has_cycle":
                return {"success": True, "has_cycle": graph.has_cycle()}

            elif operation == "topological_sort":
                order = graph.topological_sort()
                if order is None:
                    return {"success": False, "error": "Graph has cycles - cannot topologically sort"}
                return {"success": True, "order": order}

            elif operation == "pagerank":
                damping = float(params.get("damping", 0.85))
                iterations = int(params.get("iterations", 100))
                ranks = graph.pagerank(damping, iterations)
                return {"success": True, "pagerank": ranks}

            elif operation == "nodes":
                return {"success": True, "nodes": graph.nodes(), "count": len(graph.nodes())}

            elif operation == "edges":
                return {"success": True, "edges": [(u, v, w) for u, v, w in graph.edges()], "count": len(graph.edges())}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"GraphAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for graph operations."""
    return GraphAction().execute(context, params)
