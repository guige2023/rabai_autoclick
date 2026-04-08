"""Data Resolver Action Module.

Provides data resolution with dependency graphs,
circular dependency detection, and topological ordering.
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class ResolverConfig:
    """Resolver configuration."""
    allow_circular: bool = False
    on_circular: str = "error"


class DependencyNode:
    """Dependency node."""
    def __init__(
        self,
        key: str,
        data: Any,
        dependencies: Optional[List[str]] = None,
    ) -> None:
        self.key = key
        self.data = data
        self.dependencies = dependencies or []


class DataResolverAction:
    """Dependency resolver with topological sort.

    Example:
        resolver = DataResolverAction()

        resolver.add("a", data1, dependencies=["b", "c"])
        resolver.add("b", data2)
        resolver.add("c", data3)

        resolved = resolver.resolve()
        print([n.key for n in resolved])  # Topologically sorted
    """

    def __init__(self, config: Optional[ResolverConfig] = None) -> None:
        self.config = config or ResolverConfig()
        self._nodes: Dict[str, DependencyNode] = {}

    def add(
        self,
        key: str,
        data: Any,
        dependencies: Optional[List[str]] = None,
    ) -> "DataResolverAction":
        """Add node to resolver.

        Returns self for chaining.
        """
        self._nodes[key] = DependencyNode(key, data, dependencies)
        return self

    def add_many(
        self,
        nodes: List[Dict[str, Any]],
    ) -> "DataResolverAction":
        """Add multiple nodes at once.

        Args:
            nodes: List of dicts with 'key', 'data', 'dependencies'
        """
        for node in nodes:
            self.add(
                node["key"],
                node["data"],
                node.get("dependencies"),
            )
        return self

    def resolve(
        self,
        start_keys: Optional[List[str]] = None,
    ) -> List[DependencyNode]:
        """Resolve dependencies in topological order.

        Args:
            start_keys: Optional subset of keys to resolve

        Returns:
            List of DependencyNodes in dependency order
        """
        nodes_to_resolve = self._nodes

        if start_keys:
            nodes_to_resolve = {
                k: v for k, v in self._nodes.items()
                if k in start_keys
            }

        if self.config.allow_circular:
            return self._resolve_with_circular()

        return self._resolve_topological(nodes_to_resolve)

    def _resolve_topological(
        self,
        nodes: Dict[str, DependencyNode],
    ) -> List[DependencyNode]:
        """Resolve with topological sort (Kahn's algorithm)."""
        in_degree = defaultdict(int)
        adj_list = defaultdict(list)

        for key, node in nodes.items():
            if key not in in_degree:
                in_degree[key] = 0

            for dep in node.dependencies:
                if dep in nodes:
                    adj_list[dep].append(key)
                    in_degree[key] += 1

        queue = deque([
            k for k, v in in_degree.items()
            if v == 0
        ])

        result: List[DependencyNode] = []

        while queue:
            current = queue.popleft()
            if current in nodes:
                result.append(nodes[current])

            for neighbor in adj_list[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(result) != len(nodes):
            cycle = self._find_cycle(nodes)
            raise CircularDependencyError(
                f"Circular dependency detected: {' -> '.join(cycle)}"
            )

        return result

    def _resolve_with_circular(
        self,
    ) -> List[DependencyNode]:
        """Resolve with circular dependencies allowed."""
        visited = set()
        result: List[DependencyNode] = []

        def visit(key: str) -> None:
            if key in visited:
                return
            visited.add(key)

            if key in self._nodes:
                node = self._nodes[key]
                for dep in node.dependencies:
                    if dep in self._nodes:
                        visit(dep)
                result.append(node)

        for key in self._nodes:
            visit(key)

        return result

    def _find_cycle(self, nodes: Dict[str, DependencyNode]) -> List[str]:
        """Find a cycle in dependencies."""
        visited = set()
        path: List[str] = []

        def dfs(key: str) -> Optional[List[str]]:
            if key in path:
                cycle_start = path.index(key)
                return path[cycle_start:] + [key]

            if key in visited:
                return None

            visited.add(key)
            path.append(key)

            if key in nodes:
                for dep in nodes[key].dependencies:
                    if dep in nodes:
                        result = dfs(dep)
                        if result:
                            return result

            path.pop()
            return None

        for key in nodes:
            if key not in visited:
                cycle = dfs(key)
                if cycle:
                    return cycle

        return []

    def get_missing_dependencies(
        self,
        key: str,
    ) -> List[str]:
        """Get missing dependencies for a key.

        Returns:
            List of dependency keys that don't exist
        """
        if key not in self._nodes:
            return []

        node = self._nodes[key]
        missing = []

        for dep in node.dependencies:
            if dep not in self._nodes:
                missing.append(dep)

        return missing

    def validate(self) -> Dict[str, List[str]]:
        """Validate all dependencies.

        Returns:
            Dict mapping keys to lists of missing dependencies
        """
        issues: Dict[str, List[str]] = {}

        for key in self._nodes:
            missing = self.get_missing_dependencies(key)
            if missing:
                issues[key] = missing

        return issues

    def clear(self) -> None:
        """Clear all nodes."""
        self._nodes.clear()


class CircularDependencyError(Exception):
    """Raised when circular dependency detected."""
    pass
