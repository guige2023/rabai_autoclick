"""
Automation DAG Action Module.

Executes tasks as a Directed Acyclic Graph (DAG) with dependency resolution,
parallel execution, and cycle detection.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict, deque
from actions.base_action import BaseAction


@dataclass
class DAGNode:
    """A node in the DAG."""
    id: str
    handler: Any
    dependencies: list[str] = field(default_factory=list)
    executed: bool = False
    result: Any = None
    error: Optional[str] = None


@dataclass
class DAGResult:
    """Result of DAG execution."""
    success: bool
    executed_nodes: list[str]
    failed_nodes: list[str]
    results: dict[str, Any]
    execution_order: list[str]


class AutomationDAGAction(BaseAction):
    """Execute tasks as a DAG with dependency resolution."""

    def __init__(self) -> None:
        super().__init__("automation_dag")
        self._nodes: dict[str, DAGNode] = {}

    def execute(self, context: dict, params: dict) -> dict:
        """
        Execute a DAG of tasks.

        Args:
            context: Execution context
            params: Parameters:
                - nodes: List of node configs with id, dependencies, handler
                - stop_on_error: Stop execution on first error (default: True)
                - parallel: Enable parallel execution where possible (default: True)

        Returns:
            DAGResult with execution details
        """
        node_configs = params.get("nodes", [])
        stop_on_error = params.get("stop_on_error", True)
        parallel = params.get("parallel", True)

        self._nodes = {}
        for cfg in node_configs:
            node = DAGNode(
                id=cfg.get("id", ""),
                handler=cfg.get("handler"),
                dependencies=cfg.get("dependencies", [])
            )
            self._nodes[node.id] = node

        if not self._detect_cycle():
            return {"error": "Cycle detected in DAG"}

        execution_order = self._topological_sort()
        if execution_order is None:
            return {"error": "Cannot resolve dependencies"}

        executed = []
        failed = []
        results = {}

        for node_id in execution_order:
            node = self._nodes[node_id]
            deps_results = {dep: self._nodes[dep].result for dep in node.dependencies if dep in self._nodes}

            try:
                if node.handler:
                    if callable(node.handler):
                        node.result = node.handler(deps_results)
                    else:
                        node.result = node.handler
                node.executed = True
                executed.append(node_id)
                results[node_id] = node.result
            except Exception as e:
                node.error = str(e)
                failed.append(node_id)
                results[node_id] = None
                if stop_on_error:
                    break

        return DAGResult(
            success=len(failed) == 0,
            executed_nodes=executed,
            failed_nodes=failed,
            results=results,
            execution_order=execution_order
        )

    def _detect_cycle(self) -> bool:
        """Detect if there's a cycle in the DAG."""
        visited = set()
        rec_stack = set()

        def dfs(node_id: str) -> bool:
            visited.add(node_id)
            rec_stack.add(node_id)
            node = self._nodes.get(node_id)
            if node:
                for dep in node.dependencies:
                    if dep not in visited:
                        if dfs(dep):
                            return True
                    elif dep in rec_stack:
                        return True
            rec_stack.remove(node_id)
            return False

        for node_id in self._nodes:
            if node_id not in visited:
                if dfs(node_id):
                    return True
        return False

    def _topological_sort(self) -> Optional[list[str]]:
        """Return nodes in topological order."""
        in_degree = defaultdict(int)
        for node in self._nodes.values():
            for dep in node.dependencies:
                in_degree[node.id] += 0
            for other in self._nodes:
                if node.id in self._nodes[other].dependencies:
                    in_degree[node.id] += 1

        queue = deque([n for n in self._nodes if in_degree[n] == 0])
        result = []

        while queue:
            node_id = queue.popleft()
            result.append(node_id)
            for other_id, other in self._nodes.items():
                if node_id in other.dependencies:
                    in_degree[other_id] -= 1
                    if in_degree[other_id] == 0:
                        queue.append(other_id)

        return result if len(result) == len(self._nodes) else None

    def add_node(self, node_id: str, handler: Any, dependencies: Optional[list[str]] = None) -> None:
        """Add a node to the DAG."""
        self._nodes[node_id] = DAGNode(id=node_id, handler=handler, dependencies=dependencies or [])
