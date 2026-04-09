"""
Automation DAG Action Module.

Directed Acyclic Graph (DAG) execution engine for automation workflows
with topological sorting, parallel execution, and dependency resolution.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class NodeStatus(Enum):
    """Status of a DAG node."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class DAGNode:
    """A node in the DAG representing a task."""
    id: str
    func: Callable[..., Any]
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    depends_on: Set[str] = field(default_factory=set)
    status: NodeStatus = NodeStatus.PENDING
    result: Any = None
    error: Optional[str] = None


@dataclass
class DAGEdge:
    """An edge in the DAG (dependency)."""
    from_node: str
    to_node: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DAGExecutionResult:
    """Result of a DAG execution."""
    success: bool
    completed_nodes: List[str]
    failed_nodes: List[str]
    skipped_nodes: List[str]
    results: Dict[str, Any]
    execution_order: List[str]
    total_duration_ms: float


class DAGBuilder:
    """Builder for constructing DAGs."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._nodes: Dict[str, DAGNode] = {}
        self._edges: List[DAGEdge] = []

    def add_node(
        self,
        node_id: str,
        func: Callable[..., T],
        args: tuple = (),
        kwargs: Optional[Dict[str, Any]] = None,
        depends_on: Optional[List[str]] = None,
    ) -> "DAGBuilder":
        """Add a node to the DAG."""
        self._nodes[node_id] = DAGNode(
            id=node_id,
            func=func,
            args=args,
            kwargs=kwargs or {},
            depends_on=set(depends_on or []),
        )
        return self

    def add_edge(
        self,
        from_node: str,
        to_node: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "DAGBuilder":
        """Add an edge (dependency) between nodes."""
        if from_node not in self._nodes:
            raise ValueError(f"Node '{from_node}' not found")
        if to_node not in self._nodes:
            raise ValueError(f"Node '{to_node}' not found")

        self._edges.append(DAGEdge(
            from_node=from_node,
            to_node=to_node,
            metadata=metadata or {},
        ))
        # Also update the node's depends_on
        self._nodes[to_node].depends_on.add(from_node)
        return self

    def build(self) -> "DAG":
        """Build the DAG."""
        dag = DAG(name=self.name)
        dag._nodes = self._nodes
        dag._edges = self._edges
        return dag


class DAG:
    """
    Directed Acyclic Graph execution engine.

    Executes nodes in topological order with parallel execution
    where dependencies allow.

    Example:
        dag = DAGBuilder("pipeline").add_node("a", task_a).add_node("b", task_b).add_node("c", task_c).add_edge("a", "b").add_edge("b", "c").build()

        result = await dag.execute()
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self._nodes: Dict[str, DAGNode] = {}
        self._edges: List[DAGEdge] = []

    def validate(self) -> bool:
        """Validate the DAG (check for cycles)."""
        visited: Set[str] = set()
        rec_stack: Set[str] = set()

        def has_cycle(node_id: str) -> bool:
            visited.add(node_id)
            rec_stack.add(node_id)

            node = self._nodes.get(node_id)
            if node:
                for dep in node.depends_on:
                    if dep not in visited:
                        if has_cycle(dep):
                            return True
                    elif dep in rec_stack:
                        return True

            rec_stack.remove(node_id)
            return False

        for node_id in self._nodes:
            if node_id not in visited:
                if has_cycle(node_id):
                    return False
        return True

    def topological_sort(self) -> List[str]:
        """Return nodes in topological order."""
        in_degree: Dict[str, int] = {n: 0 for n in self._nodes}

        for node in self._nodes.values():
            for dep in node.depends_on:
                if dep in in_degree:
                    pass
            # Calculate in-degree from edges
            for edge in self._edges:
                if edge.from_node in in_degree and edge.to_node in self._nodes:
                    in_degree[edge.to_node] = in_degree.get(edge.to_node, 0) + 1

        # Handle nodes with no incoming edges
        for node_id in self._nodes:
            in_degree[node_id] = len([
                e for e in self._edges
                if e.to_node == node_id
            ])

        queue = deque([n for n, d in in_degree.items() if d == 0])
        result: List[str] = []

        while queue:
            node_id = queue.popleft()
            result.append(node_id)

            for edge in self._edges:
                if edge.from_node == node_id:
                    to_node = edge.to_node
                    in_degree[to_node] -= 1
                    if in_degree[to_node] == 0:
                        queue.append(to_node)

        if len(result) != len(self._nodes):
            raise RuntimeError("DAG contains a cycle")

        return result

    async def execute(
        self,
        max_parallel: int = 10,
    ) -> DAGEdge:
        """Execute the DAG with parallel node execution where possible."""
        import time

        if not self.validate():
            raise RuntimeError("DAG validation failed: cycle detected")

        start_time = time.time()
        order = self.topological_sort()

        # Group nodes by dependency level
        levels: List[Set[str]] = []
        remaining = set(self._nodes.keys())

        while remaining:
            # Find nodes whose dependencies are all satisfied
            ready = {
                n for n in remaining
                if not (self._nodes[n].depends_on & remaining)
            }
            if not ready:
                raise RuntimeError("DAG has unsatisfiable dependencies")

            levels.append(ready)
            remaining -= ready

        # Execute level by level
        semaphore = asyncio.Semaphore(max_parallel)
        results: Dict[str, Any] = {}
        completed: Set[str] = set()
        failed: Set[str] = set()
        skipped: Set[str] = set()

        for level in levels:
            if failed:
                # Skip remaining if stop_on_failure
                skipped.update(level)
                continue

            async def execute_node(node_id: str) -> tuple[str, Any, Optional[str]]:
                async with semaphore:
                    self._nodes[node_id].status = NodeStatus.RUNNING
                    try:
                        node = self._nodes[node_id]
                        if asyncio.iscoroutinefunction(node.func):
                            result = await node.func(*node.args, **node.kwargs)
                        else:
                            result = node.func(*node.args, **node.kwargs)

                        self._nodes[node_id].status = NodeStatus.COMPLETED
                        self._nodes[node_id].result = result
                        results[node_id] = result
                        return node_id, result, None

                    except Exception as e:
                        self._nodes[node_id].status = NodeStatus.FAILED
                        self._nodes[node_id].error = str(e)
                        return node_id, None, str(e)

            tasks = [execute_node(n) for n in level]
            node_results = await asyncio.gather(*tasks, return_exceptions=True)

            for res in node_results:
                if isinstance(res, tuple):
                    nid, result, error = res
                    if error:
                        failed.add(nid)
                    else:
                        completed.add(nid)
                elif isinstance(res, Exception):
                    failed.add(level.pop() if level else "")

        duration = (time.time() - start_time) * 1000

        return DAGExecutionResult(
            success=len(failed) == 0,
            completed_nodes=list(completed),
            failed_nodes=list(failed),
            skipped_nodes=list(skipped),
            results=results,
            execution_order=order,
            total_duration_ms=duration,
        )

    def get_node(self, node_id: str) -> Optional[DAGNode]:
        """Get a node by ID."""
        return self._nodes.get(node_id)

    def get_ready_nodes(self) -> List[str]:
        """Get nodes that are ready to execute (all dependencies satisfied)."""
        ready = []
        for node in self._nodes.values():
            if node.status == NodeStatus.PENDING:
                deps_satisfied = all(
                    self._nodes[d].status == NodeStatus.COMPLETED
                    for d in node.depends_on if d in self._nodes
                )
                if deps_satisfied:
                    ready.append(node.id)
        return ready
