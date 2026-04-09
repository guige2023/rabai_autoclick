"""Workflow Engine Action Module.

Generic workflow engine with parallel execution and conditional branching.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from .automation_executor_action import StepStatus


class WorkflowStatus(Enum):
    """Workflow status."""
    CREATED = "created"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class WorkflowNode:
    """Workflow graph node."""
    node_id: str
    name: str
    action: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    timeout: float = 60.0
    retries: int = 0
    max_retries: int = 3


@dataclass
class WorkflowEdge:
    """Workflow graph edge (transition)."""
    from_node: str
    to_node: str
    condition: Callable[[dict], bool] | None = None
    label: str | None = None


@dataclass
class WorkflowExecution:
    """Single workflow execution instance."""
    execution_id: str
    workflow_id: str
    status: WorkflowStatus
    started_at: float
    completed_at: float | None = None
    node_results: dict[str, Any] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)
    context: dict = field(default_factory=dict)


class WorkflowEngine:
    """Workflow engine with DAG execution."""

    def __init__(self, workflow_id: str, name: str) -> None:
        self.workflow_id = workflow_id
        self.name = name
        self._nodes: dict[str, WorkflowNode] = {}
        self._edges: list[WorkflowEdge] = []
        self._in_edges: dict[str, list[WorkflowEdge]] = {}
        self._out_edges: dict[str, list[WorkflowEdge]] = {}

    def add_node(self, node: WorkflowNode) -> WorkflowEngine:
        """Add a node to the workflow."""
        self._nodes[node.node_id] = node
        self._in_edges[node.node_id] = []
        self._out_edges[node.node_id] = []
        return self

    def add_edge(
        self,
        from_node: str,
        to_node: str,
        condition: Callable[[dict], bool] | None = None,
        label: str | None = None
    ) -> WorkflowEngine:
        """Add an edge between nodes."""
        edge = WorkflowEdge(from_node, to_node, condition, label)
        self._edges.append(edge)
        self._out_edges[from_node].append(edge)
        self._in_edges[to_node].append(edge)
        return self

    async def execute(self, initial_context: dict | None = None) -> WorkflowExecution:
        """Execute the workflow."""
        execution_id = str(uuid.uuid4())
        execution = WorkflowExecution(
            execution_id=execution_id,
            workflow_id=self.workflow_id,
            status=WorkflowStatus.RUNNING,
            started_at=time.time(),
            context=dict(initial_context or {})
        )
        pending_nodes = {n for n in self._nodes if not self._in_edges[n]}
        completed_nodes: set[str] = set()
        while pending_nodes:
            ready_nodes = set(pending_nodes)
            pending_nodes.clear()
            for node_id in ready_nodes:
                node = self._nodes[node_id]
                for edge in self._out_edges[node_id]:
                    if all(dep in completed_nodes for dep in self._get_upstream(edge.from_node)):
                        if edge.condition is None or edge.condition(execution.context):
                            pending_nodes.add(edge.to_node)
            await asyncio.gather(*[
                self._execute_node(execution, node_id)
                for node_id in ready_nodes
            ])
            completed_nodes.update(ready_nodes)
        execution.status = WorkflowStatus.COMPLETED
        execution.completed_at = time.time()
        return execution

    def _get_upstream(self, node_id: str) -> set[str]:
        """Get upstream nodes."""
        return {e.from_node for e in self._in_edges[node_id]}

    async def _execute_node(self, execution: WorkflowExecution, node_id: str) -> None:
        """Execute a single node."""
        node = self._nodes[node_id]
        attempt = 0
        while attempt <= node.retries:
            try:
                result = await asyncio.wait_for(
                    asyncio.to_thread(node.action, *node.args, **node.kwargs),
                    timeout=node.timeout
                )
                execution.node_results[node_id] = result
                execution.context[node_id] = result
                return
            except Exception as e:
                attempt += 1
                if attempt > node.max_retries:
                    execution.errors[node_id] = str(e)
                    return
                await asyncio.sleep(2 ** attempt)

    async def validate(self) -> tuple[bool, list[str]]:
        """Validate workflow DAG. Returns (is_valid, errors)."""
        errors = []
        if not self._nodes:
            return False, ["No nodes in workflow"]
        roots = [n for n in self._nodes if not self._in_edges[n]]
        if not roots:
            errors.append("No root nodes found")
        orphans = [n for n in self._nodes if not self._out_edges[n]]
        if not orphans:
            errors.append("No terminal nodes found")
        return len(errors) == 0, errors
