"""Automation DAG Action Module.

Provides DAG (Directed Acyclic Graph) based workflow
automation with topological execution and dependency management.
"""

import time
import hashlib
import asyncio
from typing import Any, Dict, List, Optional, Callable, Set
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class NodeStatus(Enum):
    """DAG node execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class DAGNode:
    """A node in the DAG workflow."""
    id: str
    name: str
    task: Callable
    dependencies: List[str] = field(default_factory=list)
    params: Dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    timeout_seconds: Optional[float] = None
    status: NodeStatus = NodeStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None


@dataclass
class DAGExecution:
    """Represents a DAG execution run."""
    execution_id: str
    dag_id: str
    status: NodeStatus
    started_at: float
    completed_at: Optional[float] = None
    node_results: Dict[str, Any] = field(default_factory=dict)
    errors: Dict[str, str] = field(default_factory=dict)


class DAGWorkflow:
    """DAG workflow definition and execution."""

    def __init__(self, dag_id: str, name: str):
        self.dag_id = dag_id
        self.name = name
        self._nodes: Dict[str, DAGNode] = {}
        self._execution_history: List[DAGExecution] = []

    def add_node(
        self,
        node_id: str,
        name: str,
        task: Callable,
        dependencies: Optional[List[str]] = None,
        params: Optional[Dict[str, Any]] = None,
        retry_count: int = 0,
        timeout_seconds: Optional[float] = None
    ) -> None:
        """Add a node to the DAG."""
        self._nodes[node_id] = DAGNode(
            id=node_id,
            name=name,
            task=task,
            dependencies=dependencies or [],
            params=params or {},
            retry_count=retry_count,
            timeout_seconds=timeout_seconds
        )

    def remove_node(self, node_id: str) -> bool:
        """Remove a node from the DAG."""
        if node_id not in self._nodes:
            return False

        del self._nodes[node_id]

        for node in self._nodes.values():
            if node_id in node.dependencies:
                node.dependencies.remove(node_id)

        return True

    def get_node(self, node_id: str) -> Optional[DAGNode]:
        """Get a node by ID."""
        return self._nodes.get(node_id)

    def validate(self) -> tuple[bool, Optional[str]]:
        """Validate DAG structure."""
        for node_id, node in self._nodes.items():
            for dep in node.dependencies:
                if dep not in self._nodes:
                    return False, f"Node {node_id} depends on missing node: {dep}"

        if self._has_cycle():
            return False, "DAG contains a cycle"

        return True, None

    def _has_cycle(self) -> bool:
        """Check if DAG contains a cycle."""
        visited: Set[str] = set()
        rec_stack: Set[str] = set()

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

    def topological_sort(self) -> List[str]:
        """Return nodes in topological order."""
        in_degree: Dict[str, int] = {
            node_id: 0 for node_id in self._nodes
        }

        for node in self._nodes.values():
            for dep in node.dependencies:
                in_degree[node.id] += 1

        queue = deque([
            node_id for node_id, degree in in_degree.items()
            if degree == 0
        ])

        sorted_nodes = []

        while queue:
            node_id = queue.popleft()
            sorted_nodes.append(node_id)

            node = self._nodes[node_id]
            for dependent_id, dependent in self._nodes.items():
                if node_id in dependent.dependencies:
                    in_degree[dependent_id] -= 1
                    if in_degree[dependent_id] == 0:
                        queue.append(dependent_id)

        return sorted_nodes

    def get_execution_order(self) -> List[List[str]]:
        """Get execution levels (parallel execution within each level)."""
        levels: List[List[str]] = []
        remaining = set(self._nodes.keys())
        executed = set()

        while remaining:
            current_level = []

            for node_id in remaining:
                node = self._nodes[node_id]
                deps_met = all(dep in executed for dep in node.dependencies)

                if deps_met:
                    current_level.append(node_id)

            if not current_level:
                break

            levels.append(current_level)
            executed.update(current_level)
            remaining -= set(current_level)

        return levels

    async def execute(
        self,
        context: Optional[Dict[str, Any]] = None,
        stop_on_error: bool = True
    ) -> DAGExecution:
        """Execute the DAG workflow."""
        valid, error = self.validate()
        if not valid:
            raise ValueError(f"DAG validation failed: {error}")

        execution_id = hashlib.md5(
            f"{self.dag_id}{time.time()}".encode()
        ).hexdigest()[:8]

        execution = DAGExecution(
            execution_id=execution_id,
            dag_id=self.dag_id,
            status=NodeStatus.RUNNING,
            started_at=time.time()
        )

        node_status = {node_id: NodeStatus.PENDING for node_id in self._nodes}

        for level in self.get_execution_order():
            if stop_on_error and any(
                node_status.get(n) == NodeStatus.FAILED for n in self._nodes
            ):
                break

            tasks = []
            for node_id in level:
                node = self._nodes[node_id]
                task = self._execute_node(node, context, execution)
                tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for node_id, result in zip(level, results):
                if isinstance(result, Exception):
                    node_status[node_id] = NodeStatus.FAILED
                    execution.errors[node_id] = str(result)
                else:
                    node_status[node_id] = NodeStatus.COMPLETED
                    execution.node_results[node_id] = result

                if stop_on_error and node_status[node_id] == NodeStatus.FAILED:
                    for remaining_id in self._nodes:
                        if node_status[remaining_id] == NodeStatus.PENDING:
                            node_status[remaining_id] = NodeStatus.SKIPPED

        execution.completed_at = time.time()

        if any(s == NodeStatus.FAILED for s in node_status.values()):
            execution.status = NodeStatus.FAILED
        else:
            execution.status = NodeStatus.COMPLETED

        self._execution_history.append(execution)
        return execution

    async def _execute_node(
        self,
        node: DAGNode,
        context: Optional[Dict[str, Any]],
        execution: DAGExecution
    ) -> Any:
        """Execute a single node."""
        node.status = NodeStatus.RUNNING
        node.started_at = time.time()

        try:
            if asyncio.iscoroutinefunction(node.task):
                result = await node.task(node.params, context, execution.node_results)
            else:
                result = node.task(node.params, context, execution.node_results)

            node.status = NodeStatus.COMPLETED
            node.result = result
            node.completed_at = time.time()

            return result

        except Exception as e:
            node.status = NodeStatus.FAILED
            node.error = str(e)
            node.completed_at = time.time()

            if node.retry_count > 0:
                for attempt in range(node.retry_count):
                    try:
                        await asyncio.sleep(2 ** attempt)
                        node.status = NodeStatus.RUNNING

                        if asyncio.iscoroutinefunction(node.task):
                            result = await node.task(node.params, context, execution.node_results)
                        else:
                            result = node.task(node.params, context, execution.node_results)

                        node.status = NodeStatus.COMPLETED
                        node.result = result
                        node.completed_at = time.time()
                        return result

                    except Exception:
                        continue

            raise

    def get_history(self, limit: int = 100) -> List[DAGExecution]:
        """Get execution history."""
        return self._execution_history[-limit:]

    def clear_history(self) -> None:
        """Clear execution history."""
        self._execution_history.clear()


class AutomationDAGAction(BaseAction):
    """Action for DAG automation operations."""

    def __init__(self):
        super().__init__("automation_dag")
        self._workflows: Dict[str, DAGWorkflow] = {}
        self._current_execution: Optional[DAGExecution] = None

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute DAG automation action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create_workflow(params)
            elif operation == "add_node":
                return self._add_node(params)
            elif operation == "validate":
                return self._validate(params)
            elif operation == "execute":
                return self._execute(params)
            elif operation == "topo_sort":
                return self._topo_sort(params)
            elif operation == "execution_order":
                return self._execution_order(params)
            elif operation == "get_workflow":
                return self._get_workflow(params)
            elif operation == "history":
                return self._get_history(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create_workflow(self, params: Dict[str, Any]) -> ActionResult:
        """Create a new DAG workflow."""
        dag_id = params.get("dag_id")
        name = params.get("name", "Unnamed Workflow")

        if not dag_id:
            return ActionResult(success=False, message="dag_id required")

        workflow = DAGWorkflow(dag_id, name)
        self._workflows[dag_id] = workflow

        return ActionResult(
            success=True,
            message=f"Workflow created: {dag_id}"
        )

    def _add_node(self, params: Dict[str, Any]) -> ActionResult:
        """Add a node to workflow."""
        dag_id = params.get("dag_id")
        node_id = params.get("node_id")
        name = params.get("name", "")
        dependencies = params.get("dependencies", [])

        if not dag_id or dag_id not in self._workflows:
            return ActionResult(success=False, message="Invalid dag_id")

        if not node_id:
            return ActionResult(success=False, message="node_id required")

        workflow = self._workflows[dag_id]

        def placeholder_task(params, context, results):
            return {"status": "completed", "node_id": node_id}

        workflow.add_node(
            node_id=node_id,
            name=name or node_id,
            task=placeholder_task,
            dependencies=dependencies,
            params=params.get("params", {}),
            retry_count=params.get("retry_count", 0)
        )

        return ActionResult(
            success=True,
            message=f"Node added: {node_id}"
        )

    def _validate(self, params: Dict[str, Any]) -> ActionResult:
        """Validate DAG structure."""
        dag_id = params.get("dag_id")

        if not dag_id or dag_id not in self._workflows:
            return ActionResult(success=False, message="Invalid dag_id")

        workflow = self._workflows[dag_id]
        valid, error = workflow.validate()

        return ActionResult(
            success=valid,
            message=error or "DAG is valid"
        )

    def _execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute DAG workflow."""
        dag_id = params.get("dag_id")

        if not dag_id or dag_id not in self._workflows:
            return ActionResult(success=False, message="Invalid dag_id")

        workflow = self._workflows[dag_id]

        execution = asyncio.run(
            workflow.execute(
                context=params.get("context"),
                stop_on_error=params.get("stop_on_error", True)
            )
        )

        self._current_execution = execution

        return ActionResult(
            success=execution.status == NodeStatus.COMPLETED,
            data={
                "execution_id": execution.execution_id,
                "status": execution.status.value,
                "node_results": execution.node_results,
                "errors": execution.errors,
                "duration_ms": (
                    execution.completed_at - execution.started_at
                ) * 1000 if execution.completed_at else None
            }
        )

    def _topo_sort(self, params: Dict[str, Any]) -> ActionResult:
        """Get topological sort of DAG."""
        dag_id = params.get("dag_id")

        if not dag_id or dag_id not in self._workflows:
            return ActionResult(success=False, message="Invalid dag_id")

        workflow = self._workflows[dag_id]
        order = workflow.topological_sort()

        return ActionResult(
            success=True,
            data={"order": order}
        )

    def _execution_order(self, params: Dict[str, Any]) -> ActionResult:
        """Get parallel execution order."""
        dag_id = params.get("dag_id")

        if not dag_id or dag_id not in self._workflows:
            return ActionResult(success=False, message="Invalid dag_id")

        workflow = self._workflows[dag_id]
        levels = workflow.get_execution_order()

        return ActionResult(
            success=True,
            data={"levels": levels}
        )

    def _get_workflow(self, params: Dict[str, Any]) -> ActionResult:
        """Get workflow details."""
        dag_id = params.get("dag_id")

        if not dag_id:
            return ActionResult(
                success=True,
                data={
                    "workflows": [
                        {"dag_id": w.dag_id, "name": w.name}
                        for w in self._workflows.values()
                    ]
                }
            )

        if dag_id not in self._workflows:
            return ActionResult(success=False, message="Workflow not found")

        workflow = self._workflows[dag_id]

        return ActionResult(
            success=True,
            data={
                "dag_id": workflow.dag_id,
                "name": workflow.name,
                "node_count": len(workflow._nodes),
                "nodes": [
                    {
                        "id": n.id,
                        "name": n.name,
                        "dependencies": n.dependencies,
                        "status": n.status.value
                    }
                    for n in workflow._nodes.values()
                ]
            }
        )

    def _get_history(self, params: Dict[str, Any]) -> ActionResult:
        """Get execution history."""
        dag_id = params.get("dag_id")
        limit = params.get("limit", 100)

        if dag_id and dag_id in self._workflows:
            history = self._workflows[dag_id].get_history(limit)
        else:
            history = []
            for workflow in self._workflows.values():
                history.extend(workflow.get_history(limit))

        return ActionResult(
            success=True,
            data={
                "history": [
                    {
                        "execution_id": e.execution_id,
                        "dag_id": e.dag_id,
                        "status": e.status.value,
                        "started_at": e.started_at
                    }
                    for e in history
                ]
            }
        )
