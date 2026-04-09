"""
Automation Workflow DAG Action Module

Executes automation workflows as directed acyclic graphs with parallel execution.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import threading
from collections import defaultdict, deque


class NodeStatus(Enum):
    """Workflow node status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class NodeType(Enum):
    """Workflow node types."""
    TASK = "task"
    CONDITION = "condition"
    PARALLEL = "parallel"
    LOOP = "loop"
    SUBWORKFLOW = "subworkflow"
    MERGE = "merge"


@dataclass
class DAGNode:
    """Single node in the workflow DAG."""
    id: str
    name: str
    node_type: NodeType = NodeType.TASK
    task_fn: Optional[Callable] = None
    condition_fn: Optional[Callable[[Any], bool]] = None
    max_retries: int = 0
    timeout_seconds: Optional[float] = None
    config: Dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    status: NodeStatus = NodeStatus.PENDING
    result: Any = None
    error: Optional[str] = None


@dataclass
class DAGEdge:
    """Edge connecting nodes in the DAG."""
    from_node: str
    to_node: str
    condition: Optional[Callable[[Any], bool]] = None  # For conditional edges


@dataclass
class DAGExecutionResult:
    """Result of DAG execution."""
    success: bool
    completed_nodes: List[str]
    failed_nodes: List[str]
    node_results: Dict[str, Any]
    execution_time_ms: float
    error: Optional[str] = None


class TopologicalSorter:
    """Topological sort using Kahn's algorithm."""

    def __init__(self, nodes: List[DAGNode], edges: List[DAGEdge]):
        self.nodes = {n.id: n for n in nodes}
        self.edges = edges
        self.in_degree: Dict[str, int] = defaultdict(int)
        self.adjacency: Dict[str, List[str]] = defaultdict(list)

        for edge in edges:
            self.adjacency[edge.from_node].append(edge.to_node)
            self.in_degree[edge.to_node] += 1

    def sort(self) -> List[List[str]]:
        """
        Return nodes grouped by level (parallel execution groups).
        Level 0 has no dependencies, level 1 depends on level 0, etc.
        """
        levels: List[List[str]] = []
        remaining = set(self.nodes.keys())
        current_in_degree = self.in_degree.copy()

        while remaining:
            # Find all nodes with no remaining dependencies
            level = [node_id for node_id in remaining if current_in_degree[node_id] == 0]

            if not level:
                raise ValueError("Cycle detected in DAG")

            levels.append(level)

            # Remove these nodes and update dependencies
            for node_id in level:
                remaining.remove(node_id)
                for neighbor in self.adjacency[node_id]:
                    current_in_degree[neighbor] -= 1

        return levels


class AsyncDAGExecutor:
    """Executes DAG nodes asynchronously with dependency tracking."""

    def __init__(self, max_parallel: int = 10):
        self.max_parallel = max_parallel
        self.running: Set[str] = set()
        self.completed: Set[str] = set()
        self.semaphore: asyncio.Semaphore = None

    async def execute_level(
        self,
        level: List[str],
        node_map: Dict[str, DAGNode],
        context: Dict[str, Any]
    ) -> None:
        """Execute all nodes in a level concurrently."""
        self.semaphore = asyncio.Semaphore(self.max_parallel)

        async def execute_node(node_id: str) -> None:
            node = node_map[node_id]
            self.running.add(node_id)

            try:
                node.status = NodeStatus.RUNNING

                if node.task_fn:
                    result = await asyncio.wait_for(
                        node.task_fn(context, node.config),
                        timeout=node.timeout_seconds
                    )
                    node.result = result
                    context[node_id] = result

                node.status = NodeStatus.COMPLETED

            except asyncio.TimeoutError:
                node.status = NodeStatus.FAILED
                node.error = f"Timeout after {node.timeout_seconds}s"
            except Exception as e:
                node.status = NodeStatus.FAILED
                node.error = str(e)
            finally:
                self.running.remove(node_id)
                self.completed.add(node_id)

        await asyncio.gather(*[execute_node(n) for n in level])


class AutomationWorkflowDAGAction:
    """
    Executes automation workflows as directed acyclic graphs.

    Supports parallel execution, conditional branching, retries, and
    comprehensive error handling with detailed execution tracking.

    Example:
        dag = AutomationWorkflowDAGAction()
        dag.add_node("start", "Start", task_fn=start_task)
        dag.add_node("process", "Process", task_fn=process_task)
        dag.add_edge("start", "process")
        result = await dag.execute({"initial_data": "value"})
    """

    def __init__(self, max_parallel: int = 10):
        """Initialize DAG workflow executor."""
        self.nodes: Dict[str, DAGNode] = {}
        self.edges: List[DAGEdge] = []
        self.parallel_executor = AsyncDAGExecutor(max_parallel)
        self._execution_context: Dict[str, Any] = {}
        self._start_time: float = 0

    def add_node(
        self,
        node_id: str,
        name: str,
        node_type: NodeType = NodeType.TASK,
        task_fn: Optional[Callable] = None,
        condition_fn: Optional[Callable[[Any], bool]] = None,
        max_retries: int = 0,
        timeout_seconds: Optional[float] = None,
        config: Optional[Dict[str, Any]] = None
    ) -> "AutomationWorkflowDAGAction":
        """
        Add a node to the DAG.

        Args:
            node_id: Unique node identifier
            name: Human-readable name
            node_type: Type of node
            task_fn: Async function to execute
            condition_fn: Function to evaluate for conditional nodes
            max_retries: Number of retries on failure
            timeout_seconds: Execution timeout
            config: Node-specific configuration

        Returns:
            Self for chaining
        """
        self.nodes[node_id] = DAGNode(
            id=node_id,
            name=name,
            node_type=node_type,
            task_fn=task_fn,
            condition_fn=condition_fn,
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
            config=config or {}
        )
        return self

    def add_edge(
        self,
        from_node: str,
        to_node: str,
        condition: Optional[Callable[[Any], bool]] = None
    ) -> "AutomationWorkflowDAGAction":
        """
        Add an edge between nodes.

        Args:
            from_node: Source node ID
            to_node: Target node ID
            condition: Optional condition for conditional edges

        Returns:
            Self for chaining
        """
        if from_node not in self.nodes:
            raise ValueError(f"Source node not found: {from_node}")
        if to_node not in self.nodes:
            raise ValueError(f"Target node not found: {to_node}")

        self.edges.append(DAGEdge(
            from_node=from_node,
            to_node=to_node,
            condition=condition
        ))
        return self

    def get_root_nodes(self) -> List[str]:
        """Get nodes with no incoming edges."""
        targets = {e.to_node for e in self.edges}
        return [n for n in self.nodes.keys() if n not in targets]

    def get_leaf_nodes(self) -> List[str]:
        """Get nodes with no outgoing edges."""
        sources = {e.from_node for e in self.edges}
        return [n for n in self.nodes.keys() if n not in sources]

    def validate(self) -> List[str]:
        """
        Validate the DAG structure.

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        # Check for cycles
        try:
            sorter = TopologicalSorter(list(self.nodes.values()), self.edges)
            sorter.sort()
        except ValueError as e:
            errors.append(str(e))

        # Check for orphaned nodes
        connected_nodes = set()
        for edge in self.edges:
            connected_nodes.add(edge.from_node)
            connected_nodes.add(edge.to_node)

        for node_id in self.nodes:
            if node_id not in connected_nodes:
                if len(self.nodes) > 1:  # Allow single-node DAGs
                    errors.append(f"Orphaned node: {node_id}")

        # Check that all nodes have a path from root
        roots = self.get_root_nodes()
        if not roots and len(self.nodes) > 1:
            errors.append("No root nodes found")

        return errors

    async def execute(
        self,
        initial_context: Optional[Dict[str, Any]] = None,
        start_nodes: Optional[List[str]] = None
    ) -> DAGExecutionResult:
        """
        Execute the workflow DAG.

        Args:
            initial_context: Initial execution context
            start_nodes: Optional subset of nodes to execute

        Returns:
            DAGExecutionResult with execution details
        """
        self._start_time = time.time()
        self._execution_context = initial_context or {}

        # Validate first
        errors = self.validate()
        if errors:
            return DAGExecutionResult(
                success=False,
                completed_nodes=[],
                failed_nodes=[],
                node_results={},
                execution_time_ms=(time.time() - self._start_time) * 1000,
                error=f"Validation errors: {'; '.join(errors)}"
            )

        # Filter nodes if start_nodes specified
        if start_nodes:
            nodes_to_run = {nid: self.nodes[nid] for nid in start_nodes if nid in self.nodes}
        else:
            nodes_to_run = self.nodes

        # Topologically sort
        edges_to_use = [e for e in self.edges
                       if e.from_node in nodes_to_run and e.to_node in nodes_to_run]
        sorter = TopologicalSorter(list(nodes_to_run.values()), edges_to_use)

        try:
            levels = sorter.sort()
        except ValueError as e:
            return DAGExecutionResult(
                success=False,
                completed_nodes=[],
                failed_nodes=[],
                node_results={},
                execution_time_ms=(time.time() - self._start_time) * 1000,
                error=str(e)
            )

        # Execute level by level
        for level in levels:
            level_to_execute = [n for n in level if n in nodes_to_run]

            # Handle conditional edges - check if should skip
            for node_id in level_to_execute[:]:
                node = self.nodes[node_id]

                # Check if all incoming edges should allow this node
                incoming_edges = [e for e in self.edges if e.to_node == node_id]
                should_skip = False

                for edge in incoming_edges:
                    if edge.condition and edge.from_node in self._execution_context:
                        try:
                            if not edge.condition(self._execution_context[edge.from_node]):
                                should_skip = True
                                break
                        except Exception:
                            should_skip = True
                            break

                if should_skip:
                    node.status = NodeStatus.SKIPPED
                    level_to_execute.remove(node_id)

            if level_to_execute:
                await self.parallel_executor.execute_level(
                    level_to_execute,
                    self.nodes,
                    self._execution_context
                )

            # Check for failures - stop on critical failure
            failed = [n for n in level_to_execute
                     if self.nodes[n].status == NodeStatus.FAILED]
            if failed:
                break

        # Collect results
        completed = [n for n in self.nodes.values() if n.status == NodeStatus.COMPLETED]
        failed = [n for n in self.nodes.values() if n.status == NodeStatus.FAILED]

        return DAGExecutionResult(
            success=len(failed) == 0,
            completed_nodes=[n.id for n in completed],
            failed_nodes=[n.id for n in failed],
            node_results={n.id: n.result for n in completed},
            execution_time_ms=(time.time() - self._start_time) * 1000
        )

    def get_node_status(self, node_id: str) -> Optional[NodeStatus]:
        """Get current status of a node."""
        if node_id in self.nodes:
            return self.nodes[node_id].status
        return None

    def get_execution_summary(self) -> Dict[str, Any]:
        """Get a summary of the current execution state."""
        status_counts = defaultdict(int)
        for node in self.nodes.values():
            status_counts[node.status] += 1

        return {
            "total_nodes": len(self.nodes),
            "status_breakdown": dict(status_counts),
            "execution_context_keys": list(self._execution_context.keys()),
            "execution_time_ms": (time.time() - self._start_time) * 1000
        }
