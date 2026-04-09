"""
Automation Workflow Builder Action Module.

Visual workflow builder with drag-and-drop style node
connections, conditional branches, and parallel execution.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class NodeType(Enum):
    """Workflow node types."""
    ACTION = "action"
    CONDITION = "condition"
    PARALLEL = "parallel"
    LOOP = "loop"
    WAIT = "wait"
    TRANSFORM = "transform"
    MERGE = "merge"
    END = "end"


@dataclass
class WorkflowNode:
    """
    Workflow node definition.

    Attributes:
        node_id: Unique node identifier.
        name: Display name.
        node_type: Type of node.
        func: Function to execute.
        config: Node-specific configuration.
        position: (x, y) position for visual editor.
    """
    node_id: str
    name: str
    node_type: NodeType
    func: Optional[Callable] = None
    config: dict = field(default_factory=dict)
    position: tuple[int, int] = (0, 0)


@dataclass
class WorkflowEdge:
    """Connection between workflow nodes."""
    edge_id: str
    from_node: str
    to_node: str
    condition: Optional[Callable[[Any], bool]] = None
    label: str = ""


@dataclass
class WorkflowExecutionResult:
    """Result of workflow execution."""
    success: bool
    outputs: dict[str, Any]
    errors: dict[str, str]
    duration: float
    node_executions: dict[str, dict]


class AutomationWorkflowBuilderAction:
    """
    Workflow builder for creating automation flows.

    Example:
        builder = AutomationWorkflowBuilderAction()
        builder.add_node("start", NodeType.ACTION, start_func)
        builder.add_node("check", NodeType.CONDITION, check_func)
        builder.add_node("process", NodeType.ACTION, process_func)
        builder.connect("start", "check")
        builder.connect("check", "process", condition=is_valid)
    """

    def __init__(self, name: str = "workflow"):
        """
        Initialize workflow builder.

        Args:
            name: Workflow identifier.
        """
        self.name = name
        self.nodes: dict[str, WorkflowNode] = {}
        self.edges: list[WorkflowEdge] = []
        self._in_degree: dict[str, int] = {}
        self._out_edges: dict[str, list[str]] = {}

    def add_node(
        self,
        node_id: str,
        node_type: NodeType,
        func: Optional[Callable] = None,
        name: Optional[str] = None,
        config: Optional[dict] = None,
        position: tuple[int, int] = (0, 0)
    ) -> WorkflowNode:
        """
        Add a node to the workflow.

        Args:
            node_id: Unique identifier.
            node_type: Type of node.
            func: Function to execute.
            name: Display name.
            config: Node configuration dict.
            position: (x, y) for visual editor.

        Returns:
            Created WorkflowNode.
        """
        node = WorkflowNode(
            node_id=node_id,
            name=name or node_id,
            node_type=node_type,
            func=func,
            config=config or {},
            position=position
        )

        self.nodes[node_id] = node
        self._in_degree[node_id] = 0
        self._out_edges[node_id] = []

        logger.debug(f"Added node: {node_id} ({node_type.value})")
        return node

    def connect(
        self,
        from_node: str,
        to_node: str,
        condition: Optional[Callable[[Any], bool]] = None,
        label: str = ""
    ) -> WorkflowEdge:
        """
        Connect two nodes with an edge.

        Args:
            from_node: Source node ID.
            to_node: Target node ID.
            condition: Optional condition for routing.
            label: Edge label for display.

        Returns:
            Created WorkflowEdge.

        Raises:
            ValueError: If node IDs don't exist.
        """
        if from_node not in self.nodes:
            raise ValueError(f"Source node '{from_node}' does not exist")
        if to_node not in self.nodes:
            raise ValueError(f"Target node '{to_node}' does not exist")

        edge = WorkflowEdge(
            edge_id=f"{from_node}->{to_node}",
            from_node=from_node,
            to_node=to_node,
            condition=condition,
            label=label
        )

        self.edges.append(edge)
        self._in_degree[to_node] = self._in_degree.get(to_node, 0) + 1
        self._out_edges[from_node].append(to_node)

        logger.debug(f"Connected {from_node} -> {to_node}")
        return edge

    def get_start_nodes(self) -> list[str]:
        """Get nodes with no incoming edges."""
        return [node_id for node_id, degree in self._in_degree.items() if degree == 0]

    def get_end_nodes(self) -> list[str]:
        """Get nodes with no outgoing edges."""
        return [node_id for node_id in self.nodes if node_id not in self._out_edges or not self._out_edges[node_id]]

    def _topological_sort(self) -> list[str]:
        """Get topological order for execution."""
        in_deg = self._in_degree.copy()
        queue = [n for n in self.get_start_nodes() if self.nodes[n].node_type != NodeType.END]
        sorted_nodes = []

        while queue:
            node_id = queue.pop(0)
            sorted_nodes.append(node_id)

            for target in self._out_edges.get(node_id, []):
                in_deg[target] -= 1
                if in_deg[target] == 0 and self.nodes[target].node_type != NodeType.END:
                    queue.append(target)

        return sorted_nodes

    async def execute(self, initial_input: Any = None) -> WorkflowExecutionResult:
        """
        Execute the workflow.

        Args:
            initial_input: Initial input data.

        Returns:
            WorkflowExecutionResult with outputs and errors.
        """
        import time
        start_time = time.time()

        context = {"input": initial_input, "variables": {}, "errors": {}}
        node_outputs: dict[str, Any] = {}
        node_executions: dict[str, dict] = {}

        for node_id in self.nodes:
            node_executions[node_id] = {"status": "pending", "duration": 0.0}

        execution_order = self._topological_sort()
        logger.info(f"Executing workflow '{self.name}' in order: {execution_order}")

        for node_id in execution_order:
            node = self.nodes[node_id]
            node_start = time.time()

            logger.debug(f"Executing node: {node_id} ({node.node_type.value})")
            node_executions[node_id]["status"] = "running"

            try:
                if node.node_type == NodeType.ACTION:
                    if node.func:
                        if asyncio.iscoroutinefunction(node.func):
                            result = await node.func(context)
                        else:
                            result = node.func(context)
                    else:
                        result = context.get("input")

                    node_outputs[node_id] = result
                    context["variables"][node_id] = result
                    context["input"] = result

                elif node.node_type == NodeType.CONDITION:
                    if node.func:
                        cond_result = node.func(context)
                    else:
                        cond_result = context.get("input")

                    node_outputs[node_id] = cond_result
                    context["variables"][node_id] = cond_result

                    true_branch = self._find_edge_with_condition(node_id, True)
                    false_branch = self._find_edge_with_condition(node_id, False)

                    if cond_result and true_branch:
                        await self._execute_branch([true_branch], context, node_outputs, node_executions)
                    elif not cond_result and false_branch:
                        await self._execute_branch([false_branch], context, node_outputs, node_executions)

                elif node.node_type == NodeType.LOOP:
                    max_iterations = node.config.get("max_iterations", 100)
                    for i in range(max_iterations):
                        if node.func:
                            should_continue = node.func(context)
                        else:
                            should_continue = context.get("input")

                        if not should_continue:
                            break

                elif node.node_type == NodeType.WAIT:
                    wait_time = node.config.get("seconds", 1.0)
                    await asyncio.sleep(wait_time)

                elif node.node_type == NodeType.TRANSFORM:
                    if node.func:
                        context["input"] = node.func(context.get("input"))
                    node_outputs[node_id] = context["input"]

                node_executions[node_id]["status"] = "completed"

            except Exception as e:
                logger.error(f"Node {node_id} failed: {e}")
                node_executions[node_id]["status"] = "failed"
                node_executions[node_id]["error"] = str(e)
                context["errors"][node_id] = str(e)

            node_executions[node_id]["duration"] = time.time() - node_start

        duration = time.time() - start_time
        success = all(
            exec_data["status"] != "failed"
            for exec_data in node_executions.values()
        )

        return WorkflowExecutionResult(
            success=success,
            outputs=node_outputs,
            errors=context["errors"],
            duration=duration,
            node_executions=node_executions
        )

    async def _execute_branch(
        self,
        node_ids: list[str],
        context: dict,
        outputs: dict,
        executions: dict
    ) -> None:
        """Execute a branch of nodes."""
        import asyncio

        for node_id in node_ids:
            node = self.nodes[node_id]
            outputs[node_id] = None

            try:
                if node.func:
                    if asyncio.iscoroutinefunction(node.func):
                        result = await node.func(context)
                    else:
                        result = node.func(context)
                    outputs[node_id] = result
                    context["variables"][node_id] = result
            except Exception as e:
                logger.error(f"Branch node {node_id} failed: {e}")
                executions[node_id]["status"] = "failed"

    def _find_edge_with_condition(self, from_node: str, condition_value: bool) -> Optional[str]:
        """Find edge matching condition value."""
        for edge in self.edges:
            if edge.from_node == from_node and edge.condition:
                try:
                    if edge.condition(condition_value):
                        return edge.to_node
                except Exception:
                    pass
        return None

    def validate(self) -> tuple[bool, list[str]]:
        """
        Validate the workflow structure.

        Returns:
            Tuple of (is_valid, list_of_errors).
        """
        errors = []

        if not self.nodes:
            errors.append("Workflow has no nodes")
            return False, errors

        start_nodes = self.get_start_nodes()
        if not start_nodes:
            errors.append("No start nodes found")

        end_nodes = self.get_end_nodes()
        if not end_nodes:
            errors.append("No end nodes found")

        for edge in self.edges:
            if edge.from_node not in self.nodes:
                errors.append(f"Edge references non-existent node: {edge.from_node}")
            if edge.to_node not in self.nodes:
                errors.append(f"Edge references non-existent node: {edge.to_node}")

        try:
            self._topological_sort()
        except Exception as e:
            errors.append(f"Cyclic dependency detected: {e}")

        return len(errors) == 0, errors

    def get_diagram_data(self) -> dict:
        """Get workflow diagram data for visualization."""
        return {
            "name": self.name,
            "nodes": [
                {
                    "id": node.node_id,
                    "name": node.name,
                    "type": node.node_type.value,
                    "position": node.position,
                    "config": node.config
                }
                for node in self.nodes.values()
            ],
            "edges": [
                {
                    "id": edge.edge_id,
                    "from": edge.from_node,
                    "to": edge.to_node,
                    "label": edge.label
                }
                for edge in self.edges
            ]
        }
