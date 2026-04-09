"""Automation BPMN Action Module.

Provides Business Process Model and Notation (BPMN) style
workflow execution with gates, parallel flows, and timers.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class FlowNodeType(Enum):
    """BPMN node types."""
    START = "start"
    END = "end"
    TASK = "task"
    GATEWAY_XOR = "gateway_xor"  # Exclusive gateway
    GATEWAY_AND = "gateway_and"  # Parallel gateway
    TIMER = "timer"
    SCRIPT_TASK = "script_task"
    SERVICE_TASK = "service_task"


class GatewayDirection(Enum):
    """Gateway branching directions."""
    DIVERGING = "diverging"
    CONVERGING = "converging"


@dataclass
class FlowNode:
    """A BPMN flow node."""
    node_id: str
    node_type: FlowNodeType
    name: str = ""
    outgoing: List[str] = field(default_factory=list)
    incoming: List[str] = field(default_factory=list)
    condition: Optional[str] = None  # For XOR gateway
    script: Optional[str] = None  # For script task
    timer_duration: Optional[float] = None  # For timer events
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Token:
    """A token representing workflow execution at a node."""
    token_id: str
    current_node: str
    variables: Dict[str, Any] = field(default_factory=dict)
    history: List[str] = field(default_factory=list)
    status: str = "active"  # active, completed, waiting


@dataclass
class BPMNProcess:
    """A BPMN process definition."""
    process_id: str
    name: str = ""
    nodes: Dict[str, FlowNode] = field(default_factory=dict)
    start_node: str = ""
    end_nodes: Set[str] = field(default_factory=set)


class AutomationBPMAction(BaseAction):
    """BPMN-style workflow execution action.

    Executes workflows modeled as BPMN processes with
    parallel gateways, XOR splits, timers, and token tracking.

    Args:
        context: Execution context.
        params: Dict with keys:
            - operation: Operation (define, execute, get_status, add_node)
            - process: Process definition dict
            - process_id: ID of process to execute
            - start_variables: Initial workflow variables
            - node_id: ID of node for add_node operation
    """
    action_type = "automation_bpmn"
    display_name = "BPMN工作流"
    description = "BPMN流程建模与执行引擎"

    def get_required_params(self) -> List[str]:
        return ["operation"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "process": None,
            "process_id": "default",
            "start_variables": {},
            "node_id": None,
            "node_definition": None,
        }

    def __init__(self) -> None:
        super().__init__()
        self._processes: Dict[str, BPMNProcess] = {}
        self._executions: Dict[str, List[Token]] = {}
        self._node_handlers: Dict[str, Callable] = {}

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute BPMN operation."""
        start_time = time.time()

        operation = params.get("operation", "status")
        process = params.get("process")
        process_id = params.get("process_id", "default")
        start_variables = params.get("start_variables", {})

        if operation == "define":
            return self._define_process(process, process_id, start_time)
        elif operation == "execute":
            return self._execute_process(process_id, start_variables, start_time)
        elif operation == "step":
            return self._step_execution(process_id, start_time)
        elif operation == "get_status":
            return self._get_execution_status(process_id, start_time)
        elif operation == "add_node":
            return self._add_node(
                process_id, params.get("node_id"), params.get("node_definition"), start_time
            )
        elif operation == "list_processes":
            return self._list_processes(start_time)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}",
                duration=time.time() - start_time
            )

    def _define_process(
        self,
        process: Optional[Dict],
        process_id: str,
        start_time: float
    ) -> ActionResult:
        """Define a new BPMN process."""
        if not process:
            return ActionResult(success=False, message="Process definition required", duration=time.time() - start_time)

        nodes = {}
        for node_data in process.get("nodes", []):
            node_type_str = node_data.get("type", "task")
            try:
                node_type = FlowNodeType(node_type_str)
            except ValueError:
                node_type = FlowNodeType.TASK

            node = FlowNode(
                node_id=node_data["id"],
                node_type=node_type,
                name=node_data.get("name", node_data["id"]),
                outgoing=node_data.get("outgoing", []),
                incoming=node_data.get("incoming", []),
                condition=node_data.get("condition"),
                script=node_data.get("script"),
                timer_duration=node_data.get("timer_duration"),
                metadata=node_data.get("metadata", {}),
            )
            nodes[node.node_id] = node

        start_node = process.get("start_node", "")
        end_nodes = set(process.get("end_nodes", []))

        bpmn_proc = BPMNProcess(
            process_id=process_id,
            name=process.get("name", process_id),
            nodes=nodes,
            start_node=start_node,
            end_nodes=end_nodes,
        )
        self._processes[process_id] = bpmn_proc

        return ActionResult(
            success=True,
            message=f"Process '{process_id}' defined with {len(nodes)} nodes",
            data={
                "process_id": process_id,
                "name": bpmn_proc.name,
                "nodes_count": len(nodes),
                "start_node": start_node,
                "end_nodes": list(end_nodes),
            },
            duration=time.time() - start_time
        )

    def _execute_process(
        self,
        process_id: str,
        start_variables: Dict[str, Any],
        start_time: float
    ) -> ActionResult:
        """Start or resume a process execution."""
        if process_id not in self._processes:
            return ActionResult(success=False, message=f"Process '{process_id}' not found", duration=time.time() - start_time)

        proc = self._processes[process_id]

        if process_id not in self._executions:
            self._executions[process_id] = []

        # Create initial token at start node
        token_id = f"token_{int(time.time() * 1000)}"
        token = Token(
            token_id=token_id,
            current_node=proc.start_node,
            variables=dict(start_variables),
            history=[proc.start_node],
        )
        self._executions[process_id].append(token)

        # Execute until we hit a gateway, timer, or end
        completed = self._advance_tokens(process_id)

        return ActionResult(
            success=True,
            message=f"Process '{process_id}' execution started",
            data={
                "process_id": process_id,
                "execution_id": token_id,
                "active_tokens": len([t for t in self._executions[process_id] if t.status == "active"]),
                "completed_tokens": len([t for t in self._executions[process_id] if t.status == "completed"]),
                "waiting_tokens": len([t for t in self._executions[process_id] if t.status == "waiting"]),
            },
            duration=time.time() - start_time
        )

    def _advance_tokens(self, process_id: str) -> int:
        """Advance tokens through the process. Returns number of tokens completed."""
        proc = self._processes[process_id]
        completed = 0

        for token in self._executions[process_id]:
            if token.status != "active":
                continue

            node = proc.nodes.get(token.current_node)
            if not node:
                token.status = "completed"
                completed += 1
                continue

            # Process node type
            if node.node_type == FlowNodeType.END:
                token.status = "completed"
                completed += 1
            elif node.node_type == FlowNodeType.TASK or node.node_type == FlowNodeType.SERVICE_TASK:
                # Execute task and move to next
                if node.outgoing:
                    next_node_id = node.outgoing[0]
                    token.current_node = next_node_id
                    token.history.append(next_node_id)
            elif node.node_type == FlowNodeType.GATEWAY_XOR:
                # Select one branch based on condition
                next_node_id = self._evaluate_xor_condition(node, token)
                if next_node_id and next_node_id in proc.nodes:
                    token.current_node = next_node_id
                    token.history.append(next_node_id)
                else:
                    token.status = "completed"
                    completed += 1
            elif node.node_type == FlowNodeType.GATEWAY_AND:
                # Fork to all outgoing
                for out_id in node.outgoing:
                    if out_id in proc.nodes:
                        new_token = Token(
                            token_id=f"{token.token_id}_branch_{out_id}",
                            current_node=out_id,
                            variables=dict(token.variables),
                            history=list(token.history) + [out_id],
                        )
                        self._executions[process_id].append(new_token)
                token.status = "completed"
                completed += 1
            elif node.node_type == FlowNodeType.TIMER:
                if node.timer_duration:
                    token.status = "waiting"
                elif node.outgoing:
                    next_node_id = node.outgoing[0]
                    token.current_node = next_node_id
                    token.history.append(next_node_id)
            else:
                # Default: move to next
                if node.outgoing:
                    next_node_id = node.outgoing[0]
                    token.current_node = next_node_id
                    token.history.append(next_node_id)

        return completed

    def _evaluate_xor_condition(self, node: FlowNode, token: Token) -> Optional[str]:
        """Evaluate XOR gateway condition."""
        # Default: take first outgoing
        if not node.condition:
            return node.outgoing[0] if node.outgoing else None

        # Try to evaluate condition expression
        try:
            condition = node.condition
            variables = token.variables
            # Simple variable substitution
            for var_name, var_value in variables.items():
                condition = condition.replace(f"${{{var_name}}}", str(var_value))
            # Very basic evaluation - in real impl, use a safe expression evaluator
            if condition == "true" or condition == "1":
                return node.outgoing[0] if node.outgoing else None
            return node.outgoing[1] if len(node.outgoing) > 1 else node.outgoing[0]
        except Exception:
            return node.outgoing[0] if node.outgoing else None

    def _step_execution(self, process_id: str, start_time: float) -> ActionResult:
        """Step through one or more nodes in execution."""
        if process_id not in self._processes:
            return ActionResult(success=False, message=f"Process '{process_id}' not found", duration=time.time() - start_time)

        completed = self._advance_tokens(process_id)
        return ActionResult(
            success=True,
            message=f"Stepped execution, {completed} tokens completed",
            data={"process_id": process_id, "completed_this_step": completed},
            duration=time.time() - start_time
        )

    def _get_execution_status(self, process_id: str, start_time: float) -> ActionResult:
        """Get execution status for a process."""
        if process_id not in self._executions:
            return ActionResult(success=True, message=f"No active execution for '{process_id}'", data={"process_id": process_id}, duration=time.time() - start_time)

        tokens = self._executions[process_id]
        active = [t for t in tokens if t.status == "active"]
        waiting = [t for t in tokens if t.status == "waiting"]
        completed = [t for t in tokens if t.status == "completed"]

        return ActionResult(
            success=True,
            message=f"Execution status for '{process_id}'",
            data={
                "process_id": process_id,
                "total_tokens": len(tokens),
                "active_tokens": len(active),
                "waiting_tokens": len(waiting),
                "completed_tokens": len(completed),
                "active_details": [
                    {"token_id": t.token_id, "current_node": t.current_node, "history": t.history}
                    for t in active[:10]
                ],
            },
            duration=time.time() - start_time
        )

    def _add_node(
        self,
        process_id: str,
        node_id: Optional[str],
        node_definition: Optional[Dict],
        start_time: float
    ) -> ActionResult:
        """Add a node to an existing process."""
        if process_id not in self._processes:
            return ActionResult(success=False, message=f"Process '{process_id}' not found", duration=time.time() - start_time)

        if not node_id or not node_definition:
            return ActionResult(success=False, message="node_id and node_definition required", duration=time.time() - start_time)

        proc = self._processes[process_id]
        node_type_str = node_definition.get("type", "task")
        try:
            node_type = FlowNodeType(node_type_str)
        except ValueError:
            node_type = FlowNodeType.TASK

        node = FlowNode(
            node_id=node_id,
            node_type=node_type,
            name=node_definition.get("name", node_id),
            outgoing=node_definition.get("outgoing", []),
            incoming=node_definition.get("incoming", []),
            condition=node_definition.get("condition"),
            script=node_definition.get("script"),
            timer_duration=node_definition.get("timer_duration"),
            metadata=node_definition.get("metadata", {}),
        )
        proc.nodes[node_id] = node

        return ActionResult(
            success=True,
            message=f"Node '{node_id}' added to process '{process_id}'",
            data={"process_id": process_id, "node_id": node_id, "total_nodes": len(proc.nodes)},
            duration=time.time() - start_time
        )

    def _list_processes(self, start_time: float) -> ActionResult:
        """List all defined processes."""
        return ActionResult(
            success=True,
            message="Processes listed",
            data={
                "processes": [
                    {"process_id": p.process_id, "name": p.name, "nodes_count": len(p.nodes)}
                    for p in self._processes.values()
                ]
            },
            duration=time.time() - start_time
        )
