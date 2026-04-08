"""Workflow Engine action module for RabAI AutoClick.

Provides workflow orchestration with DAG-based execution,
conditional branching, parallel execution, and state management.
"""

import sys
import os
import json
import time
import uuid
import asyncio
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class NodeType(Enum):
    """Workflow node types."""
    TASK = "task"
    CONDITIONAL = "conditional"
    PARALLEL = "parallel"
    MERGE = "merge"
    START = "start"
    END = "end"


class NodeStatus(Enum):
    """Node execution status."""
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    SKIPPED = "skipped"
    FAILED = "failed"


@dataclass
class WorkflowNode:
    """Represents a node in the workflow DAG."""
    node_id: str
    name: str
    node_type: NodeType
    config: Dict[str, Any] = field(default_factory=dict)
    depends_on: List[str] = field(default_factory=list)  # Node IDs
    condition: Optional[str] = None  # JS-like expression for conditional
    retry_count: int = 0
    timeout_seconds: float = 300.0
    description: str = ""
    
    # Runtime
    status: NodeStatus = NodeStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None


@dataclass
class Workflow:
    """Represents a workflow with nodes and edges."""
    workflow_id: str
    name: str
    nodes: List[WorkflowNode] = field(default_factory=dict)
    entry_point: Optional[str] = None
    description: str = ""
    
    # Runtime state
    node_map: Dict[str, WorkflowNode] = field(default_factory=dict)
    execution_order: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        if isinstance(self.nodes, list):
            self.node_map = {n.node_id: n for n in self.nodes}
            self.nodes = self.node_map


class WorkflowEngine:
    """DAG-based workflow execution engine."""
    
    def __init__(self, persistence_path: Optional[str] = None):
        self._workflows: Dict[str, Workflow] = {}
        self._executions: Dict[str, Dict[str, Any]] = {}
        self._node_functions: Dict[str, Callable] = {}
        self._persistence_path = persistence_path
    
    def register_workflow(self, workflow: Workflow) -> None:
        """Register a workflow definition."""
        # Build node map
        workflow.node_map = {n.node_id: n for n in workflow.nodes}
        self._workflows[workflow.workflow_id] = workflow
    
    def register_node_function(self, workflow_id: str, node_id: str, 
                              func: Callable) -> None:
        """Register an executable function for a workflow node."""
        key = f"{workflow_id}:{node_id}"
        self._node_functions[key] = func
    
    async def execute_workflow_async(
        self,
        workflow_id: str,
        start_data: Any = None
    ) -> Dict[str, Any]:
        """Execute a workflow by ID."""
        workflow = self._workflows.get(workflow_id)
        if not workflow:
            raise ValueError(f"Workflow '{workflow_id}' not found")
        
        # Reset node statuses
        for node in workflow.nodes:
            node.status = NodeStatus.PENDING
            node.result = None
            node.error = None
        
        # Topological sort for execution order
        execution_order = self._topological_sort(workflow)
        results = {"workflow_id": workflow_id, "status": "running", 
                   "node_results": {}, "start_time": time.time()}
        
        current_data = start_data
        
        for node_id in execution_order:
            node = workflow.node_map[node_id]
            
            # Check dependencies
            deps_completed = all(
                workflow.node_map(dep).status == NodeStatus.COMPLETED
                for dep in node.depends_on
            )
            if not deps_completed:
                node.status = NodeStatus.SKIPPED
                continue
            
            # Evaluate condition if conditional node
            if node.node_type == NodeType.CONDITIONAL:
                if not self._evaluate_condition(node.condition, current_data):
                    node.status = NodeStatus.SKIPPED
                    continue
            
            # Execute node
            node.status = NodeStatus.RUNNING
            node.started_at = time.time()
            
            try:
                func_key = f"{workflow_id}:{node_id}"
                func = self._node_functions.get(func_key)
                
                if node.node_type == NodeType.TASK and func:
                    if asyncio.iscoroutinefunction(func):
                        node.result = await func(current_data, node)
                    else:
                        node.result = func(current_data, node)
                elif node.node_type == NodeType.PARALLEL:
                    # Execute children in parallel
                    node.result = await self._execute_parallel(node, current_data)
                else:
                    node.result = current_data
                
                node.status = NodeStatus.COMPLETED
                current_data = node.result
            
            except Exception as e:
                node.status = NodeStatus.FAILED
                node.error = str(e)
                if node.retry_count > 0:
                    # Retry logic
                    for attempt in range(node.retry_count):
                        try:
                            node.status = NodeStatus.RUNNING
                            node.result = await func(current_data, node)
                            node.status = NodeStatus.COMPLETED
                            break
                        except:
                            pass
                    else:
                        results["status"] = "failed"
                        results["failed_node"] = node_id
                        results["error"] = node.error
                else:
                    results["status"] = "failed"
                    results["failed_node"] = node_id
                    results["error"] = node.error
            
            node.completed_at = time.time()
            results["node_results"][node_id] = {
                "status": node.status.value,
                "result": node.result,
                "error": node.error
            }
            
            if node.status == NodeStatus.FAILED:
                break
        
        results["end_time"] = time.time()
        return results
    
    def _topological_sort(self, workflow: Workflow) -> List[str]:
        """Perform topological sort on workflow DAG."""
        visited = set()
        order = []
        
        def visit(node_id: str):
            if node_id in visited:
                return
            visited.add(node_id)
            node = workflow.node_map[node_id]
            for dep in node.depends_on:
                visit(dep)
            order.append(node_id)
        
        for node in workflow.nodes:
            if node.node_type == NodeType.START or not node.depends_on:
                visit(node.node_id)
        
        return order
    
    def _evaluate_condition(self, condition: Optional[str], 
                           context: Any) -> bool:
        """Evaluate a condition expression."""
        if not condition:
            return True
        
        try:
            # Simple expression evaluation
            ctx = {"data": context} if context else {}
            return bool(eval(condition, {"__builtins__": {}}, ctx))
        except Exception:
            return True  # Default to true if evaluation fails
    
    async def _execute_parallel(self, node: WorkflowNode, 
                               data: Any) -> List[Any]:
        """Execute parallel branch nodes."""
        child_ids = node.config.get("parallel_nodes", [])
        results = []
        tasks = []
        
        # In real implementation, would spawn concurrent tasks
        for child_id in child_ids:
            tasks.append(self._execute_child_node(child_id, data))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if not isinstance(r, Exception)]
    
    async def _execute_child_node(self, node_id: str, data: Any) -> Any:
        """Execute a child node in parallel branch."""
        # Simplified - would look up node and execute
        return {"node_id": node_id, "data": data}


class WorkflowEngineAction(BaseAction):
    """Execute DAG-based workflows with conditional branching.
    
    Supports workflow definition, node execution, parallel branches,
    error handling, and state propagation.
    """
    action_type = "workflow_engine"
    display_name = "工作流引擎"
    description = "DAG工作流执行引擎，支持条件分支和并行执行"
    
    def __init__(self):
        super().__init__()
        self._engine = WorkflowEngine()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute workflow operation."""
        operation = params.get("operation", "")
        
        try:
            if operation == "register":
                return self._register_workflow(params)
            elif operation == "execute":
                return self._execute_workflow(params)
            elif operation == "list":
                return self._list_workflows(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _register_workflow(self, params: Dict[str, Any]) -> ActionResult:
        """Register a workflow."""
        workflow_id = params.get("workflow_id", str(uuid.uuid4()))
        name = params.get("name", "")
        nodes_data = params.get("nodes", [])
        
        nodes = []
        for nd in nodes_data:
            nodes.append(WorkflowNode(
                node_id=nd["node_id"],
                name=nd["name"],
                node_type=NodeType(nd["node_type"]),
                depends_on=nd.get("depends_on", []),
                config=nd.get("config", {})
            ))
        
        workflow = Workflow(workflow_id=workflow_id, name=name, nodes=nodes)
        self._engine.register_workflow(workflow)
        return ActionResult(success=True, message=f"Workflow '{name}' registered")
    
    def _execute_workflow(self, params: Dict[str, Any]) -> ActionResult:
        """Execute a workflow."""
        workflow_id = params.get("workflow_id", "")
        start_data = params.get("start_data")
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            results = loop.run_until_complete(
                self._engine.execute_workflow_async(workflow_id, start_data)
            )
            return ActionResult(
                success=results["status"] == "completed",
                message=f"Workflow {results['status']}",
                data=results
            )
        finally:
            loop.close()
    
    def _list_workflows(self, params: Dict[str, Any]) -> ActionResult:
        """List workflows."""
        workflows = list(self._engine._workflows.keys())
        return ActionResult(success=True, message=f"{len(workflows)} workflows", data={"workflows": workflows})
