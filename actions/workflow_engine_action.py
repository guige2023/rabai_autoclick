"""
Workflow Engine Action Module

DAG-based workflow orchestration with parallel execution,
conditional branching, and error handling. Supports long-running workflows.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class NodeStatus(Enum):
    """Workflow node status."""
    
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class WorkflowStatus(Enum):
    """Workflow execution status."""
    
    CREATED = "created"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


@dataclass
class WorkflowNode:
    """A single node in the workflow DAG."""
    
    id: str
    name: str
    action: Callable
    dependencies: List[str] = field(default_factory=list)
    condition: Optional[Callable] = None
    timeout_seconds: float = 300
    retry_count: int = 0
    max_retries: int = 3
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    status: NodeStatus = NodeStatus.PENDING
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    result: Any = None
    error: Optional[str] = None


@dataclass
class Workflow:
    """Complete workflow definition."""
    
    id: str
    name: str
    nodes: Dict[str, WorkflowNode] = field(default_factory=dict)
    entry_point: Optional[str] = None
    
    status: WorkflowStatus = WorkflowStatus.CREATED
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    results: Dict[str, Any] = field(default_factory=dict)
    errors: Dict[str, str] = field(default_factory=dict)


class DAGExecutor:
    """Executes workflows as directed acyclic graphs."""
    
    def __init__(self):
        self._running_nodes: Set[str] = set()
        self._completed_nodes: Set[str] = set()
        self._lock = asyncio.Lock()
    
    def validate(self, workflow: Workflow) -> tuple[bool, List[str]]:
        """Validate workflow DAG for cycles and missing dependencies."""
        errors = []
        
        for node_id, node in workflow.nodes.items():
            for dep in node.dependencies:
                if dep not in workflow.nodes:
                    errors.append(f"Node {node_id} depends on missing node {dep}")
        
        if self._has_cycle(workflow):
            errors.append("Workflow contains a cycle")
        
        return len(errors) == 0, errors
    
    def _has_cycle(self, workflow: Workflow) -> bool:
        """Check if workflow contains a cycle."""
        visited = set()
        rec_stack = set()
        
        def dfs(node_id: str) -> bool:
            visited.add(node_id)
            rec_stack.add(node_id)
            
            node = workflow.nodes.get(node_id)
            if node:
                for dep in node.dependencies:
                    if dep not in visited:
                        if dfs(dep):
                            return True
                    elif dep in rec_stack:
                        return True
            
            rec_stack.remove(node_id)
            return False
        
        for node_id in workflow.nodes:
            if node_id not in visited:
                if dfs(node_id):
                    return True
        
        return False
    
    def get_executable_nodes(
        self,
        workflow: Workflow
    ) -> List[WorkflowNode]:
        """Get all nodes that are ready to execute."""
        executable = []
        
        for node_id, node in workflow.nodes.items():
            if node.status != NodeStatus.PENDING:
                continue
            
            if node_id in self._completed_nodes:
                continue
            
            deps_satisfied = all(
                dep in self._completed_nodes
                for dep in node.dependencies
            )
            
            if deps_satisfied:
                executable.append(node)
        
        return executable


class WorkflowEngine:
    """Core workflow execution engine."""
    
    def __init__(self):
        self.dag = DAGExecutor()
        self._workflows: Dict[str, Workflow] = {}
        self._running: Dict[str, asyncio.Task] = {}
        self._node_handlers: Dict[str, Callable] = {}
    
    def create_workflow(
        self,
        name: str,
        nodes: List[WorkflowNode],
        entry_point: Optional[str] = None
    ) -> str:
        """Create a new workflow."""
        workflow_id = str(uuid.uuid4())
        
        nodes_dict = {node.id: node for node in nodes}
        
        workflow = Workflow(
            id=workflow_id,
            name=name,
            nodes=nodes_dict,
            entry_point=entry_point or (nodes[0].id if nodes else None)
        )
        
        valid, errors = self.dag.validate(workflow)
        if not valid:
            raise ValueError(f"Invalid workflow: {errors}")
        
        self._workflows[workflow_id] = workflow
        return workflow_id
    
    async def execute(
        self,
        workflow_id: str,
        initial_data: Optional[Dict] = None
    ) -> Workflow:
        """Execute a workflow."""
        workflow = self._workflows.get(workflow_id)
        if not workflow:
            raise ValueError(f"Workflow {workflow_id} not found")
        
        workflow.status = WorkflowStatus.RUNNING
        workflow.started_at = time.time()
        
        context = initial_data or {}
        workflow.results = {"_context": context}
        
        try:
            await self._execute_nodes(workflow)
            
            if workflow.errors:
                workflow.status = WorkflowStatus.FAILED
            else:
                workflow.status = WorkflowStatus.COMPLETED
        
        except Exception as e:
            workflow.status = WorkflowStatus.FAILED
            workflow.errors["_execution"] = str(e)
        
        finally:
            workflow.completed_at = time.time()
        
        return workflow
    
    async def _execute_nodes(self, workflow: Workflow) -> None:
        """Execute workflow nodes respecting dependencies."""
        while True:
            executable = self.dag.get_executable_nodes(workflow)
            
            if not executable:
                if workflow.errors:
                    break
                all_done = all(
                    node.status in (NodeStatus.COMPLETED, NodeStatus.SKIPPED)
                    for node in workflow.nodes.values()
                )
                if all_done:
                    break
                await asyncio.sleep(0.1)
                continue
            
            tasks = [
                self._execute_node(workflow, node)
                for node in executable
            ]
            
            await asyncio.gather(*tasks, return_exceptions=True)
            
            await asyncio.sleep(0.01)
    
    async def _execute_node(
        self,
        workflow: Workflow,
        node: WorkflowNode
    ) -> None:
        """Execute a single workflow node."""
        node.status = NodeStatus.RUNNING
        node.started_at = time.time()
        
        try:
            if node.condition:
                should_run = node.condition(workflow.results.get("_context", {}), workflow.results)
                if not should_run:
                    node.status = NodeStatus.SKIPPED
                    return
            
            result = await asyncio.wait_for(
                self._run_node_action(node),
                timeout=node.timeout_seconds
            )
            
            node.result = result
            node.status = NodeStatus.COMPLETED
            node.completed_at = time.time()
            workflow.results[node.id] = result
        
        except asyncio.TimeoutError:
            node.status = NodeStatus.FAILED
            node.error = f"Timeout after {node.timeout_seconds}s"
            workflow.errors[node.id] = node.error
        
        except Exception as e:
            node.status = NodeStatus.FAILED
            node.error = str(e)
            workflow.errors[node.id] = str(e)
            
            if node.retry_count < node.max_retries:
                node.retry_count += 1
                node.status = NodeStatus.PENDING
    
    async def _run_node_action(self, node: WorkflowNode) -> Any:
        """Run the node's action function."""
        if asyncio.iscoroutinefunction(node.action):
            return await node.action(workflow_results)
        return node.action()


class WorkflowEngineAction:
    """
    Main workflow engine action handler.
    
    Provides DAG-based workflow orchestration with support
    for parallel execution, conditional branching, and error handling.
    """
    
    def __init__(self):
        self.engine = WorkflowEngine()
        self._middleware: List[Callable] = []
    
    def define_workflow(
        self,
        name: str,
        nodes: List[Dict],
        entry_point: Optional[str] = None
    ) -> str:
        """Define a workflow from configuration."""
        workflow_nodes = []
        
        for node_config in nodes:
            node = WorkflowNode(
                id=node_config["id"],
                name=node_config["name"],
                action=self._middleware or (lambda: None),
                dependencies=node_config.get("dependencies", []),
                condition=node_config.get("condition"),
                timeout_seconds=node_config.get("timeout", 300),
                max_retries=node_config.get("max_retries", 3),
                metadata=node_config.get("metadata", {})
            )
            workflow_nodes.append(node)
        
        return self.engine.create_workflow(name, workflow_nodes, entry_point)
    
    async def run(
        self,
        workflow_id: str,
        initial_data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Run a workflow."""
        workflow = await self.engine.execute(workflow_id, initial_data)
        
        return {
            "workflow_id": workflow.id,
            "name": workflow.name,
            "status": workflow.status.value,
            "results": workflow.results,
            "errors": workflow.errors,
            "duration_ms": (
                (workflow.completed_at - workflow.started_at) * 1000
                if workflow.completed_at and workflow.started_at else 0
            )
        }
    
    async def cancel(self, workflow_id: str) -> bool:
        """Cancel a running workflow."""
        workflow = self.engine._workflows.get(workflow_id)
        if workflow:
            workflow.status = WorkflowStatus.CANCELLED
            return True
        return False
    
    def get_workflow(self, workflow_id: str) -> Optional[Dict]:
        """Get workflow status and details."""
        workflow = self.engine._workflows.get(workflow_id)
        if not workflow:
            return None
        
        return {
            "id": workflow.id,
            "name": workflow.name,
            "status": workflow.status.value,
            "nodes": {
                node_id: {
                    "name": node.name,
                    "status": node.status.value,
                    "started_at": node.started_at,
                    "completed_at": node.completed_at,
                    "error": node.error
                }
                for node_id, node in workflow.nodes.items()
            },
            "created_at": workflow.created_at,
            "started_at": workflow.started_at,
            "completed_at": workflow.completed_at
        }
    
    def list_workflows(self) -> List[Dict]:
        """List all workflows."""
        return [
            {
                "id": wf.id,
                "name": wf.name,
                "status": wf.status.value,
                "created_at": wf.created_at
            }
            for wf in self.engine._workflows.values()
        ]


workflow_results = {}
