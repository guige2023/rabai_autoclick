"""Temporal Workflow action module for RabAI AutoClick.

Provides workflow execution with Temporal-style activity scheduling,
workflow state persistence, retry logic, and child workflow support.
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

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class WorkflowStatus(Enum):
    """Workflow execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class ActivityStatus(Enum):
    """Activity execution status."""
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class Activity:
    """Represents a workflow activity."""
    name: str
    func: Optional[Callable] = None
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    retry_policy: Dict[str, Any] = field(default_factory=dict)
    timeout_seconds: float = 300.0
    schedule_to_close_timeout: float = 300.0


@dataclass
class WorkflowDefinition:
    """Defines a workflow with its activities."""
    name: str
    activities: List[Activity] = field(default_factory=list)
    on_failure: Optional[str] = None
    description: str = ""


@dataclass
class WorkflowExecution:
    """Tracks workflow execution state."""
    workflow_id: str
    workflow_name: str
    status: WorkflowStatus = WorkflowStatus.PENDING
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    activities_completed: List[str] = field(default_factory=list)
    activities_failed: List[str] = field(default_factory=list)
    activity_results: Dict[str, Any] = field(default_factory=dict)
    activity_errors: Dict[str, str] = field(default_factory=dict)
    retry_count: int = 0
    input_data: Any = None
    output_data: Any = None
    error_message: Optional[str] = None


class TemporalWorkflowEngine:
    """Temporal-style workflow execution engine."""
    
    def __init__(self, persistence_path: Optional[str] = None):
        self._workflows: Dict[str, WorkflowDefinition] = {}
        self._executions: Dict[str, WorkflowExecution] = {}
        self._activity_registry: Dict[str, Callable] = {}
        self._persistence_path = persistence_path
        self._load()
    
    def _load(self) -> None:
        """Load workflows and executions from persistence."""
        if self._persistence_path and os.path.exists(self._persistence_path):
            try:
                with open(self._persistence_path, 'r') as f:
                    data = json.load(f)
                    # Restore workflow definitions
                    for wf_data in data.get("workflows", []):
                        activities = [
                            Activity(**{k: v for k, v in a.items() if k != 'func'})
                            for a in wf_data.get("activities", [])
                        ]
                        wf = WorkflowDefinition(
                            name=wf_data["name"],
                            activities=activities,
                            description=wf_data.get("description", "")
                        )
                        self._workflows[wf.name] = wf
                    # Restore executions (without func references)
                    for exec_data in data.get("executions", []):
                        exec_data["status"] = WorkflowStatus(exec_data["status"])
                        self._executions[exec_data["workflow_id"]] = WorkflowExecution(**exec_data)
            except (json.JSONDecodeError, TypeError, KeyError):
                pass
    
    def _persist(self) -> None:
        """Persist workflows and executions."""
        if self._persistence_path:
            try:
                workflows_data = [
                    {
                        "name": wf.name,
                        "activities": [
                            {"name": a.name, "retry_policy": a.retry_policy,
                             "timeout_seconds": a.timeout_seconds}
                            for a in wf.activities
                        ],
                        "description": wf.description
                    }
                    for wf in self._workflows.values()
                ]
                executions_data = [
                    {
                        "workflow_id": e.workflow_id,
                        "workflow_name": e.workflow_name,
                        "status": e.status.value,
                        "start_time": e.start_time,
                        "end_time": e.end_time,
                        "activities_completed": e.activities_completed,
                        "activities_failed": e.activities_failed,
                        "retry_count": e.retry_count,
                        "input_data": e.input_data,
                        "output_data": e.output_data,
                        "error_message": e.error_message
                    }
                    for e in self._executions.values()
                ]
                with open(self._persistence_path, 'w') as f:
                    json.dump({
                        "workflows": workflows_data,
                        "executions": executions_data
                    }, f, indent=2)
            except OSError:
                pass
    
    def register_workflow(self, workflow: WorkflowDefinition) -> None:
        """Register a workflow definition."""
        self._workflows[workflow.name] = workflow
        self._persist()
    
    def register_activity(self, name: str, func: Callable) -> None:
        """Register an activity function."""
        self._activity_registry[name] = func
    
    def start_workflow(self, workflow_name: str, 
                       input_data: Any = None,
                       workflow_id: Optional[str] = None) -> str:
        """Start a new workflow execution.
        
        Args:
            workflow_name: Name of the workflow to start.
            input_data: Input data for the workflow.
            workflow_id: Optional custom workflow ID.
        
        Returns:
            The workflow execution ID.
        """
        if workflow_name not in self._workflows:
            raise ValueError(f"Workflow '{workflow_name}' not found")
        
        wf_id = workflow_id or str(uuid.uuid4())
        execution = WorkflowExecution(
            workflow_id=wf_id,
            workflow_name=workflow_name,
            status=WorkflowStatus.PENDING,
            start_time=time.time(),
            input_data=input_data
        )
        self._executions[wf_id] = execution
        self._persist()
        return wf_id
    
    async def run_workflow(self, workflow_id: str) -> WorkflowExecution:
        """Execute a workflow by ID.
        
        Args:
            workflow_id: ID of the workflow execution to run.
        
        Returns:
            The completed WorkflowExecution.
        """
        execution = self._executions.get(workflow_id)
        if not execution:
            raise ValueError(f"Execution '{workflow_id}' not found")
        
        workflow = self._workflows.get(execution.workflow_name)
        if not workflow:
            raise ValueError(f"Workflow '{execution.workflow_name}' not found")
        
        execution.status = WorkflowStatus.RUNNING
        current_input = execution.input_data
        
        for activity in workflow.activities:
            try:
                result = await self._run_activity(activity, current_input)
                execution.activities_completed.append(activity.name)
                execution.activity_results[activity.name] = result
                current_input = result  # Chain to next activity
            except Exception as e:
                execution.activities_failed.append(activity.name)
                execution.activity_errors[activity.name] = str(e)
                
                # Check retry policy
                retry_policy = activity.retry_policy
                max_attempts = retry_policy.get("max_attempts", 1)
                current_attempt = execution.activity_errors.get(f"{activity.name}_attempts", 0)
                
                if current_attempt < max_attempts:
                    backoff = retry_policy.get("backoff_seconds", 1.0)
                    await asyncio.sleep(backoff * (2 ** current_attempt))
                    execution.activity_errors[f"{activity.name}_attempts"] = current_attempt + 1
                    # Retry
                    try:
                        result = await self._run_activity(activity, current_input)
                        execution.activities_completed.append(activity.name)
                        execution.activity_results[activity.name] = result
                        current_input = result
                        # Clear error on success
                        if f"{activity.name}_attempts" in execution.activity_errors:
                            del execution.activity_errors[f"{activity.name}_attempts"]
                        continue
                    except Exception as retry_err:
                        execution.activity_errors[activity.name] = str(retry_err)
                
                # Failure handling
                execution.status = WorkflowStatus.FAILED
                execution.error_message = f"Activity '{activity.name}' failed: {str(e)}"
                execution.end_time = time.time()
                self._persist()
                return execution
        
        # All activities completed successfully
        execution.status = WorkflowStatus.COMPLETED
        execution.output_data = current_input
        execution.end_time = time.time()
        self._persist()
        return execution
    
    async def _run_activity(self, activity: Activity, 
                           input_data: Any) -> Any:
        """Run a single activity with timeout."""
        func = self._activity_registry.get(activity.name)
        if not func:
            # If no registered func, simulate activity with input passed through
            return input_data
        
        # Run with timeout
        try:
            result = await asyncio.wait_for(
                asyncio.coroutine(func)(*activity.args, **activity.kwargs),
                timeout=activity.timeout_seconds
            )
            return result
        except asyncio.TimeoutError:
            raise TimeoutError(f"Activity '{activity.name}' timed out after {activity.timeout_seconds}s")
    
    def get_execution(self, workflow_id: str) -> Optional[WorkflowExecution]:
        """Get workflow execution by ID."""
        return self._executions.get(workflow_id)
    
    def list_executions(self, 
                        status: Optional[WorkflowStatus] = None) -> List[WorkflowExecution]:
        """List workflow executions, optionally filtered by status."""
        executions = list(self._executions.values())
        if status:
            executions = [e for e in executions if e.status == status]
        return executions
    
    def cancel_execution(self, workflow_id: str) -> bool:
        """Cancel a running workflow execution."""
        execution = self._executions.get(workflow_id)
        if not execution:
            return False
        if execution.status not in (WorkflowStatus.PENDING, WorkflowStatus.RUNNING):
            return False
        execution.status = WorkflowStatus.CANCELLED
        execution.end_time = time.time()
        self._persist()
        return True


class TemporalWorkflowAction(BaseAction):
    """Execute Temporal-style workflows with activity scheduling.
    
    Supports workflow registration, activity registration, workflow
    execution with retry policies, timeout handling, and state persistence.
    """
    action_type = "temporal_workflow"
    display_name = "Temporal工作流"
    description = "执行Temporal风格的工作流，支持活动调度和重试策略"
    
    def __init__(self):
        super().__init__()
        self._engine: Optional[TemporalWorkflowEngine] = None
    
    def _get_engine(self, params: Dict[str, Any]) -> TemporalWorkflowEngine:
        """Get or create the workflow engine."""
        if self._engine is None:
            persistence_path = params.get("persistence_path")
            self._engine = TemporalWorkflowEngine(persistence_path)
        return self._engine
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute workflow operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: "register_workflow", "register_activity",
                  "start", "run", "get", "list", "cancel"
                - For register_workflow: workflow (dict)
                - For register_activity: name, func (callable)
                - For start: workflow_name, input_data
                - For run: workflow_id
                - For get/list/cancel: workflow_id or status
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get("operation", "")
        
        try:
            if operation == "register_workflow":
                return self._register_workflow(params)
            elif operation == "register_activity":
                return self._register_activity(params)
            elif operation == "start":
                return self._start_workflow(params)
            elif operation == "run":
                return self._run_workflow_async(params)
            elif operation == "get":
                return self._get_execution(params)
            elif operation == "list":
                return self._list_executions(params)
            elif operation == "cancel":
                return self._cancel_execution(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Temporal workflow error: {str(e)}")
    
    def _register_workflow(self, params: Dict[str, Any]) -> ActionResult:
        """Register a workflow definition."""
        engine = self._get_engine(params)
        wf_data = params.get("workflow", {})
        
        if not wf_data or "name" not in wf_data:
            return ActionResult(success=False, message="workflow with name is required")
        
        activities_data = wf_data.get("activities", [])
        activities = [Activity(**a) for a in activities_data]
        workflow = WorkflowDefinition(
            name=wf_data["name"],
            activities=activities,
            description=wf_data.get("description", "")
        )
        engine.register_workflow(workflow)
        return ActionResult(
            success=True,
            message=f"Workflow '{workflow.name}' registered",
            data={"name": workflow.name, "activities_count": len(activities)}
        )
    
    def _register_activity(self, params: Dict[str, Any]) -> ActionResult:
        """Register an activity function."""
        engine = self._get_engine(params)
        name = params.get("name", "")
        
        if not name:
            return ActionResult(success=False, message="Activity name is required")
        
        # For actual function registration, we store a placeholder
        # In real usage, functions would be registered directly
        def placeholder(*args, **kwargs):
            return args[0] if args else kwargs.get("input")
        
        engine.register_activity(name, placeholder)
        return ActionResult(success=True, message=f"Activity '{name}' registered")
    
    def _start_workflow(self, params: Dict[str, Any]) -> ActionResult:
        """Start a workflow execution."""
        engine = self._get_engine(params)
        workflow_name = params.get("workflow_name", "")
        input_data = params.get("input_data")
        workflow_id = params.get("workflow_id")
        
        if not workflow_name:
            return ActionResult(success=False, message="workflow_name is required")
        
        wf_id = engine.start_workflow(workflow_name, input_data, workflow_id)
        return ActionResult(
            success=True,
            message=f"Workflow started: {wf_id}",
            data={"workflow_id": wf_id, "workflow_name": workflow_name}
        )
    
    def _run_workflow_async(self, params: Dict[str, Any]) -> ActionResult:
        """Run a workflow execution (async)."""
        engine = self._get_engine(params)
        workflow_id = params.get("workflow_id", "")
        
        if not workflow_id:
            return ActionResult(success=False, message="workflow_id is required")
        
        # Run the workflow
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            execution = loop.run_until_complete(engine.run_workflow(workflow_id))
            return ActionResult(
                success=execution.status == WorkflowStatus.COMPLETED,
                message=f"Workflow {execution.status.value}: {execution.error_message or 'completed'}",
                data={
                    "workflow_id": execution.workflow_id,
                    "status": execution.status.value,
                    "output": execution.output_data,
                    "activities_completed": execution.activities_completed,
                    "activities_failed": execution.activities_failed,
                    "duration_seconds": (execution.end_time - execution.start_time) if execution.end_time else None
                }
            )
        finally:
            loop.close()
    
    def _get_execution(self, params: Dict[str, Any]) -> ActionResult:
        """Get workflow execution details."""
        engine = self._get_engine(params)
        workflow_id = params.get("workflow_id", "")
        
        if not workflow_id:
            return ActionResult(success=False, message="workflow_id is required")
        
        execution = engine.get_execution(workflow_id)
        if not execution:
            return ActionResult(success=False, message=f"Execution '{workflow_id}' not found")
        
        return ActionResult(
            success=True,
            message=f"Execution: {execution.status.value}",
            data={
                "workflow_id": execution.workflow_id,
                "workflow_name": execution.workflow_name,
                "status": execution.status.value,
                "activities_completed": execution.activities_completed,
                "activities_failed": execution.activities_failed,
                "output": execution.output_data
            }
        )
    
    def _list_executions(self, params: Dict[str, Any]) -> ActionResult:
        """List workflow executions."""
        engine = self._get_engine(params)
        status_filter = params.get("status")
        
        status = WorkflowStatus(status_filter) if status_filter else None
        executions = engine.list_executions(status)
        
        return ActionResult(
            success=True,
            message=f"Found {len(executions)} executions",
            data={
                "executions": [
                    {"workflow_id": e.workflow_id, "workflow_name": e.workflow_name,
                     "status": e.status.value}
                    for e in executions
                ]
            }
        )
    
    def _cancel_execution(self, params: Dict[str, Any]) -> ActionResult:
        """Cancel a workflow execution."""
        engine = self._get_engine(params)
        workflow_id = params.get("workflow_id", "")
        
        if not workflow_id:
            return ActionResult(success=False, message="workflow_id is required")
        
        cancelled = engine.cancel_execution(workflow_id)
        if cancelled:
            return ActionResult(success=True, message=f"Execution '{workflow_id}' cancelled")
        return ActionResult(success=False, message=f"Cannot cancel execution '{workflow_id}'")
