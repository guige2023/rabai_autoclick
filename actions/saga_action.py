"""Saga action module for RabAI AutoClick.

Provides saga pattern implementation for distributed transactions
with coordinated steps, compensation, and rollback support.
"""

import sys
import os
import json
import time
import uuid
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SagaStatus(Enum):
    """Saga execution status."""
    STARTING = "starting"
    RUNNING = "running"
    COMPLETED = "completed"
    COMPENSATING = "compensating"  # Rolling back
    COMPENSATED = "compensated"    # Successfully rolled back
    FAILED = "failed"              # Compensations also failed
    CANCELLED = "cancelled"


class StepStatus(Enum):
    """Individual saga step status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATED = "compensated"
    SKIPPED = "skipped"


@dataclass
class SagaStep:
    """Represents a single step in a saga."""
    name: str
    execute_func: Optional[str] = None  # Registered function name
    compensate_func: Optional[str] = None
    retry_policy: Dict[str, Any] = field(default_factory=dict)
    timeout_seconds: float = 60.0
    continue_on_failure: bool = False
    skip_compensation: bool = False
    description: str = ""
    
    # Runtime state (not set during init)
    status: StepStatus = StepStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None


@dataclass
class SagaDefinition:
    """Defines a saga with its steps."""
    name: str
    steps: List[SagaStep] = field(default_factory=list)
    on_complete: Optional[str] = None
    on_compensation_complete: Optional[str] = None
    max_compensation_attempts: int = 3
    allow_partial_compensation: bool = False
    description: str = ""


@dataclass
class SagaExecution:
    """Tracks a saga execution instance."""
    saga_id: str
    saga_name: str
    status: SagaStatus
    steps_completed: List[str] = field(default_factory=list)
    steps_failed: List[str] = field(default_factory=list)
    steps_compensated: List[str] = field(default_factory=list)
    step_results: Dict[str, Any] = field(default_factory=dict)
    step_errors: Dict[str, str] = field(default_factory=dict)
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    compensation_start_time: Optional[float] = None
    input_data: Any = None
    output_data: Any = None
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class SagaOrchestrator:
    """Orchestrates saga execution with compensation."""
    
    def __init__(self, persistence_path: Optional[str] = None):
        self._sagas: Dict[str, SagaDefinition] = {}
        self._executions: Dict[str, SagaExecution] = {}
        self._step_functions: Dict[str, Callable] = {}
        self._callback_functions: Dict[str, Callable] = {}
        self._persistence_path = persistence_path
        self._load()
    
    def _load(self) -> None:
        """Load saga data from persistence."""
        if self._persistence_path and os.path.exists(self._persistence_path):
            try:
                with open(self._persistence_path, 'r') as f:
                    data = json.load(f)
                    for saga_data in data.get("sagas", []):
                        steps = [SagaStep(**{k: v for k, v in s.items() if k in 
                                           ["name", "execute_func", "compensate_func",
                                            "retry_policy", "timeout_seconds",
                                            "continue_on_failure", "skip_compensation", "description"]})
                                for s in saga_data.get("steps", [])]
                        self._sagas[saga_data["name"]] = SagaDefinition(
                            name=saga_data["name"],
                            steps=steps,
                            description=saga_data.get("description", "")
                        )
                    # Load executions (simplified)
                    for exec_data in data.get("executions", []):
                        exec_data["status"] = SagaStatus(exec_data["status"])
                        self._executions[exec_data["saga_id"]] = SagaExecution(**exec_data)
            except (json.JSONDecodeError, TypeError, KeyError):
                pass
    
    def _persist(self) -> None:
        """Persist saga data."""
        if self._persistence_path:
            try:
                data = {
                    "sagas": [
                        {
                            "name": saga.name,
                            "steps": [
                                {"name": s.name, "execute_func": s.execute_func,
                                 "compensate_func": s.compensate_func,
                                 "retry_policy": s.retry_policy,
                                 "timeout_seconds": s.timeout_seconds,
                                 "continue_on_failure": s.continue_on_failure,
                                 "skip_compensation": s.skip_compensation,
                                 "description": s.description}
                                for s in saga.definition.steps
                            ],
                            "description": saga.definition.description
                        }
                        for saga in self._sagas.values()
                    ],
                    "executions": [
                        {
                            "saga_id": e.saga_id,
                            "saga_name": e.saga_name,
                            "status": e.status.value,
                            "start_time": e.start_time,
                            "end_time": e.end_time,
                            "input_data": e.input_data,
                            "output_data": e.output_data,
                            "error_message": e.error_message
                        }
                        for e in list(self._executions.values())[-100:]  # Last 100
                    ]
                }
                with open(self._persistence_path, 'w') as f:
                    json.dump(data, f, indent=2, default=str)
            except OSError:
                pass
    
    def register_saga(self, saga: SagaDefinition) -> None:
        """Register a saga definition."""
        self._sagas[saga.name] = saga
    
    def register_step_function(self, name: str, func: Callable) -> None:
        """Register an executable step function."""
        self._step_functions[name] = func
    
    def register_callback(self, name: str, func: Callable) -> None:
        """Register a callback function."""
        self._callback_functions[name] = func
    
    def start_saga(
        self,
        saga_name: str,
        input_data: Any = None,
        saga_id: Optional[str] = None
    ) -> str:
        """Start a new saga execution.
        
        Args:
            saga_name: Name of the saga to start.
            input_data: Input data for the saga.
            saga_id: Optional custom saga ID.
        
        Returns:
            The saga execution ID.
        """
        if saga_name not in self._sagas:
            raise ValueError(f"Saga '{saga_name}' not found")
        
        saga_id = saga_id or str(uuid.uuid4())
        execution = SagaExecution(
            saga_id=saga_id,
            saga_name=saga_name,
            status=SagaStatus.STARTING,
            start_time=time.time(),
            input_data=input_data
        )
        self._executions[saga_id] = execution
        self._persist()
        return saga_id
    
    async def execute_saga_async(
        self,
        saga_id: str,
        steps_override: Optional[List[SagaStep]] = None
    ) -> SagaExecution:
        """Execute a saga by ID.
        
        Args:
            saga_id: The saga execution ID.
            steps_override: Optional override for the steps (for testing).
        
        Returns:
            The completed SagaExecution.
        """
        execution = self._executions.get(saga_id)
        if not execution:
            raise ValueError(f"Saga execution '{saga_id}' not found")
        
        saga_def = self._sagas.get(execution.saga_name)
        if not saga_def:
            raise ValueError(f"Saga definition '{execution.saga_name}' not found")
        
        execution.status = SagaStatus.RUNNING
        steps = steps_override or saga_def.steps
        current_data = execution.input_data
        
        # Execute each step
        for step in steps:
            step.status = StepStatus.RUNNING
            step.started_at = time.time()
            
            try:
                # Get step function
                step_func = self._step_functions.get(step.execute_func)
                
                if step_func:
                    result = await step_func(current_data, step)
                else:
                    # Simulate step execution
                    result = {"step": step.name, "executed": True, "data": current_data}
                
                step.status = StepStatus.COMPLETED
                step.result = result
                step.completed_at = time.time()
                execution.steps_completed.append(step.name)
                execution.step_results[step.name] = result
                current_data = result
            
            except Exception as e:
                step.status = StepStatus.FAILED
                step.error = str(e)
                step.completed_at = time.time()
                execution.steps_failed.append(step.name)
                execution.step_errors[step.name] = str(e)
                
                # Start compensation
                execution.status = SagaStatus.COMPENSATING
                execution.compensation_start_time = time.time()
                await self._compensate(execution, saga_def, steps)
                
                if execution.status == SagaStatus.COMPENSATING:
                    execution.status = SagaStatus.COMPENSATED
                execution.end_time = time.time()
                self._persist()
                return execution
        
        # All steps completed
        execution.status = SagaStatus.COMPLETED
        execution.output_data = current_data
        execution.end_time = time.time()
        
        # Call completion callback
        if saga_def.on_complete:
            callback = self._callback_functions.get(saga_def.on_complete)
            if callback:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(execution)
                    else:
                        callback(execution)
                except Exception:
                    pass
        
        self._persist()
        return execution
    
    async def _compensate(
        self,
        execution: SagaExecution,
        saga_def: SagaDefinition,
        completed_steps: List[SagaStep]
    ) -> None:
        """Execute compensation (rollback) for completed steps.
        
        Compensation runs in reverse order of execution.
        """
        # Reverse the completed steps
        for step in reversed(completed_steps):
            if step.skip_compensation:
                step.status = StepStatus.SKIPPED
                continue
            
            step.status = StepStatus.RUNNING
            
            try:
                compensate_func = self._step_functions.get(step.compensate_func)
                
                if compensate_func:
                    await compensate_func(step.result, step)
                # If no compensation func, step is silently skipped
                
                step.status = StepStatus.COMPENSATED
                execution.steps_compensated.append(step.name)
            
            except Exception as e:
                # Compensation failed
                if not saga_def.allow_partial_compensation:
                    execution.status = SagaStatus.FAILED
                    execution.error_message = f"Compensation failed for step '{step.name}': {str(e)}"
                    return
                # Otherwise continue compensating other steps
                execution.step_errors[f"{step.name}_compensation"] = str(e)
        
        # Call compensation complete callback
        if saga_def.on_compensation_complete:
            callback = self._callback_functions.get(saga_def.on_compensation_complete)
            if callback:
                try:
                    callback(execution)
                except Exception:
                    pass
    
    def get_execution(self, saga_id: str) -> Optional[SagaExecution]:
        """Get a saga execution by ID."""
        return self._executions.get(saga_id)
    
    def list_executions(
        self,
        saga_name: Optional[str] = None,
        status: Optional[SagaStatus] = None
    ) -> List[SagaExecution]:
        """List saga executions with optional filtering."""
        executions = list(self._executions.values())
        
        if saga_name:
            executions = [e for e in executions if e.saga_name == saga_name]
        if status:
            executions = [e for e in executions if e.status == status]
        
        return sorted(executions, key=lambda e: e.start_time or 0, reverse=True)
    
    def cancel_execution(self, saga_id: str) -> bool:
        """Cancel a running saga execution."""
        execution = self._executions.get(saga_id)
        if not execution:
            return False
        
        if execution.status not in (SagaStatus.STARTING, SagaStatus.RUNNING):
            return False
        
        execution.status = SagaStatus.CANCELLED
        execution.end_time = time.time()
        self._persist()
        return True
    
    def get_saga_info(self, saga_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a saga definition."""
        saga = self._sagas.get(saga_name)
        if not saga:
            return None
        
        return {
            "name": saga.name,
            "description": saga.description,
            "step_count": len(saga.steps),
            "steps": [
                {
                    "name": s.name,
                    "execute_func": s.execute_func,
                    "compensate_func": s.compensate_func,
                    "description": s.description
                }
                for s in saga.steps
            ]
        }


import asyncio


class SagaAction(BaseAction):
    """Execute distributed transactions using the saga pattern.
    
    Supports saga definition with steps and compensation functions,
    coordinated execution, automatic rollback on failure, and
    partial compensation handling.
    """
    action_type = "saga"
    display_name = "Saga事务"
    description = "Saga分布式事务模式，支持补偿和回滚"
    
    def __init__(self):
        super().__init__()
        self._orchestrator: Optional[SagaOrchestrator] = None
    
    def _get_orchestrator(self, params: Dict[str, Any]) -> SagaOrchestrator:
        """Get or create the saga orchestrator."""
        if self._orchestrator is None:
            persistence_path = params.get("persistence_path")
            self._orchestrator = SagaOrchestrator(persistence_path)
        return self._orchestrator
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute saga operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: "register_saga", "register_step", "register_callback",
                  "start", "execute", "get", "list", "cancel", "get_info"
                - For register_saga: saga_name, steps (list of dicts)
                - For register_step: name, execute_func, compensate_func
                - For start/execute: saga_name, saga_id, input_data
                - For get/list/cancel: saga_id or saga_name
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get("operation", "")
        
        try:
            if operation == "register_saga":
                return self._register_saga(params)
            elif operation == "register_step":
                return self._register_step(params)
            elif operation == "register_callback":
                return self._register_callback(params)
            elif operation == "start":
                return self._start_saga(params)
            elif operation == "execute":
                return self._execute_saga(params)
            elif operation == "get":
                return self._get_execution(params)
            elif operation == "list":
                return self._list_executions(params)
            elif operation == "cancel":
                return self._cancel_execution(params)
            elif operation == "get_info":
                return self._get_saga_info(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Saga error: {str(e)}")
    
    def _register_saga(self, params: Dict[str, Any]) -> ActionResult:
        """Register a saga definition."""
        orchestrator = self._get_orchestrator(params)
        saga_name = params.get("saga_name", "")
        
        if not saga_name:
            return ActionResult(success=False, message="saga_name is required")
        
        steps_data = params.get("steps", [])
        steps = []
        for s_data in steps_data:
            step = SagaStep(
                name=s_data["name"],
                execute_func=s_data.get("execute_func"),
                compensate_func=s_data.get("compensate_func"),
                timeout_seconds=s_data.get("timeout_seconds", 60.0),
                continue_on_failure=s_data.get("continue_on_failure", False),
                skip_compensation=s_data.get("skip_compensation", False),
                description=s_data.get("description", "")
            )
            steps.append(step)
        
        saga = SagaDefinition(
            name=saga_name,
            steps=steps,
            on_complete=params.get("on_complete"),
            on_compensation_complete=params.get("on_compensation_complete"),
            max_compensation_attempts=params.get("max_compensation_attempts", 3),
            allow_partial_compensation=params.get("allow_partial_compensation", False),
            description=params.get("description", "")
        )
        orchestrator.register_saga(saga)
        return ActionResult(
            success=True,
            message=f"Saga '{saga_name}' registered with {len(steps)} steps",
            data={"saga_name": saga_name, "step_count": len(steps)}
        )
    
    def _register_step(self, params: Dict[str, Any]) -> ActionResult:
        """Register a step function."""
        orchestrator = self._get_orchestrator(params)
        name = params.get("name", "")
        
        if not name:
            return ActionResult(success=False, message="Step function name is required")
        
        # Placeholder - in real usage would register actual function
        def placeholder_step(data, step):
            return {"step": name, "executed": True}
        
        orchestrator.register_step_function(name, placeholder_step)
        return ActionResult(success=True, message=f"Step function '{name}' registered")
    
    def _register_callback(self, params: Dict[str, Any]) -> ActionResult:
        """Register a callback function."""
        orchestrator = self._get_orchestrator(params)
        name = params.get("name", "")
        
        if not name:
            return ActionResult(success=False, message="Callback name is required")
        
        # Placeholder
        def placeholder_callback(execution):
            pass
        
        orchestrator.register_callback(name, placeholder_callback)
        return ActionResult(success=True, message=f"Callback '{name}' registered")
    
    def _start_saga(self, params: Dict[str, Any]) -> ActionResult:
        """Start a saga execution."""
        orchestrator = self._get_orchestrator(params)
        saga_name = params.get("saga_name", "")
        input_data = params.get("input_data")
        saga_id = params.get("saga_id")
        
        if not saga_name:
            return ActionResult(success=False, message="saga_name is required")
        
        saga_id = orchestrator.start_saga(saga_name, input_data, saga_id)
        return ActionResult(
            success=True,
            message=f"Saga started: {saga_id}",
            data={"saga_id": saga_id, "saga_name": saga_name}
        )
    
    def _execute_saga(self, params: Dict[str, Any]) -> ActionResult:
        """Execute a saga (start + run)."""
        orchestrator = self._get_orchestrator(params)
        saga_name = params.get("saga_name", "")
        input_data = params.get("input_data")
        saga_id = params.get("saga_id")
        
        if not saga_name:
            return ActionResult(success=False, message="saga_name is required")
        
        saga_id = orchestrator.start_saga(saga_name, input_data, saga_id)
        
        # Execute the saga
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            execution = loop.run_until_complete(orchestrator.execute_saga_async(saga_id))
            
            return ActionResult(
                success=execution.status == SagaStatus.COMPLETED,
                message=f"Saga {execution.status.value}: {execution.error_message or 'completed'}",
                data={
                    "saga_id": execution.saga_id,
                    "saga_name": execution.saga_name,
                    "status": execution.status.value,
                    "steps_completed": execution.steps_completed,
                    "steps_failed": execution.steps_failed,
                    "steps_compensated": execution.steps_compensated,
                    "output_data": execution.output_data,
                    "duration_seconds": (execution.end_time - execution.start_time) if execution.end_time else None
                }
            )
        finally:
            loop.close()
    
    def _get_execution(self, params: Dict[str, Any]) -> ActionResult:
        """Get a saga execution."""
        orchestrator = self._get_orchestrator(params)
        saga_id = params.get("saga_id", "")
        
        if not saga_id:
            return ActionResult(success=False, message="saga_id is required")
        
        execution = orchestrator.get_execution(saga_id)
        if not execution:
            return ActionResult(success=False, message=f"Execution '{saga_id}' not found")
        
        return ActionResult(
            success=True,
            message=f"Execution: {execution.status.value}",
            data={
                "saga_id": execution.saga_id,
                "saga_name": execution.saga_name,
                "status": execution.status.value,
                "steps_completed": execution.steps_completed,
                "steps_failed": execution.steps_failed,
                "steps_compensated": execution.steps_compensated,
                "output_data": execution.output_data
            }
        )
    
    def _list_executions(self, params: Dict[str, Any]) -> ActionResult:
        """List saga executions."""
        orchestrator = self._get_orchestrator(params)
        saga_name = params.get("saga_name")
        status = params.get("status")
        
        if status:
            status = SagaStatus(status)
        
        executions = orchestrator.list_executions(saga_name, status)
        return ActionResult(
            success=True,
            message=f"Found {len(executions)} executions",
            data={
                "executions": [
                    {"saga_id": e.saga_id, "saga_name": e.saga_name,
                     "status": e.status.value, "start_time": e.start_time}
                    for e in executions[:50]  # Limit results
                ]
            }
        )
    
    def _cancel_execution(self, params: Dict[str, Any]) -> ActionResult:
        """Cancel a saga execution."""
        orchestrator = self._get_orchestrator(params)
        saga_id = params.get("saga_id", "")
        
        if not saga_id:
            return ActionResult(success=False, message="saga_id is required")
        
        cancelled = orchestrator.cancel_execution(saga_id)
        return ActionResult(
            success=cancelled,
            message=f"Saga '{saga_id}' cancelled" if cancelled else f"Cannot cancel '{saga_id}'"
        )
    
    def _get_saga_info(self, params: Dict[str, Any]) -> ActionResult:
        """Get saga definition information."""
        orchestrator = self._get_orchestrator(params)
        saga_name = params.get("saga_name", "")
        
        if not saga_name:
            return ActionResult(success=False, message="saga_name is required")
        
        info = orchestrator.get_saga_info(saga_name)
        if not info:
            return ActionResult(success=False, message=f"Saga '{saga_name}' not found")
        
        return ActionResult(success=True, message="Saga info retrieved", data=info)
