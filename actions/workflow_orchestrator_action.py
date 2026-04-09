"""
Workflow Orchestrator Action Module

Multi-step workflow orchestration with parallel branches,
conditional routing, and compensation/rollback support.

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


class StepStatus(Enum):
    """Step execution status."""
    
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"


class StepType(Enum):
    """Workflow step types."""
    
    TASK = "task"
    PARALLEL = "parallel"
    CONDITIONAL = "conditional"
    WAIT = "wait"
    COMPENSATE = "compensate"


@dataclass
class WorkflowStep:
    """A single step in a workflow."""
    
    id: str
    name: str
    step_type: StepType
    action: Callable
    depends_on: List[str] = field(default_factory=list)
    condition: Optional[Callable] = None
    compensation: Optional[Callable] = None
    timeout_seconds: float = 300
    retry_count: int = 0
    max_retries: int = 3
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    status: StepStatus = StepStatus.PENDING
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    result: Any = None
    error: Optional[str] = None


@dataclass
class WorkflowExecution:
    """Execution context for a workflow."""
    
    execution_id: str
    workflow_id: str
    status: StepStatus = StepStatus.PENDING
    step_results: Dict[str, Any] = field(default_factory=dict)
    step_errors: Dict[str, str] = field(default_factory=dict)
    started_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class WorkflowOrchestrator:
    """Core workflow orchestration logic."""
    
    def __init__(self):
        self._workflows: Dict[str, List[WorkflowStep]] = {}
        self._executions: Dict[str, WorkflowExecution] = {}
        self._running_steps: Dict[str, Set[str]] = defaultdict(set)
    
    def define_workflow(self, workflow_id: str, steps: List[WorkflowStep]) -> None:
        """Define a workflow with steps."""
        self._workflows[workflow_id] = steps
    
    async def execute(
        self,
        workflow_id: str,
        initial_context: Optional[Dict] = None
    ) -> WorkflowExecution:
        """Execute a workflow."""
        execution_id = str(uuid.uuid4())
        execution = WorkflowExecution(
            execution_id=execution_id,
            workflow_id=workflow_id,
            metadata=initial_context or {}
        )
        
        self._executions[execution_id] = execution
        
        steps = self._workflows.get(workflow_id, [])
        if not steps:
            execution.status = StepStatus.COMPLETED
            execution.completed_at = time.time()
            return execution
        
        execution.status = StepStatus.RUNNING
        
        try:
            await self._execute_steps(execution, steps)
            execution.status = StepStatus.COMPLETED
        except Exception as e:
            execution.status = StepStatus.FAILED
            execution.metadata["error"] = str(e)
        
        execution.completed_at = time.time()
        return execution
    
    async def _execute_steps(
        self,
        execution: WorkflowExecution,
        steps: List[WorkflowStep]
    ) -> None:
        """Execute workflow steps respecting dependencies."""
        pending = {s.id: s for s in steps}
        completed: Set[str] = set()
        
        while pending:
            ready = [
                s for s_id, s in pending.items()
                if s_id not in completed and
                   all(dep in completed for dep in s.depends_on)
            ]
            
            if not ready:
                if pending:
                    raise Exception("Workflow has unmet dependencies")
                break
            
            tasks = [self._execute_step(execution, step, pending) for step in ready]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for step, result in zip(ready, results):
                if isinstance(result, Exception):
                    step.status = StepStatus.FAILED
                    step.error = str(result)
                    execution.step_errors[step.id] = str(result)
                    
                    await self._compensate(execution, steps, completed)
                    raise result
                
                if step.status == StepStatus.COMPLETED:
                    completed.add(step.id)
                    execution.step_results[step.id] = step.result
                
                del pending[step.id]
    
    async def _execute_step(
        self,
        execution: WorkflowExecution,
        step: WorkflowStep,
        pending: Dict[str, WorkflowStep]
    ) -> Any:
        """Execute a single step."""
        step.status = StepStatus.RUNNING
        step.started_at = time.time()
        
        context = {**execution.metadata, **execution.step_results}
        
        if step.condition:
            should_run = step.condition(context)
            if not should_run:
                step.status = StepStatus.SKIPPED
                return None
        
        try:
            if asyncio.iscoroutinefunction(step.action):
                result = await asyncio.wait_for(
                    step.action(context),
                    timeout=step.timeout_seconds
                )
            else:
                result = step.action(context)
            
            step.result = result
            step.status = StepStatus.COMPLETED
            step.completed_at = time.time()
            
            return result
        
        except asyncio.TimeoutError:
            step.status = StepStatus.FAILED
            step.error = f"Timeout after {step.timeout_seconds}s"
            raise Exception(step.error)
        
        except Exception as e:
            step.status = StepStatus.FAILED
            step.error = str(e)
            
            if step.retry_count < step.max_retries:
                step.retry_count += 1
                step.status = StepStatus.PENDING
                pending[step.id] = step
            else:
                raise
    
    async def _compensate(
        self,
        execution: WorkflowExecution,
        steps: List[WorkflowStep],
        completed: Set[str]
    ) -> None:
        """Run compensation actions for completed steps."""
        for step in reversed(steps):
            if step.id not in completed:
                continue
            
            if not step.compensation:
                continue
            
            step.status = StepStatus.COMPENSATING
            
            try:
                if asyncio.iscoroutinefunction(step.compensation):
                    await step.compensation(execution.step_results.get(step.id))
                else:
                    step.compensation(execution.step_results.get(step.id))
                
                step.status = StepStatus.COMPENSATED
            
            except Exception as e:
                logger.error(f"Compensation failed for step {step.id}: {e}")
                step.status = StepStatus.FAILED


class WorkflowOrchestratorAction:
    """
    Main workflow orchestrator action handler.
    
    Provides multi-step workflow orchestration with parallel execution,
    conditional routing, and compensation/rollback support.
    """
    
    def __init__(self):
        self.orchestrator = WorkflowOrchestrator()
        self._middleware: List[Callable] = []
    
    def define(
        self,
        workflow_id: str,
        steps: List[Dict]
    ) -> None:
        """Define a workflow from configuration."""
        workflow_steps = []
        
        for step_config in steps:
            step = WorkflowStep(
                id=step_config["id"],
                name=step_config["name"],
                step_type=StepType(step_config.get("type", "task")),
                action=self._middleware or (lambda ctx: None),
                depends_on=step_config.get("depends_on", []),
                condition=step_config.get("condition"),
                compensation=step_config.get("compensation"),
                timeout_seconds=step_config.get("timeout", 300),
                max_retries=step_config.get("max_retries", 3),
                metadata=step_config.get("metadata", {})
            )
            workflow_steps.append(step)
        
        self.orchestrator.define_workflow(workflow_id, workflow_steps)
    
    async def run(
        self,
        workflow_id: str,
        context: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Execute a workflow."""
        execution = await self.orchestrator.execute(workflow_id, context)
        
        return {
            "execution_id": execution.execution_id,
            "workflow_id": execution.workflow_id,
            "status": execution.status.value,
            "step_results": execution.step_results,
            "step_errors": execution.step_errors,
            "started_at": datetime.fromtimestamp(execution.started_at).isoformat(),
            "completed_at": (
                datetime.fromtimestamp(execution.completed_at).isoformat()
                if execution.completed_at else None
            )
        }
    
    def get_execution(self, execution_id: str) -> Optional[Dict]:
        """Get execution details."""
        execution = self.orchestrator._executions.get(execution_id)
        if not execution:
            return None
        
        return {
            "execution_id": execution.execution_id,
            "workflow_id": execution.workflow_id,
            "status": execution.status.value,
            "step_results": execution.step_results,
            "step_errors": execution.step_errors,
            "started_at": execution.started_at,
            "completed_at": execution.completed_at
        }
    
    def list_executions(self) -> List[Dict]:
        """List all executions."""
        return [
            {
                "execution_id": e.execution_id,
                "workflow_id": e.workflow_id,
                "status": e.status.value,
                "started_at": e.started_at
            }
            for e in self.orchestrator._executions.values()
        ]
