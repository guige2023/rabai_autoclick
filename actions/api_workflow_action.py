"""
API Workflow Action Module

Provides workflow orchestration, step execution, and state management for API operations.
"""
from typing import Any, Optional, Callable, Awaitable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from collections import defaultdict
import asyncio


class WorkflowStatus(Enum):
    """Workflow execution status."""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StepStatus(Enum):
    """Step execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


@dataclass
class WorkflowStep:
    """A step in the workflow."""
    step_id: str
    name: str
    action: Callable[..., Awaitable]
    dependencies: list[str] = field(default_factory=list)
    retry_count: int = 0
    timeout_seconds: float = 300
    continue_on_failure: bool = False
    skip_if: Optional[Callable[[dict], bool]] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Workflow:
    """A workflow definition."""
    workflow_id: str
    name: str
    steps: list[WorkflowStep]
    initial_context: dict[str, Any] = field(default_factory=dict)
    timeout_seconds: Optional[float] = None
    on_completion: Optional[Callable] = None
    on_failure: Optional[Callable] = None


@dataclass
class StepResult:
    """Result of a step execution."""
    step_id: str
    status: StepStatus
    output: Any = None
    error: Optional[str] = None
    attempts: int = 1
    duration_ms: float = 0


@dataclass
class WorkflowResult:
    """Result of workflow execution."""
    workflow_id: str
    status: WorkflowStatus
    context: dict[str, Any]
    step_results: dict[str, StepResult]
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_ms: float = 0
    error: Optional[str] = None


class WorkflowEngine:
    """Workflow execution engine."""
    
    def __init__(self):
        self._running_workflows: dict[str, WorkflowResult] = {}
    
    def _build_execution_order(self, steps: list[WorkflowStep]) -> list[list[str]]:
        """Build execution order respecting dependencies."""
        in_degree = {s.step_id: len(s.dependencies) for s in steps}
        levels = []
        remaining = {s.step_id for s in steps}
        
        while remaining:
            current_level = [sid for sid in remaining if in_degree[sid] == 0]
            
            if not current_level:
                raise ValueError("Circular dependency detected")
            
            levels.append(current_level)
            
            for sid in current_level:
                remaining.remove(sid)
                for step in steps:
                    if sid in step.dependencies:
                        in_degree[step.step_id] -= 1
        
        return levels


class ApiWorkflowAction:
    """Main API workflow action handler."""
    
    def __init__(self):
        self._engine = WorkflowEngine()
        self._workflows: dict[str, Workflow] = {}
        self._workflow_results: dict[str, WorkflowResult] = {}
        self._step_handlers: dict[str, Callable] = {}
        self._stats: dict[str, Any] = defaultdict(int)
    
    def register_workflow(self, workflow: Workflow) -> "ApiWorkflowAction":
        """Register a workflow."""
        self._workflows[workflow.workflow_id] = workflow
        return self
    
    def register_step_handler(
        self,
        action_name: str,
        handler: Callable[..., Awaitable]
    ) -> "ApiWorkflowAction":
        """Register a step action handler."""
        self._step_handlers[action_name] = handler
        return self
    
    async def execute_workflow(
        self,
        workflow_id: str,
        initial_context: Optional[dict[str, Any]] = None
    ) -> WorkflowResult:
        """
        Execute a workflow.
        
        Args:
            workflow_id: ID of workflow to execute
            initial_context: Initial workflow context
            
        Returns:
            WorkflowResult with execution outcome
        """
        if workflow_id not in self._workflows:
            return WorkflowResult(
                workflow_id=workflow_id,
                status=WorkflowStatus.FAILED,
                context={},
                step_results={},
                start_time=datetime.now(),
                error=f"Workflow {workflow_id} not found"
            )
        
        workflow = self._workflows[workflow_id]
        start_time = datetime.now()
        
        # Initialize context
        context = dict(workflow.initial_context)
        if initial_context:
            context.update(initial_context)
        
        result = WorkflowResult(
            workflow_id=workflow_id,
            status=WorkflowStatus.RUNNING,
            context=context,
            step_results={},
            start_time=start_time
        )
        
        self._engine._running_workflows[workflow_id] = result
        
        try:
            # Build execution order
            execution_levels = self._engine._build_execution_order(workflow.steps)
            
            # Execute each level
            for level in execution_levels:
                # Check for cancellation
                if result.status == WorkflowStatus.CANCELLED:
                    break
                
                # Execute steps in level (parallel if independent)
                tasks = []
                for step_id in level:
                    step = next(s for s in workflow.steps if s.step_id == step_id)
                    tasks.append(self._execute_step(step, result))
                
                level_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process level results
                for step_id, step_result in zip(level, level_results):
                    if isinstance(step_result, Exception):
                        result.step_results[step_id] = StepResult(
                            step_id=step_id,
                            status=StepStatus.FAILED,
                            error=str(step_result)
                        )
                    else:
                        result.step_results[step_id] = step_result
                    
                    # Update context with step output
                    step_res = result.step_results[step_id]
                    if step_res.output:
                        context[step_id] = step_res.output
                
                # Check if any critical failure
                failed_steps = [
                    sid for sid, sr in result.step_results.items()
                    if sr.status == StepStatus.FAILED
                ]
                
                if failed_steps:
                    # Check if we should continue
                    failed_step = next(s for s in workflow.steps if s.step_id in failed_steps)
                    if not failed_step.continue_on_failure:
                        result.status = WorkflowStatus.FAILED
                        result.error = f"Step {failed_steps[0]} failed"
                        break
            
            # Set final status
            if result.status == WorkflowStatus.RUNNING:
                result.status = WorkflowStatus.COMPLETED
                if workflow.on_completion:
                    await workflow.on_completion(result)
            
            self._stats["workflows_completed"] += 1
            
        except Exception as e:
            result.status = WorkflowStatus.FAILED
            result.error = str(e)
            self._stats["workflows_failed"] += 1
            
            if workflow.on_failure:
                await workflow.on_failure(result)
        
        result.end_time = datetime.now()
        result.duration_ms = (result.end_time - start_time).total_seconds() * 1000
        
        self._workflow_results[workflow_id] = result
        self._stats["workflows_executed"] += 1
        
        return result
    
    async def _execute_step(
        self,
        step: WorkflowStep,
        workflow_result: WorkflowResult
    ) -> StepResult:
        """Execute a single workflow step."""
        step_start = datetime.now()
        
        # Check skip condition
        if step.skip_if and step.skip_if(workflow_result.context):
            return StepResult(
                step_id=step.step_id,
                status=StepStatus.SKIPPED,
                duration_ms=0
            )
        
        attempts = 0
        last_error = None
        
        while attempts <= step.retry_count:
            attempts += 1
            
            try:
                # Execute step action
                if asyncio.iscoroutinefunction(step.action):
                    output = await asyncio.wait_for(
                        step.action(workflow_result.context),
                        timeout=step.timeout_seconds
                    )
                else:
                    output = step.action(workflow_result.context)
                
                duration_ms = (datetime.now() - step_start).total_seconds() * 1000
                
                return StepResult(
                    step_id=step.step_id,
                    status=StepStatus.COMPLETED,
                    output=output,
                    attempts=attempts,
                    duration_ms=duration_ms
                )
                
            except asyncio.TimeoutError:
                last_error = f"Step timed out after {step.timeout_seconds}s"
                self._stats["step_timeouts"] += 1
            
            except Exception as e:
                last_error = str(e)
                self._stats["step_errors"] += 1
            
            # Retry if attempts remain
            if attempts <= step.retry_count:
                await asyncio.sleep(2 ** attempts)  # Exponential backoff
        
        duration_ms = (datetime.now() - step_start).total_seconds() * 1000
        
        return StepResult(
            step_id=step.step_id,
            status=StepStatus.FAILED,
            error=last_error,
            attempts=attempts,
            duration_ms=duration_ms
        )
    
    async def cancel_workflow(self, workflow_id: str) -> bool:
        """Cancel a running workflow."""
        if workflow_id in self._engine._running_workflows:
            result = self._engine._running_workflows[workflow_id]
            result.status = WorkflowStatus.CANCELLED
            self._stats["workflows_cancelled"] += 1
            return True
        return False
    
    async def pause_workflow(self, workflow_id: str) -> bool:
        """Pause a running workflow."""
        if workflow_id in self._engine._running_workflows:
            result = self._engine._running_workflows[workflow_id]
            result.status = WorkflowStatus.PAUSED
            return True
        return False
    
    async def resume_workflow(self, workflow_id: str) -> WorkflowResult:
        """Resume a paused workflow."""
        if workflow_id not in self._workflows:
            raise ValueError(f"Workflow {workflow_id} not found")
        
        # For simplicity, re-execute from beginning
        # A real implementation would save state and resume
        return await self.execute_workflow(workflow_id)
    
    def get_workflow_status(self, workflow_id: str) -> Optional[dict[str, Any]]:
        """Get workflow execution status."""
        result = self._workflow_results.get(workflow_id)
        
        if not result:
            return None
        
        return {
            "workflow_id": result.workflow_id,
            "status": result.status.value,
            "start_time": result.start_time.isoformat(),
            "end_time": result.end_time.isoformat() if result.end_time else None,
            "duration_ms": result.duration_ms,
            "step_count": len(result.step_results),
            "completed_steps": len([
                s for s in result.step_results.values()
                if s.status == StepStatus.COMPLETED
            ]),
            "failed_steps": len([
                s for s in result.step_results.values()
                if s.status == StepStatus.FAILED
            ])
        }
    
    def get_stats(self) -> dict[str, Any]:
        """Get workflow statistics."""
        return dict(self._stats)
