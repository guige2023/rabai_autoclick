"""Automation Workflow Action Module.

Provides workflow automation with state machines,
step orchestration, and conditional branching.
"""

from typing import Any, Dict, List, Optional, Callable, Set
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
import asyncio
import json
import uuid


class WorkflowStatus(Enum):
    """Workflow execution status."""
    CREATED = "created"
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
    """Represents a single workflow step."""
    id: str
    name: str
    handler: Callable
    condition: Optional[Callable[["WorkflowContext"], bool]] = None
    retry_config: Optional[Dict[str, Any]] = None
    timeout: int = 300
    on_failure: Optional[str] = None
    parallel: bool = False
    parallel_tasks: List["WorkflowStep"] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.id:
            self.id = str(uuid.uuid4())[:8]


@dataclass
class WorkflowDefinition:
    """Defines a complete workflow."""
    id: str
    name: str
    steps: List[WorkflowStep]
    initial_step: Optional[str] = None
    final_steps: List[str] = field(default_factory=list)
    error_handler: Optional[Callable] = None
    timeout: int = 3600
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WorkflowContext:
    """Execution context shared across workflow."""
    workflow_id: str
    execution_id: str
    variables: Dict[str, Any] = field(default_factory=dict)
    step_results: Dict[str, Any] = field(default_factory=dict)
    errors: List[Dict[str, Any]] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def set_var(self, key: str, value: Any):
        """Set a context variable."""
        self.variables[key] = value

    def get_var(self, key: str, default: Any = None) -> Any:
        """Get a context variable."""
        return self.variables.get(key, default)

    def set_step_result(self, step_id: str, result: Any):
        """Store step execution result."""
        self.step_results[step_id] = result

    def get_step_result(self, step_id: str) -> Optional[Any]:
        """Get step execution result."""
        return self.step_results.get(step_id)


@dataclass
class StepExecution:
    """Records a step execution."""
    step_id: str
    status: StepStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    retries: int = 0


@dataclass
class WorkflowExecution:
    """Tracks workflow execution."""
    execution_id: str
    workflow_id: str
    status: WorkflowStatus
    context: WorkflowContext
    step_executions: Dict[str, StepExecution] = field(default_factory=dict)
    completed_at: Optional[datetime] = None

    def get_step_status(self, step_id: str) -> Optional[StepStatus]:
        """Get status of a specific step."""
        exec = self.step_executions.get(step_id)
        return exec.status if exec else None


class WorkflowEngine:
    """Executes workflow definitions."""

    def __init__(self):
        self._workflows: Dict[str, WorkflowDefinition] = {}
        self._active_executions: Dict[str, WorkflowExecution] = {}
        self._listeners: Dict[str, List[Callable]] = {}

    def register_workflow(self, workflow: WorkflowDefinition):
        """Register a workflow definition."""
        self._workflows[workflow.id] = workflow

    def get_workflow(self, workflow_id: str) -> Optional[WorkflowDefinition]:
        """Get workflow by ID."""
        return self._workflows.get(workflow_id)

    async def execute(
        self,
        workflow_id: str,
        initial_context: Optional[Dict[str, Any]] = None,
        start_at: Optional[str] = None,
    ) -> WorkflowExecution:
        """Execute a workflow."""
        workflow = self._workflows.get(workflow_id)
        if not workflow:
            raise ValueError(f"Workflow {workflow_id} not found")

        execution_id = str(uuid.uuid4())
        context = WorkflowContext(
            workflow_id=workflow_id,
            execution_id=execution_id,
            variables=initial_context or {},
        )

        execution = WorkflowExecution(
            execution_id=execution_id,
            workflow_id=workflow_id,
            status=WorkflowStatus.RUNNING,
            context=context,
        )
        self._active_executions[execution_id] = execution

        try:
            async with asyncio.timeout(workflow.timeout):
                await self._execute_workflow(workflow, execution, start_at)
                execution.status = WorkflowStatus.COMPLETED
        except asyncio.TimeoutError:
            execution.status = WorkflowStatus.FAILED
            context.errors.append({
                "type": "timeout",
                "message": f"Workflow timed out after {workflow.timeout}s",
            })
        except Exception as e:
            execution.status = WorkflowStatus.FAILED
            context.errors.append({"type": "error", "message": str(e)})
            if workflow.error_handler:
                try:
                    await workflow.error_handler(context)
                except Exception:
                    pass

        execution.completed_at = datetime.now()
        self._emit("workflow_completed", execution)
        return execution

    async def _execute_workflow(
        self,
        workflow: WorkflowDefinition,
        execution: WorkflowExecution,
        start_at: Optional[str] = None,
    ):
        """Execute workflow steps in order."""
        step_map = {step.id: step for step in workflow.steps}
        step_order = self._determine_step_order(workflow, start_at)

        for step_id in step_order:
            step = step_map.get(step_id)
            if not step:
                continue

            if step.condition and not await self._evaluate_condition(step, execution.context):
                self._record_step_execution(
                    execution, step_id, StepStatus.SKIPPED, result="Condition not met"
                )
                continue

            await self._execute_step(step, execution)

            if self._get_step_status(step_id, execution) == StepStatus.FAILED:
                if step.on_failure:
                    failure_steps = self._get_steps_by_name(step.on_failure, workflow)
                    for fs_id in failure_steps:
                        await self._execute_step(step_map[fs_id], execution)
                else:
                    break

        for final_step_id in workflow.final_steps:
            if final_step_id in step_map:
                await self._execute_step(step_map[final_step_id], execution)

    async def _execute_step(self, step: WorkflowStep, execution: WorkflowExecution):
        """Execute a single workflow step."""
        self._record_step_execution(execution, step.id, StepStatus.RUNNING)
        step_execution = execution.step_executions[step.id]

        try:
            async with asyncio.timeout(step.timeout):
                if step.parallel and step.parallel_tasks:
                    results = await self._execute_parallel(step.parallel_tasks, execution)
                    result = results
                else:
                    handler = step.handler
                    if asyncio.iscoroutinefunction(handler):
                        result = await handler(execution.context)
                    else:
                        result = handler(execution.context)

                step_execution.status = StepStatus.COMPLETED
                step_execution.result = result
                execution.context.set_step_result(step.id, result)

        except asyncio.TimeoutError:
            step_execution.status = StepStatus.FAILED
            step_execution.error = f"Step timed out after {step.timeout}s"
        except Exception as e:
            step_execution.status = StepStatus.FAILED
            step_execution.error = str(e)
            await self._handle_step_retry(step, step_execution)

        step_execution.completed_at = datetime.now()
        self._emit("step_completed", step, execution)

    async def _execute_parallel(
        self,
        steps: List[WorkflowStep],
        execution: WorkflowExecution,
    ) -> List[Any]:
        """Execute steps in parallel."""
        tasks = [self._execute_step(step, execution) for step in steps]
        await asyncio.gather(*tasks, return_exceptions=True)
        return [execution.context.get_step_result(step.id) for step in steps]

    async def _handle_step_retry(self, step: WorkflowStep, step_execution: StepExecution):
        """Handle step retry if configured."""
        if not step.retry_config:
            return

        max_retries = step.retry_config.get("max_retries", 0)
        delay = step.retry_config.get("delay", 1)

        if step_execution.retries < max_retries:
            step_execution.status = StepStatus.RETRYING
            await asyncio.sleep(delay)
            step_execution.retries += 1

    async def _evaluate_condition(
        self, step: WorkflowStep, context: WorkflowContext
    ) -> bool:
        """Evaluate step condition."""
        if not step.condition:
            return True
        try:
            if asyncio.iscoroutinefunction(step.condition):
                return await step.condition(context)
            return step.condition(context)
        except Exception:
            return False

    def _determine_step_order(
        self,
        workflow: WorkflowDefinition,
        start_at: Optional[str] = None,
    ) -> List[str]:
        """Determine the execution order of steps."""
        if start_at:
            return [start_at]
        return [step.id for step in workflow.steps]

    def _get_steps_by_name(self, name: str, workflow: WorkflowDefinition) -> List[str]:
        """Get step IDs by name."""
        return [step.id for step in workflow.steps if step.name == name]

    def _record_step_execution(
        self,
        execution: WorkflowExecution,
        step_id: str,
        status: StepStatus,
        result: Any = None,
    ):
        """Record step execution."""
        execution.step_executions[step_id] = StepExecution(
            step_id=step_id,
            status=status,
            started_at=datetime.now(),
            result=result,
        )

    def _get_step_status(self, step_id: str, execution: WorkflowExecution) -> Optional[StepStatus]:
        """Get step execution status."""
        exec = execution.step_executions.get(step_id)
        return exec.status if exec else None

    def on_event(self, event: str, callback: Callable):
        """Register event listener."""
        if event not in self._listeners:
            self._listeners[event] = []
        self._listeners[event].append(callback)

    def _emit(self, event: str, *args):
        """Emit event to listeners."""
        for callback in self._listeners.get(event, []):
            try:
                callback(*args)
            except Exception:
                pass

    def get_execution(self, execution_id: str) -> Optional[WorkflowExecution]:
        """Get execution by ID."""
        return self._active_executions.get(execution_id)

    def cancel_execution(self, execution_id: str) -> bool:
        """Cancel a running execution."""
        execution = self._active_executions.get(execution_id)
        if not execution:
            return False
        execution.status = WorkflowStatus.CANCELLED
        return True

    def get_metrics(self) -> Dict[str, Any]:
        """Get workflow engine metrics."""
        return {
            "registered_workflows": len(self._workflows),
            "active_executions": len(self._active_executions),
            "running": sum(
                1 for e in self._active_executions.values()
                if e.status == WorkflowStatus.RUNNING
            ),
            "completed": sum(
                1 for e in self._active_executions.values()
                if e.status == WorkflowStatus.COMPLETED
            ),
            "failed": sum(
                1 for e in self._active_executions.values()
                if e.status == WorkflowStatus.FAILED
            ),
        }


class WorkflowBuilder:
    """Fluent builder for workflow definitions."""

    def __init__(self, workflow_id: str, name: str):
        self._workflow = WorkflowDefinition(
            id=workflow_id,
            name=name,
            steps=[],
        )

    def add_step(
        self,
        name: str,
        handler: Callable,
        step_id: Optional[str] = None,
        condition: Optional[Callable] = None,
    ) -> "WorkflowBuilder":
        """Add a step to the workflow."""
        step = WorkflowStep(
            id=step_id or str(uuid.uuid4())[:8],
            name=name,
            handler=handler,
            condition=condition,
        )
        self._workflow.steps.append(step)
        return self

    def set_initial_step(self, step_id: str) -> "WorkflowBuilder":
        """Set the initial step."""
        self._workflow.initial_step = step_id
        return self

    def set_final_steps(self, step_ids: List[str]) -> "WorkflowBuilder":
        """Set final steps."""
        self._workflow.final_steps = step_ids
        return self

    def add_error_handler(self, handler: Callable) -> "WorkflowBuilder":
        """Add error handler."""
        self._workflow.error_handler = handler
        return self

    def set_timeout(self, timeout: int) -> "WorkflowBuilder":
        """Set workflow timeout."""
        self._workflow.timeout = timeout
        return self

    def build(self) -> WorkflowDefinition:
        """Build and return the workflow."""
        return self._workflow


# Module exports
__all__ = [
    "WorkflowEngine",
    "WorkflowBuilder",
    "WorkflowDefinition",
    "WorkflowStep",
    "WorkflowContext",
    "WorkflowExecution",
    "StepExecution",
    "WorkflowStatus",
    "StepStatus",
]
