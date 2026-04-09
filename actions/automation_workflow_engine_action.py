"""
Automation Workflow Engine Action Module.

Event-driven workflow automation with state machines,
conditional branching, parallel execution, and persistence.
"""

import asyncio
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Optional

from uuid import uuid4


class WorkflowState(Enum):
    """Workflow execution states."""

    PENDING = "pending"
    RUNNING = "running"
    WAITING = "waiting"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class StepType(Enum):
    """Types of workflow steps."""

    TASK = "task"
    CONDITION = "condition"
    PARALLEL = "parallel"
    LOOP = "loop"
    WAIT = "wait"
    NOTIFY = "notify"
    SUBWORKFLOW = "subworkflow"


@dataclass
class WorkflowStepResult:
    """Result of executing a workflow step."""

    step_id: str
    success: bool
    output: Any = None
    error: Optional[str] = None
    duration: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class WorkflowContext:
    """Execution context passed through workflow steps."""

    workflow_id: str
    execution_id: str
    data: dict[str, Any] = field(default_factory=dict)
    state: dict[str, Any] = field(default_factory=dict)
    history: list[WorkflowStepResult] = field(default_factory=list)
    started_at: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)

    def get_data(self, key: str, default: Any = None) -> Any:
        """Get data from context."""
        return self.data.get(key, default)

    def set_data(self, key: str, value: Any) -> None:
        """Set data in context."""
        self.data[key] = value

    def add_history(self, result: WorkflowStepResult) -> None:
        """Add step result to history."""
        self.history.append(result)


@dataclass
class WorkflowStep:
    """A single step in a workflow."""

    step_id: str
    step_type: StepType
    name: str
    handler: Optional[Callable[..., Awaitable[WorkflowStepResult]]] = None
    condition: Optional[Callable[[WorkflowContext], bool]] = None
    steps: list["WorkflowStep"] = field(default_factory=list)
    max_retries: int = 1
    timeout: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Generate step_id if not provided."""
        if not self.step_id:
            self.step_id = str(uuid4())[:8]


class WorkflowEngine:
    """
    Event-driven workflow automation engine.

    Supports conditional branching, parallel execution,
    error handling, and workflow persistence.
    """

    def __init__(
        self,
        max_parallel_steps: int = 10,
        default_timeout: float = 300.0,
        enable_persistence: bool = False,
        persistence_path: Optional[str] = None,
    ) -> None:
        """
        Initialize the workflow engine.

        Args:
            max_parallel_steps: Maximum parallel step executions.
            default_timeout: Default step timeout in seconds.
            enable_persistence: Enable workflow state persistence.
            persistence_path: Path for persisting workflow states.
        """
        self._max_parallel = max_parallel_steps
        self._default_timeout = default_timeout
        self._enable_persistence = enable_persistence
        self._persistence_path = persistence_path
        self._workflows: dict[str, WorkflowStep] = {}
        self._active_executions: dict[str, WorkflowContext] = {}
        self._event_handlers: dict[str, list[Callable[..., Any]]] = {}
        self._semaphore = asyncio.Semaphore(max_parallel_steps)

    def define_workflow(
        self,
        workflow_id: str,
        steps: list[WorkflowStep],
    ) -> WorkflowStep:
        """
        Define a new workflow.

        Args:
            workflow_id: Unique workflow identifier.
            steps: List of workflow steps.

        Returns:
            Root workflow step.
        """
        root = WorkflowStep(
            step_id=workflow_id,
            step_type=StepType.TASK,
            name=f"workflow:{workflow_id}",
            steps=steps,
        )
        self._workflows[workflow_id] = root
        return root

    async def execute_workflow(
        self,
        workflow_id: str,
        initial_data: Optional[dict[str, Any]] = None,
        execution_id: Optional[str] = None,
    ) -> WorkflowContext:
        """
        Execute a defined workflow.

        Args:
            workflow_id: ID of workflow to execute.
            initial_data: Initial data passed to workflow.
            execution_id: Optional specific execution ID.

        Returns:
            WorkflowContext with execution results.
        """
        if workflow_id not in self._workflows:
            raise ValueError(f"Workflow '{workflow_id}' not found")

        exec_id = execution_id or str(uuid4())
        workflow = self._workflows[workflow_id]

        context = WorkflowContext(
            workflow_id=workflow_id,
            execution_id=exec_id,
            data=initial_data or {},
        )
        self._active_executions[exec_id] = context

        try:
            await self._execute_steps([workflow], context)
            context.metadata["status"] = WorkflowState.COMPLETED.value
        except asyncio.CancelledError:
            context.metadata["status"] = WorkflowState.CANCELLED.value
            raise
        except Exception as e:
            context.metadata["status"] = WorkflowState.FAILED.value
            context.metadata["error"] = str(e)
            raise
        finally:
            if self._enable_persistence:
                await self._persist_context(context)

        return context

    async def _execute_steps(
        self,
        steps: list[WorkflowStep],
        context: WorkflowContext,
    ) -> list[WorkflowStepResult]:
        """
        Execute a list of steps.

        Args:
            steps: Steps to execute.
            context: Workflow execution context.

        Returns:
            List of step results.
        """
        results: list[WorkflowStepResult] = []

        for step in steps:
            result = await self._execute_step(step, context)
            results.append(result)
            context.add_history(result)

            if step.step_type == StepType.CONDITION and result.output is False:
                continue

            if not result.success and step.max_retries <= 1:
                if step.step_type != StepType.CONDITION:
                    raise Exception(f"Step '{step.name}' failed: {result.error}")

        return results

    async def _execute_step(
        self,
        step: WorkflowStep,
        context: WorkflowContext,
    ) -> WorkflowStepResult:
        """
        Execute a single workflow step.

        Args:
            step: Step to execute.
            context: Workflow execution context.

        Returns:
            Step execution result.
        """
        start_time = time.time()

        if step.condition and not step.condition(context):
            return WorkflowStepResult(
                step_id=step.step_id,
                success=True,
                output=False,
                duration=time.time() - start_time,
            )

        if step.step_type == StepType.PARALLEL and step.steps:
            return await self._execute_parallel(step, context, start_time)

        if step.step_type == StepType.LOOP and step.steps:
            return await self._execute_loop(step, context, start_time)

        if step.step_type == StepType.WAIT:
            wait_time = step.metadata.get("wait_seconds", 1.0)
            await asyncio.sleep(wait_time)
            return WorkflowStepResult(
                step_id=step.step_id,
                success=True,
                duration=time.time() - start_time,
            )

        if step.step_type == StepType.CONDITION:
            result_val = step.condition(context) if step.condition else True
            return WorkflowStepResult(
                step_id=step.step_id,
                success=True,
                output=result_val,
                duration=time.time() - start_time,
            )

        if step.handler:
            for attempt in range(step.max_retries):
                try:
                    if step.timeout > 0:
                        result = await asyncio.wait_for(
                            step.handler(context), timeout=step.timeout
                        )
                    else:
                        result = await step.handler(context)

                    return WorkflowStepResult(
                        step_id=step.step_id,
                        success=True,
                        output=result,
                        duration=time.time() - start_time,
                    )
                except asyncio.TimeoutError:
                    if attempt == step.max_retries - 1:
                        return WorkflowStepResult(
                            step_id=step.step_id,
                            success=False,
                            error=f"Step timed out after {step.timeout}s",
                            duration=time.time() - start_time,
                        )
                except Exception as e:
                    if attempt == step.max_retries - 1:
                        return WorkflowStepResult(
                            step_id=step.step_id,
                            success=False,
                            error=str(e),
                            duration=time.time() - start_time,
                        )

        if step.steps:
            nested_results = await self._execute_steps(step.steps, context)
            return WorkflowStepResult(
                step_id=step.step_id,
                success=all(r.success for r in nested_results),
                output=[r.output for r in nested_results],
                duration=time.time() - start_time,
            )

        return WorkflowStepResult(
            step_id=step.step_id,
            success=True,
            duration=time.time() - start_time,
        )

    async def _execute_parallel(
        self,
        step: WorkflowStep,
        context: WorkflowContext,
        start_time: float,
    ) -> WorkflowStepResult:
        """Execute steps in parallel."""
        async with self._semaphore:
            tasks = [
                self._execute_step(sub_step, context)
                for sub_step in step.steps
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        outputs = []
        all_success = True
        errors = []
        for r in results:
            if isinstance(r, Exception):
                all_success = False
                errors.append(str(r))
                outputs.append(None)
            elif isinstance(r, WorkflowStepResult):
                outputs.append(r.output)
                if not r.success:
                    all_success = False
                    errors.append(r.error or "")

        return WorkflowStepResult(
            step_id=step.step_id,
            success=all_success,
            output=outputs,
            error="; ".join(errors) if errors else None,
            duration=time.time() - start_time,
        )

    async def _execute_loop(
        self,
        step: WorkflowStep,
        context: WorkflowContext,
        start_time: float,
    ) -> WorkflowStepResult:
        """Execute steps in a loop."""
        max_iterations = step.metadata.get("max_iterations", 10)
        loop_condition = step.metadata.get("condition")

        outputs = []
        for i in range(max_iterations):
            if loop_condition and not loop_condition(context, i):
                break
            results = await self._execute_steps(step.steps, context)
            outputs.extend([r.output for r in results])
            if not all(r.success for r in results):
                break

        return WorkflowStepResult(
            step_id=step.step_id,
            success=True,
            output=outputs,
            duration=time.time() - start_time,
        )

    async def _persist_context(self, context: WorkflowContext) -> None:
        """Persist workflow context to disk."""
        if not self._persistence_path:
            return
        try:
            with open(self._persistence_path, "r") as f:
                all_data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            all_data = {}

        all_data[context.execution_id] = {
            "workflow_id": context.workflow_id,
            "execution_id": context.execution_id,
            "data": context.data,
            "state": context.state,
            "started_at": context.started_at,
            "metadata": context.metadata,
        }

        with open(self._persistence_path, "w") as f:
            json.dump(all_data, f)

    def get_execution(self, execution_id: str) -> Optional[WorkflowContext]:
        """Get an active execution by ID."""
        return self._active_executions.get(execution_id)

    def cancel_execution(self, execution_id: str) -> bool:
        """Cancel a running execution."""
        if execution_id in self._active_executions:
            self._active_executions[execution_id].metadata["status"] = (
                WorkflowState.CANCELLED.value
            )
            return True
        return False


def create_workflow_engine(
    max_parallel_steps: int = 10,
    default_timeout: float = 300.0,
) -> WorkflowEngine:
    """
    Factory function to create a configured workflow engine.

    Args:
        max_parallel_steps: Max parallel step executions.
        default_timeout: Default step timeout.

    Returns:
        Configured WorkflowEngine instance.
    """
    return WorkflowEngine(
        max_parallel_steps=max_parallel_steps,
        default_timeout=default_timeout,
    )
