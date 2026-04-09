"""
Workflow Orchestrator Action Module.

Provides workflow orchestration with parallel execution,
error handling, and state management.

Author: rabai_autoclick team
"""

import asyncio
import logging
import time
from typing import (
    Optional, Dict, Any, List, Callable, Awaitable,
    Set, Union
)
from dataclasses import dataclass, field
from enum import Enum
from uuid import uuid4

logger = logging.getLogger(__name__)


class StepStatus(Enum):
    """Workflow step status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


class StepType(Enum):
    """Workflow step types."""
    TASK = "task"
    PARALLEL = "parallel"
    SEQUENCE = "sequence"
    CONDITIONAL = "conditional"
    LOOP = "loop"
    WAIT = "wait"
    APPROVAL = "approval"


@dataclass
class WorkflowStep:
    """A single workflow step."""
    step_id: str
    step_type: StepType
    name: str
    func: Optional[Callable[..., Awaitable[Any]]] = None
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    depends_on: Set[str] = field(default_factory=set)
    retry_count: int = 0
    max_retries: int = 3
    timeout: Optional[float] = None
    condition: Optional[Callable[..., bool]] = None
    items: List["WorkflowStep"] = field(default_factory=list)
    on_failure: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StepResult:
    """Result of a workflow step execution."""
    step_id: str
    status: StepStatus
    output: Any = None
    error: Optional[str] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    retries: int = 0

    @property
    def duration(self) -> Optional[float]:
        """Get step duration in seconds."""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return None


@dataclass
class WorkflowResult:
    """Result of a workflow execution."""
    workflow_id: str
    status: StepStatus
    step_results: Dict[str, StepResult] = field(default_factory=dict)
    output: Any = None
    error: Optional[str] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None

    @property
    def duration(self) -> Optional[float]:
        """Get workflow duration in seconds."""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return None

    @property
    def success(self) -> bool:
        """Check if workflow succeeded."""
        return self.status == StepStatus.COMPLETED


class WorkflowOrchestratorAction:
    """
    Workflow Orchestration Engine.

    Supports complex workflows with parallel execution,
    conditional branching, retries, and comprehensive
    error handling.

    Example:
        >>> orchestrator = WorkflowOrchestratorAction()
        >>> workflow = orchestrator.create_workflow("my_workflow")
        >>> workflow.add_step(StepType.TASK, name="step1", func=my_task)
        >>> result = await orchestrator.execute(workflow)
    """

    def __init__(self, max_parallel: int = 10):
        self.max_parallel = max_parallel
        self._workflows: Dict[str, List[WorkflowStep]] = {}
        self._active_executions: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    def create_workflow(
        self,
        workflow_id: Optional[str] = None,
        steps: Optional[List[WorkflowStep]] = None,
    ) -> List[WorkflowStep]:
        """
        Create a new workflow.

        Args:
            workflow_id: Optional workflow ID
            steps: Optional initial steps

        Returns:
            List of workflow steps
        """
        wf_id = workflow_id or str(uuid4())
        workflow = steps or []
        self._workflows[wf_id] = workflow
        return workflow

    def add_step(
        self,
        workflow: List[WorkflowStep],
        step_type: StepType,
        name: str,
        func: Optional[Callable] = None,
        step_id: Optional[str] = None,
        depends_on: Optional[Set[str]] = None,
        **kwargs,
    ) -> WorkflowStep:
        """
        Add a step to a workflow.

        Args:
            workflow: Workflow to add step to
            step_type: Type of step
            name: Step name
            func: Step function
            step_id: Optional step ID
            depends_on: Dependencies
            **kwargs: Additional step config

        Returns:
            Created WorkflowStep
        """
        step = WorkflowStep(
            step_id=step_id or str(uuid4()),
            step_type=step_type,
            name=name,
            func=func,
            depends_on=depends_on or set(),
            **kwargs,
        )
        workflow.append(step)
        return step

    def add_task(
        self,
        workflow: List[WorkflowStep],
        name: str,
        func: Callable[..., Awaitable[Any]],
        step_id: Optional[str] = None,
        depends_on: Optional[Set[str]] = None,
        **kwargs,
    ) -> WorkflowStep:
        """Add a task step to workflow."""
        return self.add_step(
            workflow,
            StepType.TASK,
            name,
            func=func,
            step_id=step_id,
            depends_on=depends_on,
            **kwargs,
        )

    def add_parallel(
        self,
        workflow: List[WorkflowStep],
        name: str,
        steps: List[WorkflowStep],
        step_id: Optional[str] = None,
        depends_on: Optional[Set[str]] = None,
    ) -> WorkflowStep:
        """
        Add a parallel execution step.

        Args:
            workflow: Workflow to add step to
            name: Step name
            steps: Steps to execute in parallel
            step_id: Optional step ID
            depends_on: Dependencies

        Returns:
            Created WorkflowStep
        """
        return self.add_step(
            workflow,
            StepType.PARALLEL,
            name,
            step_id=step_id,
            depends_on=depends_on,
            items=steps,
        )

    def add_conditional(
        self,
        workflow: List[WorkflowStep],
        name: str,
        condition: Callable[..., bool],
        then_steps: List[WorkflowStep],
        else_steps: Optional[List[WorkflowStep]] = None,
        step_id: Optional[str] = None,
        depends_on: Optional[Set[str]] = None,
    ) -> WorkflowStep:
        """Add a conditional step."""
        return self.add_step(
            workflow,
            StepType.CONDITIONAL,
            name,
            condition=condition,
            items=then_steps,
            kwargs={"else_items": else_steps or []},
            step_id=step_id,
            depends_on=depends_on,
        )

    async def execute(
        self,
        workflow: List[WorkflowStep],
        workflow_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> WorkflowResult:
        """
        Execute a workflow.

        Args:
            workflow: Workflow steps to execute
            workflow_id: Optional workflow ID
            context: Execution context

        Returns:
            WorkflowResult
        """
        wf_id = workflow_id or str(uuid4())
        result = WorkflowResult(
            workflow_id=wf_id,
            status=StepStatus.PENDING,
            started_at=time.time(),
        )

        async with self._lock:
            self._active_executions[wf_id] = {
                "workflow": workflow,
                "context": context or {},
                "completed_steps": set(),
            }

        try:
            step_results = await self._execute_steps(
                workflow,
                result,
                self._active_executions[wf_id]["context"],
            )

            for step_result in step_results.values():
                result.step_results[step_result.step_id] = step_result

            if all(r.status == StepStatus.COMPLETED for r in step_results.values()):
                result.status = StepStatus.COMPLETED
            elif any(r.status == StepStatus.FAILED for r in step_results.values()):
                result.status = StepStatus.FAILED
                result.error = "One or more steps failed"
            else:
                result.status = StepStatus.FAILED

        except Exception as e:
            logger.error(f"Workflow {wf_id} failed: {e}")
            result.status = StepStatus.FAILED
            result.error = str(e)

        finally:
            result.completed_at = time.time()
            async with self._lock:
                self._active_executions.pop(wf_id, None)

        return result

    async def _execute_steps(
        self,
        steps: List[WorkflowStep],
        workflow_result: WorkflowResult,
        context: Dict[str, Any],
    ) -> Dict[str, StepResult]:
        """Execute workflow steps respecting dependencies."""
        results: Dict[str, StepResult] = {}
        pending = {step.step_id: step for step in steps}
        running: Set[str] = set()

        while pending or running:
            ready = [
                step for step_id, step in pending.items()
                if step.depends_on <= set(results.keys())
                and all(results[sid].status == StepStatus.COMPLETED
                       for sid in step.depends_on)
            ]

            if not ready and not running:
                if pending:
                    step = list(pending.values())[0]
                    results[step.step_id] = StepResult(
                        step_id=step.step_id,
                        status=StepStatus.FAILED,
                        error="Dependencies not satisfied",
                    )
                break

            for step in ready[:self.max_parallel - len(running)]:
                running.add(step.step_id)
                asyncio.create_task(
                    self._execute_step(step, results, workflow_result, context, running)
                )

            await asyncio.sleep(0.01)

        return results

    async def _execute_step(
        self,
        step: WorkflowStep,
        results: Dict[str, StepResult],
        workflow_result: WorkflowResult,
        context: Dict[str, Any],
        running: Set[str],
    ) -> None:
        """Execute a single workflow step."""
        result = StepResult(
            step_id=step.step_id,
            status=StepStatus.RUNNING,
            started_at=time.time(),
        )
        results[step.step_id] = result

        try:
            if step.condition and not step.condition(context):
                result.status = StepStatus.SKIPPED
                logger.info(f"Step {step.step_id} skipped (condition false)")
                return

            if step.step_type == StepType.TASK:
                output = await self._execute_task(step, context)
                result.output = output
                result.status = StepStatus.COMPLETED

            elif step.step_type == StepType.PARALLEL:
                outputs = await self._execute_parallel(step.items, context)
                result.output = outputs
                result.status = StepStatus.COMPLETED

            elif step.step_type == StepType.SEQUENCE:
                outputs = await self._execute_steps(step.items, workflow_result, context)
                result.output = {k: v.output for k, v in outputs.items()}
                result.status = StepStatus.COMPLETED

            elif step.step_type == StepType.CONDITIONAL:
                output = await self._execute_conditional(step, context)
                result.output = output
                result.status = StepStatus.COMPLETED

            elif step.step_type == StepType.WAIT:
                await asyncio.sleep(step.kwargs.get("duration", 0))
                result.output = True
                result.status = StepStatus.COMPLETED

            else:
                logger.warning(f"Unsupported step type: {step.step_type}")
                result.status = StepStatus.SKIPPED

        except Exception as e:
            logger.error(f"Step {step.step_id} failed: {e}")
            result.error = str(e)

            if step.retry_count < step.max_retries:
                result.status = StepStatus.RETRYING
                step.retry_count += 1
                await asyncio.sleep(2 ** step.retry_count)
                await self._execute_step(step, results, workflow_result, context, running)
                return

            result.status = StepStatus.FAILED

        finally:
            result.completed_at = time.time()
            running.discard(step.step_id)

    async def _execute_task(
        self,
        step: WorkflowStep,
        context: Dict[str, Any],
    ) -> Any:
        """Execute a task step."""
        if step.timeout:
            return await asyncio.wait_for(
                step.func(*step.args, **step.kwargs, context=context),
                timeout=step.timeout
            )
        return await step.func(*step.args, **step.kwargs, context=context)

    async def _execute_parallel(
        self,
        steps: List[WorkflowStep],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute steps in parallel."""
        tasks = []
        for step in steps:
            task = asyncio.create_task(self._execute_single_step(step, context))
            tasks.append((step.step_id, task))

        results = {}
        for step_id, task in tasks:
            results[step_id] = await task
        return results

    async def _execute_single_step(
        self,
        step: WorkflowStep,
        context: Dict[str, Any],
    ) -> Any:
        """Execute a single step (non-workflow)."""
        if step.func:
            return await step.func(*step.args, **step.kwargs, context=context)
        return None

    async def _execute_conditional(
        self,
        step: WorkflowStep,
        context: Dict[str, Any],
    ) -> Any:
        """Execute a conditional step."""
        if step.condition(context):
            outputs = await self._execute_parallel(step.items, context)
        else:
            outputs = await self._execute_parallel(
                step.kwargs.get("else_items", []),
                context
            )
        return outputs
