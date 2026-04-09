"""
Automation Workflow Action Module

Workflow orchestration engine for automating multi-step business processes.
Supports conditional branching, parallel execution, error handling,
rollback mechanisms, and workflow persistence.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class WorkflowStatus(Enum):
    """Workflow execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class StepStatus(Enum):
    """Step execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ROLLBACK = "rollback"


@dataclass
class StepResult:
    """Result of a workflow step execution."""

    step_id: str
    status: StepStatus
    output: Any = None
    error: Optional[str] = None
    duration_ms: float = 0.0
    started_at: Optional[float] = None
    completed_at: Optional[float] = None


@dataclass
class WorkflowContext:
    """Shared context for workflow execution."""

    workflow_id: str
    data: Dict[str, Any] = field(default_factory=dict)
    step_results: Dict[str, StepResult] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None

    def add_result(self, result: StepResult) -> None:
        """Add a step result."""
        self.step_results[result.step_id] = result

    def get_result(self, step_id: str) -> Optional[StepResult]:
        """Get a step result."""
        return self.step_results.get(step_id)

    def get_output(self, step_id: str) -> Any:
        """Get step output."""
        result = self.step_results.get(step_id)
        return result.output if result else None


@dataclass
class WorkflowStep:
    """A single step in a workflow."""

    step_id: str
    name: str
    handler: Callable[..., Any]
    condition: Optional[Callable[[WorkflowContext], bool]] = None
    rollback_handler: Optional[Callable[..., Any]] = None
    retry_count: int = 0
    timeout_seconds: float = 60.0
    depends_on: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def should_execute(self, context: WorkflowContext) -> bool:
        """Check if step should execute based on condition."""
        if self.condition is None:
            return True
        return self.condition(context)

    async def execute(self, context: WorkflowContext) -> StepResult:
        """Execute the step."""
        result = StepResult(step_id=self.step_id, status=StepStatus.RUNNING)
        result.started_at = time.time()

        try:
            if asyncio.iscoroutinefunction(self.handler):
                output = await asyncio.wait_for(
                    self.handler(context),
                    timeout=self.timeout_seconds,
                )
            else:
                output = self.handler(context)

            result.status = StepStatus.COMPLETED
            result.output = output
            logger.info(f"Step {self.step_id} completed successfully")

        except asyncio.TimeoutError:
            result.status = StepStatus.FAILED
            result.error = f"Step timed out after {self.timeout_seconds}s"
            logger.error(f"Step {self.step_id} timed out")

        except Exception as e:
            result.status = StepStatus.FAILED
            result.error = f"{type(e).__name__}: {str(e)}"
            logger.error(f"Step {self.step_id} failed: {result.error}")

        result.completed_at = time.time()
        result.duration_ms = (result.completed_at - result.started_at) * 1000

        return result

    async def rollback(self, context: WorkflowContext) -> StepResult:
        """Rollback the step if rollback handler exists."""
        if self.rollback_handler is None:
            return StepResult(
                step_id=self.step_id,
                status=StepStatus.SKIPPED,
                output=None,
                error="No rollback handler defined",
            )

        result = StepResult(step_id=self.step_id, status=StepStatus.ROLLBACK)
        result.started_at = time.time()

        try:
            if asyncio.iscoroutinefunction(self.rollback_handler):
                output = await self.rollback_handler(context)
            else:
                output = self.rollback_handler(context)

            result.status = StepStatus.COMPLETED
            result.output = output
            logger.info(f"Step {self.step_id} rolled back successfully")

        except Exception as e:
            result.status = StepStatus.FAILED
            result.error = f"Rollback failed: {type(e).__name__}: {str(e)}"
            logger.error(f"Step {self.step_id} rollback failed: {result.error}")

        result.completed_at = time.time()
        result.duration_ms = (result.completed_at - result.started_at) * 1000

        return result


@dataclass
class WorkflowConfig:
    """Configuration for workflow execution."""

    name: str = "workflow"
    allow_parallel: bool = False
    max_parallel_steps: int = 3
    continue_on_error: bool = True
    rollback_on_error: bool = True
    timeout_seconds: float = 300.0
    enable_logging: bool = True


class AutomationWorkflowAction:
    """
    Workflow orchestration engine for business process automation.

    Features:
    - Sequential and parallel step execution
    - Conditional branching
    - Automatic rollback on failure
    - Retry logic for failed steps
    - Workflow state persistence
    - Comprehensive execution tracking

    Usage:
        workflow = AutomationWorkflowAction(config)
        workflow.add_step(step1)
        workflow.add_step(step2, depends_on={"step1"})
        result = await workflow.execute(context)
    """

    def __init__(self, config: Optional[WorkflowConfig] = None):
        self.config = config or WorkflowConfig()
        self._steps: Dict[str, WorkflowStep] = {}
        self._step_order: List[str] = []

    def add_step(
        self,
        step_id: str,
        name: str,
        handler: Callable[..., Any],
        condition: Optional[Callable[[WorkflowContext], bool]] = None,
        rollback_handler: Optional[Callable[..., Any]] = None,
        retry_count: int = 0,
        timeout_seconds: float = 60.0,
        depends_on: Optional[Set[str]] = None,
    ) -> "AutomationWorkflowAction":
        """Add a step to the workflow."""
        step = WorkflowStep(
            step_id=step_id,
            name=name,
            handler=handler,
            condition=condition,
            rollback_handler=rollback_handler,
            retry_count=retry_count,
            timeout_seconds=timeout_seconds,
            depends_on=depends_on or set(),
        )
        self._steps[step_id] = step
        self._step_order.append(step_id)
        return self

    def add_steps(
        self,
        steps: List[tuple],
    ) -> "AutomationWorkflowAction":
        """Add multiple steps at once."""
        for step_data in steps:
            self.add_step(*step_data)
        return self

    def get_step(self, step_id: str) -> Optional[WorkflowStep]:
        """Get a step by ID."""
        return self._steps.get(step_id)

    def get_execution_order(self) -> List[str]:
        """Get the execution order of steps based on dependencies."""
        visited: Set[str] = set()
        order: List[str] = []

        def visit(step_id: str) -> None:
            if step_id in visited:
                return
            visited.add(step_id)
            step = self._steps.get(step_id)
            if step:
                for dep in step.depends_on:
                    visit(dep)
                order.append(step_id)

        for step_id in self._step_order:
            visit(step_id)

        return order

    def _get_ready_steps(
        self,
        context: WorkflowContext,
        completed: Set[str],
    ) -> List[WorkflowStep]:
        """Get steps that are ready to execute."""
        ready = []
        for step_id in self._step_order:
            step = self._steps[step_id]
            if step_id in completed:
                continue
            # Check if all dependencies are met
            if step.depends_on and not step.depends_on.issubset(completed):
                continue
            # Check condition
            if not step.should_execute(context):
                context.add_result(StepResult(
                    step_id=step_id,
                    status=StepStatus.SKIPPED,
                    error="Condition not met",
                ))
                completed.add(step_id)
                continue
            ready.append(step)
        return ready

    async def execute(
        self,
        context: WorkflowContext,
    ) -> WorkflowContext:
        """
        Execute the workflow.

        Args:
            context: Workflow context with initial data

        Returns:
            Updated workflow context with results
        """
        logger.info(f"Starting workflow: {self.config.name}")
        context.started_at = time.time()

        completed: Set[str] = set()
        failed_steps: List[str] = []

        try:
            while completed != set(self._steps.keys()):
                ready_steps = self._get_ready_steps(context, completed)

                if not ready_steps:
                    if failed_steps:
                        logger.error(f"Workflow blocked by failed steps: {failed_steps}")
                        break
                    break

                for step in ready_steps:
                    logger.info(f"Executing step: {step.step_id}")

                    # Execute with retries
                    for attempt in range(step.retry_count + 1):
                        result = await step.execute(context)
                        context.add_result(result)

                        if result.status == StepStatus.COMPLETED:
                            break

                        if attempt < step.retry_count:
                            logger.warning(
                                f"Step {step.step_id} failed, retry {attempt + 1}/{step.retry_count}"
                            )
                            await asyncio.sleep(0.5 * (attempt + 1))

                    if result.status == StepStatus.COMPLETED:
                        completed.add(step.step_id)
                    else:
                        failed_steps.append(step.step_id)
                        if not self.config.continue_on_error:
                            break

                if failed_steps and not self.config.continue_on_error:
                    break

                if not self.config.allow_parallel:
                    break

            # Rollback on failure
            if failed_steps and self.config.rollback_on_error:
                await self._rollback(context, completed)

        except Exception as e:
            logger.error(f"Workflow execution error: {e}")
            context.metadata["error"] = str(e)

        context.completed_at = time.time()

        if failed_steps:
            context.metadata["status"] = WorkflowStatus.FAILED
            logger.error(f"Workflow failed: {len(failed_steps)} steps failed")
        else:
            context.metadata["status"] = WorkflowStatus.COMPLETED
            logger.info("Workflow completed successfully")

        return context

    async def _rollback(
        self,
        context: WorkflowContext,
        completed: Set[str],
    ) -> None:
        """Rollback completed steps in reverse order."""
        logger.info("Starting rollback")

        for step_id in reversed(list(completed)):
            step = self._steps.get(step_id)
            if step and step.rollback_handler:
                logger.info(f"Rolling back step: {step_id}")
                result = await step.rollback(context)
                context.add_result(result)

    def get_summary(self, context: WorkflowContext) -> Dict[str, Any]:
        """Get workflow execution summary."""
        total_duration = 0.0
        if context.started_at:
            end = context.completed_at or time.time()
            total_duration = (end - context.started_at) * 1000

        return {
            "workflow_id": context.workflow_id,
            "name": self.config.name,
            "status": context.metadata.get("status", WorkflowStatus.RUNNING).value,
            "total_duration_ms": total_duration,
            "total_steps": len(self._steps),
            "completed_steps": sum(
                1 for r in context.step_results.values()
                if r.status == StepStatus.COMPLETED
            ),
            "failed_steps": sum(
                1 for r in context.step_results.values()
                if r.status == StepStatus.FAILED
            ),
            "skipped_steps": sum(
                1 for r in context.step_results.values()
                if r.status == StepStatus.SKIPPED
            ),
            "steps": [
                {
                    "id": step_id,
                    "status": result.status.value,
                    "duration_ms": result.duration_ms,
                    "error": result.error,
                }
                for step_id, result in context.step_results.items()
            ],
        }


async def demo_workflow():
    """Demonstrate workflow execution."""
    workflow = AutomationWorkflowAction(
        WorkflowConfig(name="demo_workflow", allow_parallel=False)
    )

    async def step1(ctx: WorkflowContext) -> Dict[str, Any]:
        await asyncio.sleep(0.1)
        ctx.data["step1_output"] = {"status": "completed"}
        return ctx.data["step1_output"]

    async def step2(ctx: WorkflowContext) -> Dict[str, Any]:
        await asyncio.sleep(0.1)
        step1_result = ctx.get_output("step1")
        ctx.data["step2_output"] = {"status": "completed", "depends_on": step1_result}
        return ctx.data["step2_output"]

    workflow.add_step("step1", "First Step", step1)
    workflow.add_step("step2", "Second Step", step2, depends_on={"step1"})

    context = WorkflowContext(workflow_id="demo-001", data={})
    result = await workflow.execute(context)

    summary = workflow.get_summary(result)
    print(f"Workflow status: {summary['status']}")
    print(f"Completed steps: {summary['completed_steps']}")


if __name__ == "__main__":
    asyncio.run(demo_workflow())
