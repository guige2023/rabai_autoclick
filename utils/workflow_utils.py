"""
Workflow Engine Utilities

Provides a workflow engine for defining and executing
multi-step workflows with branching, loops, and error handling.
"""

from __future__ import annotations

import copy
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class WorkflowState(Enum):
    """Workflow execution states."""
    PENDING = auto()
    RUNNING = auto()
    PAUSED = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()


class StepType(Enum):
    """Types of workflow steps."""
    ACTION = auto()
    CONDITION = auto()
    PARALLEL = auto()
    LOOP = auto()
    WAIT = auto()
    SUBWORKFLOW = auto()


@dataclass
class WorkflowStep:
    """A single step in a workflow."""
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    name: str = ""
    step_type: StepType = StepType.ACTION
    action: Callable[[Any], Any] | None = None
    condition: Callable[[Any], bool] | None = None
    next_step: str | None = None  # Step ID to go to next
    on_error: str | None = None    # Step ID to go to on error
    retry_count: int = 0
    retry_delay_ms: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        return f"Step({self.name}, {self.step_type.name})"


@dataclass
class WorkflowResult:
    """Result of workflow execution."""
    success: bool
    state: WorkflowState
    output: Any = None
    error: str | None = None
    step_results: dict[str, dict[str, Any]] = field(default_factory=dict)
    total_time_ms: float = 0.0

    @property
    def failed(self) -> bool:
        return not self.success


@dataclass
class StepResult:
    """Result of a single step execution."""
    step_id: str
    success: bool
    output: Any = None
    error: str | None = None
    duration_ms: float = 0.0
    attempts: int = 1


class Workflow:
    """
    Workflow definition and execution engine.
    """

    def __init__(self, name: str = ""):
        self.name = name or f"workflow_{uuid.uuid4().hex[:8]}"
        self._steps: dict[str, WorkflowStep] = {}
        self._entry_point: str | None = None
        self._metadata: dict[str, Any] = {}

    def add_step(self, step: WorkflowStep) -> Workflow:
        """Add a step to the workflow."""
        self._steps[step.id] = step
        if self._entry_point is None:
            self._entry_point = step.id
        return self

    def step(
        self,
        name: str,
        action: Callable[[Any], Any],
        *,
        step_type: StepType = StepType.ACTION,
        next_step: str | None = None,
        on_error: str | None = None,
        retry_count: int = 0,
        retry_delay_ms: float = 0.0,
    ) -> Workflow:
        """Add a step using a fluent interface."""
        step = WorkflowStep(
            name=name,
            step_type=step_type,
            action=action,
            next_step=next_step,
            on_error=on_error,
            retry_count=retry_count,
            retry_delay_ms=retry_delay_ms,
        )
        return self.add_step(step)

    def when(
        self,
        name: str,
        condition: Callable[[Any], bool],
        *,
        then_step: str | None = None,
        else_step: str | None = None,
    ) -> Workflow:
        """Add a conditional step."""
        def condition_wrapper(ctx: Any) -> Any:
            result = condition(ctx)
            ctx["_condition_result"] = result
            return ctx

        step = WorkflowStep(
            name=name,
            step_type=StepType.CONDITION,
            action=condition_wrapper,
            next_step=then_step,
            metadata={"else_step": else_step},
        )
        return self.add_step(step)

    def execute(self, initial_input: Any = None) -> WorkflowResult:
        """
        Execute the workflow.

        Args:
            initial_input: Initial input to the workflow.

        Returns:
            WorkflowResult with execution details.
        """
        start_time = time.time()
        step_results: dict[str, StepResult] = {}
        current_step_id = self._entry_point
        state = WorkflowState.RUNNING
        context = initial_input
        error: str | None = None

        while current_step_id and state == WorkflowState.RUNNING:
            step = self._steps.get(current_step_id)
            if not step:
                error = f"Step not found: {current_step_id}"
                state = WorkflowState.FAILED
                break

            step_start = time.time()
            attempts = 0
            success = False
            output = None
            step_error = None

            # Execute step with retries
            while attempts <= step.retry_count and not success:
                attempts += 1
                try:
                    if step.step_type == StepType.ACTION and step.action:
                        output = step.action(context)
                        success = True
                    elif step.step_type == StepType.CONDITION and step.action:
                        output = step.action(context)
                        success = True
                except Exception as e:
                    step_error = str(e)
                    if attempts <= step.retry_count:
                        time.sleep(step.retry_delay_ms / 1000)

            duration = (time.time() - step_start) * 1000

            step_results[step.id] = StepResult(
                step_id=step.id,
                success=success,
                output=output,
                error=step_error,
                duration_ms=duration,
                attempts=attempts,
            )

            if not success:
                if step.on_error:
                    current_step_id = step.on_error
                else:
                    error = f"Step {step.name} failed: {step_error}"
                    state = WorkflowState.FAILED
                continue

            # Determine next step
            if step.step_type == StepType.CONDITION:
                # Check condition result
                if context and isinstance(context, dict) and context.get("_condition_result"):
                    current_step_id = step.next_step
                else:
                    current_step_id = step.metadata.get("else_step")
            else:
                current_step_id = step.next_step

        if state == WorkflowState.RUNNING:
            state = WorkflowState.COMPLETED

        return WorkflowResult(
            success=state == WorkflowState.COMPLETED,
            state=state,
            output=context,
            error=error,
            step_results={k: vars(v) for k, v in step_results.items()},
            total_time_ms=(time.time() - start_time) * 1000,
        )


class WorkflowBuilder(Generic[T]):
    """
    Fluent builder for creating workflows.
    """

    def __init__(self, name: str = ""):
        self._workflow = Workflow(name)

    def add_step(self, step: WorkflowStep) -> WorkflowBuilder[T]:
        """Add a step."""
        self._workflow.add_step(step)
        return self

    def action(
        self,
        name: str,
        action: Callable[[Any], Any],
        **kwargs: Any,
    ) -> WorkflowBuilder[T]:
        """Add an action step."""
        self._workflow.step(name, action, step_type=StepType.ACTION, **kwargs)
        return self

    def condition(
        self,
        name: str,
        condition: Callable[[Any], bool],
        then_step: str | None = None,
        else_step: str | None = None,
    ) -> WorkflowBuilder[T]:
        """Add a condition step."""
        self._workflow.when(name, condition, then_step=then_step, else_step=else_step)
        return self

    def build(self) -> Workflow:
        """Build and return the workflow."""
        return self._workflow


class WorkflowRegistry:
    """
    Registry for reusable workflow templates.
    """

    def __init__(self):
        self._workflows: dict[str, Workflow] = {}

    def register(self, name: str, workflow: Workflow) -> None:
        """Register a workflow."""
        self._workflows[name] = workflow

    def get(self, name: str) -> Workflow | None:
        """Get a workflow by name."""
        return self._workflows.get(name)

    def create(self, name: str, input_data: Any = None) -> WorkflowResult | None:
        """Create and execute a workflow."""
        workflow = self.get(name)
        if workflow:
            return workflow.execute(input_data)
        return None

    def list_workflows(self) -> list[str]:
        """List registered workflow names."""
        return list(self._workflows.keys())


class ParallelWorkflowStep:
    """
    A workflow step that executes multiple steps in parallel.
    """

    def __init__(self, steps: list[WorkflowStep]):
        self.steps = steps
        self.results: list[Any] = []

    def execute(self, context: Any) -> Any:
        """Execute all steps in parallel."""
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(step.action, context): step
                for step in self.steps
                if step.action
            }

            for future in concurrent.futures.as_completed(futures):
                step = futures[future]
                try:
                    result = future.result()
                    self.results.append(result)
                except Exception as e:
                    self.results.append({"error": str(e)})

        return self.results
