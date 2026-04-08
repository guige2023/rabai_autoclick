"""UI Workflow and Scenario Utilities.

Defines and executes multi-step UI workflows and scenarios.
Supports conditional branching, error handling, and workflow state management.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class WorkflowState(Enum):
    """State of a workflow execution."""

    IDLE = auto()
    RUNNING = auto()
    PAUSED = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()


class StepType(Enum):
    """Types of workflow steps."""

    ACTION = auto()
    CONDITION = auto()
    LOOP = auto()
    BRANCH = auto()
    WAIT = auto()
    CHECKPOINT = auto()


@dataclass
class WorkflowStep:
    """A single step in a workflow.

    Attributes:
        step_id: Unique step identifier.
        step_type: Type of step.
        name: Human-readable name.
        action_func: Function to execute.
        condition_func: Function for conditional steps.
        on_error: Error handling callback.
        retry_count: Number of retries on failure.
        timeout_ms: Step timeout in milliseconds.
    """

    step_id: str
    step_type: StepType
    name: str
    action_func: Optional[Callable[[], Any]] = None
    condition_func: Optional[Callable[[], bool]] = None
    on_error: Optional[Callable[[Exception], Any]] = None
    retry_count: int = 0
    timeout_ms: int = 30000


@dataclass
class StepResult:
    """Result of a step execution.

    Attributes:
        step_id: Step identifier.
        success: Whether step succeeded.
        result: Step result data.
        error: Error message if failed.
        duration_ms: Execution duration.
        attempts: Number of attempts made.
    """

    step_id: str
    success: bool
    result: Any = None
    error: str = ""
    duration_ms: float = 0.0
    attempts: int = 1


@dataclass
class WorkflowResult:
    """Result of workflow execution.

    Attributes:
        success: Whether workflow completed successfully.
        state: Final workflow state.
        completed_steps: Number of completed steps.
        failed_step_id: ID of failed step, if any.
        results: Step results by step ID.
        total_duration_ms: Total execution time.
    """

    success: bool
    state: WorkflowState
    completed_steps: int = 0
    failed_step_id: str = ""
    results: dict[str, StepResult] = field(default_factory=dict)
    total_duration_ms: float = 0.0


class Workflow:
    """A workflow consisting of multiple steps.

    Example:
        workflow = Workflow(name="login")
        workflow.add_step(WorkflowStep("step1", StepType.ACTION, "Enter username", action_func=enter_username))
        workflow.add_step(WorkflowStep("step2", StepType.ACTION, "Enter password", action_func=enter_password))
        workflow.add_step(WorkflowStep("step3", StepType.ACTION, "Click login", action_func=click_login))
        result = workflow.execute()
    """

    def __init__(
        self,
        name: str = "",
        on_error: Optional[Callable[[str, Exception], Any]] = None,
    ):
        """Initialize the workflow.

        Args:
            name: Workflow name.
            on_error: Global error handler.
        """
        self.name = name
        self.on_error = on_error
        self._steps: list[WorkflowStep] = []
        self._state = WorkflowState.IDLE
        self._results: dict[str, StepResult] = {}
        self._start_time: Optional[float] = None

    def add_step(self, step: WorkflowStep) -> "Workflow":
        """Add a step to the workflow.

        Args:
            step: WorkflowStep to add.

        Returns:
            Self for chaining.
        """
        self._steps.append(step)
        return self

    def add_action(
        self,
        step_id: str,
        name: str,
        action_func: Callable[[], Any],
    ) -> "Workflow":
        """Add an action step.

        Args:
            step_id: Unique step ID.
            name: Step name.
            action_func: Function to execute.

        Returns:
            Self for chaining.
        """
        self._steps.append(
            WorkflowStep(
                step_id=step_id,
                step_type=StepType.ACTION,
                name=name,
                action_func=action_func,
            )
        )
        return self

    def add_condition(
        self,
        step_id: str,
        name: str,
        condition_func: Callable[[], bool],
    ) -> "Workflow":
        """Add a condition step.

        Args:
            step_id: Unique step ID.
            name: Step name.
            condition_func: Function returning True/False.

        Returns:
            Self for chaining.
        """
        self._steps.append(
            WorkflowStep(
                step_id=step_id,
                step_type=StepType.CONDITION,
                name=name,
                condition_func=condition_func,
            )
        )
        return self

    def add_wait(
        self,
        step_id: str,
        name: str,
        duration_ms: int,
    ) -> "Workflow":
        """Add a wait step.

        Args:
            step_id: Unique step ID.
            name: Step name.
            duration_ms: Wait duration in milliseconds.

        Returns:
            Self for chaining.
        """
        def wait_func():
            time.sleep(duration_ms / 1000.0)

        self._steps.append(
            WorkflowStep(
                step_id=step_id,
                step_type=StepType.WAIT,
                name=name,
                action_func=wait_func,
            )
        )
        return self

    def execute(self) -> WorkflowResult:
        """Execute the workflow.

        Returns:
            WorkflowResult with execution details.
        """
        self._state = WorkflowState.RUNNING
        self._start_time = time.time()
        self._results = {}
        completed = 0

        for step in self._steps:
            result = self._execute_step(step)
            self._results[step.step_id] = result

            if result.success:
                completed += 1
            else:
                self._state = WorkflowState.FAILED
                total_ms = (time.time() - self._start_time) * 1000
                return WorkflowResult(
                    success=False,
                    state=self._state,
                    completed_steps=completed,
                    failed_step_id=step.step_id,
                    results=self._results,
                    total_duration_ms=total_ms,
                )

        self._state = WorkflowState.COMPLETED
        total_ms = (time.time() - self._start_time) * 1000
        return WorkflowResult(
            success=True,
            state=self._state,
            completed_steps=completed,
            results=self._results,
            total_duration_ms=total_ms,
        )

    def _execute_step(self, step: WorkflowStep) -> StepResult:
        """Execute a single step.

        Args:
            step: WorkflowStep to execute.

        Returns:
            StepResult of execution.
        """
        start_time = time.time()
        attempts = 0

        for attempt in range(step.retry_count + 1):
            attempts = attempt + 1
            try:
                if step.step_type == StepType.CONDITION and step.condition_func:
                    result = step.condition_func()
                    return StepResult(
                        step_id=step.step_id,
                        success=bool(result),
                        result=result,
                        duration_ms=(time.time() - start_time) * 1000,
                        attempts=attempts,
                    )
                elif step.action_func:
                    result = step.action_func()
                    return StepResult(
                        step_id=step.step_id,
                        success=True,
                        result=result,
                        duration_ms=(time.time() - start_time) * 1000,
                        attempts=attempts,
                    )

            except Exception as e:
                if step.on_error:
                    try:
                        step.on_error(e)
                    except Exception:
                        pass

                if attempt == step.retry_count:
                    error_msg = str(e)
                    if self.on_error:
                        self.on_error(step.step_id, e)
                    return StepResult(
                        step_id=step.step_id,
                        success=False,
                        error=error_msg,
                        duration_ms=(time.time() - start_time) * 1000,
                        attempts=attempts,
                    )

        return StepResult(
            step_id=step.step_id,
            success=False,
            error="Max retries exceeded",
            duration_ms=(time.time() - start_time) * 1000,
            attempts=attempts,
        )

    def cancel(self) -> None:
        """Cancel the workflow execution."""
        self._state = WorkflowState.CANCELLED

    def pause(self) -> None:
        """Pause the workflow execution."""
        if self._state == WorkflowState.RUNNING:
            self._state = WorkflowState.PAUSED

    def resume(self) -> None:
        """Resume a paused workflow."""
        if self._state == WorkflowState.PAUSED:
            self._state = WorkflowState.RUNNING

    def get_state(self) -> WorkflowState:
        """Get current workflow state.

        Returns:
            Current WorkflowState.
        """
        return self._state

    def get_results(self) -> dict[str, StepResult]:
        """Get all step results.

        Returns:
            Dictionary of step ID to StepResult.
        """
        return self._results


class WorkflowBuilder:
    """Builder for complex workflows.

    Example:
        builder = WorkflowBuilder()
        builder.name("login_flow")
        builder.step("enter_username", lambda: enter_username("user"))
        builder.step("enter_password", lambda: enter_password("pass"))
        builder.step("click_login", lambda: click_login())
        workflow = builder.build()
    """

    def __init__(self):
        """Initialize the workflow builder."""
        self._workflow = Workflow()

    @classmethod
    def create(cls) -> "WorkflowBuilder":
        """Create a new workflow builder.

        Returns:
            WorkflowBuilder instance.
        """
        return cls()

    def name(self, name: str) -> "WorkflowBuilder":
        """Set workflow name.

        Args:
            name: Workflow name.

        Returns:
            Self for chaining.
        """
        self._workflow.name = name
        return self

    def step(
        self,
        step_id: str,
        action: Callable[[], Any],
        name: Optional[str] = None,
    ) -> "WorkflowBuilder":
        """Add an action step.

        Args:
            step_id: Unique step ID.
            action: Action function.
            name: Optional step name.

        Returns:
            Self for chaining.
        """
        self._workflow.add_action(
            step_id=step_id,
            name=name or step_id,
            action_func=action,
        )
        return self

    def wait(self, step_id: str, duration_ms: int) -> "WorkflowBuilder":
        """Add a wait step.

        Args:
            step_id: Unique step ID.
            duration_ms: Wait duration.

        Returns:
            Self for chaining.
        """
        self._workflow.add_wait(
            step_id=step_id,
            name=step_id,
            duration_ms=duration_ms,
        )
        return self

    def on_error(
        self,
        handler: Callable[[str, Exception], Any],
    ) -> "WorkflowBuilder":
        """Set error handler.

        Args:
            handler: Error handler function.

        Returns:
            Self for chaining.
        """
        self._workflow.on_error = handler
        return self

    def build(self) -> Workflow:
        """Build the workflow.

        Returns:
            Workflow instance.
        """
        return self._workflow


class WorkflowManager:
    """Manages multiple workflow definitions.

    Example:
        manager = WorkflowManager()
        manager.register("login", workflow)
        result = manager.execute("login")
    """

    def __init__(self):
        """Initialize the workflow manager."""
        self._workflows: dict[str, Workflow] = {}

    def register(self, name: str, workflow: Workflow) -> None:
        """Register a workflow.

        Args:
            name: Workflow name.
            workflow: Workflow to register.
        """
        self._workflows[name] = workflow

    def get(self, name: str) -> Optional[Workflow]:
        """Get a workflow by name.

        Args:
            name: Workflow name.

        Returns:
            Workflow or None.
        """
        return self._workflows.get(name)

    def execute(self, name: str) -> Optional[WorkflowResult]:
        """Execute a workflow by name.

        Args:
            name: Workflow name.

        Returns:
            WorkflowResult or None if workflow not found.
        """
        workflow = self._workflows.get(name)
        if workflow:
            return workflow.execute()
        return None

    def list_workflows(self) -> list[str]:
        """List all registered workflow names.

        Returns:
            List of workflow names.
        """
        return list(self._workflows.keys())
