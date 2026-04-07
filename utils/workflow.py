"""Workflow utilities for RabAI AutoClick.

Provides:
- Workflow definition
- Workflow execution
- Workflow validation
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class WorkflowStatus(Enum):
    """Workflow execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class WorkflowStep:
    """A single step in a workflow."""
    name: str
    action: Callable[[], Any]
    condition: Optional[Callable[[], bool]] = None
    on_failure: Optional[Callable[[Exception], None]] = None
    retry_count: int = 0
    timeout: float = 0


@dataclass
class WorkflowResult:
    """Result of workflow execution."""
    status: WorkflowStatus
    step_results: Dict[str, Any] = field(default_factory=dict)
    error: Optional[Exception] = None


class Workflow:
    """Workflow definition with multiple steps."""

    def __init__(self, name: str) -> None:
        """Initialize workflow.

        Args:
            name: Workflow name.
        """
        self.name = name
        self._steps: List[WorkflowStep] = []

    def add_step(self, step: WorkflowStep) -> "Workflow":
        """Add a step to workflow.

        Args:
            step: Step to add.

        Returns:
            Self for chaining.
        """
        self._steps.append(step)
        return self

    def step(
        self,
        name: str,
        action: Callable[[], Any],
        condition: Optional[Callable[[], bool]] = None,
        on_failure: Optional[Callable[[Exception], None]] = None,
        retry_count: int = 0,
        timeout: float = 0,
    ) -> "Workflow":
        """Add a step using fluent interface.

        Args:
            name: Step name.
            action: Action to execute.
            condition: Optional condition.
            on_failure: Failure handler.
            retry_count: Number of retries.
            timeout: Timeout in seconds.

        Returns:
            Self for chaining.
        """
        step = WorkflowStep(
            name=name,
            action=action,
            condition=condition,
            on_failure=on_failure,
            retry_count=retry_count,
            timeout=timeout,
        )
        self._steps.append(step)
        return self

    def execute(self) -> WorkflowResult:
        """Execute workflow.

        Returns:
            Workflow result.
        """
        result = WorkflowResult(status=WorkflowStatus.RUNNING)
        context = {}

        for step in self._steps:
            # Check condition
            if step.condition:
                try:
                    if not step.condition():
                        continue
                except Exception as e:
                    result.status = WorkflowStatus.FAILED
                    result.error = e
                    return result

            # Execute step
            try:
                step_result = self._execute_step(step)
                result.step_results[step.name] = step_result
            except Exception as e:
                if step.on_failure:
                    try:
                        step.on_failure(e)
                    except Exception:
                        pass
                else:
                    result.status = WorkflowStatus.FAILED
                    result.error = e
                    return result

        result.status = WorkflowStatus.COMPLETED
        return result

    def _execute_step(self, step: WorkflowStep) -> Any:
        """Execute a single step with retry.

        Args:
            step: Step to execute.

        Returns:
            Step result.
        """
        last_error = None

        for attempt in range(step.retry_count + 1):
            try:
                return step.action()
            except Exception as e:
                last_error = e
                if attempt < step.retry_count:
                    continue

        if last_error:
            raise last_error
        raise RuntimeError("Step execution failed")


class WorkflowBuilder:
    """Build workflows programmatically."""

    def __init__(self, name: str) -> None:
        """Initialize builder.

        Args:
            name: Workflow name.
        """
        self._workflow = Workflow(name)

    def add_step(
        self,
        name: str,
        action: Callable[[], Any],
        **kwargs,
    ) -> "WorkflowBuilder":
        """Add a step.

        Args:
            name: Step name.
            action: Action function.
            **kwargs: Additional step options.

        Returns:
            Self for chaining.
        """
        self._workflow.add_step(WorkflowStep(name=name, action=action, **kwargs))
        return self

    def build(self) -> Workflow:
        """Build workflow.

        Returns:
            Built workflow.
        """
        return self._workflow


class WorkflowValidator:
    """Validate workflow definitions."""

    @staticmethod
    def validate(workflow: Workflow) -> List[str]:
        """Validate workflow.

        Args:
            workflow: Workflow to validate.

        Returns:
            List of validation errors (empty if valid).
        """
        errors = []

        if not workflow.name:
            errors.append("Workflow name is required")

        if not workflow._steps:
            errors.append("Workflow must have at least one step")

        for i, step in enumerate(workflow._steps):
            if not step.name:
                errors.append(f"Step {i} must have a name")
            if not callable(step.action):
                errors.append(f"Step {i} action must be callable")

        return errors

    @staticmethod
    def is_valid(workflow: Workflow) -> bool:
        """Check if workflow is valid.

        Args:
            workflow: Workflow to check.

        Returns:
            True if valid.
        """
        return len(WorkflowValidator.validate(workflow)) == 0


class WorkflowRunner:
    """Run workflows with context and cancellation."""

    def __init__(self) -> None:
        """Initialize runner."""
        self._running: Dict[str, Workflow] = {}
        self._cancelled: set = set()

    def run(self, workflow: Workflow) -> WorkflowResult:
        """Run a workflow.

        Args:
            workflow: Workflow to run.

        Returns:
            Result.
        """
        self._running[workflow.name] = workflow

        if workflow.name in self._cancelled:
            result = WorkflowResult(status=WorkflowStatus.CANCELLED)
            del self._running[workflow.name]
            return result

        result = workflow.execute()
        del self._running[workflow.name]
        return result

    def cancel(self, workflow_name: str) -> bool:
        """Cancel a running workflow.

        Args:
            workflow_name: Name of workflow to cancel.

        Returns:
            True if workflow was running.
        """
        if workflow_name in self._running:
            self._cancelled.add(workflow_name)
            return True
        return False

    def is_running(self, workflow_name: str) -> bool:
        """Check if workflow is running.

        Args:
            workflow_name: Workflow name.

        Returns:
            True if running.
        """
        return workflow_name in self._running


class WorkflowRegistry:
    """Registry for named workflows."""

    def __init__(self) -> None:
        """Initialize registry."""
        self._workflows: Dict[str, Workflow] = {}

    def register(self, workflow: Workflow) -> None:
        """Register a workflow.

        Args:
            workflow: Workflow to register.
        """
        self._workflows[workflow.name] = workflow

    def get(self, name: str) -> Optional[Workflow]:
        """Get workflow by name.

        Args:
            name: Workflow name.

        Returns:
            Workflow or None.
        """
        return self._workflows.get(name)

    def list_workflows(self) -> List[str]:
        """List registered workflow names.

        Returns:
            List of names.
        """
        return list(self._workflows.keys())
