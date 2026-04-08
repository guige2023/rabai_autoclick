"""Workflow engine action module.

Provides a programmable workflow execution engine with steps, branching,
loops, error handling, and state management.
"""

from __future__ import annotations

import time
import logging
from typing import Optional, Dict, Any, Callable, List
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class StepStatus(Enum):
    """Status of a workflow step."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class WorkflowStep:
    """A single step in a workflow."""
    name: str
    action: Callable[[Dict[str, Any]], Any]
    condition: Optional[Callable[[Dict[str, Any]], bool]] = None
    on_error: Optional[Callable[[Exception, Dict[str, Any]], Any]] = None
    retry_count: int = 0
    timeout_seconds: float = 60.0
    description: str = ""

    def __post_init__(self) -> None:
        if not callable(self.action):
            raise ValueError(f"Step '{self.name}' action must be callable")


@dataclass
class StepResult:
    """Result of executing a workflow step."""
    step_name: str
    status: StepStatus
    output: Any = None
    error: Optional[str] = None
    started_at: float = 0.0
    completed_at: float = 0.0
    duration_ms: float = 0.0

    @property
    def success(self) -> bool:
        return self.status == StepStatus.SUCCESS


class WorkflowEngine:
    """Workflow execution engine.

    Executes a sequence of steps with branching, loops, and error handling.

    Example:
        engine = WorkflowEngine()

        engine.step("fetch_data", lambda ctx: fetch(ctx["url"]))
        engine.step("process", lambda ctx: process(ctx["data"]), condition=lambda ctx: "data" in ctx)

        result = engine.execute({"url": "https://api.example.com"})
    """

    def __init__(
        self,
        name: str = "workflow",
        on_step_complete: Optional[Callable[[StepResult], None]] = None,
    ) -> None:
        """Initialize workflow engine.

        Args:
            name: Workflow name.
            on_step_complete: Callback after each step completes.
        """
        self.name = name
        self.on_step_complete = on_step_complete
        self._steps: List[WorkflowStep] = []

    def step(
        self,
        name: str,
        action: Callable[[Dict[str, Any]], Any],
        condition: Optional[Callable[[Dict[str, Any]], bool]] = None,
        on_error: Optional[Callable[[Exception, Dict[str, Any]], Any]] = None,
        retry_count: int = 0,
        timeout_seconds: float = 60.0,
        description: str = "",
    ) -> "WorkflowEngine":
        """Add a step to the workflow.

        Args:
            name: Step name (must be unique).
            action: Callable that executes the step.
            condition: Optional condition to skip step.
            on_error: Optional error handler.
            retry_count: Number of retries on failure.
            timeout_seconds: Step timeout.
            description: Step description.

        Returns:
            Self for chaining.
        """
        step = WorkflowStep(
            name=name,
            action=action,
            condition=condition,
            on_error=on_error,
            retry_count=retry_count,
            timeout_seconds=timeout_seconds,
            description=description,
        )
        self._steps.append(step)
        return self

    def branch(
        self,
        branches: Dict[str, Callable[[], "WorkflowEngine"]],
        selector: Callable[[Dict[str, Any]], str],
    ) -> "WorkflowEngine":
        """Add branching logic to the workflow.

        Args:
            branches: Dict of branch_name -> WorkflowEngine factory.
            selector: Function that returns branch name based on context.

        Returns:
            Self for chaining.
        """
        def branch_action(ctx: Dict[str, Any]) -> Any:
            branch_name = selector(ctx)
            if branch_name in branches:
                branch_engine = branches[branch_name]()
                return branch_engine.execute(ctx)
            logger.warning("No branch found for '%s'", branch_name)
            return None

        return self.step(f"branch_{len(self._steps)}", branch_action, description="Dynamic branch")

    def execute(self, initial_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute the workflow.

        Args:
            initial_context: Initial workflow context.

        Returns:
            Dict with 'status', 'results', 'context', and 'duration_ms'.
        """
        context = initial_context or {}
        results: List[StepResult] = []
        start_time = time.time()

        for step in self._steps:
            result = self._execute_step(step, context)
            results.append(result)

            if self.on_step_complete:
                self.on_step_complete(result)

            if not result.success and step.on_error is None:
                logger.warning("Step '%s' failed, continuing due to no error handler", step.name)
                context[f"{step.name}_error"] = result.error

        duration_ms = (time.time() - start_time) * 1000
        overall_status = "success" if all(r.success for r in results) else "failed"

        return {
            "status": overall_status,
            "results": [(r.step_name, r.status.value, r.output, r.error) for r in results],
            "context": context,
            "duration_ms": duration_ms,
        }

    def _execute_step(self, step: WorkflowStep, context: Dict[str, Any]) -> StepResult:
        """Execute a single step with retry and error handling."""
        if step.condition and not step.condition(context):
            return StepResult(
                step_name=step.name,
                status=StepStatus.SKIPPED,
                started_at=time.time(),
                completed_at=time.time(),
            )

        for attempt in range(step.retry_count + 1):
            started_at = time.time()
            try:
                output = self._run_with_timeout(step, context)
                completed_at = time.time()
                return StepResult(
                    step_name=step.name,
                    status=StepStatus.SUCCESS,
                    output=output,
                    started_at=started_at,
                    completed_at=completed_at,
                    duration_ms=(completed_at - started_at) * 1000,
                )
            except Exception as e:
                last_error = e
                if step.on_error and attempt == step.retry_count:
                    try:
                        output = step.on_error(last_error, context)
                        return StepResult(
                            step_name=step.name,
                            status=StepStatus.SUCCESS,
                            output=output,
                            started_at=started_at,
                            completed_at=time.time(),
                        )
                    except Exception as err:
                        last_error = err

        return StepResult(
            step_name=step.name,
            status=StepStatus.FAILED,
            error=str(last_error),
            started_at=time.time(),
            completed_at=time.time(),
        )

    def _run_with_timeout(self, step: WorkflowStep, context: Dict[str, Any]) -> Any:
        """Run step action with timeout."""
        import signal

        def timeout_handler(signum, frame):
            raise TimeoutError(f"Step '{step.name}' timed out after {step.timeout_seconds}s")

        old_handler = signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(int(step.timeout_seconds))
        try:
            result = step.action(context)
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)

        return result


class WorkflowEngineAction:
    """High-level workflow engine wrapper.

    Example:
        action = WorkflowEngineAction()
        action.add_step("validate", validate_input, condition=lambda c: c.get("validate"))
        action.add_step("process", process_data)
        result = action.run({"validate": True, "data": [1, 2, 3]})
    """

    def __init__(self, name: str = "workflow") -> None:
        self.engine = WorkflowEngine(name=name)

    def add_step(
        self,
        name: str,
        action: Callable[[Dict[str, Any]], Any],
        **kwargs,
    ) -> None:
        """Add a step to the workflow."""
        self.engine.step(name, action, **kwargs)

    def run(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute the workflow."""
        return self.engine.execute(context)
