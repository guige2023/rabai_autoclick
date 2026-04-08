"""Workflow orchestration utilities for multi-step automation sequences.

Provides a declarative workflow DSL for defining automation flows
with steps, conditions, loops, and error handling. Supports
parallel branch execution and workflow checkpoints.

Example:
    >>> from utils.workflow_utils import Workflow, Step, Condition
    >>> wf = Workflow('ExampleFlow')
    >>> wf.step('open_app').run(lambda: open_app('Safari'))
    >>> wf.step('click_login').wait_for('login_btn.png')
    >>> wf.run()
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

__all__ = [
    "Workflow",
    "Step",
    "Condition",
    "WorkflowError",
    "StepResult",
    "WorkflowResult",
]


class WorkflowError(Exception):
    """Raised when a workflow execution error occurs."""
    pass


@dataclass
class StepResult:
    """Result of a single step execution."""

    step_name: str
    success: bool
    output: Any = None
    error: Optional[Exception] = None
    duration: float = 0.0
    timestamp: float = field(default_factory=time.time)


@dataclass
class WorkflowResult:
    """Result of an entire workflow execution."""

    workflow_name: str
    success: bool
    step_results: list[StepResult] = field(default_factory=list)
    output: Any = None
    error: Optional[Exception] = None
    total_duration: float = 0.0
    started_at: float = field(default_factory=time.time)
    completed_at: float = 0.0

    def get_step(self, name: str) -> Optional[StepResult]:
        for r in self.step_results:
            if r.step_name == name:
                return r
        return None

    def all_steps_passed(self) -> bool:
        return all(r.success for r in self.step_results)

    def failed_steps(self) -> list[StepResult]:
        return [r for r in self.step_results if not r.success]


class Step:
    """A single executable step in a workflow.

    Attributes:
        name: Step identifier.
        action: Callable to execute.
        timeout: Optional timeout in seconds.
        retry: Number of retries on failure.
        condition: Optional Condition for conditional execution.
    """

    def __init__(
        self,
        name: str,
        action: Optional[Callable] = None,
        timeout: Optional[float] = None,
        retry: int = 0,
        condition: Optional["Condition"] = None,
        description: str = "",
    ):
        self.name = name
        self.action = action
        self.timeout = timeout
        self.retry = retry
        self.condition = condition
        self.description = description
        self._result: Optional[StepResult] = None

    def run(self, action: Optional[Callable] = None) -> "Step":
        """Set the action to execute for this step."""
        if action is not None:
            self.action = action
        return self

    def with_timeout(self, seconds: float) -> "Step":
        self.timeout = seconds
        return self

    def with_retry(self, count: int) -> "Step":
        self.retry = count
        return self

    def when(self, condition: "Condition") -> "Step":
        self.condition = condition
        return self

    def execute(self, context: Optional[dict] = None) -> StepResult:
        """Execute this step, handling retries and conditions."""
        ctx = context or {}
        start = time.perf_counter()

        # Check condition
        if self.condition is not None and not self.condition.evaluate(ctx):
            return StepResult(
                step_name=self.name,
                success=True,
                output="SKIPPED (condition false)",
                duration=time.perf_counter() - start,
            )

        last_error: Optional[Exception] = None
        for attempt in range(self.retry + 1):
            try:
                if self.timeout:
                    output = self._execute_with_timeout(self.timeout)
                else:
                    output = self._execute_callable(self.action, ctx)

                return StepResult(
                    step_name=self.name,
                    success=True,
                    output=output,
                    duration=time.perf_counter() - start,
                )
            except Exception as e:
                last_error = e
                if attempt < self.retry:
                    time.sleep(0.5 * (attempt + 1))  # backoff

        return StepResult(
            step_name=self.name,
            success=False,
            error=last_error,
            duration=time.perf_counter() - start,
        )

    def _execute_callable(self, action: Optional[Callable], ctx: dict) -> Any:
        if action is None:
            return None
        if isinstance(action, Step):
            return action.execute(ctx)
        result = action(ctx) if callable(action) else action
        return result

    def _execute_with_timeout(self, timeout: float) -> Any:
        import threading

        result = [None]
        error = [None]

        def target():
            try:
                result[0] = self._execute_callable(self.action, {})
            except Exception as e:
                error[0] = e

        t = threading.Thread(target=target, daemon=True)
        t.start()
        t.join(timeout=timeout)
        if t.is_alive():
            raise TimeoutError(f"Step '{self.name}' timed out after {timeout}s")
        if error[0]:
            raise error[0]
        return result[0]


@dataclass
class Condition:
    """A condition for conditional step execution."""

    expression: Callable[[dict], bool]

    def evaluate(self, context: dict) -> bool:
        try:
            return bool(self.expression(context))
        except Exception:
            return False

    @classmethod
    def equals(cls, key: str, value: Any) -> "Condition":
        return cls(lambda ctx: ctx.get(key) == value)

    @classmethod
    def truthy(cls, key: str) -> "Condition":
        return cls(lambda ctx: bool(ctx.get(key)))


class Workflow:
    """A named workflow containing ordered steps.

    Example:
        >>> wf = Workflow('TestFlow')
        >>> wf.step('one').run(lambda: print(1))
        >>> wf.step('two').run(lambda: print(2))
        >>> result = wf.run()
    """

    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self._steps: list[Step] = []
        self._context: dict[str, Any] = {}
        self._hooks: dict[str, list[Callable]] = {
            "before_step": [],
            "after_step": [],
            "on_error": [],
            "on_complete": [],
        }

    def step(self, name: str) -> Step:
        """Add a new named step and return it for configuration.

        Args:
            name: Step name.

        Returns:
            The Step instance for chaining.
        """
        step = Step(name=name)
        self._steps.append(step)
        return step

    def run(
        self,
        context: Optional[dict[str, Any]] = None,
        stop_on_error: bool = True,
    ) -> WorkflowResult:
        """Execute the workflow.

        Args:
            context: Shared context dictionary.
            stop_on_error: Stop execution on first step failure.

        Returns:
            WorkflowResult with execution details.
        """
        import time as time_module

        self._context = context or {}
        start = time_module.time()
        step_results: list[StepResult] = []
        success = True
        final_error: Optional[Exception] = None

        for step in self._steps:
            self._fire_hooks("before_step", step)

            result = step.execute(self._context)
            step_results.append(result)
            self._context[f"_{step.name}_result"] = result

            if not result.success:
                success = False
                final_error = result.error
                self._fire_hooks("on_error", step, result.error)
                if stop_on_error:
                    break

            self._fire_hooks("after_step", step, result)

        completed_at = time_module.time()
        workflow_result = WorkflowResult(
            workflow_name=self.name,
            success=success,
            step_results=step_results,
            error=final_error,
            total_duration=completed_at - start,
            completed_at=completed_at,
        )

        self._fire_hooks("on_complete", workflow_result)
        return workflow_result

    def on(self, event: str) -> Callable:
        """Register a hook for a workflow event.

        Args:
            event: One of 'before_step', 'after_step', 'on_error', 'on_complete'.

        Returns:
            Decorator for the hook function.

        Example:
            >>> @wf.on('after_step')
            ... def log_step(step, result):
            ...     print(f"Step {step.name}: {result.success}")
        """
        def decorator(func: Callable) -> Callable:
            if event in self._hooks:
                self._hooks[event].append(func)
            return func
        return decorator

    def _fire_hooks(self, event: str, *args) -> None:
        for hook in self._hooks.get(event, []):
            try:
                hook(*args)
            except Exception:
                pass

    def get_context(self) -> dict[str, Any]:
        return dict(self._context)

    def __len__(self) -> int:
        return len(self._steps)

    def __repr__(self) -> str:
        return f"Workflow(name={self.name!r}, steps={len(self)})"
