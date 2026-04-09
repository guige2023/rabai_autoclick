"""
Workflow context management utilities.

This module provides context management for multi-step
automation workflows with state persistence and sharing.
"""

from __future__ import annotations

import time
import threading
import copy
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TypeVar, Generic


T = TypeVar("T")


@dataclass
class WorkflowStep:
    """
    Represents a single step in a workflow.

    Attributes:
        step_id: Unique identifier for the step.
        name: Human-readable step name.
        action: The action to execute.
        description: Description of what the step does.
        retry_count: Number of times to retry on failure.
        timeout: Maximum execution time in seconds.
        condition: Optional condition for step execution.
    """
    step_id: str
    name: str
    action: Callable[["WorkflowContext"], Any]
    description: str = ""
    retry_count: int = 0
    timeout: float = 30.0
    condition: Optional[Callable[["WorkflowContext"], bool]] = None


@dataclass
class StepResult:
    """
    Result of a workflow step execution.

    Attributes:
        step_id: ID of the step that was executed.
        success: Whether the step completed successfully.
        result: The return value from the step.
        error: Error message if failed.
        duration: Execution time in seconds.
        attempts: Number of execution attempts.
    """
    step_id: str
    success: bool
    result: Any = None
    error: Optional[str] = None
    duration: float = 0.0
    attempts: int = 1


class WorkflowContext:
    """
    Shared context for workflow execution.

    Stores state, variables, and results that can be
    accessed and modified by workflow steps.
    """

    def __init__(self) -> None:
        self._data: Dict[str, Any] = {}
        self._results: Dict[str, StepResult] = {}
        self._lock = threading.RLock()
        self._start_time: float = field(default_factory=time.time)
        self._metadata: Dict[str, Any] = {}

    def set(self, key: str, value: Any) -> WorkflowContext:
        """Set a context variable."""
        with self._lock:
            self._data[key] = value
        return self

    def get(self, key: str, default: T = None) -> T:
        """Get a context variable."""
        with self._lock:
            return self._data.get(key, default)

    def has(self, key: str) -> bool:
        """Check if a variable exists."""
        with self._lock:
            return key in self._data

    def remove(self, key: str) -> bool:
        """Remove a context variable."""
        with self._lock:
            if key in self._data:
                del self._data[key]
                return True
            return False

    def set_result(self, step_id: str, result: StepResult) -> None:
        """Store a step execution result."""
        with self._lock:
            self._results[step_id] = result

    def get_result(self, step_id: str) -> Optional[StepResult]:
        """Get result of a specific step."""
        with self._lock:
            return self._results.get(step_id)

    def get_all_results(self) -> Dict[str, StepResult]:
        """Get all step results."""
        with self._lock:
            return copy.deepcopy(self._results)

    def set_metadata(self, key: str, value: Any) -> WorkflowContext:
        """Set workflow metadata."""
        self._metadata[key] = value
        return self

    def get_metadata(self, key: str, default: T = None) -> T:
        """Get workflow metadata."""
        return self._metadata.get(key, default)

    @property
    def elapsed_time(self) -> float:
        """Get elapsed time since workflow started."""
        return time.time() - self._start_time

    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary."""
        with self._lock:
            return {
                "data": copy.deepcopy(self._data),
                "results": {
                    k: {
                        "step_id": v.step_id,
                        "success": v.success,
                        "result": str(v.result)[:100],
                        "error": v.error,
                        "duration": v.duration,
                        "attempts": v.attempts,
                    }
                    for k, v in self._results.items()
                },
                "metadata": copy.deepcopy(self._metadata),
                "elapsed_time": self.elapsed_time,
            }


class Workflow:
    """
    Manages execution of a multi-step workflow.

    Provides step sequencing, error handling, and result
    aggregation.
    """

    def __init__(self, name: str = "") -> None:
        self._name = name
        self._steps: List[WorkflowStep] = []
        self._on_step_start: Optional[Callable[[WorkflowContext, WorkflowStep], None]] = None
        self._on_step_complete: Optional[Callable[[WorkflowContext, WorkflowStep, StepResult], None]] = None
        self._on_workflow_complete: Optional[Callable[[WorkflowContext], None]] = None
        self._on_workflow_error: Optional[Callable[[WorkflowContext, Exception], None]] = None

    @property
    def name(self) -> str:
        """Get workflow name."""
        return self._name

    def add_step(self, step: WorkflowStep) -> Workflow:
        """Add a step to the workflow."""
        self._steps.append(step)
        return self

    def step(
        self,
        step_id: str,
        name: str,
        description: str = "",
        retry_count: int = 0,
        timeout: float = 30.0,
    ) -> Callable[[Callable], Callable]:
        """
        Decorator to add a step to the workflow.

        Usage:
            @workflow.step("step1", "First Step")
            def my_action(ctx):
                return "done"
        """
        def decorator(func: Callable) -> Callable:
            step = WorkflowStep(
                step_id=step_id,
                name=name,
                action=func,
                description=description,
                retry_count=retry_count,
                timeout=timeout,
            )
            self._steps.append(step)
            return func
        return decorator

    def on_step_start(
        self,
        handler: Callable[[WorkflowContext, WorkflowStep], None],
    ) -> Workflow:
        """Set callback for step start."""
        self._on_step_start = handler
        return self

    def on_step_complete(
        self,
        handler: Callable[[WorkflowContext, WorkflowStep, StepResult], None],
    ) -> Workflow:
        """Set callback for step completion."""
        self._on_step_complete = handler
        return self

    def on_workflow_complete(
        self,
        handler: Callable[[WorkflowContext], None],
    ) -> Workflow:
        """Set callback for workflow completion."""
        self._on_workflow_complete = handler
        return self

    def on_workflow_error(
        self,
        handler: Callable[[WorkflowContext, Exception], None],
    ) -> Workflow:
        """Set callback for workflow error."""
        self._on_workflow_error = handler
        return self

    def execute(self, initial_context: Optional[Dict[str, Any]] = None) -> WorkflowContext:
        """
        Execute the workflow.

        Returns the final workflow context.
        """
        context = WorkflowContext()

        if initial_context:
            for key, value in initial_context.items():
                context.set(key, value)

        try:
            for step in self._steps:
                result = self._execute_step(context, step)
                context.set_result(step.step_id, result)

                if self._on_step_complete:
                    self._on_step_complete(context, step, result)

                if not result.success:
                    break

            if self._on_workflow_complete:
                self._on_workflow_complete(context)

        except Exception as e:
            if self._on_workflow_error:
                self._on_workflow_error(context, e)
            raise

        return context

    def _execute_step(self, context: WorkflowContext, step: WorkflowStep) -> StepResult:
        """Execute a single step with retries."""
        if step.condition and not step.condition(context):
            return StepResult(
                step_id=step.step_id,
                success=True,
                result="Skipped (condition not met)",
            )

        if self._on_step_start:
            self._on_step_start(context, step)

        attempts = 0
        last_error: Optional[str] = None

        while attempts <= step.retry_count:
            attempts += 1
            start_time = time.time()

            try:
                result = step.action(context)
                duration = time.time() - start_time

                return StepResult(
                    step_id=step.step_id,
                    success=True,
                    result=result,
                    duration=duration,
                    attempts=attempts,
                )

            except Exception as e:
                duration = time.time() - start_time
                last_error = str(e)

                if attempts > step.retry_count:
                    return StepResult(
                        step_id=step.step_id,
                        success=False,
                        error=last_error,
                        duration=duration,
                        attempts=attempts,
                    )

        return StepResult(
            step_id=step.step_id,
            success=False,
            error=last_error,
            attempts=attempts,
        )


class WorkflowBuilder:
    """
    Fluent builder for creating workflows.

    Provides a chainable API for workflow construction.
    """

    def __init__(self, name: str = "") -> None:
        self._workflow = Workflow(name)

    def name(self, name: str) -> WorkflowBuilder:
        """Set workflow name."""
        self._workflow._name = name
        return self

    def step(
        self,
        step_id: str,
        name: str,
        action: Callable[[WorkflowContext], Any],
        **kwargs: Any,
    ) -> WorkflowBuilder:
        """Add a step to the workflow."""
        self._workflow.add_step(WorkflowStep(
            step_id=step_id,
            name=name,
            action=action,
            **kwargs,
        ))
        return self

    def on_step_complete(
        self,
        handler: Callable[[WorkflowContext, WorkflowStep, StepResult], None],
    ) -> WorkflowBuilder:
        """Set step completion callback."""
        self._workflow.on_step_complete(handler)
        return self

    def on_workflow_complete(
        self,
        handler: Callable[[WorkflowContext], None],
    ) -> WorkflowBuilder:
        """Set workflow completion callback."""
        self._workflow.on_workflow_complete(handler)
        return self

    def build(self) -> Workflow:
        """Build and return the workflow."""
        return self._workflow
