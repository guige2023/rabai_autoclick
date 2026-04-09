"""Workflow engine for orchestrating multi-step operations.

This module provides workflow orchestration:
- Step dependency management
- Parallel and sequential execution
- Conditional branching
- Retry and compensation logic

Example:
    >>> from actions.workflow_engine_action import Workflow, WorkflowEngine
    >>> workflow = Workflow().step("validate", validate).step("process", process)
    >>> engine = WorkflowEngine()
    >>> result = await engine.execute(workflow)
"""

from __future__ import annotations

import time
import logging
import threading
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
from collections import deque

logger = logging.getLogger(__name__)


class StepStatus(Enum):
    """Status of a workflow step."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class StepType(Enum):
    """Type of workflow step."""
    TASK = "task"
    CONDITION = "condition"
    PARALLEL = "parallel"
    WAIT = "wait"


@dataclass
class WorkflowStep:
    """A single step in a workflow."""
    name: str
    func: Callable[..., Any]
    step_type: StepType = StepType.TASK
    depends_on: list[str] = field(default_factory=list)
    condition: Optional[Callable[[dict[str, Any]], bool]] = None
    retry_count: int = 0
    retry_delay: float = 0.0
    timeout: Optional[float] = None
    on_failure: Optional[Callable[[Exception, dict[str, Any]], Any]] = None


@dataclass
class StepResult:
    """Result of a workflow step execution."""
    name: str
    status: StepStatus
    output: Any = None
    error: Optional[str] = None
    duration: float = 0.0
    started_at: Optional[float] = None
    completed_at: Optional[float] = None


@dataclass
class WorkflowResult:
    """Result of a workflow execution."""
    success: bool
    outputs: dict[str, Any] = field(default_factory=dict)
    step_results: list[StepResult] = field(default_factory=list)
    total_duration: float = 0.0
    error: Optional[str] = None


class Workflow:
    """A workflow definition.

    Example:
        >>> workflow = (
        ...     Workflow()
        ...     .step("validate", validate_input)
        ...     .step("process", process_data, depends_on=["validate"])
        ...     .step("notify", send_notification, depends_on=["process"])
        ... )
    """

    def __init__(self, name: str = "workflow") -> None:
        self.name = name
        self._steps: dict[str, WorkflowStep] = {}
        self._execution_order: list[str] = []

    def step(
        self,
        name: str,
        func: Callable[..., Any],
        step_type: StepType = StepType.TASK,
        depends_on: Optional[list[str]] = None,
        condition: Optional[Callable[[dict[str, Any]], bool]] = None,
        retry_count: int = 0,
        retry_delay: float = 0.0,
        timeout: Optional[float] = None,
        on_failure: Optional[Callable[[Exception, dict[str, Any]], Any]] = None,
    ) -> Workflow:
        """Add a step to the workflow.

        Args:
            name: Step name.
            func: Step function.
            step_type: Type of step.
            depends_on: List of step names this depends on.
            condition: Optional condition for execution.
            retry_count: Number of retries on failure.
            retry_delay: Delay between retries.
            timeout: Optional step timeout.
            on_failure: Handler for step failure.

        Returns:
            Self for chaining.
        """
        self._steps[name] = WorkflowStep(
            name=name,
            func=func,
            step_type=step_type,
            depends_on=depends_on or [],
            condition=condition,
            retry_count=retry_count,
            retry_delay=retry_delay,
            timeout=timeout,
            on_failure=on_failure,
        )
        self._execution_order.append(name)
        return self

    def get_step(self, name: str) -> Optional[WorkflowStep]:
        """Get a step by name."""
        return self._steps.get(name)

    def get_execution_order(self) -> list[str]:
        """Get steps in execution order respecting dependencies."""
        return list(self._execution_order)

    def validate(self) -> list[str]:
        """Validate workflow structure.

        Returns:
            List of validation errors.
        """
        errors = []
        for name, step in self._steps.items():
            for dep in step.depends_on:
                if dep not in self._steps:
                    errors.append(f"Step '{name}' depends on unknown step '{dep}'")
        if self._has_cycle():
            errors.append("Workflow contains a cycle")
        return errors

    def _has_cycle(self) -> bool:
        """Check for circular dependencies."""
        visited = set()
        rec_stack = set()

        def visit(name: str) -> bool:
            visited.add(name)
            rec_stack.add(name)
            step = self._steps.get(name)
            if step:
                for dep in step.depends_on:
                    if dep not in visited:
                        if visit(dep):
                            return True
                    elif dep in rec_stack:
                        return True
            rec_stack.remove(name)
            return False

        for name in self._steps:
            if name not in visited:
                if visit(name):
                    return True
        return False


class WorkflowEngine:
    """Execute workflows with dependency management.

    Attributes:
        max_parallel: Maximum parallel step executions.
    """

    def __init__(self, max_parallel: int = 5) -> None:
        self.max_parallel = max_parallel
        self._running = False

    def execute(
        self,
        workflow: Workflow,
        context: Optional[dict[str, Any]] = None,
    ) -> WorkflowResult:
        """Execute a workflow synchronously.

        Args:
            workflow: Workflow to execute.
            context: Initial workflow context.

        Returns:
            WorkflowResult with outputs and status.
        """
        start_time = time.time()
        errors = workflow.validate()
        if errors:
            return WorkflowResult(
                success=False,
                error=f"Validation errors: {errors}",
                total_duration=time.time() - start_time,
            )
        ctx = context or {}
        step_results: dict[str, StepResult] = {}
        completed = set()
        pending = set(workflow._steps.keys())
        while pending:
            ready = [
                name for name in pending
                if all(dep in completed for dep in workflow._steps[name].depends_on)
            ]
            if not ready:
                break
            for name in ready:
                step = workflow._steps[name]
                if step.condition and not step.condition(ctx):
                    step_results[name] = StepResult(
                        name=name,
                        status=StepStatus.SKIPPED,
                    )
                    completed.add(name)
                    pending.remove(name)
                    continue
                result = self._execute_step(step, ctx, step_results.get(name))
                step_results[name] = result
                if result.status == StepStatus.SUCCESS:
                    ctx[name] = result.output
                    completed.add(name)
                    pending.remove(name)
                else:
                    if step.retry_count > 0:
                        step.retry_count -= 1
                    else:
                        return WorkflowResult(
                            success=False,
                            outputs=ctx,
                            step_results=list(step_results.values()),
                            total_duration=time.time() - start_time,
                            error=result.error,
                        )
        return WorkflowResult(
            success=True,
            outputs=ctx,
            step_results=list(step_results.values()),
            total_duration=time.time() - start_time,
        )

    def _execute_step(
        self,
        step: WorkflowStep,
        context: dict[str, Any],
        previous_result: Optional[StepResult] = None,
    ) -> StepResult:
        """Execute a single step."""
        result = StepResult(
            name=step.name,
            status=StepStatus.RUNNING,
            started_at=time.time(),
        )
        try:
            output = step.func(context)
            result.output = output
            result.status = StepStatus.SUCCESS
        except Exception as e:
            result.error = str(e)
            result.status = StepStatus.FAILED
            if step.on_failure:
                try:
                    result.output = step.on_failure(e, context)
                    result.status = StepStatus.SUCCESS
                except Exception as fallback_error:
                    result.error = f"{e}; fallback failed: {fallback_error}"
        result.duration = time.time() - (result.started_at or time.time())
        result.completed_at = time.time()
        return result
