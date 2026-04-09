"""Automation Executor with Step Tracking.

This module provides automation execution with step tracking:
- Multi-step workflow execution
- Step success/failure tracking
- Rollback support
- Execution history

Example:
    >>> from actions.automation_executor_action import AutomationExecutor
    >>> executor = AutomationExecutor()
    >>> result = executor.execute("deploy_workflow", steps=[...])
"""

from __future__ import annotations

import time
import logging
import threading
import traceback
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import Enum

logger = logging.getLogger(__name__)


class StepStatus(Enum):
    """Step execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    ROLLED_BACK = "rolled_back"


@dataclass
class Step:
    """A single automation step."""
    name: str
    func_name: str
    args: tuple = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    status: StepStatus = StepStatus.PENDING
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    duration_ms: float = 0.0
    result: Any = None
    error: Optional[str] = None
    rollback_func_name: Optional[str] = None


@dataclass
class Execution:
    """A complete automation execution."""
    execution_id: str
    workflow_name: str
    steps: list[Step]
    status: StepStatus = StepStatus.PENDING
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    duration_ms: float = 0.0
    total_steps: int = 0
    successful_steps: int = 0
    failed_steps: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


class AutomationExecutor:
    """Executes automation workflows with step tracking and rollback."""

    def __init__(self, stop_on_failure: bool = True) -> None:
        """Initialize the executor.

        Args:
            stop_on_failure: Stop execution when a step fails.
        """
        self._stop_on_failure = stop_on_failure
        self._funcs: dict[str, Callable] = {}
        self._rollbacks: dict[str, Callable] = {}
        self._executions: dict[str, Execution] = {}
        self._lock = threading.RLock()
        self._stats: dict[str, int] = {}

    def register(
        self,
        func_name: str,
        func: Callable,
        rollback: Optional[Callable] = None,
    ) -> None:
        """Register a function for use in workflows.

        Args:
            func_name: Name to register under.
            func: The function to execute.
            rollback: Optional rollback function.
        """
        with self._lock:
            self._funcs[func_name] = func
            if rollback:
                self._rollbacks[func_name] = rollback
            logger.info("Registered automation function: %s", func_name)

    def execute(
        self,
        workflow_name: str,
        steps: list[dict[str, Any]],
        metadata: Optional[dict[str, Any]] = None,
    ) -> Execution:
        """Execute a workflow.

        Args:
            workflow_name: Name for this execution.
            steps: List of step definitions with name, func_name, args, kwargs.

        Returns:
            The Execution result.
        """
        execution_id = str(uuid.uuid4())[:8]
        step_objects = []
        for s in steps:
            step = Step(
                name=s["name"],
                func_name=s["func_name"],
                args=s.get("args", ()),
                kwargs=s.get("kwargs", {}),
                rollback_func_name=s.get("rollback_func_name"),
            )
            step_objects.append(step)

        execution = Execution(
            execution_id=execution_id,
            workflow_name=workflow_name,
            steps=step_objects,
            total_steps=len(step_objects),
            metadata=metadata or {},
        )

        with self._lock:
            self._executions[execution_id] = execution

        self._run_execution(execution)

        return execution

    def _run_execution(self, execution: Execution) -> None:
        """Run an execution's steps."""
        execution.status = StepStatus.RUNNING
        execution.started_at = time.time()

        for step in execution.steps:
            if execution.status == StepStatus.FAILED and self._stop_on_failure:
                step.status = StepStatus.SKIPPED
                continue

            self._run_step(step)
            execution.successful_steps = sum(
                1 for s in execution.steps if s.status == StepStatus.SUCCESS
            )
            execution.failed_steps = sum(
                1 for s in execution.steps if s.status == StepStatus.FAILED
            )

            if step.status == StepStatus.FAILED:
                execution.status = StepStatus.FAILED
                if self._stop_on_failure:
                    for remaining in execution.steps[execution.steps.index(step) + 1:]:
                        remaining.status = StepStatus.SKIPPED

        if execution.status != StepStatus.FAILED:
            execution.status = StepStatus.SUCCESS

        execution.completed_at = time.time()
        execution.duration_ms = (execution.completed_at - execution.started_at) * 1000

        logger.info(
            "Execution %s completed: %s (%.1fms, %d/%d steps)",
            execution.execution_id,
            execution.status.value,
            execution.duration_ms,
            execution.successful_steps,
            execution.total_steps,
        )

    def _run_step(self, step: Step) -> None:
        """Run a single step."""
        step.status = StepStatus.RUNNING
        step.started_at = time.time()

        func = self._funcs.get(step.func_name)
        if func is None:
            step.status = StepStatus.FAILED
            step.error = f"Function not registered: {step.func_name}"
            step.completed_at = time.time()
            step.duration_ms = (step.completed_at - step.started_at) * 1000
            return

        try:
            result = func(*step.args, **step.kwargs)
            step.result = result
            step.status = StepStatus.SUCCESS
        except Exception as e:
            step.error = f"{type(e).__name__}: {e}"
            step.status = StepStatus.FAILED
            logger.error("Step %s failed: %s", step.name, e)

        step.completed_at = time.time()
        step.duration_ms = (step.completed_at - step.started_at) * 1000

    def rollback(self, execution_id: str) -> int:
        """Rollback a failed execution.

        Args:
            execution_id: The execution ID to rollback.

        Returns:
            Number of steps rolled back.
        """
        with self._lock:
            execution = self._executions.get(execution_id)
            if execution is None:
                return 0

        rolled_back = 0
        for step in reversed(execution.steps):
            if step.status == StepStatus.SUCCESS:
                if step.rollback_func_name:
                    rollback_func = self._rollbacks.get(step.rollback_func_name)
                    if rollback_func:
                        try:
                            rollback_func(step.result)
                            step.status = StepStatus.ROLLED_BACK
                            rolled_back += 1
                            logger.info("Rolled back step: %s", step.name)
                        except Exception as e:
                            logger.error("Rollback failed for %s: %s", step.name, e)
                else:
                    step.status = StepStatus.ROLLED_BACK
                    rolled_back += 1

        execution.status = StepStatus.ROLLED_BACK
        return rolled_back

    def get_execution(self, execution_id: str) -> Optional[Execution]:
        """Get an execution by ID."""
        with self._lock:
            return self._executions.get(execution_id)

    def list_executions(self, limit: int = 50) -> list[Execution]:
        """List recent executions."""
        with self._lock:
            return sorted(
                self._executions.values(),
                key=lambda e: e.created_at,
                reverse=True,
            )[:limit]

    def get_stats(self) -> dict[str, Any]:
        """Get execution statistics."""
        with self._lock:
            return {
                "total_executions": len(self._executions),
                "pending": sum(1 for e in self._executions.values() if e.status == StepStatus.PENDING),
                "running": sum(1 for e in self._executions.values() if e.status == StepStatus.RUNNING),
                "success": sum(1 for e in self._executions.values() if e.status == StepStatus.SUCCESS),
                "failed": sum(1 for e in self._executions.values() if e.status == StepStatus.FAILED),
            }
