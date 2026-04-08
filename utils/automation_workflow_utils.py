"""Automation workflow utilities.

This module provides utilities for managing and executing
multi-step automation workflows.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional, TypeVar
from dataclasses import dataclass, field
from enum import Enum, auto


class StepStatus(Enum):
    """Status of a workflow step."""
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    SKIPPED = auto()


@dataclass
class WorkflowStep:
    """A single step in a workflow."""
    name: str
    action: Callable[[], Any]
    description: str = ""
    timeout_seconds: float = 30.0
    retry_count: int = 0
    required: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StepResult:
    """Result of a workflow step execution."""
    step_name: str
    status: StepStatus
    started_at: float
    completed_at: float = 0.0
    error: Optional[str] = None
    result: Any = None

    @property
    def duration_seconds(self) -> float:
        if self.completed_at == 0.0:
            return 0.0
        return self.completed_at - self.started_at


@dataclass
class WorkflowResult:
    """Result of a workflow execution."""
    workflow_name: str
    status: StepStatus
    started_at: float
    completed_at: float = 0.0
    step_results: List[StepResult] = field(default_factory=list)
    error: Optional[str] = None

    @property
    def success(self) -> bool:
        return self.status == StepStatus.COMPLETED

    @property
    def total_duration_seconds(self) -> float:
        if self.completed_at == 0.0:
            return 0.0
        return self.completed_at - self.started_at


class Workflow:
    """An automation workflow."""

    def __init__(
        self,
        name: str,
        steps: Optional[List[WorkflowStep]] = None,
    ) -> None:
        self.name = name
        self._steps: List[WorkflowStep] = steps or []
        self._on_step_complete: Optional[Callable[[StepResult], None]] = None
        self._on_workflow_complete: Optional[Callable[[WorkflowResult], None]] = None

    def add_step(self, step: WorkflowStep) -> "Workflow":
        self._steps.append(step)
        return self

    def on_step_complete(self, handler: Callable[[StepResult], None]) -> None:
        self._on_step_complete = handler

    def on_workflow_complete(self, handler: Callable[[WorkflowResult], None]) -> None:
        self._on_workflow_complete = handler

    def execute(self) -> WorkflowResult:
        """Execute the workflow.

        Returns:
            WorkflowResult with execution details.
        """
        started_at = time.perf_counter()
        step_results: List[StepResult] = []
        overall_status = StepStatus.COMPLETED

        for step in self._steps:
            result = self._execute_step(step)
            step_results.append(result)

            if result.status == StepStatus.FAILED:
                if step.required:
                    overall_status = StepStatus.FAILED
                    break
                else:
                    overall_status = StepStatus.COMPLETED

            if self._on_step_complete:
                self._on_step_complete(result)

        completed_at = time.perf_counter()
        workflow_result = WorkflowResult(
            workflow_name=self.name,
            status=overall_status,
            started_at=started_at,
            completed_at=completed_at,
            step_results=step_results,
        )

        if self._on_workflow_complete:
            self._on_workflow_complete(workflow_result)

        return workflow_result

    def _execute_step(self, step: WorkflowStep) -> StepResult:
        started_at = time.perf_counter()
        for attempt in range(step.retry_count + 1):
            try:
                result = step.action()
                completed_at = time.perf_counter()
                return StepResult(
                    step_name=step.name,
                    status=StepStatus.COMPLETED,
                    started_at=started_at,
                    completed_at=completed_at,
                    result=result,
                )
            except Exception as e:
                if attempt < step.retry_count:
                    time.sleep(0.1 * (attempt + 1))
                    continue
                completed_at = time.perf_counter()
                return StepResult(
                    step_name=step.name,
                    status=StepStatus.FAILED,
                    started_at=started_at,
                    completed_at=completed_at,
                    error=str(e),
                )
        return StepResult(
            step_name=step.name,
            status=StepStatus.FAILED,
            started_at=started_at,
            completed_at=time.perf_counter(),
            error="Max retries exceeded",
        )


__all__ = [
    "StepStatus",
    "WorkflowStep",
    "StepResult",
    "WorkflowResult",
    "Workflow",
]
