"""
Workflow Orchestrator Action Module.

Orchestrates complex multi-step workflows with dependencies, parallel execution,
error handling, retry logic, and state management.

Author: RabAi Team
"""

from __future__ import annotations

import json
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set


class StepStatus(Enum):
    """Status of a workflow step."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


class StepType(Enum):
    """Types of workflow steps."""
    TASK = "task"
    CONDITIONAL = "conditional"
    PARALLEL = "parallel"
    APPROVAL = "approval"
    DELAY = "delay"
    NOTIFICATION = "notification"


@dataclass
class WorkflowStep:
    """A single step in a workflow."""
    id: str
    name: str
    step_type: StepType = StepType.TASK
    fn: Optional[Callable] = None
    depends_on: List[str] = field(default_factory=list)
    retry_count: int = 0
    max_retries: int = 3
    timeout_seconds: Optional[float] = None
    condition_fn: Optional[Callable[[Any], bool]] = None
    parallel_steps: List["WorkflowStep"] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "step_type": self.step_type.value,
            "depends_on": self.depends_on,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
        }


@dataclass
class StepResult:
    """Result of executing a workflow step."""
    step_id: str
    status: StepStatus
    output: Any = None
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_ms: Optional[float] = None
    retry_count: int = 0

    @property
    def is_success(self) -> bool:
        return self.status == StepStatus.COMPLETED


@dataclass
class WorkflowExecution:
    """Execution state of a workflow."""
    execution_id: str
    workflow_id: str
    status: StepStatus
    step_results: Dict[str, StepResult] = field(default_factory=dict)
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    context: Dict[str, Any] = field(default_factory=dict)


class WorkflowOrchestrator:
    """
    Orchestrates complex multi-step workflows.

    Manages step dependencies, parallel execution, error handling,
    retry logic, and execution state.

    Example:
        >>> orchestrator = WorkflowOrchestrator()
        >>> orchestrator.add_step("step1", task_fn)
        >>> orchestrator.add_step("step2", task_fn2, depends_on=["step1"])
        >>> execution = orchestrator.execute()
    """

    def __init__(self, workflow_id: str):
        self.workflow_id = workflow_id
        self._steps: Dict[str, WorkflowStep] = {}
        self._execution: Optional[WorkflowExecution] = None

    def add_step(
        self,
        name: str,
        fn: Optional[Callable] = None,
        step_type: StepType = StepType.TASK,
        depends_on: Optional[List[str]] = None,
        max_retries: int = 3,
        **kwargs,
    ) -> str:
        """Add a step to the workflow."""
        step_id = str(uuid.uuid4())
        step = WorkflowStep(
            id=step_id,
            name=name,
            step_type=step_type,
            fn=fn,
            depends_on=depends_on or [],
            max_retries=max_retries,
            **kwargs,
        )
        self._steps[step_id] = step
        return step_id

    def add_parallel_steps(
        self,
        name: str,
        steps: List[WorkflowStep],
    ) -> str:
        """Add parallel execution steps."""
        step_id = str(uuid.uuid4())
        step = WorkflowStep(
            id=step_id,
            name=name,
            step_type=StepType.PARALLEL,
            parallel_steps=steps,
        )
        self._steps[step_id] = step
        return step_id

    def execute(
        self,
        initial_context: Optional[Dict[str, Any]] = None,
    ) -> WorkflowExecution:
        """Execute the workflow."""
        execution_id = str(uuid.uuid4())
        self._execution = WorkflowExecution(
            execution_id=execution_id,
            workflow_id=self.workflow_id,
            status=StepStatus.RUNNING,
            context=initial_context or {},
        )

        pending_steps = set(self._steps.keys())
        completed_steps: Set[str] = set()
        failed_steps: Set[str] = set()

        while pending_steps:
            # Find steps ready to execute (all dependencies met)
            ready_steps = [
                sid for sid in pending_steps
                if all(dep in completed_steps for dep in self._steps[sid].depends_on)
                and sid not in completed_steps
                and sid not in failed_steps
            ]

            if not ready_steps:
                if failed_steps:
                    self._execution.status = StepStatus.FAILED
                    return self._execution
                break

            for step_id in ready_steps:
                step = self._steps[step_id]
                result = self._execute_step(step)

                self._execution.step_results[step_id] = result

                if result.is_success:
                    completed_steps.add(step_id)
                    if result.output is not None:
                        self._execution.context[step.name] = result.output
                else:
                    if step.retry_count < step.max_retries:
                        step.retry_count += 1
                        self._execution.step_results[step_id].status = StepStatus.RETRYING
                    else:
                        failed_steps.add(step_id)

                pending_steps.discard(step_id)

        if failed_steps:
            self._execution.status = StepStatus.FAILED
        else:
            self._execution.status = StepStatus.COMPLETED

        self._execution.completed_at = datetime.now()
        return self._execution

    def _execute_step(self, step: WorkflowStep) -> StepResult:
        """Execute a single step."""
        started = datetime.now()

        try:
            if step.condition_fn and not step.condition_fn(self._execution.context):
                return StepResult(
                    step_id=step.id,
                    status=StepStatus.SKIPPED,
                    started_at=started,
                    completed_at=datetime.now(),
                )

            if step.fn:
                output = step.fn(self._execution.context)
            else:
                output = None

            return StepResult(
                step_id=step.id,
                status=StepStatus.COMPLETED,
                output=output,
                started_at=started,
                completed_at=datetime.now(),
                duration_ms=(datetime.now() - started).total_seconds() * 1000,
            )

        except Exception as e:
            return StepResult(
                step_id=step.id,
                status=StepStatus.FAILED,
                error=str(e),
                started_at=started,
                completed_at=datetime.now(),
                duration_ms=(datetime.now() - started).total_seconds() * 1000,
                retry_count=step.retry_count,
            )

    def get_execution(self) -> Optional[WorkflowExecution]:
        """Get current execution state."""
        return self._execution


def create_orchestrator(workflow_id: str) -> WorkflowOrchestrator:
    """Factory to create a workflow orchestrator."""
    return WorkflowOrchestrator(workflow_id=workflow_id)
