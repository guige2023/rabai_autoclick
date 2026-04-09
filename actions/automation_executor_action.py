"""Automation Executor Action Module.

Execute automation workflows with step tracking and error recovery.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from .command_handler_action import CommandStatus


class StepStatus(Enum):
    """Step execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


@dataclass
class AutomationStep:
    """Single automation step."""
    step_id: str
    name: str
    action: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    timeout: float = 60.0
    retry_count: int = 0
    max_retries: int = 3
    depends_on: list[str] = field(default_factory=list)
    status: StepStatus = StepStatus.PENDING
    started_at: float | None = None
    completed_at: float | None = None
    error: str | None = None
    result: Any = None


@dataclass
class WorkflowResult:
    """Result of workflow execution."""
    workflow_id: str
    status: StepStatus
    steps_completed: int
    steps_failed: int
    total_time: float
    results: dict[str, Any]
    errors: dict[str, str]


class AutomationExecutor:
    """Execute automation workflows with step management."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.workflow_id = str(uuid.uuid4())
        self._steps: dict[str, AutomationStep] = {}
        self._step_order: list[str] = []
        self._running = False

    def add_step(
        self,
        name: str,
        action: Callable,
        *args,
        step_id: str | None = None,
        timeout: float = 60.0,
        retry_count: int = 0,
        max_retries: int = 3,
        depends_on: list[str] | None = None,
        **kwargs
    ) -> str:
        """Add a step to the workflow."""
        step_id = step_id or str(uuid.uuid4())
        step = AutomationStep(
            step_id=step_id,
            name=name,
            action=action,
            args=args,
            kwargs=kwargs,
            timeout=timeout,
            retry_count=retry_count,
            max_retries=max_retries,
            depends_on=depends_on or []
        )
        self._steps[step_id] = step
        self._step_order.append(step_id)
        return step_id

    async def execute(self) -> WorkflowResult:
        """Execute the workflow."""
        self._running = True
        start_time = time.time()
        results = {}
        errors = {}
        completed_steps: set[str] = set()
        steps_completed = 0
        steps_failed = 0
        while self._running and len(completed_steps) < len(self._steps):
            progress_made = False
            for step_id in self._step_order:
                step = self._steps[step_id]
                if step_id in completed_steps:
                    continue
                if not all(dep in completed_steps for dep in step.depends_on):
                    continue
                if step.status == StepStatus.PENDING:
                    step.status = StepStatus.RUNNING
                    step.started_at = time.time()
                    progress_made = True
                    try:
                        result = await asyncio.wait_for(
                            step.action(*step.args, **step.kwargs),
                            timeout=step.timeout
                        )
                        step.result = result
                        step.status = StepStatus.COMPLETED
                        step.completed_at = time.time()
                        results[step_id] = result
                        completed_steps.add(step_id)
                        steps_completed += 1
                    except asyncio.TimeoutError:
                        step.error = "Step timed out"
                        step.status = StepStatus.FAILED
                        step.completed_at = time.time()
                        errors[step_id] = "Timeout"
                        steps_failed += 1
                        completed_steps.add(step_id)
                    except Exception as e:
                        step.error = str(e)
                        if step.retry_count < step.max_retries:
                            step.retry_count += 1
                            step.status = StepStatus.RETRYING
                            await asyncio.sleep(2 ** step.retry_count)
                        else:
                            step.status = StepStatus.FAILED
                            step.completed_at = time.time()
                            errors[step_id] = str(e)
                            steps_failed += 1
                            completed_steps.add(step_id)
            if not progress_made and len(completed_steps) < len(self._steps):
                break
            await asyncio.sleep(0.1)
        overall_status = StepStatus.COMPLETED if steps_failed == 0 else StepStatus.FAILED
        return WorkflowResult(
            workflow_id=self.workflow_id,
            status=overall_status,
            steps_completed=steps_completed,
            steps_failed=steps_failed,
            total_time=time.time() - start_time,
            results=results,
            errors=errors
        )

    def get_step(self, step_id: str) -> AutomationStep | None:
        """Get step by ID."""
        return self._steps.get(step_id)
