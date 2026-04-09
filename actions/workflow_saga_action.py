"""Workflow Saga Action Module.

Implement saga pattern for distributed workflow compensation.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from .automation_executor_action import StepStatus


class SagaStatus(Enum):
    """Saga execution status."""
    RUNNING = "running"
    COMPLETED = "completed"
    COMPENSATING = "compensating"
    FAILED = "failed"


@dataclass
class SagaStep:
    """Single saga step with compensation."""
    step_id: str
    name: str
    execute_fn: Callable
    compensate_fn: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    timeout: float = 60.0


@dataclass
class SagaExecution:
    """Saga execution record."""
    saga_id: str
    status: SagaStatus
    started_at: float
    completed_at: float | None = None
    completed_steps: list[str] = field(default_factory=list)
    compensated_steps: list[str] = field(default_factory=list)
    results: dict[str, Any] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)


class SagaOrchestrator:
    """Orchestrate saga pattern for distributed transactions."""

    def __init__(self, saga_id: str, name: str) -> None:
        self.saga_id = saga_id
        self.name = name
        self._steps: list[SagaStep] = []
        self._executions: dict[str, SagaExecution] = {}

    def add_step(
        self,
        name: str,
        execute_fn: Callable,
        compensate_fn: Callable,
        *args,
        step_id: str | None = None,
        **kwargs
    ) -> str:
        """Add a saga step."""
        step_id = step_id or str(uuid.uuid4())
        step = SagaStep(
            step_id=step_id,
            name=name,
            execute_fn=execute_fn,
            compensate_fn=compensate_fn,
            args=args,
            kwargs=kwargs
        )
        self._steps.append(step)
        return step_id

    async def execute(self, correlation_id: str | None = None) -> SagaExecution:
        """Execute the saga with compensation on failure."""
        exec_id = correlation_id or str(uuid.uuid4())
        execution = SagaExecution(
            saga_id=exec_id,
            status=SagaStatus.RUNNING,
            started_at=time.time()
        )
        self._executions[exec_id] = execution
        try:
            for step in self._steps:
                try:
                    result = await asyncio.wait_for(
                        asyncio.to_thread(step.execute_fn, *step.args, **step.kwargs),
                        timeout=step.timeout
                    )
                    execution.results[step.step_id] = result
                    execution.completed_steps.append(step.step_id)
                except Exception as e:
                    execution.errors[step.step_id] = str(e)
                    await self._compensate(execution)
                    execution.status = SagaStatus.FAILED
                    execution.completed_at = time.time()
                    return execution
            execution.status = SagaStatus.COMPLETED
            execution.completed_at = time.time()
        except Exception as e:
            execution.errors["saga"] = str(e)
            execution.status = SagaStatus.FAILED
            execution.completed_at = time.time()
        return execution

    async def _compensate(self, execution: SagaExecution) -> None:
        """Execute compensation in reverse order."""
        execution.status = SagaStatus.COMPENSATING
        for step in reversed(self._steps):
            if step.step_id not in execution.completed_steps:
                continue
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(step.compensate_fn),
                    timeout=step.timeout
                )
                execution.completed_steps.remove(step.step_id)
                execution.compensated_steps.append(step.step_id)
            except Exception as e:
                execution.errors[f"{step.step_id}_compensation"] = str(e)

    def get_execution(self, exec_id: str) -> SagaExecution | None:
        """Get execution by ID."""
        return self._executions.get(exec_id)

    def get_all_executions(self) -> list[SagaExecution]:
        """Get all saga executions."""
        return list(self._executions.values())
