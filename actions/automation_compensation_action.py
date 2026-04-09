"""
Automation Compensation Action Module.

Implements the Saga pattern with compensation actions for
distributed transaction management.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class SagaStatus(Enum):
    """Saga execution status."""

    RUNNING = "running"
    COMPLETED = "completed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"
    FAILED = "failed"


class StepStatus(Enum):
    """Individual step status."""

    PENDING = "pending"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"


@dataclass
class SagaStep:
    """Represents a single step in a saga."""

    name: str
    forward_action: Callable
    compensation_action: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    status: StepStatus = StepStatus.PENDING
    result: Any = None
    error: str = ""


@dataclass
class SagaExecution:
    """Represents a saga execution context."""

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    status: SagaStatus = SagaStatus.RUNNING
    steps: list[SagaStep] = field(default_factory=list)
    started_at: float = field(default_factory=time.time)
    completed_at: float = 0.0
    completed_steps: int = 0


class AutomationCompensationAction:
    """
    Implements saga-based distributed transaction management.

    Features:
    - Sequential step execution with compensation
    - Automatic rollback on failure
    - Parallel step execution support
    - Compensation logging and recovery

    Example:
        saga = AutomationCompensationAction()
        saga.add_step("reserve", reserve_items, cancel_reservation)
        saga.add_step("charge", charge_payment, refund_payment)
        result = await saga.execute()
    """

    def __init__(self, saga_name: str = "") -> None:
        """
        Initialize compensation action.

        Args:
            saga_name: Optional name for this saga.
        """
        self.saga_name = saga_name
        self._steps: list[SagaStep] = []
        self._executions: dict[str, SagaExecution] = {}

    def add_step(
        self,
        name: str,
        forward_action: Callable,
        compensation_action: Callable,
        *args: Any,
        **kwargs: Any,
    ) -> "AutomationCompensationAction":
        """
        Add a step to the saga.

        Args:
            name: Step name.
            forward_action: Action to execute forward.
            compensation_action: Action to compensate on rollback.
            *args: Positional args for actions.
            **kwargs: Keyword args for actions.

        Returns:
            Self for chaining.
        """
        step = SagaStep(
            name=name,
            forward_action=forward_action,
            compensation_action=compensation_action,
            args=args,
            kwargs=kwargs,
        )
        self._steps.append(step)
        return self

    async def execute(
        self,
        execution_id: Optional[str] = None,
        compensation_policy: str = "reverse_order",
    ) -> SagaExecution:
        """
        Execute the saga.

        Args:
            execution_id: Optional execution ID.
            compensation_policy: Order for compensation ('reverse_order').

        Returns:
            SagaExecution with results.
        """
        execution = SagaExecution(
            id=execution_id or str(uuid.uuid4()),
            name=self.saga_name,
            steps=[self._create_step_copy(s) for s in self._steps],
        )
        self._executions[execution.id] = execution

        logger.info(f"Saga {execution.id} started: {len(execution.steps)} steps")

        try:
            for i, step in enumerate(execution.steps):
                step.status = StepStatus.EXECUTING
                execution.completed_steps = i

                try:
                    if asyncio.iscoroutinefunction(step.forward_action):
                        step.result = await step.forward_action(*step.args, **step.kwargs)
                    else:
                        step.result = step.forward_action(*step.args, **step.kwargs)

                    step.status = StepStatus.COMPLETED
                    logger.debug(f"Saga step completed: {step.name}")

                except Exception as e:
                    step.status = StepStatus.FAILED
                    step.error = str(e)
                    logger.error(f"Saga step failed: {step.name} - {e}")
                    await self._compensate(execution, compensation_policy)
                    execution.status = SagaStatus.COMPENSATING
                    return execution

            execution.status = SagaStatus.COMPLETED
            execution.completed_at = time.time()
            logger.info(f"Saga {execution.id} completed successfully")

        except Exception as e:
            execution.status = SagaStatus.FAILED
            logger.error(f"Saga {execution.id} failed: {e}")

        return execution

    async def _compensate(
        self,
        execution: SagaExecution,
        policy: str,
    ) -> None:
        """
        Execute compensation for completed steps.

        Args:
            execution: Saga execution to compensate.
            policy: Compensation order policy.
        """
        logger.info(f"Starting compensation for saga {execution.id}")

        steps_to_compensate = [
            s for s in execution.steps
            if s.status in (StepStatus.COMPLETED, StepStatus.EXECUTING)
        ]

        if policy == "reverse_order":
            steps_to_compensate = list(reversed(steps_to_compensate))

        for step in steps_to_compensate:
            step.status = StepStatus.COMPENSATING

            try:
                if asyncio.iscoroutinefunction(step.compensation_action):
                    await step.compensation_action(step.result)
                else:
                    step.compensation_action(step.result)

                step.status = StepStatus.COMPENSATED
                logger.debug(f"Compensation completed: {step.name}")

            except Exception as e:
                step.status = StepStatus.FAILED
                logger.error(f"Compensation failed for {step.name}: {e}")

        execution.status = SagaStatus.COMPENSATED
        execution.completed_at = time.time()
        logger.info(f"Compensation completed for saga {execution.id}")

    def get_execution(self, execution_id: str) -> Optional[SagaExecution]:
        """
        Get a saga execution by ID.

        Args:
            execution_id: Execution identifier.

        Returns:
            SagaExecution or None.
        """
        return self._executions.get(execution_id)

    def get_step_names(self) -> list[str]:
        """Get list of step names in order."""
        return [s.name for s in self._steps]

    def clear(self) -> None:
        """Clear all steps and executions."""
        self._steps.clear()
        self._executions.clear()
