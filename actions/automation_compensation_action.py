"""
Automation Compensation Action Module

Provides saga pattern and compensation logic for distributed automation workflows.
Supports forward recovery, backward recovery, optimistic and pessimistic sagas,
and automatic compensation chaining.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class SagaStatus(Enum):
    """Saga execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"
    FAILED = "failed"
    CANCELLED = "cancelled"


class SagaType(Enum):
    """Saga type/class."""

    OPTIMISTIC = "optimistic"
    PESSIMISTIC = "pessimistic"


class StepResult(Enum):
    """Step execution result."""

    SUCCESS = "success"
    FAILURE = "failure"
    COMPENSATED = "compensated"
    SKIPPED = "skipped"


@dataclass
class SagaStep:
    """A step in a saga."""

    step_id: str
    name: str
    forward_action: Callable[..., Any]
    compensation_action: Optional[Callable[..., Any]] = None
    retry_count: int = 0
    timeout_seconds: float = 60.0
    critical: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SagaStepExecution:
    """Execution record for a saga step."""

    step_id: str
    status: StepResult
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    duration_ms: float = 0.0
    result: Any = None
    error: Optional[str] = None
    compensated_at: Optional[float] = None
    compensation_result: Any = None


@dataclass
class Saga:
    """A saga instance."""

    saga_id: str
    name: str
    saga_type: SagaType
    steps: List[SagaStep] = field(default_factory=list)
    status: SagaStatus = SagaStatus.PENDING
    step_executions: Dict[str, SagaStepExecution] = field(default_factory=dict)
    context: Dict[str, Any] = field(default_factory=dict)
    created_at: Optional[float] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None


@dataclass
class SagaConfig:
    """Configuration for saga execution."""

    saga_type: SagaType = SagaType.OPTIMISTIC
    continue_on_non_critical_failure: bool = False
    parallel_compensation: bool = False
    max_compensation_retries: int = 3
    compensation_timeout_seconds: float = 120.0
    enable_checkpointing: bool = True
    forward_recovery_enabled: bool = True


class CompensationChain:
    """Manages compensation execution chain."""

    def __init__(self, config: Optional[SagaConfig] = None):
        self.config = config or SagaConfig()

    async def execute_compensation(
        self,
        saga: Saga,
        from_step_index: int,
    ) -> List[SagaStepExecution]:
        """
        Execute compensation starting from a step index.

        Args:
            saga: Saga instance
            from_step_index: Step index to start compensation from

        Returns:
            List of compensation results
        """
        results = []

        # Compensate in reverse order
        steps_to_compensate = saga.steps[:from_step_index]
        steps_to_compensate.reverse()

        if self.config.parallel_compensation:
            tasks = []
            for step in steps_to_compensate:
                if step.compensation_action:
                    tasks.append(self._compensate_step(saga, step))
            if tasks:
                step_results = await asyncio.gather(*tasks, return_exceptions=True)
                results = [r for r in step_results if isinstance(r, SagaStepExecution)]
        else:
            for step in steps_to_compensate:
                if step.compensation_action:
                    result = await self._compensate_step(saga, step)
                    results.append(result)

        return results

    async def _compensate_step(
        self,
        saga: Saga,
        step: SagaStep,
    ) -> SagaStepExecution:
        """Execute compensation for a single step."""
        execution = SagaStepExecution(
            step_id=step.step_id,
            status=StepResult.COMPENSATED,
        )
        execution.started_at = time.time()

        try:
            for attempt in range(self.config.max_compensation_retries + 1):
                try:
                    if asyncio.iscoroutinefunction(step.compensation_action):
                        result = await asyncio.wait_for(
                            step.compensation_action(saga.context),
                            timeout=self.config.compensation_timeout_seconds,
                        )
                    else:
                        result = step.compensation_action(saga.context)

                    execution.result = result
                    execution.status = StepResult.COMPENSATED
                    execution.compensated_at = time.time()
                    break

                except Exception as e:
                    if attempt >= self.config.max_compensation_retries:
                        execution.status = StepResult.FAILURE
                        execution.error = f"Compensation failed: {str(e)}"
                        logger.error(f"Compensation failed for {step.step_id}: {e}")
                    else:
                        await asyncio.sleep(0.5 * (attempt + 1))

        except Exception as e:
            execution.status = StepResult.FAILURE
            execution.error = str(e)

        execution.completed_at = time.time()
        execution.duration_ms = (execution.completed_at - execution.started_at) * 1000

        return execution


class AutomationCompensationAction:
    """
    Saga pattern implementation for distributed automation workflows.

    Features:
    - Optimistic and pessimistic saga types
    - Automatic compensation chain execution
    - Forward recovery with retries
    - Parallel and sequential compensation
    - Step-level timeout and retry control
    - Critical and non-critical step handling
    - Comprehensive execution tracking

    Usage:
        saga = AutomationCompensationAction(config)
        saga.add_step("reserve", reserve_handler, compensate_handler)
        saga.add_step("charge", charge_handler, refund_handler)
        
        result = await saga.execute(context)
    """

    def __init__(self, config: Optional[SagaConfig] = None):
        self.config = config or SagaConfig()
        self._compensation_chain = CompensationChain(self.config)
        self._sagas: Dict[str, Saga] = {}
        self._stats = {
            "sagas_started": 0,
            "sagas_completed": 0,
            "sagas_compensated": 0,
            "sagas_failed": 0,
            "steps_executed": 0,
            "compensations_executed": 0,
        }

    def create_saga(
        self,
        name: str,
        saga_type: Optional[SagaType] = None,
    ) -> Saga:
        """Create a new saga."""
        saga_id = f"saga_{uuid.uuid4().hex[:12]}"
        saga = Saga(
            saga_id=saga_id,
            name=name,
            saga_type=saga_type or self.config.saga_type,
        )
        saga.created_at = time.time()
        self._sagas[saga_id] = saga
        return saga

    def add_step(
        self,
        saga: Saga,
        step_id: str,
        name: str,
        forward_action: Callable[..., Any],
        compensation_action: Optional[Callable[..., Any]] = None,
        retry_count: int = 0,
        timeout_seconds: float = 60.0,
        critical: bool = True,
    ) -> Saga:
        """Add a step to a saga."""
        step = SagaStep(
            step_id=step_id,
            name=name,
            forward_action=forward_action,
            compensation_action=compensation_action,
            retry_count=retry_count,
            timeout_seconds=timeout_seconds,
            critical=critical,
        )
        saga.steps.append(step)
        return saga

    async def execute(
        self,
        saga: Saga,
        context: Optional[Dict[str, Any]] = None,
    ) -> Saga:
        """
        Execute a saga.

        Args:
            saga: Saga to execute
            context: Initial context data

        Returns:
            Completed saga with execution results
        """
        logger.info(f"Starting saga: {saga.saga_id}")
        saga.status = SagaStatus.RUNNING
        saga.started_at = time.time()
        saga.context = context or {}
        self._stats["sagas_started"] += 1

        completed_step_index = 0

        try:
            # Execute steps
            for step in saga.steps:
                execution = await self._execute_step(saga, step)
                saga.step_executions[step.step_id] = execution
                self._stats["steps_executed"] += 1

                if execution.status == StepResult.SUCCESS:
                    completed_step_index += 1
                    continue
                elif execution.status == StepResult.FAILURE:
                    if step.critical or self.config.continue_on_non_critical_failure:
                        logger.warning(f"Step {step.step_id} failed, initiating compensation")
                        saga.status = SagaStatus.COMPENSATING
                        await self._compensate(saga, completed_step_index)
                        return saga
                    else:
                        completed_step_index += 1

            saga.status = SagaStatus.COMPLETED
            self._stats["sagas_completed"] += 1

        except Exception as e:
            logger.error(f"Saga execution error: {e}")
            saga.status = SagaStatus.FAILED
            self._stats["sagas_failed"] += 1
            await self._compensate(saga, completed_step_index)

        saga.completed_at = time.time()
        return saga

    async def _execute_step(
        self,
        saga: Saga,
        step: SagaStep,
    ) -> SagaStepExecution:
        """Execute a single saga step with retries."""
        execution = SagaStepExecution(
            step_id=step.step_id,
            status=StepResult.SUCCESS,
        )
        execution.started_at = time.time()

        try:
            for attempt in range(step.retry_count + 1):
                try:
                    if asyncio.iscoroutinefunction(step.forward_action):
                        result = await asyncio.wait_for(
                            step.forward_action(saga.context),
                            timeout=step.timeout_seconds,
                        )
                    else:
                        result = step.forward_action(saga.context)

                    execution.result = result
                    execution.status = StepResult.SUCCESS
                    return execution

                except Exception as e:
                    if attempt >= step.retry_count:
                        execution.status = StepResult.FAILURE
                        execution.error = f"{type(e).__name__}: {str(e)}"
                        logger.error(f"Step {step.step_id} failed after {attempt + 1} attempts")
                    else:
                        await asyncio.sleep(0.5 * (attempt + 1))

        except asyncio.TimeoutError:
            execution.status = StepResult.FAILURE
            execution.error = f"Step timed out after {step.timeout_seconds}s"

        except Exception as e:
            execution.status = StepResult.FAILURE
            execution.error = str(e)

        execution.completed_at = time.time()
        execution.duration_ms = (execution.completed_at - execution.started_at) * 1000
        return execution

    async def _compensate(
        self,
        saga: Saga,
        from_step_index: int,
    ) -> None:
        """Execute compensation chain."""
        logger.info(f"Starting compensation for saga {saga.saga_id}")

        results = await self._compensation_chain.execute_compensation(
            saga, from_step_index
        )

        for result in results:
            saga.step_executions[result.step_id] = result
            self._stats["compensations_executed"] += 1

        if saga.status == SagaStatus.COMPENSATING:
            saga.status = SagaStatus.COMPENSATED
            self._stats["sagas_compensated"] += 1

    async def compensate_saga(
        self,
        saga_id: str,
    ) -> Optional[Saga]:
        """Manually trigger compensation for a saga."""
        saga = self._sagas.get(saga_id)
        if saga is None:
            return None

        saga.status = SagaStatus.COMPENSATING
        completed_count = sum(
            1 for e in saga.step_executions.values()
            if e.status == StepResult.SUCCESS
        )
        await self._compensate(saga, completed_count)
        return saga

    def get_saga(self, saga_id: str) -> Optional[Saga]:
        """Get a saga by ID."""
        return self._sagas.get(saga_id)

    def get_active_sagas(self) -> List[Saga]:
        """Get all running sagas."""
        return [
            s for s in self._sagas.values()
            if s.status == SagaStatus.RUNNING
        ]

    def get_saga_summary(self, saga_id: str) -> Optional[Dict[str, Any]]:
        """Get a summary of saga execution."""
        saga = self._sagas.get(saga_id)
        if saga is None:
            return None

        return {
            "saga_id": saga.saga_id,
            "name": saga.name,
            "status": saga.status.value,
            "total_steps": len(saga.steps),
            "completed_steps": sum(
                1 for e in saga.step_executions.values()
                if e.status == StepResult.SUCCESS
            ),
            "failed_steps": sum(
                1 for e in saga.step_executions.values()
                if e.status == StepResult.FAILURE
            ),
            "compensated_steps": sum(
                1 for e in saga.step_executions.values()
                if e.status == StepResult.COMPENSATED
            ),
            "duration_ms": (
                (saga.completed_at - saga.started_at) * 1000
                if saga.started_at and saga.completed_at else 0
            ),
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get saga execution statistics."""
        return self._stats.copy()


async def demo_saga():
    """Demonstrate saga execution."""

    async def reserve_inventory(ctx: Dict) -> Dict:
        await asyncio.sleep(0.05)
        ctx["reserved"] = True
        return {"inventory_reserved": True}

    async def charge_payment(ctx: Dict) -> Dict:
        await asyncio.sleep(0.05)
        ctx["charged"] = True
        return {"payment_charged": 100.0}

    async def compensate_inventory(ctx: Dict) -> Dict:
        await asyncio.sleep(0.05)
        return {"inventory_released": True}

    async def refund_payment(ctx: Dict) -> Dict:
        await asyncio.sleep(0.05)
        return {"payment_refunded": True}

    config = SagaConfig(parallel_compensation=False)
    saga_action = AutomationCompensationAction(config)

    saga = saga_action.create_saga("order-saga", SagaType.OPTIMISTIC)
    saga_action.add_step(saga, "reserve", "Reserve Inventory",
                          reserve_inventory, compensate_inventory)
    saga_action.add_step(saga, "charge", "Charge Payment",
                          charge_payment, refund_payment)

    result = await saga_action.execute(saga, {})

    print(f"Saga status: {result.status.value}")
    print(f"Stats: {saga_action.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_saga())
