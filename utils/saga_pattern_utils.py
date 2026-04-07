"""
Saga pattern utilities for distributed transaction management.

Provides choreograph and orchestrate saga patterns, compensation
actions, parallel execution, and failure recovery.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Coroutine, Optional

logger = logging.getLogger(__name__)


class SagaStepStatus(Enum):
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    COMPENSATING = auto()
    COMPENSATED = auto()
    FAILED = auto()


@dataclass
class SagaStepResult:
    """Result of a saga step execution."""
    step_name: str
    success: bool
    output: Any = None
    error: Optional[str] = None
    duration_seconds: float = 0.0
    compensated: bool = False


@dataclass
class SagaConfig:
    """Configuration for saga execution."""
    name: str
    timeout_seconds: float = 300.0
    parallel_steps: bool = False
    max_concurrency: int = 5
    stop_on_first_failure: bool = True


class SagaStep:
    """Represents a single step in a saga."""

    def __init__(
        self,
        name: str,
        action: Callable[..., Coroutine[Any, Any, Any]],
        compensation: Callable[..., Coroutine[Any, Any, Any]],
        input_mapper: Optional[Callable[[dict], dict]] = None,
        output_mapper: Optional[Callable[[Any], Any]] = None,
    ) -> None:
        self.name = name
        self.action = action
        self.compensation = compensation
        self.input_mapper = input_mapper or (lambda x: x)
        self.output_mapper = output_mapper or (lambda x: x)


class ChoreographySaga:
    """Choreography-based saga (events trigger next steps)."""

    def __init__(self, config: Optional[SagaConfig] = None) -> None:
        self.config = config or SagaConfig(name="saga")
        self._steps: list[SagaStep] = []
        self._results: dict[str, SagaStepResult] = {}
        self._compensations: list[SagaStepResult] = []

    def add_step(self, step: SagaStep) -> "ChoreographySaga":
        self._steps.append(step)
        return self

    async def execute(self, initial_data: dict[str, Any]) -> tuple[bool, dict[str, SagaStepResult]]:
        """Execute the saga with initial data."""
        context = dict(initial_data)
        self._results = {}

        for step in self._steps:
            start = time.perf_counter()
            try:
                step_input = step.input_mapper(context)
                output = await step.action(step_input)
                mapped_output = step.output_mapper(output)
                context[step.name] = mapped_output

                self._results[step.name] = SagaStepResult(
                    step_name=step.name,
                    success=True,
                    output=mapped_output,
                    duration_seconds=time.perf_counter() - start,
                )
                logger.info("Saga step %s completed", step.name)

            except Exception as e:
                logger.error("Saga step %s failed: %s", step.name, e)
                self._results[step.name] = SagaStepResult(
                    step_name=step.name,
                    success=False,
                    error=str(e),
                    duration_seconds=time.perf_counter() - start,
                )
                await self._compensate()
                return False, self._results

        return True, self._results

    async def _compensate(self) -> None:
        """Execute compensations in reverse order."""
        for step in reversed(self._steps):
            if step.name not in self._results:
                continue
            result = self._results[step.name]
            if not result.success:
                break

            try:
                start = time.perf_counter()
                await step.compensation(result.output)
                self._results[step.name].compensated = True
                self._results[step.name].duration_seconds = time.perf_counter() - start
                logger.info("Saga compensation %s completed", step.name)
            except Exception as e:
                logger.error("Saga compensation %s failed: %s", step.name, e)
                self._results[step.name].error = f"Compensation failed: {e}"


class OrchestrationSaga:
    """Orchestration-based saga (centralized coordinator)."""

    def __init__(self, config: Optional[SagaConfig] = None) -> None:
        self.config = config or SagaConfig(name="orchestrated_saga")
        self._steps: list[SagaStep] = []
        self._results: dict[str, SagaStepResult] = {}
        self._completed: list[str] = []

    def add_step(self, step: SagaStep) -> "OrchestrationSaga":
        self._steps.append(step)
        return self

    async def execute(self, initial_data: dict[str, Any]) -> tuple[bool, dict[str, SagaStepResult]]:
        """Execute the orchestrated saga."""
        context = dict(initial_data)
        self._results = {}
        self._completed = []

        for i, step in enumerate(self._steps):
            start = time.perf_counter()

            if time.time() - start > self.config.timeout_seconds:
                await self._rollback()
                return False, self._results

            step_input = self._build_step_input(step, context)

            try:
                output = await step.action(step_input)
                mapped_output = step.output_mapper(output)
                context[step.name] = mapped_output

                self._results[step.name] = SagaStepResult(
                    step_name=step.name,
                    success=True,
                    output=mapped_output,
                    duration_seconds=time.perf_counter() - start,
                )
                self._completed.append(step.name)
                logger.info("Orchestrated step %s completed", step.name)

            except Exception as e:
                logger.error("Orchestrated step %s failed: %s", step.name, e)
                self._results[step.name] = SagaStepResult(
                    step_name=step.name,
                    success=False,
                    error=str(e),
                    duration_seconds=time.perf_counter() - start,
                )
                await self._rollback()
                return False, self._results

        return True, self._results

    def _build_step_input(self, step: SagaStep, context: dict[str, Any]) -> dict[str, Any]:
        """Build input for a step from the saga context."""
        return step.input_mapper(context)

    async def _rollback(self) -> None:
        """Rollback completed steps in reverse order."""
        for step_name in reversed(self._completed):
            step = next((s for s in self._steps if s.name == step_name), None)
            if not step:
                continue

            result = self._results.get(step_name)
            if not result or not result.success:
                continue

            try:
                start = time.perf_counter()
                await step.compensation(result.output)
                self._results[step_name].compensated = True
                self._results[step_name].duration_seconds = time.perf_counter() - start
                logger.info("Rollback step %s completed", step_name)
            except Exception as e:
                logger.error("Rollback step %s failed: %s", step_name, e)


class ParallelSaga:
    """Saga with parallel step execution."""

    def __init__(self, config: Optional[SagaConfig] = None) -> None:
        self.config = config or SagaConfig(name="parallel_saga")
        self._steps: list[SagaStep] = []

    def add_step(self, step: SagaStep) -> "ParallelSaga":
        self._steps.append(step)
        return self

    async def execute(self, initial_data: dict[str, Any]) -> tuple[bool, dict[str, SagaStepResult]]:
        """Execute saga steps in parallel batches."""
        context = dict(initial_data)
        results: dict[str, SagaStepResult] = {}
        semaphore = asyncio.Semaphore(self.config.max_concurrency)

        async def run_step(step: SagaStep) -> SagaStepResult:
            async with semaphore:
                start = time.perf_counter()
                try:
                    step_input = step.input_mapper(context)
                    output = await step.action(step_input)
                    mapped_output = step.output_mapper(output)
                    context[step.name] = mapped_output
                    return SagaStepResult(
                        step_name=step.name,
                        success=True,
                        output=mapped_output,
                        duration_seconds=time.perf_counter() - start,
                    )
                except Exception as e:
                    return SagaStepResult(
                        step_name=step.name,
                        success=False,
                        error=str(e),
                        duration_seconds=time.perf_counter() - start,
                    )

        task_results = await asyncio.gather(*[run_step(s) for s in self._steps])
        for result in task_results:
            results[result.step_name] = result

        failed = [r for r in task_results if not r.success]
        if failed and self.config.stop_on_first_failure:
            return False, results

        return True, results
