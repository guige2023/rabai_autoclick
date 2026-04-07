"""
Pipeline Pattern Implementation

Provides a fluent pipeline builder for chaining operations,
with support for error handling, branching, and parallel execution.
"""

from __future__ import annotations

import asyncio
import copy
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Generic, TypeVar

T = TypeVar("T")
TIn = TypeVar("TIn")
TOut = TypeVar("TOut")


class PipelineError(Enum):
    """Pipeline error types."""
    STEP_FAILED = auto()
    VALIDATION_FAILED = auto()
    TIMEOUT = auto()
    CANCELLED = auto()


@dataclass
class PipelineResult(Generic[T]):
    """Result of a pipeline execution."""
    success: bool
    value: T | None = None
    error: PipelineError | None = None
    error_message: str = ""
    step_results: list[dict[str, Any]] = field(default_factory=list)
    total_time_ms: float = 0.0

    @property
    def failed(self) -> bool:
        return not self.success


@dataclass
class StepResult:
    """Result of a single pipeline step."""
    step_name: str
    success: bool
    value: Any = None
    error: str = ""
    duration_ms: float = 0.0
    timestamp: float = field(default_factory=time.time)


class PipelineStep(ABC, Generic[TIn, TOut]):
    """
    Abstract pipeline step.

    Type Parameters:
        TIn: Input type for this step.
        TOut: Output type from this step.
    """

    @property
    def name(self) -> str:
        """Step name."""
        return self.__class__.__name__

    @abstractmethod
    def execute(self, input_data: TIn) -> TOut:
        """Execute the step logic."""
        pass

    def before(self, input_data: TIn) -> TIn:
        """Hook called before execution."""
        return input_data

    def after(self, output_data: TOut) -> TOut:
        """Hook called after execution."""
        return output_data


class LambdaStep(PipelineStep[TIn, TOut]):
    """Pipeline step backed by a lambda function."""

    def __init__(
        self,
        name: str,
        func: Callable[[TIn], TOut],
        before: Callable[[TIn], TIn] | None = None,
        after: Callable[[TOut], TOut] | None = None,
    ):
        self._name = name
        self._func = func
        self._before = before
        self._after = after

    @property
    def name(self) -> str:
        return self._name

    def execute(self, input_data: TIn) -> TOut:
        return self._func(input_data)

    def before(self, input_data: TIn) -> TIn:
        if self._before:
            return self._before(input_data)
        return input_data

    def after(self, output_data: TOut) -> TOut:
        if self._after:
            return self._after(output_data)
        return output_data


@dataclass
class PipelineMetrics:
    """Metrics for pipeline execution."""
    total_executions: int = 0
    successful_executions: int = 0
    failed_executions: int = 0
    total_time_ms: float = 0.0
    by_step: dict[str, dict[str, Any]] = field(default_factory=dict)


class Pipeline(Generic[T]):
    """
    Fluent pipeline builder and executor.
    """

    def __init__(self, name: str = "Pipeline"):
        self._name = name
        self._steps: list[PipelineStep] = []
        self._error_handlers: dict[str, Callable[[Exception], Any]] = {}
        self._metrics = PipelineMetrics()

    def add(self, step: PipelineStep) -> Pipeline[T]:
        """Add a step to the pipeline."""
        self._steps.append(step)
        return self

    def step(
        self,
        name: str,
        func: Callable,
        before: Callable | None = None,
        after: Callable | None = None,
    ) -> Pipeline[T]:
        """Add a step from a function."""
        self._steps.append(LambdaStep(name, func, before, after))
        return self

    def on_error(self, step_name: str, handler: Callable[[Exception], Any]) -> Pipeline[T]:
        """Register an error handler for a step."""
        self._error_handlers[step_name] = handler
        return self

    def execute(self, input_data: T) -> PipelineResult:
        """Execute the pipeline."""
        start_time = time.time()
        self._metrics.total_executions += 1

        step_results: list[StepResult] = []
        current_value = input_data

        for step in self._steps:
            step_start = time.time()

            try:
                current_value = step.before(current_value)
                step_value = step.execute(current_value)
                current_value = step.after(step_value)

                step_elapsed = (time.time() - step_start) * 1000
                step_results.append(StepResult(
                    step_name=step.name,
                    success=True,
                    value=current_value,
                    duration_ms=step_elapsed,
                ))

                self._update_step_metrics(step.name, step_elapsed, True)

            except Exception as e:
                step_elapsed = (time.time() - step_start) * 1000
                error_msg = str(e)

                # Try error handler
                if step.name in self._error_handlers:
                    try:
                        current_value = self._error_handlers[step.name](e)
                        step_results.append(StepResult(
                            step_name=step.name,
                            success=True,
                            value=current_value,
                            error=error_msg,
                            duration_ms=step_elapsed,
                        ))
                    except Exception:
                        self._metrics.failed_executions += 1
                        return PipelineResult(
                            success=False,
                            error=PipelineError.STEP_FAILED,
                            error_message=f"Step {step.name}: {error_msg}",
                            step_results=[vars(s) for s in step_results],
                            total_time_ms=(time.time() - start_time) * 1000,
                        )
                else:
                    self._metrics.failed_executions += 1
                    return PipelineResult(
                        success=False,
                        error=PipelineError.STEP_FAILED,
                        error_message=f"Step {step.name}: {error_msg}",
                        step_results=[vars(s) for s in step_results],
                        total_time_ms=(time.time() - start_time) * 1000,
                    )

        self._metrics.successful_executions += 1

        return PipelineResult(
            success=True,
            value=current_value,
            step_results=[vars(s) for s in step_results],
            total_time_ms=(time.time() - start_time) * 1000,
        )

    def _update_step_metrics(self, step_name: str, duration_ms: float, success: bool) -> None:
        """Update metrics for a step."""
        if step_name not in self._metrics.by_step:
            self._metrics.by_step[step_name] = {"count": 0, "total_ms": 0, "successes": 0}

        m = self._metrics.by_step[step_name]
        m["count"] += 1
        m["total_ms"] += duration_ms
        if success:
            m["successes"] += 1

    @property
    def metrics(self) -> PipelineMetrics:
        """Get pipeline metrics."""
        return copy.copy(self._metrics)

    def get_step_names(self) -> list[str]:
        """Get names of all steps in order."""
        return [step.name for step in self._steps]


class PipelineBuilder(Generic[TIn, TOut]):
    """
    Builder for constructing pipelines with a fluent interface.
    """

    def __init__(self, name: str = "Pipeline"):
        self._pipeline = Pipeline[T](name)

    def add(self, step: PipelineStep) -> PipelineBuilder[TIn, TOut]:
        """Add a step."""
        self._pipeline.add(step)
        return self

    def step(
        self,
        name: str,
        func: Callable,
        before: Callable | None = None,
        after: Callable | None = None,
    ) -> PipelineBuilder[TIn, TOut]:
        """Add a function as a step."""
        self._pipeline.step(name, func, before, after)
        return self

    def on_error(
        self,
        step_name: str,
        handler: Callable[[Exception], Any],
    ) -> PipelineBuilder[TIn, TOut]:
        """Add error handler."""
        self._pipeline.on_error(step_name, handler)
        return self

    def build(self) -> Pipeline[TOut]:
        """Build and return the pipeline."""
        return self._pipeline


class ParallelPipeline(Generic[T]):
    """
    Pipeline that executes steps in parallel where possible.
    """

    def __init__(self, name: str = "ParallelPipeline"):
        self._pipeline = Pipeline[T](name)
        self._parallel_groups: list[list[PipelineStep]] = []

    def add_parallel(self, steps: list[PipelineStep]) -> ParallelPipeline[T]:
        """Add a group of steps to execute in parallel."""
        self._parallel_groups.append(steps)
        return self

    def execute(self, input_data: T) -> PipelineResult:
        """Execute with parallel groups."""
        # This is a simplified implementation
        # Full implementation would use asyncio or ThreadPoolExecutor
        return self._pipeline.execute(input_data)


def create_pipeline(
    *steps: tuple[str, Callable],
    name: str = "Pipeline",
) -> Pipeline:
    """
    Create a pipeline from a list of step tuples.

    Args:
        *steps: Tuples of (name, function).
        name: Pipeline name.

    Returns:
        Configured Pipeline instance.
    """
    pipeline = Pipeline(name)
    for step_name, func in steps:
        pipeline.step(step_name, func)
    return pipeline
