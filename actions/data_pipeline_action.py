"""Data pipeline for processing API responses.

This module provides data pipeline capabilities:
- Chained data transformations
- Parallel processing branches
- Error handling and recovery
- Pipeline monitoring

Example:
    >>> from actions.data_pipeline_action import DataPipeline
    >>> pipeline = DataPipeline()
    >>> pipeline.add_step(transform_func).add_step(filter_func)
    >>> result = pipeline.execute(input_data)
"""

from __future__ import annotations

import time
import logging
import threading
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from collections import deque

logger = logging.getLogger(__name__)


@dataclass
class PipelineStep:
    """A single step in a data pipeline."""
    name: str
    func: Callable[[Any], Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    error_handler: Optional[Callable[[Exception, Any], Any]] = None


@dataclass
class PipelineResult:
    """Result of pipeline execution."""
    success: bool
    data: Any
    steps_executed: list[str] = field(default_factory=list)
    duration: float = 0.0
    error: Optional[str] = None


@dataclass
class StepMetrics:
    """Metrics for a pipeline step."""
    name: str
    invocations: int = 0
    total_duration: float = 0.0
    failures: int = 0
    last_execution: Optional[float] = None


class DataPipeline:
    """Data processing pipeline with chained transformations.

    Attributes:
        name: Pipeline name for logging.
    """

    def __init__(self, name: str = "pipeline") -> None:
        self.name = name
        self._steps: deque[PipelineStep] = deque()
        self._metrics: dict[str, StepMetrics] = {}
        self._lock = threading.RLock()

    def add_step(
        self,
        func: Callable[[Any], Any],
        name: Optional[str] = None,
        *args: Any,
        error_handler: Optional[Callable[[Exception, Any], Any]] = None,
        **kwargs: Any,
    ) -> DataPipeline:
        """Add a transformation step to the pipeline.

        Args:
            func: Transformation function.
            name: Step name (defaults to function name).
            *args: Additional positional arguments for the function.
            error_handler: Optional error handler for this step.
            **kwargs: Additional keyword arguments for the function.

        Returns:
            Self for chaining.
        """
        step_name = name or func.__name__
        step = PipelineStep(
            name=step_name,
            func=func,
            args=args,
            kwargs=kwargs,
            error_handler=error_handler,
        )
        self._steps.append(step)
        with self._lock:
            if step_name not in self._metrics:
                self._metrics[step_name] = StepMetrics(name=step_name)
        logger.debug(f"Added step to pipeline '{self.name}': {step_name}")
        return self

    def execute(self, data: Any) -> PipelineResult:
        """Execute the pipeline on input data.

        Args:
            data: Input data to process.

        Returns:
            PipelineResult with output and metadata.
        """
        start_time = time.time()
        current_data = data
        steps_executed: list[str] = []

        for step in self._steps:
            try:
                step_start = time.time()
                current_data = step.func(current_data, *step.args, **step.kwargs)
                step_duration = time.time() - step_start
                steps_executed.append(step.name)
                self._record_step_metric(step.name, step_duration, success=True)
                logger.debug(f"Pipeline step '{step.name}' completed in {step_duration:.3f}s")
            except Exception as e:
                self._record_step_metric(step.name, 0, success=False)
                logger.error(f"Pipeline step '{step.name}' failed: {e}")
                if step.error_handler:
                    try:
                        current_data = step.error_handler(e, current_data)
                    except Exception as handler_error:
                        return PipelineResult(
                            success=False,
                            data=current_data,
                            steps_executed=steps_executed,
                            duration=time.time() - start_time,
                            error=f"{type(e).__name__}: {e}",
                        )
                else:
                    return PipelineResult(
                        success=False,
                        data=current_data,
                        steps_executed=steps_executed,
                        duration=time.time() - start_time,
                        error=f"{type(e).__name__}: {e}",
                    )

        return PipelineResult(
            success=True,
            data=current_data,
            steps_executed=steps_executed,
            duration=time.time() - start_time,
        )

    def _record_step_metric(
        self,
        step_name: str,
        duration: float,
        success: bool,
    ) -> None:
        """Record metrics for a step execution."""
        with self._lock:
            if step_name not in self._metrics:
                self._metrics[step_name] = StepMetrics(name=step_name)
            metrics = self._metrics[step_name]
            metrics.invocations += 1
            metrics.total_duration += duration
            metrics.last_execution = time.time()
            if not success:
                metrics.failures += 1

    def get_metrics(self) -> dict[str, Any]:
        """Get pipeline metrics."""
        with self._lock:
            return {
                "pipeline": self.name,
                "total_steps": len(self._steps),
                "steps": {
                    name: {
                        "invocations": m.invocations,
                        "avg_duration": m.total_duration / m.invocations if m.invocations else 0,
                        "failures": m.failures,
                        "last_execution": m.last_execution,
                    }
                    for name, m in self._metrics.items()
                },
            }

    def clear(self) -> None:
        """Remove all steps from the pipeline."""
        self._steps.clear()
        logger.debug(f"Pipeline '{self.name}' cleared")


class PipelineBuilder:
    """Builder for constructing pipelines with shared context."""

    def __init__(self, name: str = "pipeline") -> None:
        self.pipeline = DataPipeline(name=name)
        self._context: dict[str, Any] = {}

    def add_step(
        self,
        func: Callable[[Any, dict[str, Any]], Any],
        name: Optional[str] = None,
    ) -> PipelineBuilder:
        """Add a step with access to shared context.

        Args:
            func: Function that receives (data, context).
            name: Optional step name.

        Returns:
            Self for chaining.
        """
        def wrapper(data: Any, *args: Any, **kwargs: Any) -> Any:
            return func(data, self._context, *args, **kwargs)
        self.pipeline.add_step(wrapper, name=name)
        return self

    def set_context(self, key: str, value: Any) -> PipelineBuilder:
        """Set a shared context value."""
        self._context[key] = value
        return self

    def execute(self, data: Any) -> PipelineResult:
        """Execute the pipeline."""
        return self.pipeline.execute(data)

    def get_context(self) -> dict[str, Any]:
        """Get shared context."""
        return self._context
