"""Data transform pipeline action for chained data processing.

Provides a fluent pipeline API for applying sequential
transformations to data with error handling and validation.
"""

import logging
from dataclasses import dataclass
from typing import Any, Callable, Generic, TypeVar, Optional

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


@dataclass
class PipelineStep:
    """Represents a single step in the pipeline."""
    name: str
    transform: Callable[[Any], Any]
    error_handler: Optional[Callable[[Exception, Any], Any]] = None


@dataclass
class PipelineStats:
    """Statistics for pipeline execution."""
    steps_executed: int = 0
    steps_failed: int = 0
    total_time_ms: float = 0.0


class DataTransformPipelineAction(Generic[T]):
    """Chain multiple data transformations in a pipeline.

    Args:
        name: Optional name for this pipeline.

    Example:
        >>> pipeline = DataTransformPipelineAction()
        >>> result = (
        ...     pipeline
        ...     .add_step("normalize", normalize)
        ...     .add_step("filter", filter_nulls)
        ...     .execute(data)
        ... )
    """

    def __init__(self, name: str = "default") -> None:
        self.name = name
        self._steps: list[PipelineStep] = []
        self._stats = PipelineStats()

    def add_step(
        self,
        name: str,
        transform: Callable[[T], R],
        error_handler: Optional[Callable[[Exception, Any], Any]] = None,
    ) -> "DataTransformPipelineAction[T]":
        """Add a transformation step to the pipeline.

        Args:
            name: Human-readable name for this step.
            transform: Function to apply to the data.
            error_handler: Optional error handler for this step.

        Returns:
            Self for method chaining.
        """
        self._steps.append(PipelineStep(name, transform, error_handler))
        return self

    def add_validation(
        self,
        validator: Callable[[T], bool],
        error_msg: str = "Validation failed",
    ) -> "DataTransformPipelineAction[T]":
        """Add a validation step.

        Args:
            validator: Function that returns True if data is valid.
            error_msg: Error message if validation fails.

        Returns:
            Self for method chaining.
        """
        def validate(data: T) -> T:
            if not validator(data):
                raise ValueError(error_msg)
            return data
        return self.add_step(f"validate_{name(self)}", validate)

    def execute(self, data: T, stop_on_error: bool = True) -> Any:
        """Execute the pipeline on input data.

        Args:
            data: Input data to process.
            stop_on_error: If True, stop pipeline on first error.

        Returns:
            Transformed data after all steps.

        Raises:
            Exception: If a step fails and stop_on_error is True.
        """
        import time
        start_time = time.time()
        current_data: Any = data

        for step in self._steps:
            try:
                logger.debug(f"Executing step: {step.name}")
                current_data = step.transform(current_data)
                self._stats.steps_executed += 1
            except Exception as e:
                self._stats.steps_failed += 1
                logger.error(f"Step {step.name} failed: {e}")

                if step.error_handler:
                    current_data = step.error_handler(e, current_data)
                    self._stats.steps_executed += 1
                elif stop_on_error:
                    raise

        self._stats.total_time_ms = (time.time() - start_time) * 1000
        return current_data

    async def execute_async(self, data: T, stop_on_error: bool = True) -> Any:
        """Execute the pipeline asynchronously.

        Args:
            data: Input data to process.
            stop_on_error: If True, stop pipeline on first error.

        Returns:
            Transformed data after all steps.
        """
        import asyncio
        start_time = asyncio.get_event_loop().time()
        current_data: Any = data

        for step in self._steps:
            try:
                logger.debug(f"Executing step: {step.name}")
                if asyncio.iscoroutinefunction(step.transform):
                    current_data = await step.transform(current_data)
                else:
                    current_data = step.transform(current_data)
                self._stats.steps_executed += 1
            except Exception as e:
                self._stats.steps_failed += 1
                logger.error(f"Step {step.name} failed: {e}")

                if step.error_handler:
                    current_data = step.error_handler(e, current_data)
                    self._stats.steps_executed += 1
                elif stop_on_error:
                    raise

        self._stats.total_time_ms = (asyncio.get_event_loop().time() - start_time) * 1000
        return current_data

    def get_stats(self) -> PipelineStats:
        """Get pipeline execution statistics.

        Returns:
            Current pipeline statistics.
        """
        return self._stats

    def clear(self) -> None:
        """Clear all steps from the pipeline."""
        self._steps.clear()
        self._stats = PipelineStats()

    def __len__(self) -> int:
        """Get number of steps in pipeline."""
        return len(self._steps)

    def __repr__(self) -> str:
        step_names = [s.name for s in self._steps]
        return f"DataTransformPipelineAction({self.name}, steps={step_names})"
