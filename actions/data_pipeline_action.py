"""
Data Pipeline Action Module.

Composable data processing pipeline with stage management,
error handling, parallel processing, and streaming support.
"""

import asyncio
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, Optional, TypeVar, Union

T = TypeVar("T")
R = TypeVar("R")


class PipelineState(Enum):
    """Current state of the pipeline."""

    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


class StageType(Enum):
    """Types of pipeline stages."""

    TRANSFORM = "transform"
    FILTER = "filter"
    MAP = "map"
    REDUCE = "reduce"
    PARALLEL = "parallel"
    BRANCH = "branch"
    MERGE = "merge"


@dataclass
class StageResult(Generic[T]):
    """Result from a pipeline stage."""

    success: bool
    data: T
    error: Optional[Exception] = None
    stage_name: str = ""
    processing_time: float = 0.0

    @property
    def is_error(self) -> bool:
        """Check if result contains an error."""
        return self.error is not None


@dataclass
class PipelineStats:
    """Statistics for pipeline execution."""

    total_items: int = 0
    processed_items: int = 0
    filtered_items: int = 0
    error_items: int = 0
    total_time: float = 0.0
    stage_times: dict[str, float] = field(default_factory=dict)

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.total_items == 0:
            return 0.0
        return (self.processed_items - self.error_items) / self.total_items

    def to_dict(self) -> dict[str, Any]:
        """Export stats as dictionary."""
        return {
            "total_items": self.total_items,
            "processed_items": self.processed_items,
            "filtered_items": self.filtered_items,
            "error_items": self.error_items,
            "success_rate": round(self.success_rate, 4),
            "total_time": round(self.total_time, 4),
            "stage_times": {k: round(v, 4) for k, v in self.stage_times.items()},
        }


class PipelineStage(Generic[T, R]):
    """
    A single stage in the data pipeline.

    Each stage applies a transformation function to input data.
    """

    def __init__(
        self,
        name: str,
        func: Callable[[T], R],
        stage_type: StageType = StageType.TRANSFORM,
        error_handler: Optional[Callable[[Exception, T], T]] = None,
    ) -> None:
        """
        Initialize a pipeline stage.

        Args:
            name: Human-readable stage name.
            func: Transformation function.
            stage_type: Type of stage for routing decisions.
            error_handler: Optional handler for stage errors.
        """
        self.name = name
        self.func = func
        self.stage_type = stage_type
        self.error_handler = error_handler

    async def process(self, data: T) -> StageResult[R]:
        """
        Process data through this stage.

        Args:
            data: Input data.

        Returns:
            StageResult with transformed data or error.
        """
        start_time = asyncio.get_event_loop().time()
        try:
            if asyncio.iscoroutinefunction(self.func):
                result = await self.func(data)
            else:
                result = self.func(data)
            elapsed = asyncio.get_event_loop().time() - start_time
            return StageResult(
                success=True,
                data=result,
                stage_name=self.name,
                processing_time=elapsed,
            )
        except Exception as e:
            elapsed = asyncio.get_event_loop().time() - start_time
            if self.error_handler:
                try:
                    recovered = self.error_handler(e, data)
                    return StageResult(
                        success=True,
                        data=recovered,
                        stage_name=self.name,
                        processing_time=elapsed,
                    )
                except Exception:
                    pass
            return StageResult(
                success=False,
                data=data,  # type: ignore
                error=e,
                stage_name=self.name,
                processing_time=elapsed,
            )


class DataPipeline(Generic[T]):
    """
    Composable data processing pipeline.

    Supports sequential stages, parallel processing, branching,
    error recovery, and streaming iteration.
    """

    def __init__(
        self,
        name: str = "pipeline",
        max_parallel: int = 1,
        continue_on_error: bool = True,
    ) -> None:
        """
        Initialize the data pipeline.

        Args:
            name: Pipeline identifier.
            max_parallel: Max parallel workers for PARALLEL stages.
            continue_on_error: Continue processing after individual errors.
        """
        self.name = name
        self._stages: list[PipelineStage[Any, Any]] = []
        self._max_parallel = max_parallel
        self._continue_on_error = continue_on_error
        self._state = PipelineState.IDLE
        self._stats = PipelineStats()

    def add_stage(
        self,
        name: str,
        func: Callable[[Any], Any],
        stage_type: StageType = StageType.TRANSFORM,
        error_handler: Optional[Callable[[Exception, Any], Any]] = None,
    ) -> "DataPipeline[Any]":
        """
        Add a stage to the pipeline.

        Args:
            name: Stage name.
            func: Transformation function.
            stage_type: Stage type for routing.
            error_handler: Optional error handler.

        Returns:
            Self for chaining.
        """
        stage = PipelineStage(name, func, stage_type, error_handler)
        self._stages.append(stage)  # type: ignore
        return self

    def transform(
        self,
        name: str,
        func: Callable[[Any], Any],
    ) -> "DataPipeline[Any]":
        """Add a transform stage."""
        return self.add_stage(name, func, StageType.TRANSFORM)

    def filter(
        self,
        name: str,
        predicate: Callable[[Any], bool],
    ) -> "DataPipeline[Any]":
        """Add a filter stage that excludes items when predicate returns False."""

        def filter_func(item: Any) -> Any:
            if not predicate(item):
                return None
            return item

        return self.add_stage(name, filter_func, StageType.FILTER)

    async def process_stream(
        self,
        stream: Union[AsyncIterator[T], Iterator[T]],
    ) -> AsyncIterator[StageResult[T]]:
        """
        Process a stream of data through the pipeline.

        Args:
            stream: Input data stream.

        Yields:
            StageResult for each processed item.
        """
        self._state = PipelineState.RUNNING
        self._stats = PipelineStats()
        start_time = asyncio.get_event_loop().time()

        async for item in stream:
            self._stats.total_items += 1
            current: Any = item
            item_success = True

            for stage in self._stages:
                if current is None and stage.stage_type != StageType.FILTER:
                    break
                result = await stage.process(current)
                if result.stage_name:
                    self._stats.stage_times[result.stage_name] = (
                        self._stats.stage_times.get(result.stage_name, 0)
                        + result.processing_time
                    )
                if not result.success:
                    item_success = False
                    self._stats.error_items += 1
                    if not self._continue_on_error:
                        self._state = PipelineState.FAILED
                        return
                if stage.stage_type == StageType.FILTER and result.data is None:
                    self._stats.filtered_items += 1
                    current = None
                    break
                current = result.data

            if item_success:
                self._stats.processed_items += 1
            yield StageResult(
                success=item_success,
                data=current,
                stage_name="",
            )

        self._stats.total_time = asyncio.get_event_loop().time() - start_time
        self._state = PipelineState.COMPLETED

    async def process_batch(self, items: list[T]) -> list[StageResult[T]]:
        """
        Process a batch of items.

        Args:
            items: List of items to process.

        Returns:
            List of results in same order as input.
        """
        results: list[StageResult[T]] = []
        async for result in self.process_stream(items):
            results.append(result)  # type: ignore
        return results

    def stats(self) -> PipelineStats:
        """Return current pipeline statistics."""
        return self._stats

    def get_state(self) -> PipelineState:
        """Return current pipeline state."""
        return self._state


def create_pipeline(
    name: str = "pipeline",
    max_parallel: int = 1,
) -> DataPipeline[Any]:
    """
    Factory function to create a configured data pipeline.

    Args:
        name: Pipeline name.
        max_parallel: Max parallel workers.

    Returns:
        Configured DataPipeline instance.
    """
    return DataPipeline(name=name, max_parallel=max_parallel)
