"""
Data Pipeline Action Module.

Provides streaming data pipeline processing with stage transformations,
 branching, and parallel execution support.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")
U = TypeVar("U")


class PipelineMode(Enum):
    """Pipeline execution mode."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    FANOUT = "fanout"


@dataclass
class PipelineStage:
    """A single stage in the data pipeline."""
    name: str
    transform: Callable[[Any], Any]
    filter_func: Optional[Callable[[Any], bool]] = None
    error_handler: Optional[Callable[[Exception, Any], Any]] = None
    continue_on_error: bool = True


@dataclass
class PipelineStats:
    """Statistics from pipeline execution."""
    total_input: int = 0
    total_output: int = 0
    filtered: int = 0
    errors: int = 0
    duration_ms: float = 0.0


class DataPipelineAction(Generic[T]):
    """
    Streaming data pipeline processor.

    Processes data through configurable stages with support for
    filtering, error handling, and parallel execution.

    Example:
        pipeline = DataPipelineAction[T]()
        pipeline.add_stage("normalize", normalize_data)
        pipeline.add_stage("validate", validate_data, filter_func=is_valid)
        results = pipeline.process(input_data)
    """

    def __init__(
        self,
        mode: PipelineMode = PipelineMode.SEQUENTIAL,
        max_workers: int = 4,
    ) -> None:
        self.mode = mode
        self.max_workers = max_workers
        self._stages: list[PipelineStage] = []
        self._stats = PipelineStats()

    def add_stage(
        self,
        name: str,
        transform: Callable[[T], U],
        filter_func: Optional[Callable[[T], bool]] = None,
        error_handler: Optional[Callable[[Exception, T], T]] = None,
        continue_on_error: bool = True,
    ) -> "DataPipelineAction[T]":
        """Add a processing stage to the pipeline."""
        stage = PipelineStage(
            name=name,
            transform=transform,
            filter_func=filter_func,
            error_handler=error_handler,
            continue_on_error=continue_on_error,
        )
        self._stages.append(stage)
        return self

    def process(
        self,
        data: list[T],
    ) -> tuple[list[T], PipelineStats]:
        """Process data through all pipeline stages."""
        import time
        start_time = time.monotonic()

        self._stats = PipelineStats(total_input=len(data))
        current_data: list[Any] = list(data)

        for stage in self._stages:
            current_data = self._process_stage(stage, current_data)

        self._stats.total_output = len(current_data)
        self._stats.duration_ms = (time.monotonic() - start_time) * 1000

        return current_data, self._stats

    def _process_stage(
        self,
        stage: PipelineStage,
        data: list[T],
    ) -> list[T]:
        """Process data through a single stage."""
        results: list[T] = []

        for item in data:
            try:
                if stage.filter_func and not stage.filter_func(item):
                    self._stats.filtered += 1
                    continue

                transformed = stage.transform(item)
                if transformed is not None:
                    results.append(transformed)

            except Exception as e:
                self._stats.errors += 1
                if stage.error_handler:
                    try:
                        recovered = stage.error_handler(e, item)
                        if recovered is not None:
                            results.append(recovered)
                    except Exception:
                        pass

                if not stage.continue_on_error:
                    break

        return results

    def process_streaming(
        self,
        data_iterator: Any,
    ) -> tuple[list[T], PipelineStats]:
        """Process data from a streaming iterator."""
        import time
        start_time = time.monotonic()

        self._stats = PipelineStats()
        all_results: list[T] = []
        current_data: list[T] = []

        for item in data_iterator:
            self._stats.total_input += 1
            current_data.append(item)

        for stage in self._stages:
            current_data = self._process_stage(stage, current_data)

        all_results.extend(current_data)
        self._stats.total_output = len(all_results)
        self._stats.duration_ms = (time.monotonic() - start_time) * 1000

        return all_results, self._stats

    def get_stats(self) -> PipelineStats:
        """Get current pipeline statistics."""
        return self._stats

    def reset_stats(self) -> None:
        """Reset pipeline statistics."""
        self._stats = PipelineStats()
