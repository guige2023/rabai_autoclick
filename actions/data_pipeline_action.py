"""Data pipeline action module.

Provides data pipeline functionality for chaining
transformation stages with error handling.
"""

from __future__ import annotations

import logging
from typing import Any, Optional, Callable, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum
from concurrent.futures import ThreadPoolExecutor
import threading

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


class PipelineStageType(Enum):
    """Pipeline stage types."""
    MAP = "map"
    FILTER = "filter"
    REDUCE = "reduce"
    TAP = "tap"


@dataclass
class PipelineStage:
    """Single pipeline stage."""
    name: str
    stage_type: PipelineStageType
    func: Callable[..., Any]
    error_handler: Optional[Callable[[Exception], Any]] = None


@dataclass
class PipelineResult:
    """Result of pipeline execution."""
    success: bool
    data: Any
    error: Optional[Exception] = None
    stages_executed: int = 0


class DataPipeline:
    """Data processing pipeline."""

    def __init__(self, name: str = "pipeline"):
        """Initialize pipeline.

        Args:
            name: Pipeline name
        """
        self.name = name
        self._stages: list[PipelineStage] = []
        self._lock = threading.Lock()

    def add_stage(
        self,
        name: str,
        stage_type: PipelineStageType,
        func: Callable[..., Any],
        error_handler: Optional[Callable[[Exception], Any]] = None,
    ) -> DataPipeline:
        """Add stage to pipeline.

        Args:
            name: Stage name
            stage_type: Stage type
            func: Stage function
            error_handler: Optional error handler

        Returns:
            Self for chaining
        """
        with self._lock:
            stage = PipelineStage(
                name=name,
                stage_type=stage_type,
                func=func,
                error_handler=error_handler,
            )
            self._stages.append(stage)
        return self

    def map(self, name: str, func: Callable[[T], R]) -> DataPipeline:
        """Add map stage.

        Args:
            name: Stage name
            func: Transform function

        Returns:
            Self for chaining
        """
        return self.add_stage(name, PipelineStageType.MAP, func)

    def filter(self, name: str, predicate: Callable[[T], bool]) -> DataPipeline:
        """Add filter stage.

        Args:
            name: Stage name
            predicate: Filter predicate

        Returns:
            Self for chaining
        """
        return self.add_stage(name, PipelineStageType.FILTER, predicate)

    def tap(self, name: str, func: Callable[[T], None]) -> DataPipeline:
        """Add tap stage (side effect).

        Args:
            name: Stage name
            func: Side effect function

        Returns:
            Self for chaining
        """
        return self.add_stage(name, PipelineStageType.TAP, func)

    def execute(self, data: Any) -> PipelineResult:
        """Execute pipeline.

        Args:
            data: Input data

        Returns:
            PipelineResult
        """
        result = PipelineResult(success=True, data=data)
        current_data = data

        for i, stage in enumerate(self._stages):
            try:
                if stage.stage_type == PipelineStageType.MAP:
                    current_data = stage.func(current_data)
                elif stage.stage_type == PipelineStageType.FILTER:
                    if not stage.func(current_data):
                        current_data = []
                    elif isinstance(current_data, list):
                        current_data = [item for item in current_data if stage.func(item)]
                elif stage.stage_type == PipelineStageType.TAP:
                    stage.func(current_data)
                elif stage.stage_type == PipelineStageType.REDUCE:
                    current_data = stage.func(current_data)

                result.stages_executed = i + 1

            except Exception as e:
                logger.error(f"Pipeline stage {stage.name} failed: {e}")
                result.success = False
                result.error = e

                if stage.error_handler:
                    try:
                        current_data = stage.error_handler(e)
                        result.success = True
                        result.error = None
                    except Exception as handler_error:
                        result.success = False
                        result.error = handler_error
                break

        result.data = current_data
        return result

    def execute_list(self, data_list: list[Any]) -> list[Any]:
        """Execute pipeline on list items.

        Args:
            data_list: List of input data

        Returns:
            List of results
        """
        return [self.execute(item).data for item in data_list]

    def clear(self) -> None:
        """Clear all stages."""
        with self._lock:
            self._stages.clear()


class ParallelPipeline:
    """Pipeline with parallel execution support."""

    def __init__(self, name: str = "parallel_pipeline", max_workers: int = 4):
        """Initialize parallel pipeline.

        Args:
            name: Pipeline name
            max_workers: Maximum parallel workers
        """
        self.name = name
        self.max_workers = max_workers
        self._pipeline = DataPipeline(name)
        self._executor: Optional[ThreadPoolExecutor] = None

    def add_stage(self, name: str, stage_type: PipelineStageType, func: Callable[..., Any]) -> ParallelPipeline:
        """Add stage to pipeline.

        Args:
            name: Stage name
            stage_type: Stage type
            func: Stage function

        Returns:
            Self for chaining
        """
        self._pipeline.add_stage(name, stage_type, func)
        return self

    def map(self, name: str, func: Callable[[T], R]) -> ParallelPipeline:
        """Add parallel map stage."""
        return self.add_stage(name, PipelineStageType.MAP, func)

    def execute_parallel(self, data_list: list[Any]) -> list[PipelineResult]:
        """Execute pipeline in parallel.

        Args:
            data_list: List of input data

        Returns:
            List of results
        """
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._pipeline.execute, data) for data in data_list]
            return [f.result() for f in futures]


def create_pipeline(name: str = "pipeline") -> DataPipeline:
    """Create data pipeline.

    Args:
        name: Pipeline name

    Returns:
        DataPipeline instance
    """
    return DataPipeline(name)
