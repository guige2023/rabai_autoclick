"""
Data Pipeline Action Module.

Provides data pipeline construction with stages, branching,
parallel execution, and error handling for data processing workflows.
"""

import asyncio
import time
import threading
from typing import Optional, Callable, Any, List, Dict, Union, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
from abc import ABC, abstractmethod


class StageType(Enum):
    """Pipeline stage types."""
    SOURCE = "source"
    TRANSFORM = "transform"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    SINK = "sink"
    BRANCH = "branch"
    MERGE = "merge"


class StageStatus(Enum):
    """Pipeline stage execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class PipelineConfig:
    """Configuration for pipeline execution."""
    max_parallel: int = 1  # max parallel stages
    max_retries: int = 0
    retry_delay: float = 1.0
    timeout: Optional[float] = None
    continue_on_error: bool = False
    buffer_size: int = 100


@dataclass
class StageResult:
    """Result from a pipeline stage execution."""
    stage_name: str
    status: StageStatus
    input_data: Any
    output_data: Any = None
    error: Optional[str] = None
    duration_ms: float = 0.0
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None


@dataclass
class PipelineStats:
    """Statistics for pipeline execution."""
    total_stages: int = 0
    completed_stages: int = 0
    failed_stages: int = 0
    skipped_stages: int = 0
    total_duration_ms: float = 0.0
    peak_memory_mb: float = 0.0


class PipelineStage(ABC):
    """Abstract base class for pipeline stages."""

    def __init__(
        self,
        name: str,
        stage_type: StageType,
        config: Optional[PipelineConfig] = None,
    ):
        self.name = name
        self.stage_type = stage_type
        self.config = config or PipelineConfig()
        self._handlers: List[Callable] = []
        self._error_handlers: List[Callable] = []

    @abstractmethod
    async def execute_async(self, data: Any) -> Any:
        """Execute the stage logic."""
        pass

    def execute(self, data: Any) -> Any:
        """Sync version of execute."""
        if asyncio.iscoroutinefunction(self.execute_async):
            return asyncio.run(self.execute_async(data))
        return self.execute_async(data)

    def on_complete(self, handler: Callable) -> "PipelineStage":
        """Register completion handler."""
        self._handlers.append(handler)
        return self

    def on_error(self, handler: Callable) -> "PipelineStage":
        """Register error handler."""
        self._error_handlers.append(handler)
        return self


class SourceStage(PipelineStage):
    """Source stage that generates data."""

    def __init__(
        self,
        name: str,
        source_func: Callable[[], Any],
        config: Optional[PipelineConfig] = None,
    ):
        super().__init__(name, StageType.SOURCE, config)
        self.source_func = source_func

    async def execute_async(self, data: Any) -> Any:
        result = self.source_func()
        if asyncio.iscoroutinefunction(self.source_func):
            result = await result
        return result


class TransformStage(PipelineStage):
    """Transform stage that modifies data."""

    def __init__(
        self,
        name: str,
        transform_func: Callable[[Any], Any],
        config: Optional[PipelineConfig] = None,
    ):
        super().__init__(name, StageType.TRANSFORM, config)
        self.transform_func = transform_func

    async def execute_async(self, data: Any) -> Any:
        result = self.transform_func(data)
        if asyncio.iscoroutinefunction(self.transform_func):
            result = await result
        return result


class FilterStage(PipelineStage):
    """Filter stage that filters data based on condition."""

    def __init__(
        self,
        name: str,
        filter_func: Callable[[Any], bool],
        config: Optional[PipelineConfig] = None,
    ):
        super().__init__(name, StageType.FILTER, config)
        self.filter_func = filter_func

    async def execute_async(self, data: Any) -> Any:
        result = self.filter_func(data)
        if asyncio.iscoroutinefunction(self.filter_func):
            result = await result
        if result:
            return data
        return None


class BranchStage(PipelineStage):
    """Branch stage that splits data flow."""

    def __init__(
        self,
        name: str,
        branches: Dict[str, PipelineStage],
        branch_func: Callable[[Any], List[str]],
        config: Optional[PipelineConfig] = None,
    ):
        super().__init__(name, StageType.BRANCH, config)
        self.branches = branches
        self.branch_func = branch_func

    async def execute_async(self, data: Any) -> Dict[str, Any]:
        result_keys = self.branch_func(data)
        if asyncio.iscoroutinefunction(self.branch_func):
            result_keys = await result_keys

        results = {}
        for key in result_keys:
            if key in self.branches:
                stage = self.branches[key]
                stage_result = await stage.execute_async(data)
                results[key] = stage_result
        return results


class DataPipelineAction:
    """
    Data pipeline action for chaining processing stages.

    Supports linear pipelines, branching, parallel execution,
    error handling, and result collection.
    """

    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or PipelineConfig()
        self._stages: List[PipelineStage] = []
        self._stage_map: Dict[str, PipelineStage] = {}
        self._results: List[StageResult] = []
        self._stats = PipelineStats()
        self._lock = threading.RLock()

    def add_stage(self, stage: PipelineStage) -> "DataPipelineAction":
        """Add a stage to the pipeline."""
        self._stages.append(stage)
        self._stage_map[stage.name] = stage
        self._stats.total_stages = len(self._stages)
        return self

    def add_source(
        self,
        name: str,
        source_func: Callable[[], Any],
    ) -> "DataPipelineAction":
        """Add a source stage."""
        stage = SourceStage(name, source_func)
        return self.add_stage(stage)

    def add_transform(
        self,
        name: str,
        transform_func: Callable[[Any], Any],
    ) -> "DataPipelineAction":
        """Add a transform stage."""
        stage = TransformStage(name, transform_func)
        return self.add_stage(stage)

    def add_filter(
        self,
        name: str,
        filter_func: Callable[[Any], bool],
    ) -> "DataPipelineAction":
        """Add a filter stage."""
        stage = FilterStage(name, filter_func)
        return self.add_stage(stage)

    def add_branch(
        self,
        name: str,
        branches: Dict[str, PipelineStage],
        branch_func: Callable[[Any], List[str]],
    ) -> "DataPipelineAction":
        """Add a branch stage."""
        stage = BranchStage(name, branches, branch_func)
        return self.add_stage(stage)

    async def execute_async(self, initial_data: Any = None) -> Any:
        """
        Execute the pipeline asynchronously.

        Args:
            initial_data: Initial data to pass to first stage

        Returns:
            Final output from last stage
        """
        self._results.clear()
        start_time = time.time()
        current_data = initial_data

        for stage in self._stages:
            stage_start = time.time()

            try:
                output = await stage.execute_async(current_data)
                duration = (time.time() - stage_start) * 1000

                result = StageResult(
                    stage_name=stage.name,
                    status=StageStatus.COMPLETED,
                    input_data=current_data,
                    output_data=output,
                    duration_ms=duration,
                )
                self._results.append(result)
                current_data = output

                # Run handlers
                for handler in stage._handlers:
                    if asyncio.iscoroutinefunction(handler):
                        await handler(result)
                    else:
                        handler(result)

            except Exception as e:
                duration = (time.time() - stage_start) * 1000

                result = StageResult(
                    stage_name=stage.name,
                    status=StageStatus.FAILED,
                    input_data=current_data,
                    error=str(e),
                    duration_ms=duration,
                )
                self._results.append(result)
                self._stats.failed_stages += 1

                # Run error handlers
                for handler in stage._error_handlers:
                    if asyncio.iscoroutinefunction(handler):
                        await handler(result)
                    else:
                        handler(result)

                if not self.config.continue_on_error:
                    break

        self._stats.total_duration_ms = (time.time() - start_time) * 1000
        return current_data

    def execute(self, initial_data: Any = None) -> Any:
        """Execute the pipeline synchronously."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                future = asyncio.run_coroutine_threadsafe(
                    self.execute_async(initial_data), loop
                )
                return future.result(timeout=self.config.timeout)
            return asyncio.run(self.execute_async(initial_data))
        except Exception:
            raise

    def get_stage_result(self, stage_name: str) -> Optional[StageResult]:
        """Get result for a specific stage."""
        for result in self._results:
            if result.stage_name == stage_name:
                return result
        return None

    def get_all_results(self) -> List[StageResult]:
        """Get all stage results."""
        return list(self._results)

    def get_stats(self) -> PipelineStats:
        """Get pipeline statistics."""
        self._stats.completed_stages = sum(
            1 for r in self._results if r.status == StageStatus.COMPLETED
        )
        self._stats.failed_stages = sum(
            1 for r in self._results if r.status == StageStatus.FAILED
        )
        self._stats.skipped_stages = sum(
            1 for r in self._results if r.status == StageStatus.SKIPPED
        )
        return self._stats

    def reset(self) -> None:
        """Reset pipeline state."""
        self._results.clear()
        self._stats = PipelineStats()


class PipelineBuilder:
    """Builder for constructing complex pipelines."""

    def __init__(self, name: str):
        self.name = name
        self._pipeline = DataPipelineAction()

    def source(self, source_func: Callable[[], Any]) -> "PipelineBuilder":
        """Add source stage."""
        self._pipeline.add_source(f"{self.name}_source", source_func)
        return self

    def transform(self, name: str, func: Callable[[Any], Any]) -> "PipelineBuilder":
        """Add transform stage."""
        self._pipeline.add_transform(f"{self.name}_{name}", func)
        return self

    def filter(self, name: str, func: Callable[[Any], bool]) -> "PipelineBuilder":
        """Add filter stage."""
        self._pipeline.add_filter(f"{self.name}_{name}", func)
        return self

    def build(self) -> DataPipelineAction:
        """Build and return the pipeline."""
        return self._pipeline
