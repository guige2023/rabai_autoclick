"""Data pipeline builder action for constructing data processing pipelines.

Provides a fluent API for building multi-stage data pipelines
with transformations, filters, and aggregations.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Iterator, Optional

logger = logging.getLogger(__name__)


class PipelineStageType(Enum):
    SOURCE = "source"
    TRANSFORM = "transform"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    SINK = "sink"


@dataclass
class PipelineStage:
    name: str
    stage_type: PipelineStageType
    func: Callable
    enabled: bool = True
    error_handler: Optional[Callable] = None


@dataclass
class Pipeline:
    name: str
    stages: list[PipelineStage] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    execution_count: int = 0


class DataPipelineBuilderAction:
    """Build and execute data processing pipelines.

    Args:
        pipeline_name: Name of the pipeline.
        enable_profiling: Enable pipeline execution profiling.
    """

    def __init__(
        self,
        pipeline_name: str = "pipeline",
        enable_profiling: bool = True,
    ) -> None:
        self._pipelines: dict[str, Pipeline] = {}
        self._current_pipeline: Optional[Pipeline] = None
        self._pipeline_name = pipeline_name
        self._enable_profiling = enable_profiling
        self._execution_stats: dict[str, dict[str, Any]] = {}

    def create_pipeline(self, name: str) -> "DataPipelineBuilderAction":
        """Create a new pipeline.

        Args:
            name: Pipeline name.

        Returns:
            Self for chaining.
        """
        if name not in self._pipelines:
            self._pipelines[name] = Pipeline(name=name)

        self._current_pipeline = self._pipelines[name]
        return self

    def source(
        self,
        name: str,
        func: Callable[[], Iterator[Any]],
    ) -> "DataPipelineBuilderAction":
        """Add a source stage.

        Args:
            name: Stage name.
            func: Source function.

        Returns:
            Self for chaining.
        """
        if self._current_pipeline:
            stage = PipelineStage(
                name=name,
                stage_type=PipelineStageType.SOURCE,
                func=func,
            )
            self._current_pipeline.stages.append(stage)
        return self

    def transform(
        self,
        name: str,
        func: Callable[[Any], Any],
        error_handler: Optional[Callable] = None,
    ) -> "DataPipelineBuilderAction":
        """Add a transformation stage.

        Args:
            name: Stage name.
            func: Transform function.
            error_handler: Optional error handler.

        Returns:
            Self for chaining.
        """
        if self._current_pipeline:
            stage = PipelineStage(
                name=name,
                stage_type=PipelineStageType.TRANSFORM,
                func=func,
                error_handler=error_handler,
            )
            self._current_pipeline.stages.append(stage)
        return self

    def filter(
        self,
        name: str,
        predicate: Callable[[Any], bool],
    ) -> "DataPipelineBuilderAction":
        """Add a filter stage.

        Args:
            name: Stage name.
            predicate: Filter predicate function.

        Returns:
            Self for chaining.
        """
        if self._current_pipeline:
            stage = PipelineStage(
                name=name,
                stage_type=PipelineStageType.FILTER,
                func=predicate,
            )
            self._current_pipeline.stages.append(stage)
        return self

    def aggregate(
        self,
        name: str,
        func: Callable[[list[Any]], Any],
    ) -> "DataPipelineBuilderAction":
        """Add an aggregation stage.

        Args:
            name: Stage name.
            func: Aggregation function.

        Returns:
            Self for chaining.
        """
        if self._current_pipeline:
            stage = PipelineStage(
                name=name,
                stage_type=PipelineStageType.AGGREGATE,
                func=func,
            )
            self._current_pipeline.stages.append(stage)
        return self

    def sink(
        self,
        name: str,
        func: Callable[[Any], None],
    ) -> "DataPipelineBuilderAction":
        """Add a sink stage.

        Args:
            name: Stage name.
            func: Sink function.

        Returns:
            Self for chaining.
        """
        if self._current_pipeline:
            stage = PipelineStage(
                name=name,
                stage_type=PipelineStageType.SINK,
                func=func,
            )
            self._current_pipeline.stages.append(stage)
        return self

    def build(self) -> Optional[Pipeline]:
        """Build and return the current pipeline.

        Returns:
            Built pipeline or None.
        """
        pipeline = self._current_pipeline
        self._current_pipeline = None
        return pipeline

    def execute(self, pipeline_name: Optional[str] = None) -> list[Any]:
        """Execute a pipeline and return results.

        Args:
            pipeline_name: Pipeline name (uses current if None).

        Returns:
            List of output records.
        """
        pipeline = self._pipelines.get(pipeline_name or self._pipeline_name)
        if not pipeline:
            logger.error(f"Pipeline not found: {pipeline_name}")
            return []

        start_time = time.time()
        results = []
        stage_times: dict[str, float] = {}

        source_stage = None
        for stage in pipeline.stages:
            if stage.stage_type == PipelineStageType.SOURCE:
                source_stage = stage
                break

        if not source_stage:
            logger.error("No source stage found")
            return []

        try:
            data = list(source_stage.func())
        except Exception as e:
            logger.error(f"Source stage error: {e}")
            return []

        for stage in pipeline.stages[1:]:
            if not stage.enabled:
                continue

            stage_start = time.time()

            try:
                if stage.stage_type == PipelineStageType.TRANSFORM:
                    data = self._apply_transform(data, stage)
                elif stage.stage_type == PipelineStageType.FILTER:
                    data = self._apply_filter(data, stage)
                elif stage.stage_type == PipelineStageType.AGGREGATE:
                    data = [stage.func(data)]
                elif stage.stage_type == PipelineStageType.SINK:
                    self._apply_sink(data, stage)
                    data = []

            except Exception as e:
                logger.error(f"Stage {stage.name} error: {e}")
                if stage.error_handler:
                    try:
                        data = stage.error_handler(data, e)
                    except Exception as err:
                        logger.error(f"Error handler error: {err}")

            stage_times[stage.name] = time.time() - stage_start

        results = data
        pipeline.execution_count += 1

        if self._enable_profiling:
            total_time = time.time() - start_time
            self._execution_stats[pipeline.name] = {
                "total_time_ms": total_time * 1000,
                "stage_times_ms": {k: v * 1000 for k, v in stage_times.items()},
                "records_processed": len(results),
                "last_execution": time.time(),
            }

        return results

    def _apply_transform(
        self,
        data: list[Any],
        stage: PipelineStage,
    ) -> list[Any]:
        """Apply transform stage to data.

        Args:
            data: Input data.
            stage: Transform stage.

        Returns:
            Transformed data.
        """
        return [stage.func(item) for item in data]

    def _apply_filter(
        self,
        data: list[Any],
        stage: PipelineStage,
    ) -> list[Any]:
        """Apply filter stage to data.

        Args:
            data: Input data.
            stage: Filter stage.

        Returns:
            Filtered data.
        """
        return [item for item in data if stage.func(item)]

    def _apply_sink(
        self,
        data: list[Any],
        stage: PipelineStage,
    ) -> None:
        """Apply sink stage to data.

        Args:
            data: Input data.
            stage: Sink stage.
        """
        for item in data:
            stage.func(item)

    def get_pipeline(self, name: str) -> Optional[Pipeline]:
        """Get a pipeline by name.

        Args:
            name: Pipeline name.

        Returns:
            Pipeline or None.
        """
        return self._pipelines.get(name)

    def list_pipelines(self) -> list[str]:
        """List all pipeline names.

        Returns:
            List of pipeline names.
        """
        return list(self._pipelines.keys())

    def delete_pipeline(self, name: str) -> bool:
        """Delete a pipeline.

        Args:
            name: Pipeline name.

        Returns:
            True if deleted.
        """
        if name in self._pipelines:
            del self._pipelines[name]
            return True
        return False

    def get_execution_stats(self, pipeline_name: str) -> Optional[dict[str, Any]]:
        """Get execution statistics for a pipeline.

        Args:
            pipeline_name: Pipeline name.

        Returns:
            Execution stats or None.
        """
        return self._execution_stats.get(pipeline_name)

    def get_stats(self) -> dict[str, Any]:
        """Get pipeline builder statistics.

        Returns:
            Dictionary with stats.
        """
        return {
            "total_pipelines": len(self._pipelines),
            "pipelines": list(self._pipelines.keys()),
            "enable_profiling": self._enable_profiling,
            "total_executions": sum(p.execution_count for p in self._pipelines.values()),
        }
