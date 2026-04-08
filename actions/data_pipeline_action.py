"""Data Pipeline Action Module.

Provides ETL-like data processing pipeline with transformation,
filtering, aggregation, and error handling.
"""

from typing import Any, Dict, List, Optional, Callable, Generic, TypeVar
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
import json
import asyncio


T = TypeVar("T")
R = TypeVar("R")


class PipelineStageType(Enum):
    """Types of pipeline stages."""
    SOURCE = "source"
    TRANSFORM = "transform"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    SINK = "sink"
    BRANCH = "branch"
    MERGE = "merge"


class PipelineStatus(Enum):
    """Pipeline execution status."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class PipelineStage:
    """Represents a single stage in the pipeline."""
    name: str
    stage_type: PipelineStageType
    handler: Callable[[Any], Any]
    error_handler: Optional[Callable[[Exception, Any], Any]] = None
    skip_on_error: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

    def process(self, data: Any) -> Any:
        """Process data through this stage."""
        try:
            result = self.handler(data)
            if asyncio.iscoroutine(result):
                return result
            return result
        except Exception as e:
            if self.error_handler:
                return self.error_handler(e, data)
            if self.skip_on_error:
                return data
            raise


@dataclass
class PipelineMetrics:
    """Metrics for pipeline execution."""
    records_processed: int = 0
    records_filtered: int = 0
    records_failed: int = 0
    stages_executed: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    stage_metrics: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def duration_seconds(self) -> float:
        """Calculate duration in seconds."""
        if not self.start_time:
            return 0
        end = self.end_time or datetime.now()
        return (end - self.start_time).total_seconds()

    def throughput(self) -> float:
        """Calculate records per second."""
        duration = self.duration_seconds()
        return self.records_processed / duration if duration > 0 else 0


@dataclass
class PipelineExecution:
    """Tracks a pipeline execution instance."""
    pipeline_id: str
    status: PipelineStatus
    metrics: PipelineMetrics = field(default_factory=PipelineMetrics)
    errors: List[Dict[str, Any]] = field(default_factory=list)
    current_stage: Optional[str] = None


class DataPipeline:
    """Configurable data processing pipeline."""

    def __init__(self, pipeline_id: str, name: str = ""):
        self.pipeline_id = pipeline_id
        self.name = name or pipeline_id
        self._stages: List[PipelineStage] = []
        self._source_stages: List[PipelineStage] = []
        self._sink_stages: List[PipelineStage] = []
        self._current_execution: Optional[PipelineExecution] = None
        self._listeners: Dict[str, List[Callable]] = {}

    def add_stage(
        self,
        name: str,
        stage_type: PipelineStageType,
        handler: Callable[[Any], Any],
        error_handler: Optional[Callable[[Exception, Any], Any]] = None,
        skip_on_error: bool = False,
    ) -> "DataPipeline":
        """Add a stage to the pipeline."""
        stage = PipelineStage(
            name=name,
            stage_type=stage_type,
            handler=handler,
            error_handler=error_handler,
            skip_on_error=skip_on_error,
        )
        self._stages.append(stage)

        if stage_type == PipelineStageType.SOURCE:
            self._source_stages.append(stage)
        elif stage_type == PipelineStageType.SINK:
            self._sink_stages.append(stage)

        return self

    def source(self, handler: Callable[[], Any]) -> "DataPipeline":
        """Add a source stage."""
        return self.add_stage(
            name=f"source_{len(self._source_stages)}",
            stage_type=PipelineStageType.SOURCE,
            handler=handler,
        )

    def transform(
        self,
        name: str,
        handler: Callable[[Any], Any],
        error_handler: Optional[Callable[[Exception, Any], Any]] = None,
    ) -> "DataPipeline":
        """Add a transformation stage."""
        return self.add_stage(
            name=name,
            stage_type=PipelineStageType.TRANSFORM,
            handler=handler,
            error_handler=error_handler,
        )

    def filter(
        self,
        name: str,
        predicate: Callable[[Any], bool],
        error_handler: Optional[Callable[[Exception, Any], Any]] = None,
    ) -> "DataPipeline":
        """Add a filter stage."""
        return self.add_stage(
            name=name,
            stage_type=PipelineStageType.FILTER,
            handler=lambda data: data if predicate(data) else None,
            error_handler=error_handler,
            skip_on_error=True,
        )

    def aggregate(
        self,
        name: str,
        aggregator: Callable[[List[Any]], Any],
    ) -> "DataPipeline":
        """Add an aggregation stage."""
        return self.add_stage(
            name=name,
            stage_type=PipelineStageType.AGGREGATE,
            handler=aggregator,
        )

    def sink(
        self,
        name: str,
        handler: Callable[[Any], Any],
        error_handler: Optional[Callable[[Exception, Any], Any]] = None,
    ) -> "DataPipeline":
        """Add a sink stage."""
        return self.add_stage(
            name=name,
            stage_type=PipelineStageType.SINK,
            handler=handler,
            error_handler=error_handler,
        )
        return self

    def branch(
        self,
        name: str,
        condition: Callable[[Any], bool],
        true_pipeline: Optional["DataPipeline"] = None,
        false_pipeline: Optional["DataPipeline"] = None,
    ) -> "DataPipeline":
        """Add a branch stage for conditional processing."""
        def branch_handler(data: Any) -> Dict[str, Any]:
            result = condition(data)
            branches = {"condition": result, "data": data}
            if result and true_pipeline:
                branches["true_result"] = true_pipeline.run(data)
            if not result and false_pipeline:
                branches["false_result"] = false_pipeline.run(data)
            return branches

        return self.add_stage(
            name=name,
            stage_type=PipelineStageType.BRANCH,
            handler=branch_handler,
        )

    async def run(
        self,
        data: Optional[Any] = None,
        **kwargs,
    ) -> PipelineExecution:
        """Execute the pipeline."""
        execution = PipelineExecution(
            pipeline_id=self.pipeline_id,
            status=PipelineStatus.RUNNING,
        )
        execution.metrics.start_time = datetime.now()
        self._current_execution = execution

        try:
            if not self._source_stages and data is None:
                raise ValueError("No data source and no initial data provided")

            current_data = data
            if self._source_stages:
                for source in self._source_stages:
                    execution.current_stage = source.name
                    result = source.process(None)
                    if asyncio.iscoroutine(result):
                        current_data = await result
                    else:
                        current_data = result

            for stage in self._stages:
                if stage.stage_type in (PipelineStageType.SOURCE, PipelineStageType.SINK):
                    continue

                execution.current_stage = stage.name
                result = stage.process(current_data)

                if asyncio.iscoroutine(result):
                    result = await result

                if result is None and stage.stage_type == PipelineStageType.FILTER:
                    execution.metrics.records_filtered += 1
                    current_data = None
                    continue

                current_data = result
                execution.metrics.stages_executed += 1
                execution.metrics.records_processed += 1

                self._update_stage_metrics(execution, stage, result)

            for sink in self._sink_stages:
                execution.current_stage = sink.name
                result = sink.process(current_data)
                if asyncio.iscoroutine(result):
                    await result

            execution.status = PipelineStatus.COMPLETED

        except Exception as e:
            execution.status = PipelineStatus.FAILED
            execution.errors.append({
                "stage": execution.current_stage,
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
            })

        finally:
            execution.end_time = datetime.now()
            execution.current_stage = None
            self._current_execution = None

        return execution

    def _update_stage_metrics(
        self,
        execution: PipelineExecution,
        stage: PipelineStage,
        result: Any,
    ):
        """Update metrics for a stage."""
        if stage.name not in execution.metrics.stage_metrics:
            execution.metrics.stage_metrics[stage.name] = {
                "executions": 0,
                "last_result_size": 0,
            }
        execution.metrics.stage_metrics[stage.name]["executions"] += 1
        if isinstance(result, (list, dict)):
            execution.metrics.stage_metrics[stage.name]["last_result_size"] = (
                len(result) if isinstance(result, list) else len(result.keys())
            )

    def on_event(self, event: str, callback: Callable):
        """Register event listener."""
        if event not in self._listeners:
            self._listeners[event] = []
        self._listeners[event].append(callback)

    def _emit(self, event: str, data: Any):
        """Emit event to listeners."""
        for callback in self._listeners.get(event, []):
            try:
                callback(data)
            except Exception:
                pass

    def get_execution(self) -> Optional[PipelineExecution]:
        """Get current execution status."""
        return self._current_execution

    def get_metrics(self) -> PipelineMetrics:
        """Get pipeline metrics."""
        if self._current_execution:
            return self._current_execution.metrics
        return PipelineMetrics()


class PipelineBuilder:
    """Fluent builder for data pipelines."""

    def __init__(self, pipeline_id: str):
        self._pipeline = DataPipeline(pipeline_id)

    def source(self, handler: Callable[[], Any]) -> "PipelineBuilder":
        """Add source stage."""
        self._pipeline.source(handler)
        return self

    def transform(
        self,
        name: str,
        handler: Callable[[Any], Any],
    ) -> "PipelineBuilder":
        """Add transform stage."""
        self._pipeline.transform(name, handler)
        return self

    def filter(
        self,
        name: str,
        predicate: Callable[[Any], bool],
    ) -> "PipelineBuilder":
        """Add filter stage."""
        self._pipeline.filter(name, predicate)
        return self

    def sink(
        self,
        name: str,
        handler: Callable[[Any], Any],
    ) -> "PipelineBuilder":
        """Add sink stage."""
        self._pipeline.sink(name, handler)
        return self

    def build(self) -> DataPipeline:
        """Build and return the pipeline."""
        return self._pipeline

    async def run(self, data: Optional[Any] = None) -> PipelineExecution:
        """Run the pipeline."""
        return await self._pipeline.run(data)


# Module exports
__all__ = [
    "DataPipeline",
    "PipelineBuilder",
    "PipelineExecution",
    "PipelineMetrics",
    "PipelineStage",
    "PipelineStageType",
    "PipelineStatus",
]
