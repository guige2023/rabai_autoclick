"""
Data Pipeline Action - Data processing pipeline with stages.

This module provides a configurable data processing pipeline with
stages, transformations, filtering, and error handling.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar, Generic
from enum import Enum
from collections import deque


T = TypeVar("T")
R = TypeVar("R")


class StageType(Enum):
    """Types of pipeline stages."""
    SOURCE = "source"
    TRANSFORM = "transform"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    SINK = "sink"


class PipelineStatus(Enum):
    """Status of a pipeline execution."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class PipelineStage:
    """A single stage in the data pipeline."""
    stage_id: str
    name: str
    stage_type: StageType
    handler: Callable[[Any], Any]
    error_handler: Callable[[Exception, Any], Any] | None = None
    skip_on_error: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineMetrics:
    """Metrics for pipeline execution."""
    items_processed: int = 0
    items_filtered: int = 0
    items_failed: int = 0
    total_duration_ms: float = 0.0
    stage_durations: dict[str, float] = field(default_factory=dict)


@dataclass
class PipelineResult:
    """Result of a pipeline execution."""
    success: bool
    output: list[Any]
    metrics: PipelineMetrics
    errors: list[str] = field(default_factory=list)


class Pipeline:
    """
    A data processing pipeline with multiple stages.
    
    Example:
        pipeline = Pipeline("etl")
        pipeline.source(load_from_db)
        pipeline.transform(normalize_data)
        pipeline.filter(validate_record)
        pipeline.sink(save_to_file)
        result = await pipeline.execute(input_data)
    """
    
    def __init__(self, pipeline_id: str | None = None) -> None:
        self.pipeline_id = pipeline_id or str(uuid.uuid4())
        self._stages: list[PipelineStage] = []
        self._status = PipelineStatus.IDLE
    
    def source(
        self,
        handler: Callable[[], Any] | Callable[[Any], Any],
        name: str = "source",
    ) -> Pipeline:
        """Add a source stage that provides initial data."""
        stage = PipelineStage(
            stage_id=str(uuid.uuid4()),
            name=name,
            stage_type=StageType.SOURCE,
            handler=handler,
        )
        self._stages.insert(0, stage)
        return self
    
    def transform(
        self,
        handler: Callable[[Any], Any],
        name: str | None = None,
        error_handler: Callable[[Exception, Any], Any] | None = None,
        skip_on_error: bool = False,
    ) -> Pipeline:
        """Add a transformation stage."""
        stage_name = name or f"transform_{len([s for s in self._stages if s.stage_type == StageType.TRANSFORM])}"
        stage = PipelineStage(
            stage_id=str(uuid.uuid4()),
            name=stage_name,
            stage_type=StageType.TRANSFORM,
            handler=handler,
            error_handler=error_handler,
            skip_on_error=skip_on_error,
        )
        self._stages.append(stage)
        return self
    
    def filter(
        self,
        predicate: Callable[[Any], bool],
        name: str | None = None,
    ) -> Pipeline:
        """Add a filtering stage."""
        def filter_handler(data: Any) -> Any:
            if isinstance(data, list):
                return [item for item in data if predicate(item)]
            return [data] if predicate(data) else []
        
        stage_name = name or f"filter_{len([s for s in self._stages if s.stage_type == StageType.FILTER])}"
        stage = PipelineStage(
            stage_id=str(uuid.uuid4()),
            name=stage_name,
            stage_type=StageType.FILTER,
            handler=filter_handler,
        )
        self._stages.append(stage)
        return self
    
    def aggregate(
        self,
        aggregator: Callable[[list[Any]], Any],
        name: str | None = None,
    ) -> Pipeline:
        """Add an aggregation stage."""
        def aggregate_handler(data: Any) -> Any:
            if isinstance(data, list):
                return aggregator(data)
            return data
        
        stage_name = name or f"aggregate_{len([s for s in self._stages if s.stage_type == StageType.AGGREGATE])}"
        stage = PipelineStage(
            stage_id=str(uuid.uuid4()),
            name=stage_name,
            stage_type=StageType.AGGREGATE,
            handler=aggregate_handler,
        )
        self._stages.append(stage)
        return self
    
    def sink(
        self,
        handler: Callable[[Any], None],
        name: str = "sink",
    ) -> Pipeline:
        """Add a sink stage that outputs the result."""
        stage = PipelineStage(
            stage_id=str(uuid.uuid4()),
            name=name,
            stage_type=StageType.SINK,
            handler=handler,
        )
        self._stages.append(stage)
        return self
    
    async def execute(
        self,
        input_data: Any = None,
        context: dict[str, Any] | None = None,
    ) -> PipelineResult:
        """
        Execute the pipeline.
        
        Args:
            input_data: Initial input data
            context: Optional shared context
            
        Returns:
            PipelineResult with output and metrics
        """
        self._status = PipelineStatus.RUNNING
        metrics = PipelineMetrics()
        errors: list[str] = []
        
        start_time = time.time()
        current_data = input_data
        items_filtered = 0
        
        for stage in self._stages:
            stage_start = time.time()
            
            try:
                if stage.stage_type == StageType.SOURCE:
                    if asyncio.iscoroutinefunction(stage.handler):
                        current_data = await stage.handler()
                    else:
                        current_data = stage.handler()
                else:
                    if asyncio.iscoroutinefunction(stage.handler):
                        current_data = await stage.handler(current_data)
                    else:
                        current_data = stage.handler(current_data)
                
                if stage.stage_type == StageType.FILTER:
                    if isinstance(current_data, list):
                        items_filtered += len(input_data) - len(current_data) if isinstance(input_data, list) else 0
                
                if stage.stage_type == StageType.SINK:
                    current_data = None
                
            except Exception as e:
                if stage.error_handler:
                    try:
                        current_data = stage.error_handler(e, current_data)
                    except Exception as inner:
                        errors.append(f"Stage {stage.name} error_handler failed: {inner}")
                        if stage.skip_on_error:
                            continue
                        metrics.items_failed += 1
                else:
                    errors.append(f"Stage {stage.name} failed: {str(e)}")
                    if stage.skip_on_error:
                        continue
                    self._status = PipelineStatus.FAILED
                    return PipelineResult(
                        success=False,
                        output=[],
                        metrics=metrics,
                        errors=errors,
                    )
            
            stage_duration = (time.time() - stage_start) * 1000
            metrics.stage_durations[stage.name] = stage_duration
        
        metrics.items_processed = len(current_data) if isinstance(current_data, list) else 1
        metrics.items_filtered = items_filtered
        metrics.total_duration_ms = (time.time() - start_time) * 1000
        
        self._status = PipelineStatus.COMPLETED
        
        return PipelineResult(
            success=True,
            output=current_data if isinstance(current_data, list) else [current_data],
            metrics=metrics,
            errors=errors,
        )


class DataPipelineAction:
    """
    Data pipeline action for automation workflows.
    
    Example:
        action = DataPipelineAction()
        
        @action.pipeline()
        def my_pipeline(pipeline):
            pipeline.source(lambda: load_data())
            pipeline.transform(normalize)
            pipeline.filter(validate)
            pipeline.sink(store)
    """
    
    def __init__(self) -> None:
        self._pipelines: dict[str, Pipeline] = {}
        self._default_pipeline: Pipeline | None = None
    
    def create_pipeline(
        self,
        pipeline_id: str,
    ) -> Pipeline:
        """Create a new named pipeline."""
        pipeline = Pipeline(pipeline_id)
        self._pipelines[pipeline_id] = pipeline
        if self._default_pipeline is None:
            self._default_pipeline = pipeline
        return pipeline
    
    def get_pipeline(self, pipeline_id: str) -> Pipeline | None:
        """Get a pipeline by ID."""
        return self._pipelines.get(pipeline_id)
    
    async def execute(
        self,
        pipeline_id: str | None = None,
        input_data: Any = None,
        context: dict[str, Any] | None = None,
    ) -> PipelineResult:
        """Execute a pipeline by ID or default."""
        if pipeline_id:
            pipeline = self._pipelines.get(pipeline_id)
        else:
            pipeline = self._default_pipeline
        
        if pipeline is None:
            return PipelineResult(
                success=False,
                output=[],
                metrics=PipelineMetrics(),
                errors=[f"Pipeline {pipeline_id or 'default'} not found"],
            )
        
        return await pipeline.execute(input_data, context)
    
    def list_pipelines(self) -> list[str]:
        """List all pipeline IDs."""
        return list(self._pipelines.keys())


# Export public API
__all__ = [
    "StageType",
    "PipelineStatus",
    "PipelineStage",
    "PipelineMetrics",
    "PipelineResult",
    "Pipeline",
    "DataPipelineAction",
]
