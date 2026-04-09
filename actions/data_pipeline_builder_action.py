"""
Data Pipeline Builder Action Module

Provides a fluent API for building data processing pipelines.
Supports transformation, filtering, aggregation, and routing.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, Optional, TypeVar, Union
from datetime import datetime

T = TypeVar('T')
R = TypeVar('R')


class PipelineStatus(Enum):
    """Pipeline execution status."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StageType(Enum):
    """Types of pipeline stages."""
    SOURCE = "source"
    TRANSFORM = "transform"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    ROUTE = "route"
    SINK = "sink"
    BRANCH = "branch"
    MERGE = "merge"


@dataclass
class PipelineMetrics:
    """Metrics for pipeline execution."""
    items_processed: int = 0
    items_filtered: int = 0
    items_routed: int = 0
    errors_count: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    stage_durations: dict[str, float] = field(default_factory=dict)
    
    @property
    def duration_seconds(self) -> float:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    @property
    def throughput(self) -> float:
        duration = self.duration_seconds
        return self.items_processed / duration if duration > 0 else 0.0


class PipelineStage(ABC, Generic[T, R]):
    """Base class for pipeline stages."""
    
    def __init__(self, name: str, stage_type: StageType):
        self.name = name
        self.stage_type = stage_type
        self.next_stage: Optional[PipelineStage] = None
        self.error_handler: Optional[Callable[[Exception, Any], Any]] = None
    
    @abstractmethod
    async def process(self, input_data: T) -> Optional[R]:
        """Process input and return output."""
        pass
    
    async def execute(self, input_data: T) -> Optional[R]:
        """Execute with error handling."""
        try:
            return await self.process(input_data)
        except Exception as e:
            if self.error_handler:
                return self.error_handler(e, input_data)
            raise
    
    def then(self, stage: PipelineStage) -> PipelineStage:
        """Chain another stage after this one."""
        self.next_stage = stage
        return stage
    
    def on_error(self, handler: Callable[[Exception, Any], Any]) -> PipelineStage:
        """Set error handler for this stage."""
        self.error_handler = handler
        return self


class SourceStage(PipelineStage[None, T]):
    """Stage that produces data from a source."""
    
    def __init__(self, name: str, source_fn: Callable[[], Any]):
        super().__init__(name, StageType.SOURCE)
        self.source_fn = source_fn
    
    async def process(self, input_data: None) -> Optional[T]:
        return self.source_fn()


class TransformStage(PipelineStage[T, R]):
    """Stage that transforms data."""
    
    def __init__(self, name: str, transform_fn: Callable[[T], R]):
        super().__init__(name, StageType.TRANSFORM)
        self.transform_fn = transform_fn
    
    async def process(self, input_data: T) -> Optional[R]:
        return self.transform_fn(input_data)


class FilterStage(PipelineStage[T, T]):
    """Stage that filters data."""
    
    def __init__(self, name: str, predicate: Callable[[T], bool]):
        super().__init__(name, StageType.FILTER)
        self.predicate = predicate
        self.filtered_count: int = 0
    
    async def process(self, input_data: T) -> Optional[T]:
        if self.predicate(input_data):
            return input_data
        self.filtered_count += 1
        return None


class AggregateStage(PipelineStage[T, R]):
    """Stage that aggregates data."""
    
    def __init__(self, name: str, aggregator: Callable[[list[T]], R],
                 window_size: Optional[int] = None):
        super().__init__(name, StageType.AGGREGATE)
        self.aggregator = aggregator
        self.window_size = window_size
        self._buffer: list[T] = []
    
    async def process(self, input_data: T) -> Optional[R]:
        self._buffer.append(input_data)
        
        if self.window_size and len(self._buffer) < self.window_size:
            return None
        
        if self.window_size is None or len(self._buffer) == self.window_size:
            result = self.aggregator(self._buffer)
            if self.window_size:
                self._buffer.clear()
            return result
        
        return None


class RouteStage(PipelineStage[T, Union[R, Any]]):
    """Stage that routes data to different paths."""
    
    def __init__(self, name: str, router: Callable[[T], str]):
        super().__init__(name, StageType.ROUTE)
        self.router = router
        self.routes: dict[str, PipelineStage] = {}
        self.route_counts: dict[str, int] = {}
    
    def add_route(self, route_key: str, stage: PipelineStage) -> RouteStage:
        """Add a route for a given key."""
        self.routes[route_key] = stage
        self.route_counts[route_key] = 0
        return self
    
    async def process(self, input_data: T) -> Optional[Union[R, Any]]:
        route_key = self.router(input_data)
        self.route_counts[route_key] = self.route_counts.get(route_key, 0) + 1
        
        if route_key in self.routes:
            stage = self.routes[route_key]
            return await stage.execute(input_data)
        return input_data


class SinkStage(PipelineStage[T, None]):
    """Stage that writes data to a sink."""
    
    def __init__(self, name: str, sink_fn: Callable[[T], None]):
        super().__init__(name, StageType.SINK)
        self.sink_fn = sink_fn
        self.items_written: int = 0
    
    async def process(self, input_data: T) -> None:
        self.sink_fn(input_data)
        self.items_written += 1


class DataPipelineBuilder:
    """
    Fluent builder for data processing pipelines.
    
    Example:
        pipeline = (
            DataPipelineBuilder()
            .source(my_api.fetch_all)
            .transform(lambda x: x * 2)
            .filter(lambda x: x > 0)
            .sink(print)
            .build()
        )
        await pipeline.run()
    """
    
    def __init__(self, name: str = "pipeline"):
        self.name = name
        self._stages: list[PipelineStage] = []
        self._current_stage: Optional[PipelineStage] = None
        self._metrics = PipelineMetrics()
        self._status = PipelineStatus.IDLE
        self._cancellation_event: Optional[asyncio.Event] = None
    
    def source(self, source_fn: Callable[[], Any]) -> DataPipelineBuilder:
        """Add a source stage."""
        stage = SourceStage(f"{self.name}_source", source_fn)
        self._stages.append(stage)
        self._current_stage = stage
        return self
    
    def transform(self, fn: Callable[[Any], Any], name: Optional[str] = None) -> DataPipelineBuilder:
        """Add a transform stage."""
        stage_name = name or f"{self.name}_transform_{len(self._stages)}"
        stage = TransformStage(stage_name, fn)
        self._add_stage(stage)
        return self
    
    def filter(self, predicate: Callable[[Any], bool], name: Optional[str] = None) -> DataPipelineBuilder:
        """Add a filter stage."""
        stage_name = name or f"{self.name}_filter_{len(self._stages)}"
        stage = FilterStage(stage_name, predicate)
        self._add_stage(stage)
        return self
    
    def aggregate(self, fn: Callable[[list], Any], window_size: Optional[int] = None,
                  name: Optional[str] = None) -> DataPipelineBuilder:
        """Add an aggregate stage."""
        stage_name = name or f"{self.name}_aggregate_{len(self._stages)}"
        stage = AggregateStage(stage_name, fn, window_size)
        self._add_stage(stage)
        return self
    
    def route(self, router_fn: Callable[[Any], str], name: Optional[str] = None) -> DataPipelineBuilder:
        """Add a route stage."""
        stage_name = name or f"{self.name}_route_{len(self._stages)}"
        stage = RouteStage(stage_name, router_fn)
        self._add_stage(stage)
        return self
    
    def to(self, route_key: str, stage: PipelineStage) -> DataPipelineBuilder:
        """Add a destination stage for the last route."""
        if self._current_stage and isinstance(self._current_stage, RouteStage):
            self._current_stage.add_route(route_key, stage)
        return self
    
    def sink(self, sink_fn: Callable[[Any], None], name: Optional[str] = None) -> DataPipelineBuilder:
        """Add a sink stage."""
        stage_name = name or f"{self.name}_sink_{len(self._stages)}"
        stage = SinkStage(stage_name, sink_fn)
        self._add_stage(stage)
        return self
    
    def _add_stage(self, stage: PipelineStage) -> None:
        """Add a stage to the pipeline."""
        if self._stages:
            self._stages[-1].next_stage = stage
        self._stages.append(stage)
        self._current_stage = stage
    
    def on_error(self, handler: Callable[[Exception, Any], Any]) -> DataPipelineBuilder:
        """Set error handler for the last stage."""
        if self._current_stage:
            self._current_stage.on_error(handler)
        return self
    
    def build(self) -> DataPipeline:
        """Build the pipeline."""
        return DataPipeline(self.name, self._stages.copy())
    
    async def run(self) -> PipelineMetrics:
        """Build and run the pipeline."""
        pipeline = self.build()
        return await pipeline.run()


class DataPipeline:
    """Executable data pipeline."""
    
    def __init__(self, name: str, stages: list[PipelineStage]):
        self.name = name
        self._stages = stages
        self._status = PipelineStatus.IDLE
        self._metrics = PipelineMetrics()
        self._cancellation_event = asyncio.Event()
    
    @property
    def status(self) -> PipelineStatus:
        return self._status
    
    @property
    def metrics(self) -> PipelineMetrics:
        return self._metrics
    
    async def run(self) -> PipelineMetrics:
        """Run the pipeline."""
        self._status = PipelineStatus.RUNNING
        self._metrics = PipelineMetrics(start_time=datetime.now())
        
        try:
            if self._stages:
                current_data = None
                stage = self._stages[0]
                
                while stage and self._status == PipelineStatus.RUNNING:
                    if self._cancellation_event.is_set():
                        self._status = PipelineStatus.CANCELLED
                        break
                    
                    current_data = await stage.execute(current_data)
                    self._metrics.items_processed += 1
                    stage = stage.next_stage
                
                if self._status == PipelineStatus.RUNNING:
                    self._status = PipelineStatus.COMPLETED
        except Exception as e:
            self._status = PipelineStatus.FAILED
            self._metrics.errors_count += 1
            raise
        finally:
            self._metrics.end_time = datetime.now()
        
        return self._metrics
    
    def pause(self) -> None:
        """Pause the pipeline."""
        self._status = PipelineStatus.PAUSED
    
    def resume(self) -> None:
        """Resume the pipeline."""
        if self._status == PipelineStatus.PAUSED:
            self._status = PipelineStatus.RUNNING
    
    def cancel(self) -> None:
        """Cancel the pipeline."""
        self._status = PipelineStatus.CANCELLED
        self._cancellation_event.set()
