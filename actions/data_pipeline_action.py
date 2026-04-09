"""
Data Pipeline Action Module.

Provides data processing pipeline with stages, transformations,
and error handling for streaming data workflows.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, Union


class PipelineState(Enum):
    """Pipeline execution states."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


class StageType(Enum):
    """Types of pipeline stages."""
    SOURCE = "source"
    TRANSFORM = "transform"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    SINK = "sink"


@dataclass
class StageResult:
    """Result from a pipeline stage."""
    success: bool
    data: Any = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineStage:
    """A single stage in the pipeline."""
    name: str
    stage_type: StageType
    func: Callable[..., Awaitable[StageResult]]
    error_handler: Optional[Callable] = None
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineStats:
    """Pipeline execution statistics."""
    items_processed: int = 0
    items_succeeded: int = 0
    items_failed: int = 0
    stages_completed: List[str] = field(default_factory=list)
    start_time: Optional[float] = None
    end_time: Optional[float] = None

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.items_processed == 0:
            return 0.0
        return self.items_succeeded / self.items_processed

    @property
    def duration(self) -> float:
        """Calculate execution duration."""
        if self.start_time is None:
            return 0.0
        end = self.end_time or asyncio.get_event_loop().time()
        return end - self.start_time


class DataPipeline:
    """Data processing pipeline."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.stages: List[PipelineStage] = []
        self.state = PipelineState.IDLE
        self.stats = PipelineStats()
        self.buffer_size: int = 100
        self._buffer: Deque[Any] = deque(maxlen=self.buffer_size)
        self._cancellation_requested = False

    def add_stage(
        self,
        name: str,
        stage_type: StageType,
        func: Callable[..., Awaitable[StageResult]],
        error_handler: Optional[Callable] = None,
    ) -> "DataPipeline":
        """Add a stage to the pipeline."""
        stage = PipelineStage(
            name=name,
            stage_type=stage_type,
            func=func,
            error_handler=error_handler,
        )
        self.stages.append(stage)
        return self

    def add_source(
        self,
        name: str,
        func: Callable[..., Awaitable[StageResult]],
    ) -> "DataPipeline":
        """Add a source stage."""
        return self.add_stage(name, StageType.SOURCE, func)

    def add_transform(
        self,
        name: str,
        func: Callable[..., Awaitable[StageResult]],
        error_handler: Optional[Callable] = None,
    ) -> "DataPipeline":
        """Add a transformation stage."""
        return self.add_stage(name, StageType.TRANSFORM, func, error_handler)

    def add_filter(
        self,
        name: str,
        func: Callable[..., Awaitable[StageResult]],
    ) -> "DataPipeline":
        """Add a filter stage."""
        return self.add_stage(name, StageType.FILTER, func)

    def add_sink(
        self,
        name: str,
        func: Callable[..., Awaitable[StageResult]],
    ) -> "DataPipeline":
        """Add a sink stage."""
        return self.add_stage(name, StageType.SINK, func)

    async def _execute_stage(
        self,
        stage: PipelineStage,
        data: Any,
    ) -> StageResult:
        """Execute a single stage."""
        try:
            return await stage.func(data)
        except Exception as e:
            if stage.error_handler:
                try:
                    return await stage.error_handler(data, e)
                except Exception:
                    pass
            return StageResult(success=False, error=str(e))

    async def run(self, initial_data: Any = None) -> PipelineStats:
        """Run the pipeline."""
        if not self.stages:
            raise ValueError("Pipeline has no stages")

        self.state = PipelineState.RUNNING
        self.stats = PipelineStats()
        self.stats.start_time = asyncio.get_event_loop().time()
        self._cancellation_requested = False

        current_data = initial_data
        stage_index = 0

        try:
            while stage_index < len(self.stages):
                if self._cancellation_requested:
                    self.state = PipelineState.IDLE
                    break

                stage = self.stages[stage_index]
                if not stage.enabled:
                    stage_index += 1
                    continue

                result = await self._execute_stage(stage, current_data)
                self.stats.items_processed += 1

                if result.success:
                    self.stats.items_succeeded += 1
                    current_data = result.data
                    stage_index += 1
                else:
                    self.stats.items_failed += 1
                    if stage.error_handler is None:
                        self.state = PipelineState.FAILED
                        raise Exception(f"Stage {stage.name} failed: {result.error}")

            self.state = PipelineState.COMPLETED
            self.stats.end_time = asyncio.get_event_loop().time()

        except Exception as e:
            self.state = PipelineState.FAILED
            self.stats.end_time = asyncio.get_event_loop().time()
            raise e

        return self.stats

    def cancel(self) -> None:
        """Request pipeline cancellation."""
        self._cancellation_requested = True

    def pause(self) -> None:
        """Pause the pipeline."""
        if self.state == PipelineState.RUNNING:
            self.state = PipelineState.PAUSED

    def resume(self) -> None:
        """Resume a paused pipeline."""
        if self.state == PipelineState.PAUSED:
            self.state = PipelineState.RUNNING

    def get_stats(self) -> PipelineStats:
        """Get current pipeline statistics."""
        return self.stats


class PipelineBuilder:
    """Builder for constructing pipelines."""

    def __init__(self, name: str) -> None:
        self.pipeline = DataPipeline(name)

    def source(
        self,
        name: str,
        func: Callable[..., Awaitable[StageResult]],
    ) -> "PipelineBuilder":
        """Add source stage."""
        self.pipeline.add_source(name, func)
        return self

    def transform(
        self,
        name: str,
        func: Callable[..., Awaitable[StageResult]],
    ) -> "PipelineBuilder":
        """Add transform stage."""
        self.pipeline.add_transform(name, func)
        return self

    def filter(
        self,
        name: str,
        func: Callable[..., Awaitable[StageResult]],
    ) -> "PipelineBuilder":
        """Add filter stage."""
        self.pipeline.add_filter(name, func)
        return self

    def sink(
        self,
        name: str,
        func: Callable[..., Awaitable[StageResult]],
    ) -> "PipelineBuilder":
        """Add sink stage."""
        self.pipeline.add_sink(name, func)
        return self

    def build(self) -> DataPipeline:
        """Build the pipeline."""
        return self.pipeline
