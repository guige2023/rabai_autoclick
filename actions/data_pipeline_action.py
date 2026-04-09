"""
Data Pipeline Action Module

Composable data processing pipeline with stage-based architecture,
parallel execution support, error handling, and comprehensive monitoring.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    TypeVar,
    Union,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")
S = TypeVar("S")


class StageType(Enum):
    """Types of pipeline stages."""

    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    VALIDATE = "validate"
    ENRICH = "enrich"
    CUSTOM = "custom"


class StageStatus(Enum):
    """Execution status of a pipeline stage."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class StageMetrics:
    """Metrics for a pipeline stage."""

    stage_name: str
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    duration_ms: float = 0.0
    items_processed: int = 0
    items_succeeded: int = 0
    items_failed: int = 0
    status: StageStatus = StageStatus.PENDING
    error: Optional[str] = None

    def start(self) -> None:
        """Mark stage as started."""
        self.start_time = time.time()
        self.status = StageStatus.RUNNING

    def complete(self, items_processed: int = 0) -> None:
        """Mark stage as completed."""
        self.end_time = time.time()
        self.duration_ms = (self.end_time - self.start_time) * 1000 if self.start_time else 0
        self.items_processed = items_processed
        self.status = StageStatus.COMPLETED

    def fail(self, error: str) -> None:
        """Mark stage as failed."""
        self.end_time = time.time()
        self.duration_ms = (self.end_time - self.start_time) * 1000 if self.start_time else 0
        self.status = StageStatus.FAILED
        self.error = error


@dataclass
class PipelineConfig:
    """Configuration for pipeline execution."""

    name: str = "data_pipeline"
    max_parallel_stages: int = 3
    continue_on_error: bool = True
    timeout_seconds: float = 300.0
    retry_failed_items: bool = False
    max_retries: int = 2
    batch_size: int = 100
    enable_metrics: bool = True


@dataclass
class PipelineContext:
    """Shared context passed through pipeline stages."""

    data: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    errors: List[Dict[str, Any]] = field(default_factory=list)
    metrics: Dict[str, StageMetrics] = field(default_factory=dict)

    def add_error(self, stage: str, error: str, item: Any = None) -> None:
        """Add an error to the context."""
        self.errors.append({
            "stage": stage,
            "error": error,
            "item": item,
            "timestamp": time.time(),
        })

    def get_metric(self, stage_name: str) -> Optional[StageMetrics]:
        """Get metrics for a specific stage."""
        return self.metrics.get(stage_name)


class PipelineStage(Generic[T, R]):
    """
    A single stage in the data pipeline.

    Each stage receives input, processes it, and returns output.
    """

    def __init__(
        self,
        name: str,
        stage_type: StageType,
        handler: Callable[[T, PipelineContext], Awaitable[R]],
        config: Optional[Dict[str, Any]] = None,
    ):
        self.name = name
        self.stage_type = stage_type
        self.handler = handler
        self.config = config or {}
        self._metrics = StageMetrics(stage_name=name)

    async def execute(self, input_data: T, context: PipelineContext) -> R:
        """Execute the stage with given input."""
        logger.debug(f"Executing stage: {self.name}")
        self._metrics.start()
        context.metrics[self.name] = self._metrics

        try:
            result = await self.handler(input_data, context)
            self._metrics.complete()
            self._metrics.items_succeeded = getattr(result, "__len__", lambda: 1)()
            return result
        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}"
            logger.error(f"Stage {self.name} failed: {error_msg}")
            self._metrics.fail(error_msg)
            context.add_error(self.name, error_msg)
            raise

    @property
    def metrics(self) -> StageMetrics:
        """Get stage metrics."""
        return self._metrics


class DataPipeline(Generic[T]):
    """
    Composable data processing pipeline.

    Supports:
    - Sequential and parallel stage execution
    - Error handling and recovery
    - Metrics collection
    - Context passing between stages

    Usage:
        pipeline = DataPipeline(config)
        pipeline.add_stage(extract_handler, "extract", StageType.EXTRACT)
        pipeline.add_stage(transform_handler, "transform", StageType.TRANSFORM)
        result = await pipeline.execute(input_data)
    """

    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or PipelineConfig()
        self._stages: List[PipelineStage] = []
        self._context = PipelineContext()

    def add_stage(
        self,
        handler: Callable[[Any, PipelineContext], Awaitable[Any]],
        name: str,
        stage_type: StageType = StageType.CUSTOM,
        config: Optional[Dict[str, Any]] = None,
    ) -> "DataPipeline":
        """Add a stage to the pipeline. Returns self for chaining."""
        stage = PipelineStage(name=name, stage_type=stage_type, handler=handler, config=config)
        self._stages.append(stage)
        return self

    def add_stages(
        self,
        stages: List[tuple[Callable[[Any, PipelineContext], Awaitable[Any]], str, StageType]],
    ) -> "DataPipeline":
        """Add multiple stages at once."""
        for handler, name, stage_type in stages:
            self.add_stage(handler, name, stage_type)
        return self

    async def execute(
        self,
        input_data: T,
        context: Optional[PipelineContext] = None,
    ) -> Any:
        """
        Execute the pipeline with the given input.

        Args:
            input_data: Initial data to process
            context: Optional shared context

        Returns:
            Output from the final stage
        """
        ctx = context or PipelineContext()
        ctx.data = input_data

        logger.info(f"Starting pipeline: {self.config.name} with {len(self._stages)} stages")

        for stage in self._stages:
            if not self.config.continue_on_error and ctx.errors:
                logger.warning(f"Pipeline aborted due to previous errors at stage: {ctx.errors[-1]['stage']}")
                break

            try:
                ctx.data = await stage.execute(ctx.data, ctx)
            except Exception as e:
                if not self.config.continue_on_error:
                    raise PipelineExecutionError(f"Stage {stage.name} failed: {e}") from e
                logger.error(f"Stage {stage.name} failed, continuing: {e}")

        logger.info(f"Pipeline completed: {self.config.name}")
        return ctx.data

    def get_metrics(self) -> Dict[str, StageMetrics]:
        """Get metrics for all stages."""
        return {s.name: s.metrics for s in self._stages}

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of pipeline execution."""
        total_duration = sum(s.metrics.duration_ms for s in self._stages)
        return {
            "name": self.config.name,
            "stages": len(self._stages),
            "total_duration_ms": total_duration,
            "stage_metrics": {
                name: {
                    "status": m.status.value,
                    "duration_ms": m.duration_ms,
                    "items_processed": m.items_processed,
                }
                for name, m in self.get_metrics().items()
            },
            "errors": len(self._context.errors),
        }


class PipelineBuilder:
    """
    Fluent builder for constructing pipelines.

    Usage:
        pipeline = (
            PipelineBuilder()
            .with_name("etl_pipeline")
            .with_extract(extract_handler)
            .with_transform(transform_handler)
            .with_load(load_handler)
            .build()
        )
    """

    def __init__(self):
        self._config = PipelineConfig()
        self._stages: List[tuple[Callable, str, StageType]] = []

    def with_name(self, name: str) -> "PipelineBuilder":
        """Set pipeline name."""
        self._config.name = name
        return self

    def with_max_parallel(self, max_parallel: int) -> "PipelineBuilder":
        """Set maximum parallel stages."""
        self._config.max_parallel_stages = max_parallel
        return self

    def with_continue_on_error(self, continue_on_error: bool) -> "PipelineBuilder":
        """Set continue on error behavior."""
        self._config.continue_on_error = continue_on_error
        return self

    def with_extract(
        self,
        handler: Callable[[Any, PipelineContext], Awaitable[Any]],
        name: str = "extract",
    ) -> "PipelineBuilder":
        """Add an extract stage."""
        self._stages.append((handler, name, StageType.EXTRACT))
        return self

    def with_transform(
        self,
        handler: Callable[[Any, PipelineContext], Awaitable[Any]],
        name: str = "transform",
    ) -> "PipelineBuilder":
        """Add a transform stage."""
        self._stages.append((handler, name, StageType.TRANSFORM))
        return self

    def with_load(
        self,
        handler: Callable[[Any, PipelineContext], Awaitable[Any]],
        name: str = "load",
    ) -> "PipelineBuilder":
        """Add a load stage."""
        self._stages.append((handler, name, StageType.LOAD))
        return self

    def with_filter(
        self,
        handler: Callable[[Any, PipelineContext], Awaitable[Any]],
        name: str = "filter",
    ) -> "PipelineBuilder":
        """Add a filter stage."""
        self._stages.append((handler, name, StageType.FILTER))
        return self

    def with_validate(
        self,
        handler: Callable[[Any, PipelineContext], Awaitable[Any]],
        name: str = "validate",
    ) -> "PipelineBuilder":
        """Add a validate stage."""
        self._stages.append((handler, name, StageType.VALIDATE))
        return self

    def with_stage(
        self,
        handler: Callable[[Any, PipelineContext], Awaitable[Any]],
        name: str,
        stage_type: StageType = StageType.CUSTOM,
    ) -> "PipelineBuilder":
        """Add a custom stage."""
        self._stages.append((handler, name, stage_type))
        return self

    def build(self) -> DataPipeline:
        """Build the pipeline."""
        pipeline = DataPipeline(self._config)
        pipeline.add_stages(self._stages)
        return pipeline


class PipelineExecutionError(Exception):
    """Raised when pipeline execution fails."""

    pass


async def demo_pipeline():
    """Demonstrate pipeline usage."""
    async def extract(data: Any, ctx: PipelineContext) -> List[Dict]:
        await asyncio.sleep(0.1)
        return [{"id": i, "value": i * 10} for i in range(5)]

    async def transform(data: List[Dict], ctx: PipelineContext) -> List[Dict]:
        await asyncio.sleep(0.1)
        return [{"id": d["id"], "value": d["value"], "processed": True} for d in data]

    pipeline = (
        PipelineBuilder()
        .with_name("demo_pipeline")
        .with_extract(extract)
        .with_transform(transform)
        .build()
    )

    result = await pipeline.execute(None)
    print(f"Result: {result}")
    print(f"Summary: {pipeline.get_summary()}")


if __name__ == "__main__":
    asyncio.run(demo_pipeline())
