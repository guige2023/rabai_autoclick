"""Data pipeline orchestration module.

This module provides a flexible data pipeline system for chaining operations,
managing data flow, handling errors, and supporting parallel processing.

Author: rabai_autoclick team
License: MIT
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional, Callable, TypeVar, Generic, Union
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from abc import ABC, abstractmethod
import logging
import traceback


T = TypeVar("T")
R = TypeVar("R")


class PipelineStatus(Enum):
    """Status of pipeline execution."""
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
    SINK = "sink"
    BRANCH = "branch"
    MERGE = "merge"


@dataclass
class PipelineContext:
    """Context passed through pipeline stages."""
    execution_id: str
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    metadata: Dict[str, Any] = field(default_factory=dict)
    counters: Dict[str, int] = field(default_factory=lambda: {"processed": 0, "filtered": 0, "errors": 0})
    errors: List[Dict[str, Any]] = field(default_factory=list)
    
    def add_error(self, stage: str, error: Exception) -> None:
        """Record an error."""
        self.errors.append({
            "stage": stage,
            "error": str(error),
            "traceback": traceback.format_exc(),
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        self.counters["errors"] += 1
    
    def increment(self, counter: str, value: int = 1) -> None:
        """Increment a counter."""
        self.counters[counter] = self.counters.get(counter, 0) + value


@dataclass
class StageResult:
    """Result of a stage execution."""
    stage_name: str
    success: bool
    data: Any = None
    error: Optional[str] = None
    duration_ms: float = 0.0
    records_in: int = 0
    records_out: int = 0


@dataclass
class PipelineResult:
    """Result of complete pipeline execution."""
    execution_id: str
    status: PipelineStatus
    total_duration_ms: float = 0.0
    stages: List[StageResult] = field(default_factory=list)
    final_data: Any = None
    context: Optional[PipelineContext] = None
    error: Optional[str] = None


class PipelineStage(ABC, Generic[T, R]):
    """Abstract base class for pipeline stages.
    
    Example:
        >>> class UppercaseStage(PipelineStage[str, str]):
        ...     def execute(self, data: str, ctx: PipelineContext) -> str:
        ...         return data.upper()
        ...     def get_type(self) -> StageType:
        ...         return StageType.TRANSFORM
    """
    
    def __init__(self, name: str):
        self.name = name
        self._logger = logging.getLogger(f"pipeline.{name}")
    
    @abstractmethod
    def execute(self, data: T, ctx: PipelineContext) -> R:
        """Execute the stage transformation."""
        pass
    
    @abstractmethod
    def get_type(self) -> StageType:
        """Get the type of this stage."""
        pass
    
    def validate_input(self, data: Any) -> bool:
        """Validate input data. Override for custom validation."""
        return True
    
    def on_error(self, error: Exception, ctx: PipelineContext) -> Optional[R]:
        """Handle errors. Override for custom error handling."""
        ctx.add_error(self.name, error)
        return None


class SourceStage(PipelineStage[Any, Any]):
    """Stage that generates or fetches initial data."""
    
    def __init__(self, name: str, source_fn: Callable[[PipelineContext], Any]):
        super().__init__(name)
        self._source_fn = source_fn
    
    def execute(self, data: Any, ctx: PipelineContext) -> Any:
        return self._source_fn(ctx)
    
    def get_type(self) -> StageType:
        return StageType.SOURCE


class TransformStage(PipelineStage[T, R]):
    """Stage that transforms data."""
    
    def __init__(self, name: str, transform_fn: Callable[[T, PipelineContext], R]):
        super().__init__(name)
        self._transform_fn = transform_fn
    
    def execute(self, data: T, ctx: PipelineContext) -> R:
        return self._transform_fn(data, ctx)
    
    def get_type(self) -> StageType:
        return StageType.TRANSFORM


class FilterStage(PipelineStage[T, Optional[T]]):
    """Stage that filters data based on predicate."""
    
    def __init__(self, name: str, predicate: Callable[[T, PipelineContext], bool]):
        super().__init__(name)
        self._predicate = predicate
    
    def execute(self, data: T, ctx: PipelineContext) -> Optional[T]:
        if self._predicate(data, ctx):
            return data
        ctx.increment("filtered")
        return None
    
    def get_type(self) -> StageType:
        return StageType.FILTER


class SinkStage(PipelineStage[T, None]):
    """Stage that writes data to destination."""
    
    def __init__(self, name: str, sink_fn: Callable[[T, PipelineContext], None]):
        super().__init__(name)
        self._sink_fn = sink_fn
    
    def execute(self, data: T, ctx: PipelineContext) -> None:
        self._sink_fn(data, ctx)
    
    def get_type(self) -> StageType:
        return StageType.SINK


class Pipeline:
    """Data pipeline orchestrator.
    
    Example:
        >>> pipeline = Pipeline("etl_pipeline")
        >>> pipeline.source(lambda ctx: [1, 2, 3, 4, 5])\\
        ...     .filter(lambda x, ctx: x > 2)\\
        ...     .transform(lambda x, ctx: x * 10)\\
        ...     .sink(lambda x, ctx: print(f"Result: {x}"))
        >>> result = pipeline.execute()
        >>> print(result.status)
    """
    
    def __init__(self, name: str):
        self.name = name
        self._stages: List[PipelineStage] = []
        self._logger = logging.getLogger(f"pipeline.{name}")
        self._status = PipelineStatus.IDLE
    
    def source(self, source_fn: Callable[[PipelineContext], Any]) -> "Pipeline":
        """Add a source stage."""
        stage = SourceStage(f"{self.name}_source", source_fn)
        self._stages.append(stage)
        return self
    
    def transform(
        self,
        transform_fn: Callable[[Any, PipelineContext], Any],
        name: Optional[str] = None
    ) -> "Pipeline":
        """Add a transform stage."""
        stage_name = name or f"{self.name}_transform_{len(self._stages)}"
        stage = TransformStage(stage_name, transform_fn)
        self._stages.append(stage)
        return self
    
    def filter(
        self,
        predicate: Callable[[Any, PipelineContext], bool],
        name: Optional[str] = None
    ) -> "Pipeline":
        """Add a filter stage."""
        stage_name = name or f"{self.name}_filter_{len(self._stages)}"
        stage = FilterStage(stage_name, predicate)
        self._stages.append(stage)
        return self
    
    def sink(
        self,
        sink_fn: Callable[[Any, PipelineContext], None],
        name: Optional[str] = None
    ) -> "Pipeline":
        """Add a sink stage."""
        stage_name = name or f"{self.name}_sink_{len(self._stages)}"
        stage = SinkStage(stage_name, sink_fn)
        self._stages.append(stage)
        return self
    
    def add_stage(self, stage: PipelineStage) -> "Pipeline":
        """Add a custom stage."""
        self._stages.append(stage)
        return self
    
    def execute(self, context: Optional[PipelineContext] = None) -> PipelineResult:
        """Execute the pipeline synchronously."""
        import time
        
        execution_id = f"{self.name}_{int(time.time() * 1000)}"
        if context is None:
            context = PipelineContext(execution_id=execution_id)
        else:
            context.execution_id = execution_id
        
        start_time = time.time()
        self._status = PipelineStatus.RUNNING
        stage_results: List[StageResult] = []
        
        try:
            data = None
            current_data = None
            
            for i, stage in enumerate(self._stages):
                stage_start = time.time()
                
                if stage.get_type() == StageType.SOURCE:
                    current_data = stage.execute(None, context)
                    data = current_data
                else:
                    if current_data is None and stage.get_type() != StageType.SOURCE:
                        self._logger.warning(f"Stage {stage.name}: No data to process")
                        continue
                    
                    current_data = stage.execute(current_data, context)
                
                stage_duration = (time.time() - stage_start) * 1000
                
                stage_results.append(StageResult(
                    stage_name=stage.name,
                    success=True,
                    data=current_data,
                    duration_ms=stage_duration,
                    records_in=context.counters.get("processed", 0),
                    records_out=context.counters.get("processed", 0)
                ))
                
                if current_data is None:
                    break
                
                context.increment("processed")
            
            self._status = PipelineStatus.COMPLETED
            
            return PipelineResult(
                execution_id=execution_id,
                status=self._status,
                total_duration_ms=(time.time() - start_time) * 1000,
                stages=stage_results,
                final_data=data,
                context=context
            )
            
        except Exception as e:
            self._status = PipelineStatus.FAILED
            return PipelineResult(
                execution_id=execution_id,
                status=self._status,
                total_duration_ms=(time.time() - start_time) * 1000,
                stages=stage_results,
                error=str(e),
                context=context
            )
    
    async def execute_async(self, context: Optional[PipelineContext] = None) -> PipelineResult:
        """Execute the pipeline asynchronously."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.execute, context)
    
    def pause(self) -> None:
        """Pause pipeline execution."""
        self._status = PipelineStatus.PAUSED
    
    def resume(self) -> None:
        """Resume pipeline execution."""
        if self._status == PipelineStatus.PAUSED:
            self._status = PipelineStatus.RUNNING
    
    def cancel(self) -> None:
        """Cancel pipeline execution."""
        self._status = PipelineStatus.CANCELLED


class ParallelPipeline:
    """Pipeline that executes branches in parallel."""
    
    def __init__(self, name: str, max_workers: int = 4):
        self.name = name
        self._pipelines: List[Pipeline] = []
        self._max_workers = max_workers
    
    def add_branch(self, pipeline: Pipeline) -> "ParallelPipeline":
        """Add a branch pipeline."""
        self._pipelines.append(pipeline)
        return self
    
    def execute(self) -> List[PipelineResult]:
        """Execute all branches in parallel."""
        import concurrent.futures
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures = [executor.submit(p.execute) for p in self._pipelines]
            return [f.result() for f in concurrent.futures.as_completed(futures)]


__all__ = [
    "PipelineStatus",
    "StageType",
    "PipelineContext",
    "StageResult",
    "PipelineResult",
    "PipelineStage",
    "SourceStage",
    "TransformStage",
    "FilterStage",
    "SinkStage",
    "Pipeline",
    "ParallelPipeline",
]
