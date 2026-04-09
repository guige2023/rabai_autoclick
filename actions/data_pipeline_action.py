"""Data Pipeline Action Module.

Composable data processing pipeline with parallel execution support.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")
R = TypeVar("R")


class PipelineStatus(Enum):
    """Pipeline execution status."""
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class PipelineStage(ABC, Generic[T, R]):
    """Abstract base class for pipeline stages."""

    def __init__(self, name: str) -> None:
        self.name = name

    @abstractmethod
    async def process(self, input_data: T) -> R:
        """Process input and return output."""
        pass

    async def validate(self, input_data: Any) -> bool:
        """Validate input data. Override for custom validation."""
        return input_data is not None


@dataclass
class PipelineStats:
    """Statistics for pipeline execution."""
    stages_executed: int = 0
    total_processing_time: float = 0.0
    errors: list[str] = field(default_factory=list)


class Pipeline:
    """Data processing pipeline."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.stages: list[PipelineStage] = []
        self.status = PipelineStatus.IDLE
        self.stats = PipelineStats()
        self._cancel_event = asyncio.Event()

    def add_stage(self, stage: PipelineStage[T, R]) -> Pipeline:
        """Add a stage to the pipeline. Returns self for chaining."""
        self.stages.append(stage)
        return self

    def add_stage_func(self, name: str, func: Callable[[T], R | asyncio.coroutine]) -> Pipeline:
        """Add a function as a pipeline stage."""
        async def wrapper(input_data: T) -> R:
            if asyncio.iscoroutinefunction(func):
                return await func(input_data)
            return func(input_data)
        wrapper.__name__ = name
        stage = PipelineStage(name)
        stage.process = wrapper
        self.stages.append(stage)
        return self

    async def execute(self, input_data: T) -> Any:
        """Execute pipeline on input data."""
        self.status = PipelineStatus.RUNNING
        self.stats = PipelineStats()
        current = input_data
        import time
        start_time = time.monotonic()
        try:
            for i, stage in enumerate(self.stages):
                if self._cancel_event.is_set():
                    self.status = PipelineStatus.CANCELLED
                    return None
                if not await stage.validate(current):
                    raise ValueError(f"Validation failed at stage {i}: {stage.name}")
                current = await stage.process(current)
                self.stats.stages_executed += 1
            self.status = PipelineStatus.COMPLETED
            self.stats.total_processing_time = time.monotonic() - start_time
            return current
        except Exception as e:
            self.status = PipelineStatus.FAILED
            self.stats.errors.append(str(e))
            self.stats.total_processing_time = time.monotonic() - start_time
            raise
        finally:
            if self.status == PipelineStatus.RUNNING:
                self.status = PipelineStatus.COMPLETED

    def cancel(self) -> None:
        """Cancel pipeline execution."""
        self._cancel_event.set()


class ParallelPipeline:
    """Pipeline with parallel stage execution branches."""

    def __init__(self, name: str, max_workers: int = 4) -> None:
        self.name = name
        self.branches: list[Pipeline] = []
        self.max_workers = max_workers
        self.status = PipelineStatus.IDLE

    def add_branch(self, pipeline: Pipeline) -> ParallelPipeline:
        """Add a parallel branch."""
        self.branches.append(pipeline)
        return self

    async def execute(self, input_data: Any) -> list[Any]:
        """Execute all branches in parallel."""
        self.status = PipelineStatus.RUNNING
        if not self.branches:
            return []
        semaphore = asyncio.Semaphore(self.max_workers)
        async def run_branch(branch: Pipeline, data: Any) -> Any:
            async with semaphore:
                return await branch.execute(data)
        results = await asyncio.gather(
            *[run_branch(branch, input_data) for branch in self.branches],
            return_exceptions=True
        )
        self.status = PipelineStatus.COMPLETED
        return results


class DataTransformStage(PipelineStage):
    """Stage that applies a transformation function."""

    def __init__(self, name: str, transform: Callable[[T], R]) -> None:
        super().__init__(name)
        self.transform = transform

    async def process(self, input_data: T) -> R:
        return self.transform(input_data)


class DataFilterStage(PipelineStage):
    """Stage that filters data based on predicate."""

    def __init__(self, name: str, predicate: Callable[[T], bool]) -> None:
        super().__init__(name)
        self.predicate = predicate

    async def process(self, input_data: list[T]) -> list[T]:
        return [item for item in input_data if self.predicate(item)]
