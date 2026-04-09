"""Data pipeline framework for composable data transformation.

Supports map, filter, reduce, branch, merge operations
with error handling and checkpointing.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")
U = TypeVar("U")


class StageStatus(Enum):
    """Pipeline stage execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class PipelineContext:
    """Shared context passed through pipeline stages."""

    metadata: dict[str, Any] = field(default_factory=dict)
    errors: list[dict[str, Any]] = field(default_factory=list)
    metrics: dict[str, int] = field(default_factory=dict)
    checkpoint_data: dict[str, Any] = field(default_factory=dict)

    def record_error(self, stage: str, error: Exception) -> None:
        """Record an error from a pipeline stage."""
        self.errors.append({"stage": stage, "error": str(error), "type": type(error).__name__})

    def increment_metric(self, key: str, value: int = 1) -> None:
        """Increment a metric counter."""
        self.metrics[key] = self.metrics.get(key, 0) + value


class PipelineStage(ABC, Generic[T, R]):
    """Abstract base class for pipeline stages."""

    def __init__(self, name: str | None = None) -> None:
        self.name = name or self.__class__.__name__

    @abstractmethod
    def process(self, data: T, ctx: PipelineContext) -> R:
        """Process input data and return output."""
        pass

    def on_error(self, error: Exception, data: T, ctx: PipelineContext) -> R | None:
        """Handle errors. Return fallback value or re-raise."""
        raise error


class MapStage(PipelineStage[T, R]):
    """Map transformation stage."""

    def __init__(
        self,
        fn: Callable[[T], R],
        name: str | None = None,
    ) -> None:
        super().__init__(name)
        self.fn = fn

    def process(self, data: T, ctx: PipelineContext) -> R:
        """Apply transformation function."""
        return self.fn(data)


class FilterStage(PipelineStage[list[T], list[T]]):
    """Filter stage that passes only items matching predicate."""

    def __init__(
        self,
        predicate: Callable[[T], bool],
        name: str | None = None,
    ) -> None:
        super().__init__(name)
        self.predicate = predicate

    def process(self, data: list[T], ctx: PipelineContext) -> list[T]:
        """Filter items based on predicate."""
        result = [item for item in data if self.predicate(item)]
        ctx.increment_metric(f"{self.name}_filtered", len(data) - len(result))
        return result


class FlatMapStage(PipelineStage[T, list[R]]):
    """FlatMap stage that maps and flattens results."""

    def __init__(
        self,
        fn: Callable[[T], list[R]],
        name: str | None = None,
    ) -> None:
        super().__init__(name)
        self.fn = fn

    def process(self, data: T, ctx: PipelineContext) -> list[R]:
        """Apply function and flatten results."""
        result = self.fn(data)
        ctx.increment_metric(f"{self.name}_flatmapped", len(result))
        return result


class BatchStage(PipelineStage[list[T], list[list[T]]]):
    """Batch items into groups."""

    def __init__(
        self,
        batch_size: int = 100,
        name: str | None = None,
    ) -> None:
        super().__init__(name)
        self.batch_size = batch_size

    def process(self, data: list[T], ctx: PipelineContext) -> list[list[T]]:
        """Split data into batches."""
        batches = []
        for i in range(0, len(data), self.batch_size):
            batches.append(data[i : i + self.batch_size])
        ctx.increment_metric(f"{self.name}_batches", len(batches))
        return batches


class BranchStage(PipelineStage[T, dict[str, Any]]):
    """Branch stage that routes data to multiple sub-stages."""

    def __init__(
        self,
        branches: dict[str, PipelineStage],
        name: str | None = None,
    ) -> None:
        super().__init__(name)
        self.branches = branches

    def process(self, data: T, ctx: PipelineContext) -> dict[str, Any]:
        """Run all branches and return combined results."""
        results = {}
        for branch_name, stage in self.branches.items():
            try:
                results[branch_name] = stage.process(data, ctx)
            except Exception as e:
                logger.error("Branch %s failed: %s", branch_name, e)
                ctx.record_error(f"{self.name}.{branch_name}", e)
                results[branch_name] = None
        return results


class MergeStage(PipelineStage[list[Any], R]):
    """Merge stage that combines multiple inputs."""

    def __init__(
        self,
        merge_fn: Callable[[list[Any]], R],
        name: str | None = None,
    ) -> None:
        super().__init__(name)
        self.merge_fn = merge_fn

    def process(self, data: list[Any], ctx: PipelineContext) -> R:
        """Merge inputs using merge function."""
        return self.merge_fn(data)


@dataclass
class Pipeline:
    """Composable data pipeline."""

    stages: list[PipelineStage] = field(default_factory=list)
    name: str = "pipeline"
    continue_on_error: bool = False

    def add(self, stage: PipelineStage) -> "Pipeline":
        """Add a stage to the pipeline."""
        self.stages.append(stage)
        return self

    def map(self, fn: Callable[[T], R], name: str | None = None) -> "Pipeline":
        """Add a map stage."""
        return self.add(MapStage(fn, name))

    def filter(self, predicate: Callable[[T], bool], name: str | None = None) -> "Pipeline":
        """Add a filter stage."""
        return self.add(FilterStage(predicate, name))

    def flat_map(self, fn: Callable[[T], list[R]], name: str | None = None) -> "Pipeline":
        """Add a flatMap stage."""
        return self.add(FlatMapStage(fn, name))

    def batch(self, batch_size: int = 100, name: str | None = None) -> "Pipeline":
        """Add a batch stage."""
        return self.add(BatchStage(batch_size, name))

    def branch(self, branches: dict[str, PipelineStage], name: str | None = None) -> "Pipeline":
        """Add a branch stage."""
        return self.add(BranchStage(branches, name))

    def merge(self, merge_fn: Callable[[list[Any]], R], name: str | None = None) -> "Pipeline":
        """Add a merge stage."""
        return self.add(MergeStage(merge_fn, name))

    def run(self, data: Any) -> tuple[Any, PipelineContext]:
        """Run the pipeline on input data.

        Returns:
            Tuple of (result, context).
        """
        ctx = PipelineContext()
        current_data = data

        for i, stage in enumerate(self.stages):
            stage_status = StageStatus.RUNNING
            try:
                current_data = stage.process(current_data, ctx)
                stage_status = StageStatus.COMPLETED
                ctx.increment_metric(f"stage_{i}_completed")
            except Exception as e:
                logger.error("Stage %s failed: %s", stage.name, e)
                ctx.record_error(stage.name, e)
                stage_status = StageStatus.FAILED
                ctx.increment_metric(f"stage_{i}_failed")

                if not self.continue_on_error:
                    raise

            ctx.metadata[f"stage_{i}_status"] = stage_status.value
            ctx.metadata[f"stage_{i}_name"] = stage.name

        return current_data, ctx


class PipelineBuilder:
    """Fluent builder for pipelines."""

    def __init__(self, name: str = "pipeline") -> None:
        self._pipeline = Pipeline(name=name)

    def add(self, stage: PipelineStage) -> "PipelineBuilder":
        """Add a stage."""
        self._pipeline.add(stage)
        return self

    def map(self, fn: Callable[[T], R], name: str | None = None) -> "PipelineBuilder":
        """Add map stage."""
        self._pipeline.add(MapStage(fn, name))
        return self

    def filter(self, predicate: Callable[[T], bool], name: str | None = None) -> "PipelineBuilder":
        """Add filter stage."""
        self._pipeline.add(FilterStage(predicate, name))
        return self

    def flat_map(self, fn: Callable[[T], list[R]], name: str | None = None) -> "PipelineBuilder":
        """Add flatMap stage."""
        self._pipeline.add(FlatMapStage(fn, name))
        return self

    def batch(self, batch_size: int = 100, name: str | None = None) -> "PipelineBuilder":
        """Add batch stage."""
        self._pipeline.add(BatchStage(batch_size, name))
        return self

    def on_error(self, continue_on_error: bool) -> "PipelineBuilder":
        """Set error handling behavior."""
        self._pipeline.continue_on_error = continue_on_error
        return self

    def build(self) -> Pipeline:
        """Build the pipeline."""
        return self._pipeline
