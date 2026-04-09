"""
Data Transform Pipeline Action Module

Provides data transformation pipeline with configurable stages for UI automation
workflows. Supports filtering, mapping, aggregation, and custom transformations.

Author: AI Agent
Version: 1.0.0
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Generic, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


class PipelineStage(Enum):
    """Pipeline stage types."""
    SOURCE = auto()
    FILTER = auto()
    MAP = auto()
    FLATMAP = auto()
    AGGREGATE = auto()
    SORT = auto()
    LIMIT = auto()
    DEDUP = auto()
    MERGE = auto()
    BRANCH = auto()
    CUSTOM = auto()


@dataclass
class StageConfig:
    """Configuration for a pipeline stage."""
    name: str
    stage_type: PipelineStage
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineMetrics:
    """Pipeline execution metrics."""
    total_input: int = 0
    total_output: int = 0
    filtered_count: int = 0
    error_count: int = 0
    stage_timings: dict[str, float] = field(default_factory=dict)
    start_time: float = field(default_factory=lambda: datetime.utcnow().timestamp())
    end_time: Optional[float] = None

    @property
    def duration(self) -> float:
        """Calculate duration in seconds."""
        if self.end_time is None:
            return datetime.utcnow().timestamp() - self.start_time
        return self.end_time - self.start_time

    @property
    def throughput(self) -> float:
        """Calculate throughput (records/sec)."""
        duration = self.duration
        if duration == 0:
            return 0.0
        return self.total_output / duration


class PipelineStageBase(Generic[T, R]):
    """Base class for pipeline stages."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._next: Optional["PipelineStageBase"] = None

    def set_next(self, next_stage: "PipelineStageBase") -> "PipelineStageBase":
        """Set next stage in pipeline."""
        self._next = next_stage
        return next_stage

    def execute(self, data: list[T]) -> list[R]:
        """Execute stage transformation."""
        raise NotImplementedError

    def process(self, data: list[T]) -> list[R]:
        """Execute stage and pass to next."""
        result = self.execute(data)
        if self._next:
            return self._next.process(result)
        return result


class SourceStage(PipelineStageBase[T, T]):
    """Source stage for pipeline."""

    def __init__(self, name: str, source_fn: Callable[[], list[T]]) -> None:
        super().__init__(name)
        self.source_fn = source_fn

    def execute(self, data: list[T]) -> list[T]:
        """Get data from source."""
        return self.source_fn()


class FilterStage(PipelineStageBase[T, T]):
    """Filter stage for conditional processing."""

    def __init__(
        self,
        name: str,
        predicate: Callable[[T], bool],
    ) -> None:
        super().__init__(name)
        self.predicate = predicate

    def execute(self, data: list[T]) -> list[T]:
        """Filter data based on predicate."""
        return [item for item in data if self.predicate(item)]


class MapStage(PipelineStageBase[T, R]):
    """Map stage for transformation."""

    def __init__(
        self,
        name: str,
        transform_fn: Callable[[T], R],
    ) -> None:
        super().__init__(name)
        self.transform_fn = transform_fn

    def execute(self, data: list[T]) -> list[R]:
        """Transform each item."""
        result = []
        for item in data:
            try:
                result.append(self.transform_fn(item))
            except Exception as e:
                logger.error(f"Map error on {self.name}: {e}")
        return result


class FlatMapStage(PipelineStageBase[T, R]):
    """FlatMap stage for one-to-many transformation."""

    def __init__(
        self,
        name: str,
        flatmap_fn: Callable[[T], list[R]],
    ) -> None:
        super().__init__(name)
        self.flatmap_fn = flatmap_fn

    def execute(self, data: list[T]) -> list[R]:
        """Transform and flatten."""
        result = []
        for item in data:
            try:
                result.extend(self.flatmap_fn(item))
            except Exception as e:
                logger.error(f"FlatMap error on {self.name}: {e}")
        return result


class AggregateStage(PipelineStageBase[T, dict]):
    """Aggregation stage."""

    def __init__(
        self,
        name: str,
        group_by: Optional[Callable[[T], Any]] = None,
        aggregations: Optional[dict[str, Callable[[list], Any]]] = None,
    ) -> None:
        super().__init__(name)
        self.group_by = group_by
        self.aggregations = aggregations or {}

    def execute(self, data: list[T]) -> list[dict]:
        """Aggregate data."""
        if self.group_by is None:
            result: dict[str, list] = {"_all": []}
            for item in data:
                result["_all"].append(item)

            aggregated: list[dict] = []
            for key, items in result.items():
                agg_result = {}
                for agg_name, agg_fn in self.aggregations.items():
                    try:
                        agg_result[agg_name] = agg_fn(items)
                    except Exception as e:
                        logger.error(f"Aggregation error: {e}")
                        agg_result[agg_name] = None
                aggregated.append(agg_result)
            return aggregated

        groups: dict[Any, list[T]] = {}
        for item in data:
            key = self.group_by(item)
            if key not in groups:
                groups[key] = []
            groups[key].append(item)

        aggregated: list[dict] = []
        for key, items in groups.items():
            agg_result: dict[str, Any] = {"_group_key": key}
            for agg_name, agg_fn in self.aggregations.items():
                try:
                    agg_result[agg_name] = agg_fn(items)
                except Exception as e:
                    logger.error(f"Aggregation error: {e}")
                    agg_result[agg_name] = None
            aggregated.append(agg_result)

        return aggregated


class SortStage(PipelineStageBase[T, T]):
    """Sort stage."""

    def __init__(
        self,
        name: str,
        key_fn: Callable[[T], Any],
        reverse: bool = False,
    ) -> None:
        super().__init__(name)
        self.key_fn = key_fn
        self.reverse = reverse

    def execute(self, data: list[T]) -> list[T]:
        """Sort data."""
        return sorted(data, key=self.key_fn, reverse=self.reverse)


class LimitStage(PipelineStageBase[T, T]):
    """Limit stage for taking first N items."""

    def __init__(self, name: str, limit: int, offset: int = 0) -> None:
        super().__init__(name)
        self.limit = limit
        self.offset = offset

    def execute(self, data: list[T]) -> list[T]:
        """Take limited items."""
        return data[self.offset:self.offset + self.limit]


class DeduplicateStage(PipelineStageBase[T, T]):
    """Deduplication stage."""

    def __init__(
        self,
        name: str,
        key_fn: Optional[Callable[[T], Any]] = None,
    ) -> None:
        super().__init__(name)
        self.key_fn = key_fn

    def execute(self, data: list[T]) -> list[T]:
        """Remove duplicates."""
        if self.key_fn is None:
            seen: set = set()
            result = []
            for item in data:
                key = id(item) if self.key_fn is None else self.key_fn(item)
                if key not in seen:
                    seen.add(key)
                    result.append(item)
            return result

        seen: set = set()
        result = []
        for item in data:
            key = self.key_fn(item)
            if key not in seen:
                seen.add(key)
                result.append(item)
        return result


class CustomStage(PipelineStageBase[T, R]):
    """Custom transformation stage."""

    def __init__(
        self,
        name: str,
        transform_fn: Callable[[list[T]], list[R]],
    ) -> None:
        super().__init__(name)
        self.transform_fn = transform_fn

    def execute(self, data: list[T]) -> list[R]:
        """Execute custom transformation."""
        try:
            return self.transform_fn(data)
        except Exception as e:
            logger.error(f"Custom stage error on {self.name}: {e}")
            return []


class DataPipeline(Generic[T]):
    """
    Configurable data transformation pipeline.

    Example:
        >>> pipeline = DataPipeline()
        >>> pipeline.from_source(lambda: fetch_data())
        >>> pipeline.filter(lambda x: x["active"])
        >>> pipeline.map(lambda x: x["value"])
        >>> result = await pipeline.execute()
    """

    def __init__(self) -> None:
        self._stages: list[PipelineStageBase] = []
        self._metrics = PipelineMetrics()

    def from_source(self, source_fn: Callable[[], list[T]]) -> "DataPipeline":
        """Add source stage."""
        stage = SourceStage("source", source_fn)
        self._stages.append(stage)
        return self

    def filter(self, predicate: Callable[[T], bool]) -> "DataPipeline":
        """Add filter stage."""
        stage = FilterStage(f"filter_{len(self._stages)}", predicate)
        self._add_stage(stage)
        return self

    def map(self, transform_fn: Callable[[T], R]) -> "DataPipeline":
        """Add map stage."""
        stage = MapStage(f"map_{len(self._stages)}", transform_fn)
        self._add_stage(stage)
        return self

    def flatmap(self, flatmap_fn: Callable[[T], list[R]]) -> "DataPipeline":
        """Add flatMap stage."""
        stage = FlatMapStage(f"flatmap_{len(self._stages)}", flatmap_fn)
        self._add_stage(stage)
        return self

    def aggregate(
        self,
        group_by: Optional[Callable[[T], Any]] = None,
        **aggregations: Callable[[list], Any],
    ) -> "DataPipeline":
        """Add aggregation stage."""
        stage = AggregateStage(f"aggregate_{len(self._stages)}", group_by, aggregations)
        self._add_stage(stage)
        return self

    def sort(self, key_fn: Callable[[T], Any], reverse: bool = False) -> "DataPipeline":
        """Add sort stage."""
        stage = SortStage(f"sort_{len(self._stages)}", key_fn, reverse)
        self._add_stage(stage)
        return self

    def limit(self, limit: int, offset: int = 0) -> "DataPipeline":
        """Add limit stage."""
        stage = LimitStage(f"limit_{len(self._stages)}", limit, offset)
        self._add_stage(stage)
        return self

    def dedup(self, key_fn: Optional[Callable[[T], Any]] = None) -> "DataPipeline":
        """Add deduplication stage."""
        stage = DeduplicateStage(f"dedup_{len(self._stages)}", key_fn)
        self._add_stage(stage)
        return self

    def custom(self, transform_fn: Callable[[list], list]) -> "DataPipeline":
        """Add custom transformation stage."""
        stage = CustomStage(f"custom_{len(self._stages)}", transform_fn)
        self._add_stage(stage)
        return self

    def _add_stage(self, stage: PipelineStageBase) -> None:
        """Add stage to pipeline."""
        if self._stages:
            self._stages[-1].set_next(stage)
        self._stages.append(stage)

    def execute(self) -> list[Any]:
        """Execute pipeline."""
        self._metrics = PipelineMetrics()
        self._metrics.start_time = datetime.utcnow().timestamp()

        if not self._stages:
            return []

        data: list[Any] = []
        for i, stage in enumerate(self._stages):
            stage_start = datetime.utcnow().timestamp()
            try:
                if i == 0:
                    data = stage.execute([])
                else:
                    data = stage.execute(data)

                self._metrics.total_input += len(data) if i > 0 else 0
                self._metrics.total_output = len(data)
                self._metrics.stage_timings[stage.name] = (
                    datetime.utcnow().timestamp() - stage_start
                )
            except Exception as e:
                logger.error(f"Stage {stage.name} failed: {e}")
                self._metrics.error_count += 1

        self._metrics.end_time = datetime.utcnow().timestamp()
        return data

    def get_metrics(self) -> PipelineMetrics:
        """Get pipeline execution metrics."""
        return self._metrics

    def __repr__(self) -> str:
        stage_names = [s.name for s in self._stages]
        return f"DataPipeline(stages={len(self._stages)}, names={stage_names})"
