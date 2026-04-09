"""
Data pipeline framework for processing structured data flows.

This module provides a flexible pipeline framework for chaining data
transformations, filtering, and aggregations with support for branching.

Author: RabAiBot
License: MIT
"""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, Generic, List, Optional, TypeVar, Union
from collections import defaultdict

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


class PipelineStageType(Enum):
    """Types of pipeline stages."""
    SOURCE = auto()
    TRANSFORM = auto()
    FILTER = auto()
    MAP = auto()
    FLATMAP = auto()
    REDUCE = auto()
    AGGREGATE = auto()
    BRANCH = auto()
    MERGE = auto()
    SINK = auto()


@dataclass
class PipelineStats:
    """Statistics for pipeline execution."""
    items_processed: int = 0
    items_filtered: int = 0
    items_errored: int = 0
    total_duration: float = 0.0
    stage_durations: Dict[str, float] = field(default_factory=dict)

    @property
    def items_per_second(self) -> float:
        """Calculate processing rate."""
        if self.total_duration == 0:
            return 0.0
        return self.items_processed / self.total_duration


class PipelineStage(ABC, Generic[T, R]):
    """Base class for pipeline stages."""

    def __init__(self, name: str):
        self.name = name
        self._stats = {"processed": 0, "errors": 0, "duration": 0.0}

    @abstractmethod
    async def process(self, item: T) -> Optional[R]:
        """Process a single item."""
        pass

    async def process_batch(self, items: List[T]) -> List[R]:
        """Process a batch of items."""
        results = []
        for item in items:
            try:
                result = await self.process(item)
                if result is not None:
                    results.append(result)
            except Exception as e:
                self._stats["errors"] += 1
                logger.warning(f"Stage {self.name} error: {e}")
        self._stats["processed"] += len(items)
        return results

    def get_stats(self) -> Dict[str, Any]:
        """Get stage statistics."""
        return {**self._stats, "name": self.name}


class SourceStage(PipelineStage, Generic[T]):
    """Data source stage that produces items."""

    def __init__(
        self,
        name: str,
        source_fn: Callable[[], List[T]],
        is_async: bool = False,
    ):
        super().__init__(name)
        self.source_fn = source_fn
        self.is_async = is_async

    async def process(self, item: T) -> Optional[T]:
        """This stage doesn't process items, it produces them."""
        return item

    async def produce(self) -> List[T]:
        """Produce items from source."""
        start = time.time()
        if self.is_async:
            items = await self.source_fn()
        else:
            items = self.source_fn()
        self._stats["processed"] = len(items)
        self._stats["duration"] = time.time() - start
        return items


class TransformStage(PipelineStage[T, R]):
    """Transform stage that modifies items."""

    def __init__(
        self,
        name: str,
        transform_fn: Callable[[T], R],
        is_async: bool = False,
    ):
        super().__init__(name)
        self.transform_fn = transform_fn
        self.is_async = is_async

    async def process(self, item: T) -> Optional[R]:
        start = time.time()
        try:
            if self.is_async:
                result = await self.transform_fn(item)
            else:
                result = self.transform_fn(item)
            self._stats["processed"] += 1
            return result
        except Exception as e:
            self._stats["errors"] += 1
            raise
        finally:
            self._stats["duration"] += time.time() - start


class FilterStage(PipelineStage[T, T]):
    """Filter stage that selectively passes items."""

    def __init__(
        self,
        name: str,
        predicate: Callable[[T], bool],
        is_async: bool = False,
    ):
        super().__init__(name)
        self.predicate = predicate
        self.is_async = is_async

    async def process(self, item: T) -> Optional[T]:
        try:
            if self.is_async:
                keep = await self.predicate(item)
            else:
                keep = self.predicate(item)

            if keep:
                self._stats["processed"] += 1
                return item
            else:
                return None
        except Exception as e:
            self._stats["errors"] += 1
            logger.warning(f"Filter {self.name} predicate error: {e}")
            return None


class MapStage(PipelineStage[T, R]):
    """Map stage that transforms each item to a new value."""

    def __init__(
        self,
        name: str,
        mapper: Callable[[T], R],
        is_async: bool = False,
    ):
        super().__init__(name)
        self.mapper = mapper
        self.is_async = is_async

    async def process(self, item: T) -> Optional[R]:
        start = time.time()
        try:
            if self.is_async:
                result = await self.mapper(item)
            else:
                result = self.mapper(item)
            self._stats["processed"] += 1
            return result
        except Exception as e:
            self._stats["errors"] += 1
            raise
        finally:
            self._stats["duration"] += time.time() - start


class FlatMapStage(PipelineStage[T, R]):
    """FlatMap stage that can produce multiple outputs per input."""

    def __init__(
        self,
        name: str,
        flatmapper: Callable[[T], List[R]],
        is_async: bool = False,
    ):
        super().__init__(name)
        self.flatmapper = flatmapper
        self.is_async = is_async

    async def process(self, item: T) -> Optional[List[R]]:
        try:
            if self.is_async:
                results = await self.flatmapper(item)
            else:
                results = self.flatmapper(item)
            self._stats["processed"] += 1
            return results
        except Exception as e:
            self._stats["errors"] += 1
            raise


class ReduceStage(PipelineStage[List[T], R]):
    """Reduce stage that aggregates items."""

    def __init__(
        self,
        name: str,
        reducer: Callable[[R, T], R],
        initial_value: Optional[R] = None,
        is_async: bool = False,
    ):
        super().__init__(name)
        self.reducer = reducer
        self.initial_value = initial_value
        self.is_async = is_async
        self._accumulator: Optional[R] = None

    async def process(self, item: T) -> Optional[R]:
        try:
            if self._accumulator is None:
                self._accumulator = self.initial_value

            if self.is_async:
                self._accumulator = await self.reducer(self._accumulator, item)
            else:
                self._accumulator = self.reducer(self._accumulator, item)

            self._stats["processed"] += 1
            return self._accumulator
        except Exception as e:
            self._stats["errors"] += 1
            raise

    def get_result(self) -> Optional[R]:
        """Get the final accumulated result."""
        return self._accumulator

    def reset(self) -> None:
        """Reset the accumulator."""
        self._accumulator = self.initial_value


class SinkStage(PipelineStage[T, None]):
    """Sink stage that outputs data (database, file, etc.)."""

    def __init__(
        self,
        name: str,
        sink_fn: Callable[[T], None],
        is_async: bool = False,
        batch_size: int = 1,
    ):
        super().__init__(name)
        self.sink_fn = sink_fn
        self.is_async = is_async
        self.batch_size = batch_size
        self._buffer: List[T] = []

    async def process(self, item: T) -> None:
        self._buffer.append(item)
        if len(self._buffer) >= self.batch_size:
            await self.flush()

    async def flush(self) -> None:
        """Flush buffered items to sink."""
        if not self._buffer:
            return

        try:
            if self.is_async:
                await self.sink_fn(self._buffer)
            else:
                self.sink_fn(self._buffer)
            self._stats["processed"] += len(self._buffer)
        except Exception as e:
            self._stats["errors"] += len(self._buffer)
            raise
        finally:
            self._buffer.clear()


class DataPipeline:
    """
    Data processing pipeline with configurable stages.

    Features:
    - Composable stages (source, transform, filter, map, reduce, sink)
    - Parallel stage execution with branching
    - Async support
    - Comprehensive statistics
    - Error handling and recovery

    Example:
        >>> pipeline = DataPipeline()
        >>> pipeline.add_stage(SourceStage("read", lambda: load_data()))
        >>> pipeline.add_stage(FilterStage("valid", lambda x: x is not None))
        >>> pipeline.add_stage(MapStage("transform", lambda x: x * 2))
        >>> pipeline.add_stage(SinkStage("write", lambda items: save(items)))
        >>> results = await pipeline.run()
    """

    def __init__(self, name: str = "pipeline"):
        """Initialize the pipeline."""
        self.name = name
        self.stages: List[PipelineStage] = []
        self._source: Optional[SourceStage] = None
        self._sink: Optional[SinkStage] = None
        self._stats = PipelineStats()
        self._running = False
        logger.info(f"DataPipeline '{name}' initialized")

    def add_stage(self, stage: PipelineStage) -> "DataPipeline":
        """Add a stage to the pipeline."""
        self.stages.append(stage)

        if isinstance(stage, SourceStage):
            self._source = stage
        elif isinstance(stage, SinkStage):
            self._sink = stage

        return self

    def source(
        self,
        source_fn: Callable[[], List[T]],
        is_async: bool = False,
    ) -> "DataPipeline":
        """Add a source stage."""
        stage = SourceStage(f"source_{len(self.stages)}", source_fn, is_async)
        return self.add_stage(stage)

    def transform(
        self,
        fn: Callable[[T], R],
        is_async: bool = False,
    ) -> "DataPipeline":
        """Add a transform stage."""
        stage = TransformStage(f"transform_{len(self.stages)}", fn, is_async)
        return self.add_stage(stage)

    def filter(
        self,
        predicate: Callable[[T], bool],
        is_async: bool = False,
    ) -> "DataPipeline":
        """Add a filter stage."""
        stage = FilterStage(f"filter_{len(self.stages)}", predicate, is_async)
        return self.add_stage(stage)

    def map(
        self,
        mapper: Callable[[T], R],
        is_async: bool = False,
    ) -> "DataPipeline":
        """Add a map stage."""
        stage = MapStage(f"map_{len(self.stages)}", mapper, is_async)
        return self.add_stage(stage)

    def flatmap(
        self,
        flatmapper: Callable[[T], List[R]],
        is_async: bool = False,
    ) -> "DataPipeline":
        """Add a flatmap stage."""
        stage = FlatMapStage(f"flatmap_{len(self.stages)}", flatmapper, is_async)
        return self.add_stage(stage)

    def reduce(
        self,
        reducer: Callable[[R, T], R],
        initial: Optional[R] = None,
        is_async: bool = False,
    ) -> "DataPipeline":
        """Add a reduce stage."""
        stage = ReduceStage(f"reduce_{len(self.stages)}", reducer, initial, is_async)
        return self.add_stage(stage)

    def sink(
        self,
        sink_fn: Callable[[List], None],
        is_async: bool = False,
        batch_size: int = 1,
    ) -> "DataPipeline":
        """Add a sink stage."""
        stage = SinkStage(f"sink_{len(self.stages)}", sink_fn, is_async, batch_size)
        return self.add_stage(stage)

    async def run(self, data: Optional[List[T]] = None) -> List[Any]:
        """
        Run the pipeline.

        Args:
            data: Optional input data (if no source stage)

        Returns:
            Pipeline results
        """
        self._running = True
        start_time = time.time()
        results = []

        try:
            if data is not None:
                items = data
            elif self._source:
                items = await self._source.produce()
            else:
                logger.warning("No data source, returning empty results")
                return []

            self._stats.items_processed = len(items)

            for stage in self.stages:
                stage_start = time.time()

                if isinstance(stage, SourceStage):
                    continue

                if isinstance(stage, SinkStage):
                    for item in items:
                        await stage.process(item)
                    await stage.flush()
                    continue

                if isinstance(stage, FlatMapStage):
                    flat_results = []
                    for item in items:
                        result = await stage.process(item)
                        if result:
                            flat_results.extend(result)
                    items = flat_results
                elif isinstance(stage, ReduceStage):
                    for item in items:
                        await stage.process(item)
                    results = [stage.get_result()] if stage.get_result() else []
                    items = results
                else:
                    new_items = []
                    for item in items:
                        result = await stage.process(item)
                        if result is not None:
                            new_items.append(result)
                    items = new_items

                stage_duration = time.time() - stage_start
                self._stats.stage_durations[stage.name] = stage_duration

            self._stats.total_duration = time.time() - start_time
            logger.info(
                f"Pipeline '{self.name}' completed: "
                f"{self._stats.items_processed} items in {self._stats.total_duration:.2f}s"
            )

            return items if not results else results

        except Exception as e:
            logger.exception(f"Pipeline '{self.name}' failed: {e}")
            self._stats.items_errored += 1
            raise
        finally:
            self._running = False

    def get_stats(self) -> Dict[str, Any]:
        """Get pipeline statistics."""
        return {
            "name": self.name,
            "total_items": self._stats.items_processed,
            "filtered_items": self._stats.items_filtered,
            "errored_items": self._stats.items_errored,
            "total_duration": self._stats.total_duration,
            "items_per_second": self._stats.items_per_second,
            "stages": {
                stage.name: stage.get_stats()
                for stage in self.stages
            },
        }

    def reset_stats(self) -> None:
        """Reset pipeline statistics."""
        self._stats = PipelineStats()
        for stage in self.stages:
            stage._stats = {"processed": 0, "errors": 0, "duration": 0.0}
