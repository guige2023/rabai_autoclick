"""Pipeline utilities for RabAI AutoClick.

Provides:
- Data pipeline builder
- Stage composition
- Pipeline execution with error handling
- Async pipeline support
- Pipeline monitoring and statistics
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Deque,
    Generic,
    Iterator,
    List,
    Optional,
    TypeVar,
)


T = TypeVar("T")
U = TypeVar("U")
R = TypeVar("R")


@dataclass
class PipelineStats:
    """Statistics for a pipeline execution."""

    total_items: int = 0
    processed_items: int = 0
    failed_items: int = 0
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    stage_times: dict[str, float] = field(default_factory=dict)

    @property
    def success_rate(self) -> float:
        if self.processed_items == 0:
            return 0.0
        return self.processed_items / self.total_items

    @property
    def duration(self) -> float:
        if self.end_time is None:
            return time.time() - self.start_time
        return self.end_time - self.start_time

    @property
    def throughput(self) -> float:
        dur = self.duration
        if dur == 0:
            return 0.0
        return self.processed_items / dur


@dataclass
class PipelineStage(Generic[T, U]):
    """A single stage in a pipeline.

    Attributes:
        name: Stage identifier.
        transform: Function to transform input to output.
        error_handler: Optional error handler function.
        skip_on_error: If True, skip this stage on error instead of failing.
    """

    name: str
    transform: Callable[[T], U]
    error_handler: Optional[Callable[[T, Exception], U]] = None
    skip_on_error: bool = False


class Pipeline(Generic[T, R]):
    """Data processing pipeline.

    Allows chaining multiple transformation stages. Each stage
    receives output from the previous stage.

    Example:
        pipeline = (
            Pipeline[str]()
            .stage("normalize", normalize_text)
            .stage("tokenize", tokenize)
            .stage("filter", filter_words)
        )

        results = pipeline.execute(["Hello World", "Foo Bar"])
    """

    def __init__(self) -> None:
        self._stages: List[PipelineStage[Any, Any]] = []
        self._stats: PipelineStats = PipelineStats()

    def stage(
        self,
        name: str,
        transform: Callable[[T], U],
        error_handler: Optional[Callable[[T, Exception], U]] = None,
        skip_on_error: bool = False,
    ) -> Pipeline[T, Any]:
        """Add a stage to the pipeline.

        Args:
            name: Stage identifier.
            transform: Transformation function.
            error_handler: Optional error handler (item, error) -> output.
            skip_on_error: If True, pass item through unchanged on error.

        Returns:
            Self for method chaining.
        """
        stage_obj = PipelineStage(
            name=name,
            transform=transform,
            error_handler=error_handler,
            skip_on_error=skip_on_error,
        )
        self._stages.append(stage_obj)  # type: ignore
        return self  # type: ignore

    def execute(self, items: List[T]) -> List[Any]:
        """Execute pipeline on a list of items.

        Args:
            items: Input items to process.

        Returns:
            List of processed items.
        """
        self._reset_stats(len(items))
        results: List[Any] = []
        stage_times: Deque[float] = deque(maxlen=1)

        for item in items:
            try:
                result = self._process_item(item, stage_times)
                results.append(result)
                self._stats.processed_items += 1
            except Exception as e:
                self._stats.failed_items += 1

        self._stats.end_time = time.time()
        return results

    def _process_item(self, item: T, stage_times: Deque[float]) -> Any:
        current = item
        for stage in self._stages:
            start = time.time()
            try:
                current = stage.transform(current)
            except Exception as e:
                if stage.error_handler is not None:
                    current = stage.error_handler(item, e)
                elif stage.skip_on_error:
                    pass
                else:
                    raise
            stage_times.append(time.time() - start)

            if stage.name not in self._stats.stage_times:
                self._stats.stage_times[stage.name] = 0.0
            self._stats.stage_times[stage.name] += stage_times[-1]

        return current

    def _reset_stats(self, total_items: int) -> None:
        self._stats = PipelineStats(total_items=total_items)

    def execute_one(self, item: T) -> Any:
        """Execute pipeline on a single item.

        Args:
            item: Input item to process.

        Returns:
            Processed item.

        Raises:
            Exception from last stage if pipeline fails.
        """
        return self._process_item(item, deque(maxlen=1))

    def stream(self, items: Iterator[T]) -> Iterator[Any]:
        """Stream items through pipeline.

        Args:
            items: Iterator of input items.

        Yields:
            Processed items.
        """
        for item in items:
            try:
                yield self._process_item(item, deque(maxlen=1))
                self._stats.processed_items += 1
            except Exception:
                self._stats.failed_items += 1
            self._stats.total_items += 1

    @property
    def stats(self) -> PipelineStats:
        """Get pipeline execution statistics."""
        return self._stats

    def __len__(self) -> int:
        return len(self._stages)

    def __repr__(self) -> str:
        stage_names = [s.name for s in self._stages]
        return f"Pipeline({' -> '.join(stage_names)})"


class AsyncPipeline(Generic[T, R]):
    """Async data processing pipeline."""

    def __init__(self, max_concurrency: int = 10) -> None:
        self._stages: List[PipelineStage[Any, Any]] = []
        self._max_concurrency = max_concurrency
        self._stats = PipelineStats()

    def stage(
        self,
        name: str,
        transform: Callable[[T], U],
        error_handler: Optional[Callable[[T, Exception], U]] = None,
        skip_on_error: bool = False,
    ) -> AsyncPipeline[T, Any]:
        stage_obj = PipelineStage(
            name=name,
            transform=transform,
            error_handler=error_handler,
            skip_on_error=skip_on_error,
        )
        self._stages.append(stage_obj)  # type: ignore
        return self  # type: ignore

    async def execute(self, items: List[T]) -> List[Any]:
        self._reset_stats(len(items))
        semaphore = asyncio.Semaphore(self._max_concurrency)

        async def process_with_semaphore(item: T) -> Any:
            async with semaphore:
                return await self._process_item_async(item)

        results = await asyncio.gather(
            *(process_with_semaphore(item) for item in items),
            return_exceptions=True,
        )

        processed: List[Any] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self._stats.failed_items += 1
            else:
                processed.append(result)
                self._stats.processed_items += 1

        self._stats.end_time = time.time()
        return processed

    async def _process_item_async(self, item: T) -> Any:
        current = item
        for stage in self._stages:
            try:
                result = stage.transform(current)
                if asyncio.iscoroutine(result):
                    current = await result
                else:
                    current = result
            except Exception as e:
                if stage.error_handler is not None:
                    current = stage.error_handler(item, e)
                elif stage.skip_on_error:
                    pass
                else:
                    raise
        return current

    def _reset_stats(self, total_items: int) -> None:
        self._stats = PipelineStats(total_items=total_items)

    @property
    def stats(self) -> PipelineStats:
        return self._stats


def compose(*functions: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Compose functions right-to-left.

    Args:
        *functions: Functions to compose.

    Returns:
        Composed function.

    Example:
        f = compose(str.lower, str.strip)
        f("  Hello  ")  # "hello"
    """
    if not functions:
        return lambda x: x

    def composed(x: Any) -> Any:
        result = x
        for func in reversed(functions):
            result = func(result)
        return result

    return composed


def pipe(*functions: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Pipe functions left-to-right.

    Args:
        *functions: Functions to pipe.

    Returns:
        Piped function.

    Example:
        f = pipe(str.strip, str.lower, lambda s: s.replace(" ", ""))
        f("  Hello World  ")  # "helloworld"
    """
    if not functions:
        return lambda x: x

    def piped(x: Any) -> Any:
        result = x
        for func in functions:
            result = func(result)
        return result

    return piped
