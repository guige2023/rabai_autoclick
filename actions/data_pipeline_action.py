"""Data Pipeline Action Module.

Provides composable data processing pipeline with filtering,
transformation, aggregation, and error handling stages.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Generic, List, Optional, TypeVar, Union
from collections import defaultdict

T = TypeVar("T")
U = TypeVar("U")
R = TypeVar("R")


class PipelineStageType(Enum):
    """Pipeline stage type."""
    FILTER = "filter"
    MAP = "map"
    FLATMAP = "flatmap"
    REDUCE = "reduce"
    AGGREGATE = "aggregate"
    BRANCH = "branch"
    MERGE = "merge"


@dataclass
class PipelineStage(Generic[T, U]):
    """A single pipeline stage."""
    name: str
    stage_type: PipelineStageType
    func: Callable[[T], U]
    error_handler: Optional[Callable[[Exception, T], U]] = None
    skip_on_error: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineResult(Generic[R]):
    """Pipeline execution result."""
    success: bool
    data: R
    errors: List[Dict[str, Any]] = field(default_factory=list)
    stages_completed: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataPipelineAction:
    """Composable data processing pipeline.

    Example:
        pipeline = DataPipelineAction()

        pipeline.add_stage("filter", lambda x: x > 0)
        pipeline.add_stage("double", lambda x: x * 2)
        pipeline.add_stage("sum", lambda x: sum(x))

        result = await pipeline.execute([1, -2, 3, 4, -5])
        print(result.data)  # 16
    """

    def __init__(self, name: str = "pipeline") -> None:
        self.name = name
        self._stages: List[PipelineStage] = []
        self._stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "processed": 0,
            "errors": 0,
            "skipped": 0,
        })

    def add_stage(
        self,
        name: str,
        func: Callable,
        stage_type: PipelineStageType = PipelineStageType.MAP,
        error_handler: Optional[Callable] = None,
        skip_on_error: bool = False,
    ) -> "DataPipelineAction":
        """Add a stage to the pipeline.

        Returns self for chaining.
        """
        stage = PipelineStage(
            name=name,
            stage_type=stage_type,
            func=func,
            error_handler=error_handler,
            skip_on_error=skip_on_error,
        )
        self._stages.append(stage)
        return self

    def filter(self, predicate: Callable[[T], bool]) -> "DataPipelineAction":
        """Add a filter stage."""
        return self.add_stage(
            f"filter_{len(self._stages)}",
            predicate,
            PipelineStageType.FILTER
        )

    def map(self, transform: Callable[[T], U]) -> "DataPipelineAction":
        """Add a map stage."""
        return self.add_stage(
            f"map_{len(self._stages)}",
            transform,
            PipelineStageType.MAP
        )

    def flatmap(self, func: Callable[[T], List[U]]) -> "DataPipelineAction":
        """Add a flatmap stage."""
        return self.add_stage(
            f"flatmap_{len(self._stages)}",
            func,
            PipelineStageType.FLATMAP
        )

    def reduce(
        self,
        func: Callable[[U, T], U],
        initial: Optional[U] = None
    ) -> "DataPipelineAction":
        """Add a reduce stage."""
        return self.add_stage(
            f"reduce_{len(self._stages)}",
            lambda data: func(initial, data) if initial is not None else data,
            PipelineStageType.REDUCE
        )

    async def execute(self, data: Any) -> PipelineResult:
        """Execute pipeline on data.

        Args:
            data: Input data (list, dict, or single item)

        Returns:
            PipelineResult with output and statistics
        """
        result = data
        errors: List[Dict[str, Any]] = []
        completed: List[str] = []

        for stage in self._stages:
            try:
                result = await self._execute_stage(stage, result)
                completed.append(stage.name)
                self._stats[stage.name]["processed"] += 1
            except Exception as e:
                self._stats[stage.name]["errors"] += 1

                if stage.error_handler:
                    result = stage.error_handler(e, result)
                    completed.append(stage.name)
                elif stage.skip_on_error:
                    self._stats[stage.name]["skipped"] += 1
                    completed.append(stage.name)
                else:
                    errors.append({
                        "stage": stage.name,
                        "error": str(e),
                        "type": type(e).__name__,
                    })
                    return PipelineResult(
                        success=False,
                        data=result,
                        errors=errors,
                        stages_completed=completed,
                    )

        return PipelineResult(
            success=True,
            data=result,
            errors=errors,
            stages_completed=completed,
        )

    async def execute_parallel(
        self,
        data: List[T],
        max_concurrency: int = 10,
    ) -> PipelineResult:
        """Execute pipeline on list of items in parallel.

        Args:
            data: List of items to process
            max_concurrency: Maximum concurrent tasks

        Returns:
            PipelineResult with aggregated results
        """
        semaphore = asyncio.Semaphore(max_concurrency)

        async def process_item(item: T) -> Any:
            async with semaphore:
                result = await self.execute(item)
                return result.data if result.success else None

        results = await asyncio.gather(
            *[process_item(item) for item in data],
            return_exceptions=True
        )

        valid_results = [r for r in results if r is not None and not isinstance(r, Exception)]

        return PipelineResult(
            success=len(valid_results) == len(data),
            data=valid_results,
            errors=[],
            stages_completed=[s.name for s in self._stages],
        )

    async def _execute_stage(self, stage: PipelineStage, data: Any) -> Any:
        """Execute a single pipeline stage."""
        if stage.stage_type == PipelineStageType.FILTER:
            if asyncio.iscoroutinefunction(stage.func):
                return await stage.func(data)
            return [item for item in data if stage.func(item)]

        elif stage.stage_type == PipelineStageType.MAP:
            if asyncio.iscoroutinefunction(stage.func):
                return await stage.func(data)
            return stage.func(data)

        elif stage.stage_type == PipelineStageType.FLATMAP:
            if asyncio.iscoroutinefunction(stage.func):
                result = await stage.func(data)
            else:
                result = stage.func(data)
            return [item for sublist in result for item in sublist]

        elif stage.stage_type == PipelineStageType.REDUCE:
            if asyncio.iscoroutinefunction(stage.func):
                return await stage.func(data)
            return stage.func(data)

        return data

    def get_stats(self) -> Dict[str, Any]:
        """Get pipeline statistics."""
        return {
            "name": self.name,
            "total_stages": len(self._stages),
            "stages": dict(self._stats),
        }

    def clear(self) -> None:
        """Clear all stages and stats."""
        self._stages.clear()
        self._stats.clear()
