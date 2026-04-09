"""
Data Pipeline Action Module.

Provides composable data processing pipelines with
filtering, transformation, and aggregation stages.

Author: rabai_autoclick team
"""

import logging
from typing import (
    Optional, Dict, Any, List, Callable, Union,
    TypeVar, Generic, Awaitable
)
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


class StageType(Enum):
    """Pipeline stage types."""
    SOURCE = "source"
    FILTER = "filter"
    MAP = "map"
    FLATMAP = "flatmap"
    REDUCE = "reduce"
    AGGREGATE = "aggregate"
    SORT = "sort"
    GROUP = "group"
    LIMIT = "limit"
    SKIP = "skip"
    DEDUP = "dedup"
    BRANCH = "branch"
    MERGE = "merge"


@dataclass
class PipelineStage:
    """A single pipeline stage."""
    stage_type: StageType
    func: Callable
    name: Optional[str] = None
    config: Dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        return f"PipelineStage({self.stage_type.value}, name={self.name})"


@dataclass
class PipelineConfig:
    """Configuration for pipeline execution."""
    name: str = "pipeline"
    parallel: bool = False
    max_workers: int = 4
    buffer_size: int = 100
    error_mode: str = "stop"
    log_level: str = "INFO"


@dataclass
class PipelineResult:
    """Result of pipeline execution."""
    success: bool
    output: Any = None
    error: Optional[str] = None
    processed_count: int = 0
    skipped_count: int = 0
    error_count: int = 0


class DataPipelineAction:
    """
    Composable Data Processing Pipeline.

    Provides a fluent API for building data processing
    pipelines with various transformation stages.

    Example:
        >>> pipeline = DataPipelineAction()
        >>> result = (pipeline
        ...     .source(data)
        ...     .filter(lambda x: x["active"])
        ...     .map(lambda x: x["value"] * 2)
        ...     .execute())
    """

    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or PipelineConfig()
        self._stages: List[PipelineStage] = []
        self._source_data: Any = None

    def source(self, data: Any) -> "DataPipelineAction":
        """
        Set the data source.

        Args:
            data: Source data (list, generator, etc.)

        Returns:
            Self for chaining
        """
        self._source_data = data
        self._stages.append(
            PipelineStage(
                stage_type=StageType.SOURCE,
                func=lambda x: x,
                name="source",
            )
        )
        return self

    def filter(
        self,
        predicate: Callable[[Any], bool],
        name: Optional[str] = None,
    ) -> "DataPipelineAction":
        """
        Add a filter stage.

        Args:
            predicate: Function that returns True to keep item
            name: Optional stage name

        Returns:
            Self for chaining
        """
        self._stages.append(
            PipelineStage(
                stage_type=StageType.FILTER,
                func=predicate,
                name=name or f"filter_{len(self._stages)}",
            )
        )
        return self

    def map(
        self,
        transform: Callable[[Any], Any],
        name: Optional[str] = None,
    ) -> "DataPipelineAction":
        """
        Add a map (transform) stage.

        Args:
            transform: Transformation function
            name: Optional stage name

        Returns:
            Self for chaining
        """
        self._stages.append(
            PipelineStage(
                stage_type=StageType.MAP,
                func=transform,
                name=name or f"map_{len(self._stages)}",
            )
        )
        return self

    def flatmap(
        self,
        func: Callable[[Any], List[Any]],
        name: Optional[str] = None,
    ) -> "DataPipelineAction":
        """
        Add a flatmap stage (map + flatten).

        Args:
            func: Function that returns list of items
            name: Optional stage name

        Returns:
            Self for chaining
        """
        self._stages.append(
            PipelineStage(
                stage_type=StageType.FLATMAP,
                func=func,
                name=name or f"flatmap_{len(self._stages)}",
            )
        )
        return self

    def reduce(
        self,
        reducer: Callable[[Any, Any], Any],
        initial: Any = None,
        name: Optional[str] = None,
    ) -> "DataPipelineAction":
        """
        Add a reduce stage.

        Args:
            reducer: Reduction function (accumulator, item) -> new_accumulator
            initial: Initial accumulator value
            name: Optional stage name

        Returns:
            Self for chaining
        """
        self._stages.append(
            PipelineStage(
                stage_type=StageType.REDUCE,
                func=reducer,
                name=name or f"reduce_{len(self._stages)}",
                config={"initial": initial},
            )
        )
        return self

    def sort(
        self,
        key: Optional[Callable[[Any], Any]] = None,
        reverse: bool = False,
        name: Optional[str] = None,
    ) -> "DataPipelineAction":
        """
        Add a sort stage.

        Args:
            key: Optional sort key function
            reverse: Sort in descending order
            name: Optional stage name

        Returns:
            Self for chaining
        """
        self._stages.append(
            PipelineStage(
                stage_type=StageType.SORT,
                func=key,
                name=name or f"sort_{len(self._stages)}",
                config={"reverse": reverse},
            )
        )
        return self

    def group_by(
        self,
        key: Callable[[Any], Any],
        name: Optional[str] = None,
    ) -> "DataPipelineAction":
        """
        Add a group by stage.

        Args:
            key: Group key function
            name: Optional stage name

        Returns:
            Self for chaining
        """
        self._stages.append(
            PipelineStage(
                stage_type=StageType.GROUP,
                func=key,
                name=name or f"group_{len(self._stages)}",
            )
        )
        return self

    def limit(self, n: int, name: Optional[str] = None) -> "DataPipelineAction":
        """
        Add a limit stage.

        Args:
            n: Maximum number of items
            name: Optional stage name

        Returns:
            Self for chaining
        """
        self._stages.append(
            PipelineStage(
                stage_type=StageType.LIMIT,
                func=lambda x, n=n: x[:n],
                name=name or f"limit_{len(self._stages)}",
                config={"n": n},
            )
        )
        return self

    def skip(self, n: int, name: Optional[str] = None) -> "DataPipelineAction":
        """
        Add a skip stage.

        Args:
            n: Number of items to skip
            name: Optional stage name

        Returns:
            Self for chaining
        """
        self._stages.append(
            PipelineStage(
                stage_type=StageType.SKIP,
                func=lambda x, n=n: x[n:],
                name=name or f"skip_{len(self._stages)}",
                config={"n": n},
            )
        )
        return self

    def dedup(
        self,
        key: Optional[Callable[[Any], Any]] = None,
        name: Optional[str] = None,
    ) -> "DataPipelineAction":
        """
        Add a deduplication stage.

        Args:
            key: Optional key function for dedup
            name: Optional stage name

        Returns:
            Self for chaining
        """
        self._stages.append(
            PipelineStage(
                stage_type=StageType.DEDUP,
                func=key,
                name=name or f"dedup_{len(self._stages)}",
            )
        )
        return self

    def execute(self) -> PipelineResult:
        """
        Execute the pipeline.

        Returns:
            PipelineResult with output and statistics
        """
        result = PipelineResult(success=False)

        if self._source_data is None:
            result.error = "No source data set"
            return result

        try:
            data = self._source_data
            processed = 0
            skipped = 0
            errors = 0

            for stage in self._stages:
                if stage.stage_type == StageType.SOURCE:
                    continue

                try:
                    data, p, s = self._execute_stage(stage, data)
                    processed += p
                    skipped += s
                except Exception as e:
                    logger.error(f"Stage '{stage.name}' failed: {e}")
                    errors += 1
                    if self.config.error_mode == "stop":
                        result.error = f"Stage '{stage.name}': {str(e)}"
                        return result

            result.success = True
            result.output = data
            result.processed_count = processed
            result.skipped_count = skipped
            result.error_count = errors

        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            result.error = str(e)

        return result

    def _execute_stage(
        self,
        stage: PipelineStage,
        data: Any,
    ) -> tuple:
        """Execute a single pipeline stage."""
        processed = 0
        skipped = 0

        if stage.stage_type == StageType.FILTER:
            if isinstance(data, list):
                filtered = []
                for item in data:
                    try:
                        if stage.func(item):
                            filtered.append(item)
                            processed += 1
                        else:
                            skipped += 1
                    except Exception:
                        skipped += 1
                return filtered, processed, skipped
            return data, processed, skipped

        elif stage.stage_type == StageType.MAP:
            if isinstance(data, list):
                result = []
                for item in data:
                    try:
                        result.append(stage.func(item))
                        processed += 1
                    except Exception:
                        pass
                return result, processed, skipped
            return stage.func(data), processed, skipped

        elif stage.stage_type == StageType.FLATMAP:
            if isinstance(data, list):
                result = []
                for item in data:
                    try:
                        items = stage.func(item)
                        result.extend(items)
                        processed += len(items)
                    except Exception:
                        pass
                return result, processed, skipped
            return stage.func(data), processed, skipped

        elif stage.stage_type == StageType.REDUCE:
            initial = stage.config.get("initial")
            if isinstance(data, list):
                return functools.reduce(stage.func, data, initial), processed, skipped
            return data, processed, skipped

        elif stage.stage_type == StageType.SORT:
            if isinstance(data, list):
                reverse = stage.config.get("reverse", False)
                return sorted(data, key=stage.func, reverse=reverse), len(data), skipped
            return data, processed, skipped

        elif stage.stage_type == StageType.GROUP:
            if isinstance(data, list):
                groups: Dict[Any, List] = {}
                for item in data:
                    try:
                        key = stage.func(item)
                        if key not in groups:
                            groups[key] = []
                        groups[key].append(item)
                    except Exception:
                        pass
                return groups, len(data), skipped
            return data, processed, skipped

        elif stage.stage_type == StageType.LIMIT:
            n = stage.config.get("n", 0)
            if isinstance(data, list):
                return data[:n], min(len(data), n), skipped
            return data, processed, skipped

        elif stage.stage_type == StageType.SKIP:
            n = stage.config.get("n", 0)
            if isinstance(data, list):
                return data[n:], max(len(data) - n, 0), skipped
            return data, processed, skipped

        elif stage.stage_type == StageType.DEDUP:
            if isinstance(data, list):
                seen: Set = set()
                result = []
                for item in data:
                    key = stage.func(item) if stage.func else item
                    if key not in seen:
                        seen.add(key)
                        result.append(item)
                return result, len(result), len(data) - len(result)
            return data, processed, skipped

        else:
            if callable(stage.func):
                return stage.func(data), processed, skipped
            return data, processed, skipped

    def __repr__(self) -> str:
        stages = " -> ".join(s.name or s.stage_type.value for s in self._stages)
        return f"DataPipelineAction({stages})"


import functools
from collections import defaultdict
