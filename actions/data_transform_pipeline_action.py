"""
Data Transform Pipeline Action Module.

Pipeline for chained data transformations,
supports mapping, filtering, aggregation, and custom transforms.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Union
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class TransformStage:
    """Single transformation stage."""
    name: str
    func: Callable[[Any], Any]
    skip_on_error: bool = False


class DataTransformPipelineAction:
    """
    Data transformation pipeline with composable stages.

    Chains together map, filter, reduce, and custom
    transformations for batch data processing.

    Example:
        pipeline = DataTransformPipelineAction()
        pipeline.map(normalize_fields)
        pipeline.filter(drop_nulls)
        pipeline.aggregate(compute_stats)
        result = pipeline.run(raw_data)
    """

    def __init__(self, name: str = "transform") -> None:
        self.name = name
        self._stages: list[TransformStage] = []

    def map(
        self,
        func: Callable[[dict], dict],
        name: Optional[str] = None,
        skip_on_error: bool = False,
    ) -> "DataTransformPipelineAction":
        """Add a map transformation stage."""
        stage_name = name or f"map_{len(self._stages)}"
        self._stages.append(TransformStage(
            name=stage_name,
            func=func,
            skip_on_error=skip_on_error,
        ))
        return self

    def filter(
        self,
        predicate: Callable[[dict], bool],
        name: Optional[str] = None,
    ) -> "DataTransformPipelineAction":
        """Add a filter stage."""
        def filter_func(data: list) -> list:
            return [item for item in data if predicate(item)]

        stage_name = name or f"filter_{len(self._stages)}"
        self._stages.append(TransformStage(
            name=stage_name,
            func=filter_func,
        ))
        return self

    def aggregate(
        self,
        func: Callable[[list], Any],
        name: Optional[str] = None,
    ) -> "DataTransformPipelineAction":
        """Add an aggregation stage."""
        stage_name = name or f"aggregate_{len(self._stages)}"
        self._stages.append(TransformStage(
            name=stage_name,
            func=func,
        ))
        return self

    def run(
        self,
        data: Any,
        stop_on_error: bool = True,
    ) -> Any:
        """Execute the pipeline on data."""
        current = data

        for stage in self._stages:
            try:
                current = stage.func(current)
            except Exception as e:
                if stop_on_error and not stage.skip_on_error:
                    logger.error("Pipeline stage '%s' failed: %s", stage.name, e)
                    raise
                else:
                    logger.warning("Pipeline stage '%s' failed, skipping: %s", stage.name, e)

        return current

    def run_batch(
        self,
        items: list,
        stop_on_error: bool = True,
    ) -> list:
        """Run pipeline on each item in a batch."""
        results = []

        for item in items:
            try:
                result = self.run(item, stop_on_error=stop_on_error)
                results.append(result)
            except Exception as e:
                if stop_on_error:
                    raise
                logger.warning("Batch item failed: %s", e)

        return results

    def clear(self) -> None:
        """Remove all stages."""
        self._stages.clear()

    def get_stage_names(self) -> list[str]:
        """Get names of all stages in order."""
        return [s.name for s in self._stages]
