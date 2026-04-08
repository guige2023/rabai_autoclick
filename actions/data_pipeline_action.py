"""Data Pipeline Action Module.

Provides data processing pipeline capabilities including filtering,
transformation, aggregation, and parallel processing stages.
"""

import sys
import os
import json
import time
from typing import Any, Dict, List, Optional, Callable, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PipelineStageType(Enum):
    """Pipeline stage types."""
    FILTER = "filter"
    TRANSFORM = "transform"
    MAP = "map"
    REDUCE = "reduce"
    AGGREGATE = "aggregate"
    SPLIT = "split"
    MERGE = "merge"
    VALIDATE = "validate"


@dataclass
class PipelineStage:
    """Represents a single stage in the pipeline."""
    name: str
    stage_type: PipelineStageType
    func: Callable
    args: Dict[str, Any] = field(default_factory=dict)
    error_handler: Optional[Callable] = None
    skip_on_error: bool = False
    enabled: bool = True


class DataPipelineAction(BaseAction):
    """Execute a multi-stage data processing pipeline.
    
    Supports sequential stages, parallel execution, error handling,
    and pipeline introspection.
    """
    action_type = "data_pipeline"
    display_name = "数据流水线"
    description = "执行多阶段数据处理，支持并行和错误处理"

    def __init__(self):
        super().__init__()
        self._stages: List[PipelineStage] = []
        self._execution_log: List[Dict] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute the data pipeline.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - input_data: Initial data to process.
                - stages: List of stage definitions.
                - parallel: Whether to run independent stages in parallel.
                - max_workers: Max parallel workers.
                - stop_on_error: Stop pipeline on first error.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with pipeline result or error.
        """
        input_data = params.get('input_data', [])
        stage_defs = params.get('stages', [])
        parallel = params.get('parallel', False)
        max_workers = params.get('max_workers', 4)
        stop_on_error = params.get('stop_on_error', True)
        output_var = params.get('output_var', 'pipeline_result')

        if not isinstance(input_data, (list, dict)):
            return ActionResult(
                success=False,
                message=f"input_data must be list or dict, got {type(input_data).__name__}"
            )

        try:
            # Build stages from definitions
            self._stages = self._build_stages(stage_defs, context)

            # Convert single item to list for uniform processing
            if isinstance(input_data, dict):
                input_data = [input_data]

            current_data = input_data
            pipeline_start = time.time()
            self._execution_log = []

            # Execute stages
            for stage in self._stages:
                if not stage.enabled:
                    continue

                stage_start = time.time()
                try:
                    if parallel and self._can_parallelize(stage):
                        current_data = self._execute_parallel(
                            current_data, stage, max_workers
                        )
                    else:
                        current_data = stage.func(current_data, **stage.args)

                    stage_duration = time.time() - stage_start
                    self._execution_log.append({
                        'stage': stage.name,
                        'type': stage.stage_type.value,
                        'duration': stage_duration,
                        'input_count': len(input_data) if isinstance(input_data, list) else 1,
                        'output_count': len(current_data) if isinstance(current_data, list) else 1,
                        'success': True
                    })

                except Exception as e:
                    stage_duration = time.time() - stage_start
                    self._execution_log.append({
                        'stage': stage.name,
                        'type': stage.stage_type.value,
                        'duration': stage_duration,
                        'error': str(e),
                        'success': False
                    })

                    if stage.error_handler:
                        try:
                            current_data = stage.error_handler(current_data, e)
                        except Exception:
                            if stop_on_error:
                                raise
                    elif stop_on_error:
                        raise

            pipeline_duration = time.time() - pipeline_start

            result = {
                'data': current_data,
                'stages_executed': len([s for s in self._execution_log if s.get('success')]),
                'total_duration': pipeline_duration,
                'log': self._execution_log
            }

            context.variables[output_var] = result
            return ActionResult(
                success=True,
                data=result,
                message=f"Pipeline completed: {len(self._stages)} stages in {pipeline_duration:.3f}s"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Pipeline execution failed: {str(e)}"
            )

    def _build_stages(self, stage_defs: List[Dict], context: Any) -> List[PipelineStage]:
        """Build pipeline stages from definitions."""
        stages = []
        for defn in stage_defs:
            stage_type = PipelineStageType(defn.get('type', 'transform'))
            func = self._resolve_func(defn.get('func', ''), context)
            stages.append(PipelineStage(
                name=defn.get('name', f"stage_{len(stages)}"),
                stage_type=stage_type,
                func=func or self._default_transform,
                args=defn.get('args', {}),
                skip_on_error=defn.get('skip_on_error', False),
                enabled=defn.get('enabled', True)
            ))
        return stages

    def _resolve_func(self, func_ref: str, context: Any) -> Optional[Callable]:
        """Resolve a function reference."""
        if not func_ref:
            return None
        if callable(func_ref):
            return func_ref

        parts = func_ref.split('.')
        if len(parts) >= 2:
            try:
                import importlib
                module = importlib.import_module('.'.join(parts[:-1]))
                return getattr(module, parts[-1], None)
            except ImportError:
                return None
        return None

    def _default_transform(self, data: Any, **kwargs) -> Any:
        """Default transform if no function specified."""
        return data

    def _can_parallelize(self, stage: PipelineStage) -> bool:
        """Check if a stage can be parallelized."""
        return stage.stage_type in [
            PipelineStageType.MAP,
            PipelineStageType.FILTER
        ]

    def _execute_parallel(
        self, data: List, stage: PipelineStage, max_workers: int
    ) -> List:
        """Execute a stage in parallel."""
        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(stage.func, item, **stage.args): i
                for i, item in enumerate(data)
            }
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    if stage.skip_on_error:
                        continue
                    raise
        return results


class PipelineFilterStage:
    """Filter stage for data pipeline."""

    @staticmethod
    def gt(field: str, threshold: float) -> Callable:
        """Filter where field > threshold."""
        def filter_fn(data: List[Dict]) -> List[Dict]:
            return [item for item in data if item.get(field, 0) > threshold]
        return filter_fn

    @staticmethod
    def lt(field: str, threshold: float) -> Callable:
        """Filter where field < threshold."""
        def filter_fn(data: List[Dict]) -> List[Dict]:
            return [item for item in data if item.get(field, float('inf')) < threshold]
        return filter_fn

    @staticmethod
    def eq(field: str, value: Any) -> Callable:
        """Filter where field == value."""
        def filter_fn(data: List[Dict]) -> List[Dict]:
            return [item for item in data if item.get(field) == value]
        return filter_fn

    @staticmethod
    def contains(field: str, substring: str) -> Callable:
        """Filter where field contains substring."""
        def filter_fn(data: List[Dict]) -> List[Dict]:
            return [item for item in data if substring in str(item.get(field, ''))]
        return filter_fn

    @staticmethod
    def in_list(field: str, values: List[Any]) -> Callable:
        """Filter where field in list."""
        def filter_fn(data: List[Dict]) -> List[Dict]:
            return [item for item in data if item.get(field) in values]
        return filter_fn


class PipelineTransformStage:
    """Transform stage utilities for data pipeline."""

    @staticmethod
    def rename_field(old_name: str, new_name: str) -> Callable:
        """Rename a field in each item."""
        def transform_fn(data: List[Dict]) -> List[Dict]:
            result = []
            for item in data:
                new_item = {k: v for k, v in item.items()}
                if old_name in new_item:
                    new_item[new_name] = new_item.pop(old_name)
                result.append(new_item)
            return result
        return transform_fn

    @staticmethod
    def add_field(field: str, value: Any) -> Callable:
        """Add a computed field to each item."""
        def transform_fn(data: List[Dict]) -> List[Dict]:
            for item in data:
                item[field] = value
            return data
        return transform_fn

    @staticmethod
    def compute_field(field: str, func: Callable) -> Callable:
        """Add a computed field using a function."""
        def transform_fn(data: List[Dict]) -> List[Dict]:
            for item in data:
                item[field] = func(item)
            return data
        return transform_fn

    @staticmethod
    def flatten(nested_field: str, separator: str = '.') -> Callable:
        """Flatten a nested dictionary field."""
        def transform_fn(data: List[Dict]) -> List[Dict]:
            result = []
            for item in data:
                new_item = dict(item)
                nested = item.get(nested_field, {})
                if isinstance(nested, dict):
                    for k, v in nested.items():
                        new_item[f"{nested_field}{separator}{k}"] = v
                result.append(new_item)
            return result
        return transform_fn

    @staticmethod
    def select_fields(fields: List[str]) -> Callable:
        """Select only specified fields from each item."""
        def transform_fn(data: List[Dict]) -> List[Dict]:
            return [{f: item.get(f) for f in fields} for item in data]
        return transform_fn


class PipelineAggregator:
    """Aggregator utilities for data pipeline reduce stage."""

    @staticmethod
    def sum(field: str) -> Callable:
        """Sum values of a field."""
        def reduce_fn(data: List[Dict]) -> Dict:
            return {'sum': sum(item.get(field, 0) for item in data)}
        return reduce_fn

    @staticmethod
    def average(field: str) -> Callable:
        """Calculate average of a field."""
        def reduce_fn(data: List[Dict]) -> Dict:
            values = [item.get(field, 0) for item in data]
            return {'average': sum(values) / len(values) if values else 0}
        return reduce_fn

    @staticmethod
    def count() -> Callable:
        """Count items."""
        def reduce_fn(data: List[Dict]) -> Dict:
            return {'count': len(data)}
        return reduce_fn

    @staticmethod
    def group_by(field: str) -> Callable:
        """Group items by field value."""
        def reduce_fn(data: List[Dict]) -> Dict:
            groups: Dict[str, List] = {}
            for item in data:
                key = str(item.get(field, 'unknown'))
                if key not in groups:
                    groups[key] = []
                groups[key].append(item)
            return {'groups': groups, 'group_count': len(groups)}
        return reduce_fn

    @staticmethod
    def statistics(fields: List[str]) -> Callable:
        """Calculate statistics for multiple fields."""
        def reduce_fn(data: List[Dict]) -> Dict:
            import statistics
            result = {'count': len(data)}
            for field in fields:
                values = [item.get(field, 0) for item in data if field in item]
                if values:
                    result[f"{field}_min"] = min(values)
                    result[f"{field}_max"] = max(values)
                    result[f"{field}_avg"] = statistics.mean(values)
                    result[f"{field}_median"] = statistics.median(values)
            return result
        return reduce_fn
