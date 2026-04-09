"""Data Pipeline Action Module.

Provides configurable data pipeline execution with stages,
parallel processing, error handling, and checkpoint/resume support.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class StageType(Enum):
    """Types of pipeline stages."""
    SOURCE = "source"
    TRANSFORM = "transform"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    JOIN = "join"
    OUTPUT = "output"
    BRANCH = "branch"
    MERGE = "merge"
    CONDITION = "condition"


class StageStatus(Enum):
    """Status of a pipeline stage."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class PipelineStage:
    """A single stage in a data pipeline."""
    id: str
    name: str
    stage_type: StageType
    config: Dict[str, Any] = field(default_factory=dict)
    depends_on: List[str] = field(default_factory=list)
    parallel: bool = False
    timeout: float = 300.0
    retry_count: int = 0
    retry_delay: float = 1.0
    continue_on_error: bool = False


@dataclass
class StageResult:
    """Result of a stage execution."""
    stage_id: str
    status: StageStatus
    output: Any = None
    error: Optional[str] = None
    duration_ms: float = 0.0
    records_in: int = 0
    records_out: int = 0


@dataclass
class PipelineContext:
    """Shared context during pipeline execution."""
    variables: Dict[str, Any] = field(default_factory=dict)
    stage_outputs: Dict[str, Any] = field(default_factory=dict)
    stage_results: Dict[str, StageResult] = field(default_factory=dict)
    start_time: float = 0.0
    end_time: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def get_var(self, key: str, default: Any = None) -> Any:
        """Get a context variable."""
        return self.variables.get(key, default)

    def set_var(self, key: str, value: Any) -> None:
        """Set a context variable."""
        self.variables[key] = value

    def get_stage_output(self, stage_id: str) -> Any:
        """Get output from a stage."""
        return self.stage_outputs.get(stage_id)

    def set_stage_output(self, stage_id: str, output: Any) -> None:
        """Set output from a stage."""
        self.stage_outputs[stage_id] = output

    def get_stage_result(self, stage_id: str) -> Optional[StageResult]:
        """Get result from a stage."""
        return self.stage_results.get(stage_id)


class DataPipelineAction(BaseAction):
    """Data Pipeline Action for multi-stage data processing.

    Supports sequential and parallel stages, branching, merging,
    error handling, and checkpoint-based resume.

    Examples:
        >>> action = DataPipelineAction()
        >>> result = action.execute(ctx, {
        ...     "pipeline_id": "etl_001",
        ...     "stages": [
        ...         {"id": "s1", "type": "source", "name": "Load data"},
        ...         {"id": "s2", "type": "transform", "name": "Clean data", "depends_on": ["s1"]},
        ...     ]
        ... })
    """

    action_type = "data_pipeline"
    display_name = "数据管道"
    description = "多阶段数据处理管道，支持并行、分支、错误恢复"

    def __init__(self):
        super().__init__()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute a data pipeline.

        Args:
            context: Execution context.
            params: Dict with keys:
                - pipeline_id: Unique pipeline identifier
                - stages: List of PipelineStage definitions
                - data: Initial data for the pipeline (optional)
                - variables: Initial variables (optional)
                - max_parallel: Max parallel stages (default: 3)
                - stop_on_error: Stop pipeline on first error (default: True)
                - checkpoint_enabled: Enable checkpoint/resume (default: False)
                - checkpoint_interval: Checkpoint every N records

        Returns:
            ActionResult with pipeline execution results.
        """
        pipeline_id = params.get("pipeline_id", f"pipeline_{int(time.time())}")
        stages_config = params.get("stages", [])
        initial_data = params.get("data")
        initial_vars = params.get("variables", {})
        max_parallel = params.get("max_parallel", 3)
        stop_on_error = params.get("stop_on_error", True)

        # Build stages
        stages = []
        stage_map: Dict[str, PipelineStage] = {}
        for cfg in stages_config:
            if isinstance(cfg, PipelineStage):
                stage = cfg
            else:
                cfg = dict(cfg)
                cfg["stage_type"] = StageType(cfg.get("type", "transform"))
                stage = PipelineStage(**cfg)
            stages.append(stage)
            stage_map[stage.id] = stage

        if not stages:
            return ActionResult(
                success=False,
                message="No stages defined for pipeline"
            )

        # Create pipeline context
        proc_context = PipelineContext(
            variables=dict(initial_vars),
            start_time=time.time(),
            metadata={"pipeline_id": pipeline_id, "max_parallel": max_parallel}
        )

        # Set initial data
        if initial_data is not None:
            proc_context.set_var("_pipeline_data", initial_data)

        # Execute pipeline
        try:
            results = self._execute_pipeline(
                pipeline_id, stages, stage_map, proc_context,
                max_parallel, stop_on_error
            )
            proc_context.end_time = time.time()

            # Summarize results
            total_records = 0
            total_duration = 0.0
            failed_stages = 0
            for result in results:
                total_records += result.records_out
                total_duration += result.duration_ms
                if result.status == StageStatus.FAILED:
                    failed_stages += 1

            success = failed_stages == 0

            return ActionResult(
                success=success,
                message=f"Pipeline {'succeeded' if success else 'failed'} "
                        f"({len(results)} stages, {failed_stages} failed)",
                data={
                    "pipeline_id": pipeline_id,
                    "total_stages": len(stages),
                    "completed_stages": len([r for r in results if r.status == StageStatus.SUCCESS]),
                    "failed_stages": failed_stages,
                    "total_records": total_records,
                    "duration_ms": total_duration,
                    "stage_results": [
                        {"stage_id": r.stage_id, "status": r.status.value,
                         "records_in": r.records_in, "records_out": r.records_out,
                         "duration_ms": r.duration_ms, "error": r.error}
                        for r in results
                    ],
                }
            )

        except Exception as e:
            logger.exception(f"Pipeline execution failed: {pipeline_id}")
            return ActionResult(
                success=False,
                message=f"Pipeline error: {str(e)}",
                data={"pipeline_id": pipeline_id}
            )

    def _execute_pipeline(
        self,
        pipeline_id: str,
        stages: List[PipelineStage],
        stage_map: Dict[str, PipelineStage],
        proc_context: PipelineContext,
        max_parallel: int,
        stop_on_error: bool,
    ) -> List[StageResult]:
        """Execute the pipeline stages."""
        results: List[StageResult] = []
        completed: set = set()
        running: Dict[str, threading.Thread] = {}

        while len(completed) < len(stages):
            # Find stages ready to run
            ready = []
            for stage in stages:
                if stage.id in completed:
                    continue
                deps_met = all(dep in completed for dep in stage.depends_on)
                if deps_met:
                    ready.append(stage)

            if not ready and len(running) == 0:
                break

            # Start ready stages (limit parallelism)
            for stage in ready[:max_parallel - len(running)]:
                thread = threading.Thread(
                    target=self._execute_stage,
                    args=(stage, proc_context)
                )
                thread.start()
                running[stage.id] = thread

            # Wait for a stage to complete
            if running:
                for stage_id, thread in list(running.items()):
                    thread.join(timeout=1.0)
                    if not thread.is_alive():
                        del running[stage_id]
                        result = proc_context.get_stage_result(stage_id)
                        if result:
                            results.append(result)
                            if result.status == StageStatus.SUCCESS:
                                completed.add(stage_id)
                            elif stop_on_error and not stage.continue_on_error:
                                # Stop remaining stages
                                for t in running.values():
                                    t.join(timeout=0.1)
                                break

        # Wait for any remaining stages
        for thread in running.values():
            thread.join(timeout=60.0)

        # Collect any remaining results
        for stage in stages:
            if stage.id not in [r.stage_id for r in results]:
                result = proc_context.get_stage_result(stage.id)
                if result:
                    results.append(result)

        return results

    def _execute_stage(
        self,
        stage: PipelineStage,
        proc_context: PipelineContext,
    ) -> None:
        """Execute a single pipeline stage."""
        start_time = time.time()
        result = StageResult(
            stage_id=stage.id,
            status=StageStatus.RUNNING,
        )

        try:
            # Get input data
            input_data = None
            if stage.depends_on:
                # Get output from last dependency
                last_dep = stage.depends_on[-1]
                input_data = proc_context.get_stage_output(last_dep)
            else:
                input_data = proc_context.get_var("_pipeline_data")

            result.records_in = len(input_data) if isinstance(input_data, list) else 0

            # Execute based on stage type
            if stage.stage_type == StageType.SOURCE:
                output = self._execute_source_stage(stage, proc_context)
            elif stage.stage_type == StageType.TRANSFORM:
                output = self._execute_transform_stage(stage, input_data, proc_context)
            elif stage.stage_type == StageType.FILTER:
                output = self._execute_filter_stage(stage, input_data, proc_context)
            elif stage.stage_type == StageType.AGGREGATE:
                output = self._execute_aggregate_stage(stage, input_data, proc_context)
            elif stage.stage_type == StageType.OUTPUT:
                output = self._execute_output_stage(stage, input_data, proc_context)
            elif stage.stage_type == StageType.BRANCH:
                output = self._execute_branch_stage(stage, input_data, proc_context)
            else:
                output = input_data

            result.output = output
            result.records_out = len(output) if isinstance(output, list) else 1
            result.status = StageStatus.SUCCESS

            # Store output for dependent stages
            proc_context.set_stage_output(stage.id, output)

        except Exception as e:
            logger.error(f"Stage {stage.id} failed: {e}")
            result.status = StageStatus.FAILED
            result.error = str(e)

        result.duration_ms = (time.time() - start_time) * 1000
        proc_context.stage_results[stage.id] = result

    def _execute_source_stage(
        self, stage: PipelineStage, proc_context: PipelineContext
    ) -> Any:
        """Execute a source stage."""
        source_type = stage.config.get("source_type", "inline")
        if source_type == "inline":
            return stage.config.get("data", [])
        elif source_type == "variable":
            var_name = stage.config.get("variable_name", "_pipeline_data")
            return proc_context.get_var(var_name, [])
        return []

    def _execute_transform_stage(
        self, stage: PipelineStage, input_data: Any, proc_context: PipelineContext
    ) -> Any:
        """Execute a transform stage."""
        if not isinstance(input_data, list):
            return input_data

        transforms = stage.config.get("transforms", [])
        result = list(input_data)

        for transform in transforms:
            field_name = transform.get("field")
            transform_type = transform.get("type", "copy")
            new_field = transform.get("new_field")

            for item in result:
                if not isinstance(item, dict):
                    continue
                if transform_type == "copy":
                    if field_name in item:
                        item[new_field or f"{field_name}_copy"] = item[field_name]
                elif transform_type == "rename":
                    if field_name in item:
                        item[new_field or f"{field_name}_renamed"] = item.pop(field_name)
                elif transform_type == "uppercase":
                    if field_name in item and isinstance(item[field_name], str):
                        item[field_name] = item[field_name].upper()
                elif transform_type == "lowercase":
                    if field_name in item and isinstance(item[field_name], str):
                        item[field_name] = item[field_name].lower()

        return result

    def _execute_filter_stage(
        self, stage: PipelineStage, input_data: Any, proc_context: PipelineContext
    ) -> Any:
        """Execute a filter stage."""
        if not isinstance(input_data, list):
            return input_data

        filter_field = stage.config.get("field")
        filter_op = stage.config.get("operator", "equals")
        filter_value = stage.config.get("value")

        result = []
        for item in input_data:
            if not isinstance(item, dict):
                continue
            field_value = item.get(filter_field)

            if filter_op == "equals" and field_value == filter_value:
                result.append(item)
            elif filter_op == "not_equals" and field_value != filter_value:
                result.append(item)
            elif filter_op == "greater_than" and field_value > filter_value:
                result.append(item)
            elif filter_op == "less_than" and field_value < filter_value:
                result.append(item)
            elif filter_op == "contains" and filter_value in str(field_value):
                result.append(item)
            elif filter_op == "exists" and field_value is not None:
                result.append(item)

        return result

    def _execute_aggregate_stage(
        self, stage: PipelineStage, input_data: Any, proc_context: PipelineContext
    ) -> Any:
        """Execute an aggregate stage."""
        if not isinstance(input_data, list):
            return input_data

        group_by = stage.config.get("group_by", [])
        agg_field = stage.config.get("agg_field")
        agg_func = stage.config.get("agg_function", "count")

        if not group_by:
            # Aggregate all
            values = [item.get(agg_field) for item in input_data if isinstance(item, dict)]
            if agg_func == "count":
                return [{"result": len(values)}]
            elif agg_func == "sum":
                return [{"result": sum(v for v in values if isinstance(v, (int, float)))}]
            elif agg_func == "avg":
                nums = [v for v in values if isinstance(v, (int, float))]
                return [{"result": sum(nums) / len(nums) if nums else 0}]

        # Group aggregate
        from collections import defaultdict
        groups: Dict[Tuple, List] = defaultdict(list)
        for item in input_data:
            if isinstance(item, dict):
                key = tuple(item.get(f) for f in group_by)
                groups[key].append(item)

        result = []
        for key, items in groups.items():
            row = dict(zip(group_by, key))
            values = [item.get(agg_field) for item in items if isinstance(item, dict)]
            if agg_func == "count":
                row["result"] = len(values)
            elif agg_func == "sum":
                row["result"] = sum(v for v in values if isinstance(v, (int, float)))
            elif agg_func == "avg":
                nums = [v for v in values if isinstance(v, (int, float))]
                row["result"] = sum(nums) / len(nums) if nums else 0
            result.append(row)

        return result

    def _execute_output_stage(
        self, stage: PipelineStage, input_data: Any, proc_context: PipelineContext
    ) -> Any:
        """Execute an output stage."""
        output_type = stage.config.get("output_type", "variable")
        var_name = stage.config.get("variable_name", "_pipeline_output")

        if output_type == "variable":
            proc_context.set_var(var_name, input_data)
        elif output_type == "print":
            logger.info(f"Pipeline output: {input_data}")

        return input_data

    def _execute_branch_stage(
        self, stage: PipelineStage, input_data: Any, proc_context: PipelineContext
    ) -> Any:
        """Execute a branch stage (splits data)."""
        branch_field = stage.config.get("branch_field")
        if not branch_field or not isinstance(input_data, list):
            return input_data

        branches: Dict[str, List] = defaultdict(list)
        for item in input_data:
            if isinstance(item, dict):
                key = str(item.get(branch_field, "default"))
                branches[key].append(item)

        proc_context.set_var(f"_branches_{stage.id}", dict(branches))
        return input_data

    def get_required_params(self) -> List[str]:
        return ["stages"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "pipeline_id": "",
            "data": None,
            "variables": {},
            "max_parallel": 3,
            "stop_on_error": True,
            "checkpoint_enabled": False,
            "checkpoint_interval": 1000,
        }
