"""Data Pipeline Action Module.

Provides configurable data processing pipelines with stages,
filters, transforms, aggregations, and error handling.
"""

from __future__ import annotations

import sys
import os
import time
import hashlib
from typing import Any, Callable, Dict, List, Optional, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


T = TypeVar("T")


class PipelineStageType(Enum):
    """Types of pipeline stages."""
    SOURCE = "source"
    FILTER = "filter"
    TRANSFORM = "transform"
    AGGREGATE = "aggregate"
    SPLIT = "split"
    MERGE = "merge"
    SINK = "sink"


class StageStatus(Enum):
    """Execution status of a pipeline stage."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class PipelineStage:
    """Definition of a pipeline stage."""
    stage_id: str
    stage_type: PipelineStageType
    name: str
    config: Dict[str, Any] = field(default_factory=dict)
    on_error: str = "skip"
    timeout: float = 60.0


@dataclass
class StageResult:
    """Result of a pipeline stage execution."""
    stage_id: str
    status: StageStatus
    records_in: int = 0
    records_out: int = 0
    duration: float = 0.0
    error: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineMetrics:
    """Overall pipeline execution metrics."""
    total_records: int = 0
    records_processed: int = 0
    records_filtered: int = 0
    records_failed: int = 0
    stages_executed: int = 0
    stages_failed: int = 0
    total_duration: float = 0.0


class DataPipelineAction(BaseAction):
    """
    Configurable data processing pipeline.

    Supports multi-stage pipelines with filtering, transformation,
    aggregation, branching, and error handling.

    Example:
        pipeline = DataPipelineAction()
        result = pipeline.execute(ctx, {
            "action": "run",
            "stages": [...],
            "data": [...]
        })
    """
    action_type = "data_pipeline"
    display_name = "数据流水线"
    description = "多阶段数据处理流水线，支持过滤、转换、聚合、分支和错误处理"

    def __init__(self) -> None:
        super().__init__()
        self._stage_handlers: Dict[PipelineStageType, Callable] = {
            PipelineStageType.SOURCE: self._handle_source,
            PipelineStageType.FILTER: self._handle_filter,
            PipelineStageType.TRANSFORM: self._handle_transform,
            PipelineStageType.AGGREGATE: self._handle_aggregate,
            PipelineStageType.SPLIT: self._handle_split,
            PipelineStageType.MERGE: self._handle_merge,
            PipelineStageType.SINK: self._handle_sink,
        }

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute a pipeline action.

        Args:
            context: Execution context.
            params: Dict with keys: action (run|validate|get_stage),
                   stages, data, options.

        Returns:
            ActionResult with pipeline execution result.
        """
        action = params.get("action", "")

        try:
            if action == "run":
                return self._run_pipeline(params)
            elif action == "validate":
                return self._validate_pipeline(params)
            elif action == "get_stage":
                return self._get_stage_info(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline error: {str(e)}")

    def _run_pipeline(self, params: Dict[str, Any]) -> ActionResult:
        """Run a data pipeline."""
        stages_def = params.get("stages", [])
        data = params.get("data", [])
        options = params.get("options", {})

        if not stages_def:
            return ActionResult(success=False, message="stages are required")

        stages = self._build_stages(stages_def)
        records = self._ensure_list(data)
        stage_results: Dict[str, StageResult] = {}
        metrics = PipelineMetrics(total_records=len(records))
        start_time = time.time()

        for stage in stages:
            if not records and stage.stage_type != PipelineStageType.SOURCE:
                result = StageResult(
                    stage_id=stage.stage_id,
                    status=StageStatus.SKIPPED,
                    records_in=0,
                    records_out=0,
                )
                stage_results[stage.stage_id] = result
                continue

            handler = self._stage_handlers.get(stage.stage_type, self._handle_generic)
            stage_start = time.time()

            try:
                new_records, result = handler(stage, records, params)

                if stage.config.get("output_var"):
                    params[stage.config["output_var"]] = new_records

                records = new_records
                result.duration = time.time() - stage_start
                stage_results[stage.stage_id] = result

                metrics.records_processed += result.records_out
                metrics.records_filtered += result.records_in - result.records_out
                metrics.stages_executed += 1

            except Exception as e:
                result = StageResult(
                    stage_id=stage.stage_id,
                    status=StageStatus.FAILED,
                    records_in=len(records),
                    records_out=0,
                    duration=time.time() - stage_start,
                    error=str(e),
                )
                stage_results[stage.stage_id] = result
                metrics.stages_failed += 1

                if stage.on_error == "fail":
                    break
                elif stage.on_error == "skip":
                    continue
                else:
                    records = []

        metrics.total_duration = time.time() - start_time

        return ActionResult(
            success=metrics.stages_failed == 0,
            message=f"Pipeline completed: {metrics.records_processed}/{metrics.total_records} records",
            data={
                "metrics": {
                    "total_records": metrics.total_records,
                    "records_processed": metrics.records_processed,
                    "records_filtered": metrics.records_filtered,
                    "stages_executed": metrics.stages_executed,
                    "stages_failed": metrics.stages_failed,
                    "duration": metrics.total_duration,
                },
                "output": records,
                "stage_results": {
                    k: {"status": v.status.value, "records_in": v.records_in, "records_out": v.records_out}
                    for k, v in stage_results.items()
                }
            }
        )

    def _handle_source(self, stage: PipelineStage, records: List[Any], params: Dict[str, Any]) -> tuple[List[Any], StageResult]:
        """Handle source stage that produces records."""
        source_type = stage.config.get("type", "inline")
        stage_records: List[Any] = []

        if source_type == "inline":
            stage_records = self._ensure_list(stage.config.get("data", []))
        elif source_type == "sequence":
            start = stage.config.get("start", 0)
            end = stage.config.get("end", 10)
            stage_records = list(range(start, end))
        elif source_type == "repeat":
            value = stage.config.get("value", None)
            count = stage.config.get("count", 1)
            stage_records = [value] * count
        elif source_type == "generator":
            generator_func = stage.config.get("func")
            if generator_func:
                count = stage.config.get("count", 10)
                stage_records = [generator_func(i) for i in range(count)]

        result = StageResult(
            stage_id=stage.stage_id,
            status=StageStatus.SUCCESS,
            records_in=0,
            records_out=len(stage_records),
        )

        return stage_records, result

    def _handle_filter(self, stage: PipelineStage, records: List[Any], params: Dict[str, Any]) -> tuple[List[Any], StageResult]:
        """Handle filter stage that removes records."""
        filter_type = stage.config.get("type", "expression")
        filtered: List[Any] = []
        filtered_count = 0

        if filter_type == "expression":
            expression = stage.config.get("expression", "True")
            for record in records:
                try:
                    record_copy = record.copy() if isinstance(record, dict) else {"value": record}
                    if eval(expression, {"__builtins__": {}}, {"record": record_copy}):
                        filtered.append(record)
                    else:
                        filtered_count += 1
                except Exception:
                    filtered.append(record)

        elif filter_type == "condition":
            field_name = stage.config.get("field")
            operator = stage.config.get("operator", "eq")
            value = stage.config.get("value")

            for record in records:
                if isinstance(record, dict):
                    field_value = record.get(field_name)
                    if self._evaluate_operator(field_value, operator, value):
                        filtered.append(record)
                    else:
                        filtered_count += 1
                else:
                    filtered.append(record)

        elif filter_type == "unique":
            seen = set()
            seen_add = seen.add
            for record in records:
                key = str(record.get(stage.config.get("key", "id"), record))
                if key not in seen:
                    seen_add(key)
                    filtered.append(record)
                else:
                    filtered_count += 1

        elif filter_type == "limit":
            limit = stage.config.get("limit", len(records))
            offset = stage.config.get("offset", 0)
            filtered = records[offset:offset + limit]

        result = StageResult(
            stage_id=stage.stage_id,
            status=StageStatus.SUCCESS,
            records_in=len(records),
            records_out=len(filtered),
            metrics={"filtered_count": filtered_count},
        )

        return filtered, result

    def _handle_transform(self, stage: PipelineStage, records: List[Any], params: Dict[str, Any]) -> tuple[List[Any], StageResult]:
        """Handle transform stage that modifies records."""
        transform_type = stage.config.get("type", "map")
        transformed: List[Any] = []

        if transform_type == "map":
            field_mappings = stage.config.get("mappings", {})
            for record in records:
                if isinstance(record, dict):
                    new_record = record.copy()
                    for old_field, new_field in field_mappings.items():
                        if old_field in new_record:
                            new_record[new_field] = new_record.pop(old_field)
                    transformed.append(new_record)
                else:
                    transformed.append(record)

        elif transform_type == "rename":
            rename_map = stage.config.get("rename", {})
            for record in records:
                if isinstance(record, dict):
                    new_record = {}
                    for key, value in record.items():
                        new_key = rename_map.get(key, key)
                        new_record[new_key] = value
                    transformed.append(new_record)
                else:
                    transformed.append(record)

        elif transform_type == "add_field":
            field_name = stage.config.get("field")
            field_value = stage.config.get("value")
            for record in records:
                new_record = record.copy() if isinstance(record, dict) else {"value": record}
                if callable(field_value):
                    new_record[field_name] = field_value(record)
                else:
                    new_record[field_name] = field_value
                transformed.append(new_record)

        elif transform_type == "remove_field":
            field_name = stage.config.get("field")
            for record in records:
                if isinstance(record, dict):
                    new_record = {k: v for k, v in record.items() if k != field_name}
                    transformed.append(new_record)
                else:
                    transformed.append(record)

        elif transform_type == "type_convert":
            field_name = stage.config.get("field")
            target_type = stage.config.get("target_type", "str")
            for record in records:
                if isinstance(record, dict) and field_name in record:
                    new_record = record.copy()
                    new_record[field_name] = self._convert_type(new_record[field_name], target_type)
                    transformed.append(new_record)
                else:
                    transformed.append(record)

        result = StageResult(
            stage_id=stage.stage_id,
            status=StageStatus.SUCCESS,
            records_in=len(records),
            records_out=len(transformed),
        )

        return transformed, result

    def _handle_aggregate(self, stage: PipelineStage, records: List[Any], params: Dict[str, Any]) -> tuple[List[Any], StageResult]:
        """Handle aggregation stage."""
        agg_type = stage.config.get("type", "group_by")
        aggregated: List[Any] = []

        if agg_type == "group_by":
            group_field = stage.config.get("group_by", "category")
            aggregations = stage.config.get("aggregations", [])

            groups: Dict[Any, List[Any]] = {}
            for record in records:
                if isinstance(record, dict):
                    key = record.get(group_field, "unknown")
                    if key not in groups:
                        groups[key] = []
                    groups[key].append(record)

            for key, group_records in groups.items():
                result_record: Dict[str, Any] = {group_field: key}

                for agg in aggregations:
                    field_name = agg.get("field")
                    agg_op = agg.get("operation", "sum")

                    if field_name:
                        values = [r.get(field_name, 0) for r in group_records if isinstance(r.get(field_name), (int, float))]
                        result_record[f"{field_name}_{agg_op}"] = self._apply_aggregation(values, agg_op)
                    else:
                        result_record[f"count"] = len(group_records)

                aggregated.append(result_record)

        elif agg_type == "sum":
            field_name = stage.config.get("field")
            total = sum(r.get(field_name, 0) for r in records if isinstance(r.get(field_name), (int, float)))
            aggregated = [{"field": field_name, "sum": total}]

        elif agg_type == "count":
            aggregated = [{"total_count": len(records)}]

        elif agg_type == "average":
            field_name = stage.config.get("field")
            values = [r.get(field_name, 0) for r in records if isinstance(r.get(field_name), (int, float))]
            avg = sum(values) / len(values) if values else 0
            aggregated = [{"field": field_name, "average": avg, "count": len(values)}]

        result = StageResult(
            stage_id=stage.stage_id,
            status=StageStatus.SUCCESS,
            records_in=len(records),
            records_out=len(aggregated),
        )

        return aggregated, result

    def _handle_split(self, stage: PipelineStage, records: List[Any], params: Dict[str, Any]) -> tuple[List[Any], StageResult]:
        """Handle split stage that branches the pipeline."""
        split_type = stage.config.get("type", "conditional")
        branch_var = stage.config.get("branch_var", "branch")

        if split_type == "conditional":
            condition = stage.config.get("condition", "True")
            true_records = []
            false_records = []

            for record in records:
                try:
                    record_copy = record.copy() if isinstance(record, dict) else {"value": record}
                    if eval(condition, {"__builtins__": {}}, {"record": record_copy}):
                        true_records.append(record)
                    else:
                        false_records.append(record)
                except Exception:
                    false_records.append(record)

            params[f"{branch_var}_true"] = true_records
            params[f"{branch_var}_false"] = false_records

            result = StageResult(
                stage_id=stage.stage_id,
                status=StageStatus.SUCCESS,
                records_in=len(records),
                records_out=len(true_records),
                metrics={"true_count": len(true_records), "false_count": len(false_records)},
            )

            return true_records, result

        return records, StageResult(stage_id=stage.stage_id, status=StageStatus.SUCCESS, records_in=len(records), records_out=len(records))

    def _handle_merge(self, stage: PipelineStage, records: List[Any], params: Dict[str, Any]) -> tuple[List[Any], StageResult]:
        """Handle merge stage that combines streams."""
        merge_type = stage.config.get("type", "concat")
        sources = stage.config.get("sources", [])

        merged: List[Any] = list(records)

        if merge_type == "concat":
            for source_var in sources:
                if source_var in params:
                    source_data = self._ensure_list(params[source_var])
                    merged.extend(source_data)

        elif merge_type == "union":
            seen = set()
            for source_var in sources:
                if source_var in params:
                    for record in self._ensure_list(params[source_var]):
                        key = str(record)
                        if key not in seen:
                            seen.add(key)
                            merged.append(record)

        result = StageResult(
            stage_id=stage.stage_id,
            status=StageStatus.SUCCESS,
            records_in=len(records),
            records_out=len(merged),
        )

        return merged, result

    def _handle_sink(self, stage: PipelineStage, records: List[Any], params: Dict[str, Any]) -> tuple[List[Any], StageResult]:
        """Handle sink stage that outputs records."""
        sink_type = stage.config.get("type", "return")
        stored_records: List[Any] = []

        if sink_type == "return":
            stored_records = records

        elif sink_type == "store":
            var_name = stage.config.get("var", "pipeline_output")
            params[var_name] = records
            stored_records = records

        result = StageResult(
            stage_id=stage.stage_id,
            status=StageStatus.SUCCESS,
            records_in=len(records),
            records_out=len(stored_records),
        )

        return stored_records, result

    def _handle_generic(self, stage: PipelineStage, records: List[Any], params: Dict[str, Any]) -> tuple[List[Any], StageResult]:
        """Generic handler for unknown stage types."""
        return records, StageResult(stage_id=stage.stage_id, status=StageStatus.SUCCESS, records_in=len(records), records_out=len(records))

    def _build_stages(self, stages_def: List[Dict[str, Any]]) -> List[PipelineStage]:
        """Build PipelineStage objects from definitions."""
        stages = []
        for stage_data in stages_def:
            stage_type_str = stage_data.get("stage_type", "transform")
            try:
                stage_type = PipelineStageType(stage_type_str)
            except ValueError:
                stage_type = PipelineStageType.TRANSFORM

            stage = PipelineStage(
                stage_id=stage_data.get("stage_id", self._generate_stage_id()),
                stage_type=stage_type,
                name=stage_data.get("name", stage_type_str),
                config=stage_data.get("config", {}),
                on_error=stage_data.get("on_error", "skip"),
                timeout=stage_data.get("timeout", 60.0),
            )
            stages.append(stage)

        return stages

    def _validate_pipeline(self, params: Dict[str, Any]) -> ActionResult:
        """Validate a pipeline definition."""
        stages_def = params.get("stages", [])

        if not stages_def:
            return ActionResult(success=False, message="stages are required")

        errors: List[str] = []
        warnings: List[str] = []

        has_source = False
        has_sink = False

        for i, stage_data in enumerate(stages_def):
            stage_type_str = stage_data.get("stage_type", "transform")

            try:
                PipelineStageType(stage_type_str)
            except ValueError:
                errors.append(f"Stage {i}: Unknown stage type '{stage_type_str}'")

            if stage_type_str == "source":
                has_source = True
            if stage_type_str == "sink":
                has_sink = True

            if stage_data.get("stage_id") is None:
                warnings.append(f"Stage {i}: Missing stage_id")

        if not has_source:
            warnings.append("Pipeline has no source stage")

        if not has_sink:
            warnings.append("Pipeline has no sink stage")

        return ActionResult(
            success=len(errors) == 0,
            message="Validation passed" if not errors else "Validation failed",
            data={"errors": errors, "warnings": warnings}
        )

    def _get_stage_info(self, params: Dict[str, Any]) -> ActionResult:
        """Get information about a stage type."""
        stage_type = params.get("stage_type", "transform")

        try:
            st = PipelineStageType(stage_type)
        except ValueError:
            return ActionResult(success=False, message=f"Unknown stage type: {stage_type}")

        info = {
            "stage_type": st.value,
            "description": self._get_stage_description(st),
            "config_options": self._get_stage_config_options(st),
        }

        return ActionResult(success=True, data=info)

    def _get_stage_description(self, stage_type: PipelineStageType) -> str:
        """Get description for a stage type."""
        descriptions = {
            PipelineStageType.SOURCE: "Produces records for the pipeline",
            PipelineStageType.FILTER: "Removes records based on conditions",
            PipelineStageType.TRANSFORM: "Modifies record structure or values",
            PipelineStageType.AGGREGATE: "Groups and aggregates records",
            PipelineStageType.SPLIT: "Branches pipeline based on conditions",
            PipelineStageType.MERGE: "Combines multiple record streams",
            PipelineStageType.SINK: "Outputs records from the pipeline",
        }
        return descriptions.get(stage_type, "Unknown stage type")

    def _get_stage_config_options(self, stage_type: PipelineStageType) -> List[str]:
        """Get config options for a stage type."""
        options = {
            PipelineStageType.SOURCE: ["type", "data", "start", "end", "count", "value"],
            PipelineStageType.FILTER: ["type", "expression", "field", "operator", "value", "key", "limit", "offset"],
            PipelineStageType.TRANSFORM: ["type", "mappings", "rename", "field", "value", "target_type"],
            PipelineStageType.AGGREGATE: ["type", "group_by", "aggregations", "field"],
            PipelineStageType.SPLIT: ["type", "condition", "branch_var"],
            PipelineStageType.MERGE: ["type", "sources"],
            PipelineStageType.SINK: ["type", "var"],
        }
        return options.get(stage_type, [])

    def _evaluate_operator(self, field_value: Any, operator: str, expected: Any) -> bool:
        """Evaluate a filter operator."""
        ops = {
            "eq": lambda a, b: a == b,
            "ne": lambda a, b: a != b,
            "gt": lambda a, b: a > b,
            "ge": lambda a, b: a >= b,
            "lt": lambda a, b: a < b,
            "le": lambda a, b: a <= b,
            "in": lambda a, b: a in b if isinstance(b, (list, tuple, set)) else False,
            "not_in": lambda a, b: a not in b if isinstance(b, (list, tuple, set)) else True,
            "contains": lambda a, b: b in a if a else False,
            "starts_with": lambda a, b: str(a).startswith(str(b)) if a else False,
            "ends_with": lambda a, b: str(a).endswith(str(b)) if a else False,
        }
        op_func = ops.get(operator, ops["eq"])
        return op_func(field_value, expected)

    def _apply_aggregation(self, values: List[float], operation: str) -> float:
        """Apply an aggregation operation to values."""
        if not values:
            return 0.0

        ops = {
            "sum": sum,
            "avg": lambda v: sum(v) / len(v),
            "min": min,
            "max": max,
            "count": len,
            "first": lambda v: v[0],
            "last": lambda v: v[-1],
        }

        op_func = ops.get(operation, sum)
        return op_func(values)

    def _convert_type(self, value: Any, target_type: str) -> Any:
        """Convert a value to a target type."""
        converters = {
            "str": str,
            "int": lambda v: int(v) if v is not None else 0,
            "float": lambda v: float(v) if v is not None else 0.0,
            "bool": lambda v: bool(v) if v is not None else False,
        }

        converter = converters.get(target_type, str)
        return converter(value)

    def _ensure_list(self, data: Any) -> List[Any]:
        """Ensure data is a list."""
        if data is None:
            return []
        if isinstance(data, list):
            return data
        return [data]

    def _generate_stage_id(self) -> str:
        """Generate a unique stage ID."""
        return f"stage_{hashlib.sha1(str(time.time_ns()).encode()).hexdigest()[:8]}"
