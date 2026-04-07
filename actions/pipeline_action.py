"""Data pipeline processing action module for RabAI AutoClick.

Provides data pipeline operations:
- PipelineCreateAction: Create a processing pipeline
- PipelineExecuteAction: Execute pipeline stages
- PipelineFilterAction: Filter data in pipeline
- PipelineMapAction: Transform data in pipeline
- PipelineReduceAction: Reduce/combine pipeline data
- PipelineBranchAction: Branch pipeline execution
"""

from typing import Any, Callable, Dict, List, Optional, Union
from collections import deque

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PipelineContext:
    """Pipeline execution context."""
    def __init__(self):
        self.data = None
        self.metadata: Dict[str, Any] = {}
        self.errors: List[str] = []
        self.stages_completed: List[str] = []

    def set_data(self, data: Any) -> None:
        self.data = data

    def get_data(self) -> Any:
        return self.data

    def add_metadata(self, key: str, value: Any) -> None:
        self.metadata[key] = value

    def get_metadata(self, key: str, default: Any = None) -> Any:
        return self.metadata.get(key, default)

    def add_error(self, error: str) -> None:
        self.errors.append(error)

    def record_stage(self, stage: str) -> None:
        self.stages_completed.append(stage)


class PipelineCreateAction(BaseAction):
    """Create a data processing pipeline."""
    action_type = "pipeline_create"
    display_name = "创建数据流水线"
    description = "创建数据处理流水线"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "unnamed_pipeline")
            stages = params.get("stages", [])
            error_mode = params.get("error_mode", "stop")
            parallel = params.get("parallel", False)

            pipeline = {
                "name": name,
                "stages": stages,
                "error_mode": error_mode,
                "parallel": parallel,
                "created_at": "now"
            }

            return ActionResult(
                success=True,
                message=f"Created pipeline '{name}' with {len(stages)} stages",
                data={"pipeline": pipeline, "stage_count": len(stages)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Create pipeline error: {str(e)}")


class PipelineExecuteAction(BaseAction):
    """Execute a pipeline with data."""
    action_type = "pipeline_execute"
    display_name = "执行数据流水线"
    description = "执行数据处理流水线"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            stages = params.get("stages", [])
            error_mode = params.get("error_mode", "stop")

            if not stages:
                return ActionResult(success=False, message="No stages to execute")

            pipeline_ctx = PipelineContext()
            pipeline_ctx.set_data(data)

            for i, stage in enumerate(stages):
                stage_name = stage.get("name", f"stage_{i}")
                stage_type = stage.get("type", "identity")
                stage_params = stage.get("params", {})

                try:
                    result = self._execute_stage(stage_type, stage_params, pipeline_ctx)

                    if not result.success:
                        pipeline_ctx.add_error(f"Stage {stage_name} failed: {result.message}")
                        if error_mode == "stop":
                            break
                        else:
                            continue

                    pipeline_ctx.record_stage(stage_name)

                except Exception as e:
                    pipeline_ctx.add_error(f"Stage {stage_name} exception: {str(e)}")
                    if error_mode == "stop":
                        break

            return ActionResult(
                success=True,
                message=f"Pipeline executed: {len(pipeline_ctx.stages_completed)}/{len(stages)} stages completed",
                data={
                    "result": pipeline_ctx.get_data(),
                    "stages_completed": pipeline_ctx.stages_completed,
                    "errors": pipeline_ctx.errors
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline execute error: {str(e)}")

    def _execute_stage(self, stage_type: str, params: Dict, ctx: PipelineContext) -> ActionResult:
        """Execute a single pipeline stage."""
        if stage_type == "identity":
            return ActionResult(success=True, message="Identity stage")

        elif stage_type == "filter":
            records = ctx.get_data() or []
            column = params.get("column", "")
            operator = params.get("operator", "eq")
            value = params.get("value", None)

            filtered = []
            for record in records:
                if isinstance(record, dict):
                    cell = record.get(column, "")
                    if self._compare(cell, operator, value):
                        filtered.append(record)
                elif callable(record) and callable(value):
                    if value(record):
                        filtered.append(record)

            ctx.set_data(filtered)
            return ActionResult(success=True, message=f"Filtered to {len(filtered)} records")

        elif stage_type == "map":
            records = ctx.get_data() or []
            field = params.get("field", "")
            transform = params.get("transform", "uppercase")

            mapped = []
            for record in records:
                if isinstance(record, dict):
                    new_record = dict(record)
                    if field in new_record:
                        if transform == "uppercase":
                            new_record[field] = str(new_record[field]).upper()
                        elif transform == "lowercase":
                            new_record[field] = str(new_record[field]).lower()
                        elif transform == "trim":
                            new_record[field] = str(new_record[field]).strip()
                        elif transform == "abs":
                            try:
                                new_record[field] = abs(float(new_record[field]))
                            except:
                                pass
                    mapped.append(new_record)
                else:
                    mapped.append(record)

            ctx.set_data(mapped)
            return ActionResult(success=True, message=f"Mapped {len(mapped)} records")

        elif stage_type == "reduce":
            records = ctx.get_data() or []
            group_by = params.get("group_by", [])
            aggregations = params.get("aggregations", {})

            groups = {}
            for record in records:
                if not isinstance(record, dict):
                    continue
                key = tuple(record.get(col) for col in group_by) if group_by else ("all",)
                if key not in groups:
                    groups[key] = []
                groups[key].append(record)

            results = []
            for key, group_records in groups.items():
                result = dict(zip(group_by, key)) if group_by else {}
                for field, agg_func in aggregations.items():
                    values = []
                    for record in group_records:
                        val = record.get(field)
                        if val is not None:
                            try:
                                values.append(float(val))
                            except:
                                pass

                    if agg_func == "sum":
                        result[f"{field}_sum"] = sum(values) if values else 0
                    elif agg_func == "avg" or agg_func == "mean":
                        result[f"{field}_mean"] = sum(values) / len(values) if values else 0
                    elif agg_func == "count":
                        result[f"{field}_count"] = len(values)
                    elif agg_func == "min":
                        result[f"{field}_min"] = min(values) if values else None
                    elif agg_func == "max":
                        result[f"{field}_max"] = max(values) if values else None

                results.append(result)

            ctx.set_data(results)
            return ActionResult(success=True, message=f"Reduced to {len(results)} groups")

        elif stage_type == "sort":
            records = ctx.get_data() or []
            sort_by = params.get("sort_by", [])
            ascending = params.get("ascending", True)

            if isinstance(sort_by, str):
                sort_by = [sort_by]

            def sort_key(record):
                values = []
                for col in sort_by:
                    val = record.get(col) if isinstance(record, dict) else record
                    try:
                        values.append(float(val))
                    except:
                        values.append(str(val).lower())
                return values

            sorted_records = sorted(records, key=sort_key, reverse=not ascending)
            ctx.set_data(sorted_records)
            return ActionResult(success=True, message=f"Sorted {len(sorted_records)} records")

        elif stage_type == "limit":
            records = ctx.get_data() or []
            limit = params.get("limit", 10)
            offset = params.get("offset", 0)

            limited = records[offset:offset + limit]
            ctx.set_data(limited)
            return ActionResult(success=True, message=f"Limited to {len(limited)} records")

        elif stage_type == "dedupe":
            records = ctx.get_data() or []
            key_fields = params.get("key_fields", None)

            seen = set()
            deduped = []
            for record in records:
                if key_fields:
                    key = tuple(record.get(f) for f in key_fields)
                else:
                    key = str(record)

                if key not in seen:
                    seen.add(key)
                    deduped.append(record)

            ctx.set_data(deduped)
            return ActionResult(success=True, message=f"Deduped to {len(deduped)} records")

        else:
            return ActionResult(success=False, message=f"Unknown stage type: {stage_type}")

    def _compare(self, cell: Any, operator: str, value: Any) -> bool:
        ops = {
            "eq": lambda a, b: a == b,
            "ne": lambda a, b: a != b,
            "gt": lambda a, b: a > b,
            "ge": lambda a, b: a >= b,
            "lt": lambda a, b: a < b,
            "le": lambda a, b: a <= b,
            "contains": lambda a, b: str(b) in str(a),
            "startswith": lambda a, b: str(a).startswith(str(b)),
            "endswith": lambda a, b: str(a).endswith(str(b)),
        }
        op_func = ops.get(operator, ops["eq"])
        try:
            return op_func(cell, value)
        except:
            return False


class PipelineFilterAction(BaseAction):
    """Filter data in pipeline style."""
    action_type = "pipeline_filter"
    display_name = "流水线过滤"
    description = "流水线式数据过滤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            conditions = params.get("conditions", [])

            if not records:
                return ActionResult(success=False, message="records list is required")

            filtered = records

            for condition in conditions:
                column = condition.get("column", "")
                operator = condition.get("operator", "eq")
                value = condition.get("value", None)
                logic = condition.get("logic", "and")

                temp_filtered = []
                for record in filtered:
                    if isinstance(record, dict):
                        cell = record.get(column, "")
                        if self._compare(cell, operator, value):
                            temp_filtered.append(record)

                if logic == "or":
                    filtered = list(set(filtered + temp_filtered))
                else:
                    filtered = temp_filtered

            return ActionResult(
                success=True,
                message=f"Filtered to {len(filtered)} records",
                data={"records": filtered, "count": len(filtered)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Filter error: {str(e)}")

    def _compare(self, cell: Any, operator: str, value: Any) -> bool:
        ops = {
            "eq": lambda a, b: a == b,
            "ne": lambda a, b: a != b,
            "gt": lambda a, b: a > b,
            "ge": lambda a, b: a >= b,
            "lt": lambda a, b: a < b,
            "le": lambda a, b: a <= b,
            "contains": lambda a, b: str(b) in str(a),
        }
        op_func = ops.get(operator, ops["eq"])
        try:
            return op_func(cell, value)
        except:
            return False


class PipelineMapAction(BaseAction):
    """Transform data in pipeline style."""
    action_type = "pipeline_map"
    display_name = "流水线映射"
    description = "流水线式数据转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            field_mappings = params.get("field_mappings", {})

            if not records:
                return ActionResult(success=False, message="records list is required")

            mapped = []
            for record in records:
                if isinstance(record, dict):
                    new_record = dict(record)
                    for source_field, target_config in field_mappings.items():
                        if isinstance(target_config, str):
                            new_record[target_config] = new_record.get(source_field)
                        elif isinstance(target_config, dict):
                            target_field = target_config.get("to", source_field)
                            transform = target_config.get("transform", "passthrough")
                            default = target_config.get("default", None)

                            value = new_record.get(source_field, default)

                            if transform == "uppercase":
                                value = str(value).upper() if value else value
                            elif transform == "lowercase":
                                value = str(value).lower() if value else value
                            elif transform == "titlecase":
                                value = str(value).title() if value else value
                            elif transform == "trim":
                                value = str(value).strip() if value else value
                            elif transform == "abs":
                                try:
                                    value = abs(float(value))
                                except:
                                    pass
                            elif transform == "str":
                                value = str(value) if value is not None else ""
                            elif transform == "int":
                                try:
                                    value = int(float(value))
                                except:
                                    pass
                            elif transform == "float":
                                try:
                                    value = float(value)
                                except:
                                    pass

                            new_record[target_field] = value

                    mapped.append(new_record)
                else:
                    mapped.append(record)

            return ActionResult(
                success=True,
                message=f"Mapped {len(mapped)} records",
                data={"records": mapped, "count": len(mapped)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Map error: {str(e)}")


class PipelineReduceAction(BaseAction):
    """Reduce/combine pipeline data."""
    action_type = "pipeline_reduce"
    display_name = "流水线聚合"
    description = "流水线式数据聚合"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            group_by = params.get("group_by", [])
            aggregations = params.get("aggregations", {})
            having = params.get("having", None)

            if not records:
                return ActionResult(success=False, message="records list is required")

            if isinstance(group_by, str):
                group_by = [group_by]

            groups = {}
            for record in records:
                if not isinstance(record, dict):
                    continue
                key = tuple(record.get(col) for col in group_by) if group_by else ("all",)
                if key not in groups:
                    groups[key] = []
                groups[key].append(record)

            results = []
            for key, group_records in groups.items():
                result = dict(zip(group_by, key)) if group_by else {}

                for field, agg_func in aggregations.items():
                    values = []
                    for record in group_records:
                        val = record.get(field)
                        if val is not None:
                            try:
                                values.append(float(val))
                            except:
                                pass

                    if agg_func == "sum":
                        result[f"{field}_sum"] = sum(values) if values else 0
                    elif agg_func == "avg" or agg_func == "mean":
                        result[f"{field}_avg"] = sum(values) / len(values) if values else 0
                    elif agg_func == "count":
                        result[f"{field}_count"] = len(values)
                    elif agg_func == "min":
                        result[f"{field}_min"] = min(values) if values else None
                    elif agg_func == "max":
                        result[f"{field}_max"] = max(values) if values else None
                    elif agg_func == "first":
                        result[f"{field}_first"] = values[0] if values else None
                    elif agg_func == "last":
                        result[f"{field}_last"] = values[-1] if values else None

                if having:
                    if self._check_having(result, having):
                        results.append(result)
                else:
                    results.append(result)

            return ActionResult(
                success=True,
                message=f"Reduced to {len(results)} groups",
                data={"results": results, "count": len(results)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Reduce error: {str(e)}")

    def _check_having(self, record: Dict, condition: Dict) -> bool:
        field = condition.get("field", "")
        operator = condition.get("operator", "eq")
        value = condition.get("value", 0)

        record_value = record.get(field, 0)
        try:
            record_value = float(record_value)
            value = float(value)
        except:
            pass

        ops = {"eq": lambda a, b: a == b, "gt": lambda a, b: a > b, "ge": lambda a, b: a >= b,
               "lt": lambda a, b: a < b, "le": lambda a, b: a <= b}
        op_func = ops.get(operator, ops["eq"])
        return op_func(record_value, value)


class PipelineBranchAction(BaseAction):
    """Branch pipeline execution."""
    action_type = "pipeline_branch"
    display_name = "流水线分支"
    description = "流水线分支执行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            branches = params.get("branches", [])
            branch_on = params.get("branch_on", "")

            if not records:
                return ActionResult(success=False, message="records list is required")

            if not branches:
                return ActionResult(success=False, message="branches required")

            branch_results = {}

            for branch in branches:
                branch_name = branch.get("name", "unnamed")
                condition = branch.get("condition", {})
                filter_column = condition.get("column", "")
                filter_operator = condition.get("operator", "eq")
                filter_value = condition.get("value", None)

                filtered = []
                for record in records:
                    if isinstance(record, dict):
                        if not filter_column:
                            filtered.append(record)
                        else:
                            cell = record.get(filter_column, "")
                            if self._compare(cell, filter_operator, filter_value):
                                filtered.append(record)

                branch_results[branch_name] = filtered

            return ActionResult(
                success=True,
                message=f"Branched into {len(branch_results)} paths",
                data={"branches": branch_results, "branch_count": len(branch_results)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Branch error: {str(e)}")

    def _compare(self, cell: Any, operator: str, value: Any) -> bool:
        ops = {
            "eq": lambda a, b: a == b, "ne": lambda a, b: a != b,
            "gt": lambda a, b: a > b, "ge": lambda a, b: a >= b,
            "lt": lambda a, b: a < b, "le": lambda a, b: a <= b,
            "contains": lambda a, b: str(b) in str(a),
        }
        op_func = ops.get(operator, ops["eq"])
        try:
            return op_func(cell, value)
        except:
            return False
