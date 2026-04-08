"""Group operations action module for RabAI AutoClick.

Provides grouping operations:
- GroupByAction: Group data by fields
- GroupConcatAction: Group and concatenate
- GroupCountAction: Group and count
- GroupAggregateAction: Group and aggregate
"""

from typing import Any, Dict, List, Optional
from collections import defaultdict


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class GroupByAction(BaseAction):
    """Group data by fields."""
    action_type = "group_by"
    display_name = "分组"
    description = "按字段分组"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            group_fields = params.get("group_fields", [])
            preserve_order = params.get("preserve_order", True)

            if not data:
                return ActionResult(success=False, message="data is required")
            if not group_fields:
                return ActionResult(success=False, message="group_fields are required")

            groups = defaultdict(list)
            for record in data:
                if isinstance(record, dict):
                    key = tuple(record.get(f) for f in group_fields)
                    groups[key].append(record)
                else:
                    groups[(record,)].append(record)

            result = []
            for key, records in groups.items():
                group_dict = {f: v for f, v in zip(group_fields, key)}
                group_dict["_items"] = records
                group_dict["_count"] = len(records)
                result.append(group_dict)

            if preserve_order:
                result.sort(key=lambda x: x["_count"], reverse=True)

            return ActionResult(
                success=True,
                message=f"Grouped into {len(result)} groups",
                data={"groups": result, "group_count": len(result)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Group by failed: {str(e)}")


class GroupConcatAction(BaseAction):
    """Group and concatenate values."""
    action_type = "group_concat"
    display_name = "分组拼接"
    description = "分组并拼接值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            group_fields = params.get("group_fields", [])
            concat_field = params.get("concat_field", "")
            separator = params.get("separator", ",")
            distinct = params.get("distinct", False)

            if not data:
                return ActionResult(success=False, message="data is required")
            if not group_fields or not concat_field:
                return ActionResult(success=False, message="group_fields and concat_field are required")

            groups = defaultdict(list)
            for record in data:
                if isinstance(record, dict):
                    key = tuple(record.get(f) for f in group_fields)
                    val = record.get(concat_field)
                    if val is not None:
                        groups[key].append(val)

            result = []
            for key, values in groups.items():
                group_dict = {f: v for f, v in zip(group_fields, key)}
                if distinct:
                    values = list(dict.fromkeys(values))
                group_dict["concatenated"] = separator.join(str(v) for v in values)
                group_dict["_count"] = len(values)
                result.append(group_dict)

            return ActionResult(
                success=True,
                message=f"Group concat: {len(result)} groups",
                data={"result": result, "group_count": len(result)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Group concat failed: {str(e)}")


class GroupCountAction(BaseAction):
    """Group and count."""
    action_type = "group_count"
    display_name = "分组计数"
    description = "分组并计数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            group_fields = params.get("group_fields", [])
            count_field = params.get("count_field", None)
            sort_by = params.get("sort_by", "count")
            ascending = params.get("ascending", False)

            if not data:
                return ActionResult(success=False, message="data is required")

            if group_fields:
                groups = defaultdict(int)
                for record in data:
                    if isinstance(record, dict):
                        key = tuple(record.get(f) for f in group_fields)
                        groups[key] += 1

                result = []
                for key, count in groups.items():
                    group_dict = {f: v for f, v in zip(group_fields, key)}
                    group_dict["count"] = count
                    result.append(group_dict)
            else:
                counts = defaultdict(int)
                for record in data:
                    if count_field and isinstance(record, dict):
                        key = record.get(count_field)
                    else:
                        key = record if not isinstance(record, (dict, list)) else str(type(record).__name__)
                    counts[key] += 1

                result = [{"value": k, "count": v} for k, v in counts.items()]

            if sort_by == "count":
                result.sort(key=lambda x: x["count"], reverse=not ascending)
            elif sort_by == "value":
                result.sort(key=lambda x: str(x.get("value", "")), reverse=not ascending)

            return ActionResult(
                success=True,
                message=f"Group count: {len(result)} groups",
                data={"result": result, "group_count": len(result)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Group count failed: {str(e)}")


class GroupAggregateAction(BaseAction):
    """Group and aggregate."""
    action_type = "group_aggregate"
    display_name = "分组聚合"
    description = "分组并聚合"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            group_fields = params.get("group_fields", [])
            aggregations = params.get("aggregations", [])

            if not data:
                return ActionResult(success=False, message="data is required")
            if not group_fields or not aggregations:
                return ActionResult(success=False, message="group_fields and aggregations are required")

            groups = defaultdict(list)
            for record in data:
                if isinstance(record, dict):
                    key = tuple(record.get(f) for f in group_fields)
                    groups[key].append(record)

            result = []
            for key, records in groups.items():
                group_dict = {f: v for f, v in zip(group_fields, key)}
                for agg in aggregations:
                    field = agg.get("field", "")
                    func = agg.get("func", "sum")
                    alias = agg.get("alias", f"{field}_{func}")

                    values = []
                    for r in records:
                        if isinstance(r, dict) and field in r and r[field] is not None:
                            try:
                                values.append(float(r[field]))
                            except (ValueError, TypeError):
                                pass

                    if func == "sum":
                        group_dict[alias] = sum(values) if values else 0
                    elif func == "avg":
                        group_dict[alias] = sum(values) / len(values) if values else 0
                    elif func == "count":
                        group_dict[alias] = len(values)
                    elif func == "min":
                        group_dict[alias] = min(values) if values else None
                    elif func == "max":
                        group_dict[alias] = max(values) if values else None
                    elif func == "first":
                        group_dict[alias] = values[0] if values else None
                    elif func == "last":
                        group_dict[alias] = values[-1] if values else None

                result.append(group_dict)

            return ActionResult(
                success=True,
                message=f"Group aggregate: {len(result)} groups",
                data={"result": result, "group_count": len(result)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Group aggregate failed: {str(e)}")
