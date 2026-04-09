"""Data grouping action module for RabAI AutoClick.

Provides data grouping operations:
- DataGroupingAction: Group data by key
- DataGroupByMultipleAction: Multi-level grouping
- DataGroupAggregationAction: Aggregate within groups
- DataGroupTransformAction: Transform within groups
"""

from typing import Any, Dict, List, Optional, Callable
from collections import defaultdict
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataGroupingAction(BaseAction):
    """Group data by a key."""
    action_type = "data_grouping"
    display_name = "数据分组"
    description = "按键分组数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            key = params.get("key")
            sort_groups = params.get("sort_groups", False)
            sort_order = params.get("sort_order", "asc")

            if not data:
                return ActionResult(success=False, message="data is required")
            if not key:
                return ActionResult(success=False, message="key is required")

            groups: Dict[str, List[Any]] = defaultdict(list)

            for item in data:
                if isinstance(item, dict):
                    group_key = str(item.get(key, "None"))
                else:
                    group_key = str(item)
                groups[group_key].append(item)

            if sort_groups:
                sorted_keys = sorted(groups.keys(), reverse=(sort_order == "desc"))
                groups = {k: groups[k] for k in sorted_keys}

            group_info = {k: len(v) for k, v in groups.items()}

            return ActionResult(
                success=True,
                message=f"Grouped into {len(groups)} groups",
                data={"groups": dict(groups), "group_info": group_info, "total_groups": len(groups), "total_items": len(data)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Grouping error: {e}")


class DataGroupByMultipleAction(BaseAction):
    """Multi-level grouping with multiple keys."""
    action_type = "data_group_by_multiple"
    display_name = "数据多键分组"
    description = "多层级多键分组"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            keys = params.get("keys", [])
            max_depth = params.get("max_depth", 5)

            if not data:
                return ActionResult(success=False, message="data is required")
            if not keys:
                return ActionResult(success=False, message="keys list is required")

            if len(keys) > max_depth:
                return ActionResult(success=False, message=f"Max depth is {max_depth}")

            def build_tree(items: List[Any], depth: int) -> Any:
                if depth >= len(keys):
                    return items

                key = keys[depth]
                groups: Dict[str, List[Any]] = defaultdict(list)
                for item in items:
                    if isinstance(item, dict):
                        group_key = str(item.get(key, "None"))
                    else:
                        group_key = "None"
                    groups[group_key].append(item)

                return {k: build_tree(v, depth + 1) for k, v in groups.items()}

            tree = build_tree(data, 0)

            return ActionResult(
                success=True,
                message=f"Multi-level grouping with {len(keys)} levels",
                data={"tree": tree, "levels": len(keys), "total_items": len(data)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Multi-level grouping error: {e}")


class DataGroupAggregationAction(BaseAction):
    """Aggregate values within groups."""
    action_type = "data_group_aggregation"
    display_name = "数据分组聚合"
    description = "分组内聚合计算"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            group_key = params.get("group_key")
            agg_field = params.get("agg_field")
            agg_funcs = params.get("agg_funcs", ["count"])
            output_format = params.get("output_format", "list")

            if not data:
                return ActionResult(success=False, message="data is required")
            if not group_key:
                return ActionResult(success=False, message="group_key is required")

            groups: Dict[str, List[Any]] = defaultdict(list)
            for item in data:
                if isinstance(item, dict):
                    gk = str(item.get(group_key, "None"))
                else:
                    gk = "None"
                groups[gk].append(item)

            results = []
            for group_name, items in groups.items():
                row = {"_group": group_name}

                if agg_field:
                    values = [item.get(agg_field) for item in items if isinstance(item, dict) and agg_field in item]
                    numeric_values = [v for v in values if isinstance(v, (int, float))]

                    for func in agg_funcs:
                        if func == "count":
                            row[f"{agg_field}_{func}"] = len(values)
                        elif func == "sum" and numeric_values:
                            row[f"{agg_field}_{func}"] = sum(numeric_values)
                        elif func == "mean" and numeric_values:
                            row[f"{agg_field}_{func}"] = sum(numeric_values) / len(numeric_values)
                        elif func == "min" and numeric_values:
                            row[f"{agg_field}_{func}"] = min(numeric_values)
                        elif func == "max" and numeric_values:
                            row[f"{agg_field}_{func}"] = max(numeric_values)
                        elif func == "first" and values:
                            row[f"{agg_field}_{func}"] = values[0]
                        elif func == "last" and values:
                            row[f"{agg_field}_{func}"] = values[-1]
                        elif func == "list":
                            row[f"{agg_field}_{func}"] = values
                else:
                    row["count"] = len(items)

                results.append(row)

            return ActionResult(
                success=True,
                message=f"Aggregated {len(groups)} groups",
                data={"results": results, "group_count": len(groups), "format": output_format}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Group aggregation error: {e}")


class DataGroupTransformAction(BaseAction):
    """Transform data within each group."""
    action_type = "data_group_transform"
    display_name = "数据分组转换"
    description = "在每个分组内进行转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            group_key = params.get("group_key")
            transform_fn = params.get("transform_fn")
            transform_type = params.get("transform_type", "rank")
            output_field = params.get("output_field")

            if not data:
                return ActionResult(success=False, message="data is required")
            if not group_key:
                return ActionResult(success=False, message="group_key is required")

            groups: Dict[str, List[Any]] = defaultdict(list)
            indices: Dict[str, List[int]] = defaultdict(list)

            for i, item in enumerate(data):
                if isinstance(item, dict):
                    gk = str(item.get(group_key, "None"))
                else:
                    gk = "None"
                groups[gk].append(item)
                indices[gk].append(i)

            transformed_data = list(data)

            for group_name, items in groups.items():
                group_indices = indices[group_name]

                if transform_type == "rank":
                    sort_key = params.get("sort_by")
                    if sort_key:
                        sorted_items = sorted(enumerate(items), key=lambda x: x[1].get(sort_key, 0) if isinstance(x[1], dict) else 0)
                        for rank, (orig_idx, _) in enumerate(sorted_items):
                            actual_idx = group_indices[orig_idx]
                            if isinstance(transformed_data[actual_idx], dict):
                                transformed_data[actual_idx][output_field or "_rank"] = rank + 1

                elif transform_type == "normalize":
                    norm_field = params.get("normalize_field")
                    if norm_field:
                        values = [item.get(norm_field) for item in items if isinstance(item, dict) and norm_field in item]
                        numeric = [v for v in values if isinstance(v, (int, float))]
                        if numeric:
                            min_val, max_val = min(numeric), max(numeric)
                            range_val = max_val - min_val
                            for item in items:
                                if isinstance(item, dict) and norm_field in item and isinstance(item[norm_field], (int, float)) and range_val != 0:
                                    item[output_field or f"{norm_field}_normalized"] = (item[norm_field] - min_val) / range_val

                elif transform_type == "cumulative":
                    cum_field = params.get("cumulative_field")
                    if cum_field:
                        cumulative = 0
                        for orig_idx, item in enumerate(items):
                            if isinstance(item, dict) and cum_field in item and isinstance(item[cum_field], (int, float)):
                                cumulative += item[cum_field]
                                item[output_field or f"{cum_field}_cumulative"] = cumulative

                elif callable(transform_fn):
                    for item in items:
                        if isinstance(item, dict):
                            result = transform_fn(item)
                            if output_field:
                                item[output_field] = result
                            elif isinstance(result, dict):
                                item.update(result)

            return ActionResult(
                success=True,
                message=f"Transformed {len(groups)} groups with {transform_type}",
                data={"data": transformed_data, "group_count": len(groups)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Group transform error: {e}")
