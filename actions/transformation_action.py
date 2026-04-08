"""Transformation action module for RabAI AutoClick.

Provides data transformation operations:
- TransformMapAction: Map/transform fields
- TransformFilterAction: Filter data
- TransformSortAction: Sort data
- TransformGroupAction: Group data
- TransformAggregateAction: Aggregate data
- TransformPivotAction: Pivot data
- TransformJoinAction: Join datasets
- TransformNormalizeAction: Normalize data
"""

import json
import os
import sys
from collections import defaultdict
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TransformMapAction(BaseAction):
    """Map and transform fields."""
    action_type = "transform_map"
    display_name = "字段映射"
    description = "映射转换字段"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            mappings = params.get("mappings", {})
            
            if not data:
                return ActionResult(success=False, message="data required")
            
            transformed = []
            for item in data:
                new_item = {}
                for old_key, new_key in mappings.items():
                    if old_key in item:
                        if callable(new_key):
                            new_item[old_key] = new_key(item[old_key])
                        else:
                            new_item[new_key] = item[old_key]
                transformed.append(new_item)
            
            return ActionResult(
                success=True,
                message=f"Mapped {len(data)} items",
                data={"transformed": transformed[:100], "count": len(transformed)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Transform map failed: {str(e)}")


class TransformFilterAction(BaseAction):
    """Filter data."""
    action_type = "transform_filter"
    display_name = "数据过滤"
    description = "过滤数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "")
            operator = params.get("operator", "eq")
            value = params.get("value")
            
            if not data:
                return ActionResult(success=False, message="data required")
            
            filtered = []
            for item in data:
                if field not in item:
                    continue
                
                item_value = item[field]
                
                if operator == "eq" and item_value == value:
                    filtered.append(item)
                elif operator == "ne" and item_value != value:
                    filtered.append(item)
                elif operator == "gt" and item_value > value:
                    filtered.append(item)
                elif operator == "gte" and item_value >= value:
                    filtered.append(item)
                elif operator == "lt" and item_value < value:
                    filtered.append(item)
                elif operator == "lte" and item_value <= value:
                    filtered.append(item)
                elif operator == "in" and item_value in value:
                    filtered.append(item)
                elif operator == "contains" and value in str(item_value):
                    filtered.append(item)
            
            return ActionResult(
                success=True,
                message=f"Filtered {len(data)} to {len(filtered)} items",
                data={"filtered": filtered[:100], "count": len(filtered), "original_count": len(data)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Transform filter failed: {str(e)}")


class TransformSortAction(BaseAction):
    """Sort data."""
    action_type = "transform_sort"
    display_name = "数据排序"
    description = "排序数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            sort_by = params.get("sort_by", "")
            order = params.get("order", "asc")
            
            if not data:
                return ActionResult(success=False, message="data required")
            
            if not sort_by:
                return ActionResult(success=False, message="sort_by required")
            
            reverse = order == "desc"
            
            sorted_data = sorted(data, key=lambda x: x.get(sort_by, ""), reverse=reverse)
            
            return ActionResult(
                success=True,
                message=f"Sorted {len(data)} items by {sort_by} ({order})",
                data={"sorted": sorted_data[:100], "count": len(sorted_data)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Transform sort failed: {str(e)}")


class TransformGroupAction(BaseAction):
    """Group data."""
    action_type = "transform_group"
    display_name = "数据分组"
    description = "分组数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            group_by = params.get("group_by", "")
            
            if not data:
                return ActionResult(success=False, message="data required")
            
            if not group_by:
                return ActionResult(success=False, message="group_by required")
            
            groups = defaultdict(list)
            for item in data:
                key = item.get(group_by, "unknown")
                groups[key].append(item)
            
            return ActionResult(
                success=True,
                message=f"Grouped into {len(groups)} groups",
                data={"groups": dict(groups), "group_count": len(groups)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Transform group failed: {str(e)}")


class TransformAggregateAction(BaseAction):
    """Aggregate data."""
    action_type = "transform_aggregate"
    display_name = "数据聚合"
    description = "聚合数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            group_by = params.get("group_by", "")
            aggregations = params.get("aggregations", {})
            
            if not data:
                return ActionResult(success=False, message="data required")
            
            if not aggregations:
                return ActionResult(success=False, message="aggregations required")
            
            if group_by:
                groups = defaultdict(list)
                for item in data:
                    key = item.get(group_by, "unknown")
                    groups[key].append(item)
                
                results = []
                for key, items in groups.items():
                    result = {group_by: key}
                    for field, agg_func in aggregations.items():
                        values = [item.get(field, 0) for item in items if field in item]
                        if agg_func == "sum":
                            result[f"{field}_sum"] = sum(values)
                        elif agg_func == "avg":
                            result[f"{field}_avg"] = sum(values) / len(values) if values else 0
                        elif agg_func == "min":
                            result[f"{field}_min"] = min(values) if values else None
                        elif agg_func == "max":
                            result[f"{field}_max"] = max(values) if values else None
                        elif agg_func == "count":
                            result[f"{field}_count"] = len(values)
                    results.append(result)
            else:
                result = {}
                for field, agg_func in aggregations.items():
                    values = [item.get(field, 0) for item in data if field in item]
                    if agg_func == "sum":
                        result[f"{field}_sum"] = sum(values)
                    elif agg_func == "avg":
                        result[f"{field}_avg"] = sum(values) / len(values) if values else 0
                    elif agg_func == "min":
                        result[f"{field}_min"] = min(values) if values else None
                    elif agg_func == "max":
                        result[f"{field}_max"] = max(values) if values else None
                    elif agg_func == "count":
                        result[f"{field}_count"] = len(values)
                results = [result]
            
            return ActionResult(
                success=True,
                message=f"Aggregated data: {len(results)} result(s)",
                data={"results": results}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Transform aggregate failed: {str(e)}")


class TransformPivotAction(BaseAction):
    """Pivot data."""
    action_type = "transform_pivot"
    display_name = "数据透视"
    description = "数据透视表"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            rows = params.get("rows", [])
            columns = params.get("columns", "")
            values = params.get("values", "")
            
            if not data:
                return ActionResult(success=False, message="data required")
            
            if not rows or not columns or not values:
                return ActionResult(success=False, message="rows, columns, values required")
            
            pivot = defaultdict(lambda: defaultdict(list))
            row_keys = set()
            
            for item in data:
                row_key = tuple(item.get(r, "unknown") for r in rows)
                col_key = item.get(columns, "unknown")
                val = item.get(values, 0)
                
                pivot[row_key][col_key].append(val)
                row_keys.add(row_key)
            
            results = []
            for row_key in row_keys:
                result = dict(zip(rows, row_key))
                for col_key, vals in pivot[row_key].items():
                    result[col_key] = sum(vals) / len(vals) if vals else 0
                results.append(result)
            
            return ActionResult(
                success=True,
                message=f"Pivoted {len(data)} items into {len(results)} rows",
                data={"pivot": results[:100], "count": len(results)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Transform pivot failed: {str(e)}")


class TransformJoinAction(BaseAction):
    """Join datasets."""
    action_type = "transform_join"
    display_name = "数据连接"
    description = "连接两个数据集"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            left = params.get("left", [])
            right = params.get("right", [])
            left_key = params.get("left_key", "")
            right_key = params.get("right_key", "")
            join_type = params.get("join_type", "inner")
            
            if not left or not right:
                return ActionResult(success=False, message="left and right datasets required")
            
            if not left_key or not right_key:
                return ActionResult(success=False, message="left_key and right_key required")
            
            right_index = {item.get(right_key): item for item in right}
            results = []
            
            for l_item in left:
                r_item = right_index.get(l_item.get(left_key))
                
                if r_item:
                    merged = {**l_item, **{f"right_{k}": v for k, v in r_item.items() if k != right_key}}
                    results.append(merged)
                elif join_type == "left":
                    results.append({**l_item, "right_*": None})
            
            if join_type == "outer":
                left_keys = {item.get(left_key) for item in left}
                for r_item in right:
                    if r_item.get(right_key) not in left_keys:
                        merged = {**r_item, **{f"left_{k}": None for k in left[0].keys() if k != left_key}}
                        results.append(merged)
            
            return ActionResult(
                success=True,
                message=f"Joined: {len(results)} results",
                data={"joined": results[:100], "count": len(results)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Transform join failed: {str(e)}")


class TransformNormalizeAction(BaseAction):
    """Normalize data."""
    action_type = "transform_normalize"
    display_name = "数据标准化"
    description = "标准化数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            fields = params.get("fields", [])
            method = params.get("method", "minmax")
            
            if not data:
                return ActionResult(success=False, message="data required")
            
            if not fields:
                numeric_fields = [k for k in data[0].keys() if isinstance(data[0][k], (int, float))] if data else []
                fields = numeric_fields
            
            if method == "minmax":
                for field in fields:
                    values = [item.get(field, 0) for item in data if field in item]
                    min_val = min(values) if values else 0
                    max_val = max(values) if values else 1
                    range_val = max_val - min_val or 1
                    
                    for item in data:
                        if field in item:
                            item[f"{field}_normalized"] = (item[field] - min_val) / range_val
            
            elif method == "zscore":
                for field in fields:
                    values = [item.get(field, 0) for item in data if field in item]
                    mean = sum(values) / len(values) if values else 0
                    variance = sum((v - mean) ** 2 for v in values) / len(values) if values else 1
                    std = variance ** 0.5 or 1
                    
                    for item in data:
                        if field in item:
                            item[f"{field}_normalized"] = (item[field] - mean) / std
            
            return ActionResult(
                success=True,
                message=f"Normalized {len(fields)} fields for {len(data)} items",
                data={"data": data[:100], "count": len(data), "fields": fields}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Transform normalize failed: {str(e)}")
