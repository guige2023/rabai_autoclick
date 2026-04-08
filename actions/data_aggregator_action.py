"""Data aggregator action module for RabAI AutoClick.

Provides data aggregation:
- DataAggregatorAction: Aggregate data
- GroupAggregatorAction: Group and aggregate
- WindowAggregatorAction: Window aggregation
"""

from typing import Any, Dict, List, Optional
from datetime import datetime
from collections import defaultdict

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataAggregatorAction(BaseAction):
    """Aggregate data."""
    action_type = "data_aggregator"
    display_name = "数据聚合"
    description = "聚合数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            group_by = params.get("group_by", None)
            aggregations = params.get("aggregations", [])

            if not isinstance(data, list):
                return ActionResult(success=False, message="data must be a list")

            if not group_by:
                result = {}
                for agg in aggregations:
                    field = agg.get("field", "")
                    func = agg.get("function", "sum")
                    values = [item.get(field, 0) if isinstance(item, dict) else item for item in data]

                    if func == "sum":
                        result[agg.get("alias", field)] = sum(values)
                    elif func == "avg":
                        result[agg.get("alias", field)] = sum(values) / len(values) if values else 0
                    elif func == "min":
                        result[agg.get("alias", field)] = min(values) if values else None
                    elif func == "max":
                        result[agg.get("alias", field)] = max(values) if values else None
                    elif func == "count":
                        result[agg.get("alias", field)] = len(values)
                return ActionResult(
                    success=True,
                    data={"aggregated": result},
                    message=f"Aggregated: {len(aggregations)} aggregations"
                )
            else:
                groups = defaultdict(list)
                for item in data:
                    key = item.get(group_by, "unknown") if isinstance(item, dict) else str(item)
                    groups[key].append(item)

                results = []
                for group_key, group_items in groups.items():
                    group_result = {group_by: group_key}
                    for agg in aggregations:
                        field = agg.get("field", "")
                        func = agg.get("function", "sum")
                        values = [item.get(field, 0) if isinstance(item, dict) else item for item in group_items]

                        if func == "sum":
                            group_result[agg.get("alias", field)] = sum(values)
                        elif func == "avg":
                            group_result[agg.get("alias", field)] = sum(values) / len(values) if values else 0
                        elif func == "count":
                            group_result[agg.get("alias", field)] = len(values)
                    results.append(group_result)

                return ActionResult(
                    success=True,
                    data={
                        "groups": results,
                        "group_count": len(groups),
                        "group_by": group_by
                    },
                    message=f"Aggregated: {len(groups)} groups"
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Data aggregator error: {str(e)}")


class GroupAggregatorAction(BaseAction):
    """Group and aggregate."""
    action_type = "group_aggregator"
    display_name = "分组聚合"
    description = "分组聚合"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            group_fields = params.get("group_fields", [])
            agg_field = params.get("agg_field", "value")
            agg_func = params.get("agg_func", "sum")

            groups = defaultdict(list)
            for item in data:
                if isinstance(item, dict):
                    key = tuple(item.get(f, None) for f in group_fields)
                    groups[key].append(item.get(agg_field, 0))

            results = []
            for key, values in groups.items():
                if agg_func == "sum":
                    agg_value = sum(values)
                elif agg_func == "avg":
                    agg_value = sum(values) / len(values)
                elif agg_func == "count":
                    agg_value = len(values)
                elif agg_func == "min":
                    agg_value = min(values)
                elif agg_func == "max":
                    agg_value = max(values)
                else:
                    agg_value = sum(values)

                result = dict(zip(group_fields, key))
                result["agg"] = agg_value
                results.append(result)

            return ActionResult(
                success=True,
                data={
                    "group_count": len(results),
                    "results": results
                },
                message=f"Group aggregation: {len(results)} groups"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Group aggregator error: {str(e)}")


class WindowAggregatorAction(BaseAction):
    """Window aggregation."""
    action_type = "window_aggregator"
    display_name = "窗口聚合"
    description = "窗口函数聚合"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            window_size = params.get("window_size", 3)
            agg_func = params.get("agg_func", "avg")
            field = params.get("field", "value")

            values = [item.get(field, 0) if isinstance(item, dict) else item for item in data]

            results = []
            for i in range(len(values)):
                window = values[max(0, i - window_size + 1):i + 1]

                if agg_func == "avg":
                    window_val = sum(window) / len(window)
                elif agg_func == "sum":
                    window_val = sum(window)
                elif agg_func == "min":
                    window_val = min(window)
                elif agg_func == "max":
                    window_val = max(window)
                elif agg_func == "count":
                    window_val = len(window)
                else:
                    window_val = sum(window) / len(window)

                results.append(window_val)

            return ActionResult(
                success=True,
                data={
                    "window_size": window_size,
                    "windowed_values": results,
                    "original_size": len(data)
                },
                message=f"Window aggregation: size={window_size}, func={agg_func}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Window aggregator error: {str(e)}")
