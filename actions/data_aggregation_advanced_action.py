"""Data aggregation advanced action module for RabAI AutoClick.

Provides advanced aggregation operations:
- AggregateGroupByAction: Group by aggregation
- AggregateWindowAction: Window aggregation
- AggregateRollupAction: Rollup aggregation
- AggregateCubeAction: Cube aggregation
- AggregateHistogramAction: Histogram aggregation
"""

from typing import Any, Dict, List

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AggregateGroupByAction(BaseAction):
    """Group by aggregation."""
    action_type = "aggregate_group_by"
    display_name = "分组聚合"
    description = "分组聚合操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            group_by = params.get("group_by", [])
            aggregations = params.get("aggregations", [])

            if not data:
                return ActionResult(success=False, message="data is required")
            if not group_by:
                return ActionResult(success=False, message="group_by is required")

            groups: Dict = {}
            for item in data:
                key = tuple(str(item.get(g, "")) for g in group_by)
                if key not in groups:
                    groups[key] = []
                groups[key].append(item)

            results = []
            for key, items in groups.items():
                row = dict(zip(group_by, key))
                for agg in aggregations:
                    field = agg.get("field", "")
                    func = agg.get("func", "sum")
                    values = [i.get(field, 0) for i in items]
                    if func == "sum":
                        row[f"{field}_{func}"] = sum(values)
                    elif func == "avg":
                        row[f"{field}_{func}"] = sum(values) / len(values) if values else 0
                    elif func == "min":
                        row[f"{field}_{func}"] = min(values) if values else None
                    elif func == "max":
                        row[f"{field}_{func}"] = max(values) if values else None
                    elif func == "count":
                        row[f"{field}_{func}"] = len(values)
                results.append(row)

            return ActionResult(
                success=True,
                data={"group_count": len(results), "groups": results},
                message=f"Grouped into {len(results)} groups",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Aggregate group_by failed: {e}")


class AggregateWindowAction(BaseAction):
    """Window aggregation."""
    action_type = "aggregate_window"
    display_name = "窗口聚合"
    description = "窗口聚合操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            window_size = params.get("window_size", 3)
            window_func = params.get("func", "avg")
            field = params.get("field", "value")

            if not data:
                return ActionResult(success=False, message="data is required")

            values = [d.get(field, 0) for d in data]
            results = []
            for i in range(len(values)):
                window = values[max(0, i - window_size + 1) : i + 1]
                if window_func == "avg":
                    window_val = sum(window) / len(window)
                elif window_func == "sum":
                    window_val = sum(window)
                elif window_func == "min":
                    window_val = min(window)
                elif window_func == "max":
                    window_val = max(window)
                else:
                    window_val = window[-1]
                results.append({"index": i, "window_value": window_val})

            return ActionResult(
                success=True,
                data={"result_count": len(results), "window_size": window_size, "func": window_func},
                message=f"Window aggregation: {len(results)} results",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Aggregate window failed: {e}")


class AggregateRollupAction(BaseAction):
    """Rollup aggregation."""
    action_type = "aggregate_rollup"
    display_name = "上卷聚合"
    description = "上卷聚合操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            group_by = params.get("group_by", [])
            measure = params.get("measure", "value")

            if not data:
                return ActionResult(success=False, message="data is required")

            groups: Dict = {}
            total = 0
            for item in data:
                key = tuple(str(item.get(g, "")) for g in group_by)
                val = item.get(measure, 0)
                groups[key] = groups.get(key, 0) + val
                total += val

            rollup_rows = [{"key": k, "measure_sum": v} for k, v in groups.items()]
            rollup_rows.append({"key": "TOTAL", "measure_sum": total})

            return ActionResult(
                success=True,
                data={"rollup_rows": rollup_rows, "group_count": len(rollup_rows) - 1},
                message=f"Rollup: {len(rollup_rows)} levels",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Aggregate rollup failed: {e}")


class AggregateCubeAction(BaseAction):
    """Cube aggregation."""
    action_type = "aggregate_cube"
    display_name = "数据立方"
    description = "数据立方聚合操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            dimensions = params.get("dimensions", [])
            measure = params.get("measure", "value")

            if not data:
                return ActionResult(success=False, message="data is required")

            cube_rows = [{"dimensions": dimensions, "measure_sum": sum(d.get(measure, 0) for d in data)}]

            return ActionResult(
                success=True,
                data={"cube_rows": cube_rows, "dimension_count": len(dimensions)},
                message=f"Cube aggregation: {len(dimensions)} dimensions",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Aggregate cube failed: {e}")


class AggregateHistogramAction(BaseAction):
    """Histogram aggregation."""
    action_type = "aggregate_histogram"
    display_name = "直方图聚合"
    description = "直方图聚合"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            bin_count = params.get("bin_count", 10)

            if not data:
                return ActionResult(success=False, message="data is required")

            values = [d.get(field, 0) for d in data if field in d]
            if not values:
                return ActionResult(success=False, message=f"No values found for field '{field}'")

            min_val = min(values)
            max_val = max(values)
            bin_width = (max_val - min_val) / bin_count if bin_count > 0 else 1

            bins = [0] * bin_count
            for v in values:
                bin_idx = min(int((v - min_val) / bin_width), bin_count - 1) if bin_width > 0 else 0
                bins[bin_idx] += 1

            histogram = [{"bin_start": min_val + i * bin_width, "bin_end": min_val + (i + 1) * bin_width, "count": c} for i, c in enumerate(bins)]

            return ActionResult(
                success=True,
                data={"histogram": histogram, "bin_count": bin_count, "min": min_val, "max": max_val},
                message=f"Histogram: {bin_count} bins, {len(values)} values",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Aggregate histogram failed: {e}")
