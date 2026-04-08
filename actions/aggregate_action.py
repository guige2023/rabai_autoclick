"""Aggregation engine action module for RabAI AutoClick.

Provides data aggregation operations:
- AggregateSumAction: Sum aggregation
- AggregateGroupAction: Group by aggregation
- AggregateWindowAction: Window-based aggregation
- AggregateHistogramAction: Histogram aggregation
"""

from typing import Any, Callable, Dict, List, Optional
from collections import defaultdict
from datetime import datetime


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AggregateSumAction(BaseAction):
    """Sum aggregation."""
    action_type = "aggregate_sum"
    display_name = "求和聚合"
    description = "对数值字段求和"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "")
            group_by = params.get("group_by", None)
            filter_expr = params.get("filter", None)

            if not data:
                return ActionResult(success=False, message="data is required")

            filtered = data
            if filter_expr:
                column = filter_expr.get("column", "")
                operator = filter_expr.get("operator", "==")
                value = filter_expr.get("value", None)
                if column:
                    if operator == "==":
                        filtered = [r for r in filtered if r.get(column) == value]
                    elif operator == "!=":
                        filtered = [r for r in filtered if r.get(column) != value]
                    elif operator == ">":
                        filtered = [r for r in filtered if r.get(column) is not None and r.get(column) > value]
                    elif operator == "<":
                        filtered = [r for r in filtered if r.get(column) is not None and r.get(column) < value]

            if group_by:
                groups = defaultdict(list)
                for r in filtered:
                    key = tuple(r.get(g) for g in group_by)
                    groups[key].append(r)
                result = {}
                for keys, records in groups.items():
                    if field:
                        try:
                            total = sum(float(r.get(field, 0)) for r in records)
                            result[keys if len(group_by) > 1 else keys[0]] = total
                        except (ValueError, TypeError):
                            result[keys if len(group_by) > 1 else keys[0]] = 0
                return ActionResult(
                    success=True,
                    message=f"Grouped sum: {len(result)} groups",
                    data={"groups": result, "group_by": group_by, "field": field}
                )
            else:
                if field:
                    try:
                        total = sum(float(r.get(field, 0)) for r in filtered)
                    except (ValueError, TypeError):
                        total = 0
                else:
                    total = len(filtered)
                return ActionResult(
                    success=True,
                    message=f"Sum: {total}",
                    data={"sum": total, "count": len(filtered)}
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Aggregate sum failed: {str(e)}")


class AggregateGroupAction(BaseAction):
    """Group by aggregation."""
    action_type = "aggregate_group"
    display_name = "分组聚合"
    description = "分组聚合数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            group_by = params.get("group_by", [])
            aggregations = params.get("aggregations", [])

            if not data:
                return ActionResult(success=False, message="data is required")
            if not group_by:
                return ActionResult(success=False, message="group_by is required")
            if not aggregations:
                return ActionResult(success=False, message="aggregations is required")

            groups = defaultdict(list)
            for r in data:
                key = tuple(r.get(g) for g in group_by)
                groups[key].append(r)

            results = []
            for keys, records in groups.items():
                row = {g: k for g, k in zip(group_by, keys)}
                for agg in aggregations:
                    field_name = agg.get("field", "")
                    func = agg.get("func", "sum")
                    alias = agg.get("alias", f"{field_name}_{func}")

                    values = [r.get(field_name) for r in records if r.get(field_name) is not None]

                    if func == "sum":
                        try:
                            row[alias] = sum(float(v) for v in values)
                        except (ValueError, TypeError):
                            row[alias] = 0
                    elif func == "avg":
                        try:
                            row[alias] = sum(float(v) for v in values) / len(values) if values else 0
                        except (ValueError, TypeError):
                            row[alias] = 0
                    elif func == "count":
                        row[alias] = len(values)
                    elif func == "min":
                        try:
                            row[alias] = min(float(v) for v in values) if values else None
                        except (ValueError, TypeError):
                            row[alias] = None
                    elif func == "max":
                        try:
                            row[alias] = max(float(v) for v in values) if values else None
                        except (ValueError, TypeError):
                            row[alias] = None
                    elif func == "first":
                        row[alias] = values[0] if values else None
                    elif func == "last":
                        row[alias] = values[-1] if values else None

                results.append(row)

            return ActionResult(
                success=True,
                message=f"Grouped into {len(results)} groups",
                data={"results": results, "group_count": len(results)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Aggregate group failed: {str(e)}")


class AggregateWindowAction(BaseAction):
    """Window-based aggregation."""
    action_type = "aggregate_window"
    display_name = "窗口聚合"
    description = "基于窗口的聚合"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            window_size = params.get("window_size", 5)
            field = params.get("field", "")
            func = params.get("func", "avg")

            if not data:
                return ActionResult(success=False, message="data is required")

            result = []
            for i in range(len(data)):
                window_start = max(0, i - window_size + 1)
                window = data[window_start:i + 1]
                if field:
                    values = [float(w.get(field, 0)) for w in window if w.get(field) is not None]
                else:
                    values = [float(w) for w in window if w is not None]

                if func == "avg":
                    val = sum(values) / len(values) if values else 0
                elif func == "sum":
                    val = sum(values) if values else 0
                elif func == "min":
                    val = min(values) if values else 0
                elif func == "max":
                    val = max(values) if values else 0
                elif func == "count":
                    val = len(values)
                else:
                    val = None

                result.append({
                    "index": i,
                    "window_start": window_start,
                    "window_end": i,
                    "window_size": len(window),
                    "value": val,
                    "original": data[i]
                })

            return ActionResult(
                success=True,
                message=f"Window aggregation: {len(result)} rows",
                data={"results": result, "window_size": window_size}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Aggregate window failed: {str(e)}")


class AggregateHistogramAction(BaseAction):
    """Histogram aggregation."""
    action_type = "aggregate_histogram"
    display_name = "直方图聚合"
    description = "生成直方图分布"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "")
            bins = params.get("bins", 10)
            bin_width = params.get("bin_width", None)

            if not data:
                return ActionResult(success=False, message="data is required")

            values = []
            for r in data:
                if field:
                    v = r.get(field)
                    if v is not None:
                        try:
                            values.append(float(v))
                        except (ValueError, TypeError):
                            pass
                else:
                    try:
                        values.append(float(r))
                    except (ValueError, TypeError):
                        pass

            if not values:
                return ActionResult(success=False, message="No numeric values found")

            min_val = min(values)
            max_val = max(values)

            if bin_width:
                num_bins = max(1, int((max_val - min_val) / bin_width))
            else:
                num_bins = bins

            bin_width = (max_val - min_val) / num_bins if num_bins > 0 else 1
            histogram = [0] * num_bins
            bin_edges = [min_val + i * bin_width for i in range(num_bins + 1)]

            for v in values:
                bin_idx = min(int((v - min_val) / bin_width), num_bins - 1)
                histogram[bin_idx] += 1

            bins_data = [
                {
                    "bin_start": bin_edges[i],
                    "bin_end": bin_edges[i + 1],
                    "count": histogram[i],
                    "bin_center": (bin_edges[i] + bin_edges[i + 1]) / 2
                }
                for i in range(num_bins)
            ]

            return ActionResult(
                success=True,
                message=f"Histogram with {num_bins} bins",
                data={
                    "histogram": bins_data,
                    "min": min_val,
                    "max": max_val,
                    "mean": sum(values) / len(values),
                    "total_count": len(values)
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Aggregate histogram failed: {str(e)}")
