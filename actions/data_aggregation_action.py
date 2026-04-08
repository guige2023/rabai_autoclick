"""Data aggregation action module for RabAI AutoClick.

Provides data aggregation operations:
- GroupAggregationAction: Group and aggregate data
- PivotTableAction: Create pivot table aggregations
- TimeSeriesAggregationAction: Time-series aggregation
- MultiDimensionalAggregationAction: Multi-dimensional data aggregation
"""

from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class GroupAggregationAction(BaseAction):
    """Group and aggregate data."""
    action_type = "group_aggregation"
    display_name = "分组聚合"
    description = "对数据进行分组和聚合"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            group_by = params.get("group_by", [])
            aggregations = params.get("aggregations", [])
            having = params.get("having", None)

            if not isinstance(data, list):
                return ActionResult(success=False, message="data must be a list")

            if not group_by:
                return ActionResult(success=False, message="group_by is required")

            if not aggregations:
                return ActionResult(success=False, message="aggregations is required")

            groups: Dict[Tuple, List[Dict]] = defaultdict(list)
            for item in data:
                if not isinstance(item, dict):
                    continue
                key = tuple(item.get(g, None) for g in group_by)
                groups[key].append(item)

            results = []
            for group_key, group_items in groups.items():
                row = {g: group_key[i] for i, g in enumerate(group_by)}

                for agg in aggregations:
                    field = agg.get("field")
                    func = agg.get("func", "sum")
                    alias = agg.get("alias", f"{func}_{field}")

                    values = [item.get(field, 0) for item in group_items if field in item]
                    values = [v for v in values if isinstance(v, (int, float))]

                    if func == "sum":
                        row[alias] = sum(values) if values else 0
                    elif func == "avg":
                        row[alias] = sum(values) / len(values) if values else 0
                    elif func == "count":
                        row[alias] = len(values)
                    elif func == "min":
                        row[alias] = min(values) if values else None
                    elif func == "max":
                        row[alias] = max(values) if values else None
                    elif func == "first":
                        row[alias] = values[0] if values else None
                    elif func == "last":
                        row[alias] = values[-1] if values else None
                    elif func == "count_all":
                        row[alias] = len(group_items)
                    elif func == "collect":
                        row[alias] = values

                if having:
                    condition_field = having.get("field")
                    condition_op = having.get("operator")
                    condition_value = having.get("value")
                    row_value = row.get(condition_field)
                    should_include = False
                    if condition_op == "gt":
                        should_include = row_value > condition_value
                    elif condition_op == "ge":
                        should_include = row_value >= condition_value
                    elif condition_op == "lt":
                        should_include = row_value < condition_value
                    elif condition_op == "le":
                        should_include = row_value <= condition_value
                    elif condition_op == "eq":
                        should_include = row_value == condition_value
                    if not should_include:
                        continue

                results.append(row)

            return ActionResult(
                success=True,
                message=f"Grouped into {len(results)} groups",
                data={"results": results, "group_count": len(results)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"GroupAggregation error: {e}")


class PivotTableAction(BaseAction):
    """Create pivot table aggregations."""
    action_type = "pivot_table"
    display_name = "透视表"
    description = "创建透视表聚合"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            rows = params.get("rows", [])
            columns = params.get("columns", [])
            values = params.get("values", [])
            aggregation = params.get("aggregation", "sum")
            fill_value = params.get("fill_value", 0)

            if not isinstance(data, list):
                return ActionResult(success=False, message="data must be a list")

            if not rows or not values:
                return ActionResult(success=False, message="rows and values are required")

            pivot: Dict[Tuple, Dict[Tuple, float]] = defaultdict(lambda: defaultdict(float))

            for item in data:
                if not isinstance(item, dict):
                    continue
                row_key = tuple(item.get(r) for r in rows)
                col_key = tuple(item.get(c) for c in columns) if columns else (None,)

                for val_spec in values:
                    field = val_spec if isinstance(val_spec, str) else val_spec.get("field")
                    val = item.get(field, 0)
                    if isinstance(val, (int, float)):
                        if aggregation == "sum":
                            pivot[row_key][col_key] += val
                        elif aggregation == "count":
                            pivot[row_key][col_key] += 1
                        elif aggregation == "avg":
                            pivot[row_key][("__avg_count",) + col_key] = pivot[row_key].get(("__avg_count",) + col_key, 0) + 1
                            pivot[row_key][col_key] = (pivot[row_key].get(col_key, 0) * (pivot[row_key].get(("__avg_count",) + col_key, 1) - 1) + val) / pivot[row_key].get(("__avg_count",) + col_key, 1)

            all_col_keys = sorted(set(col for row in pivot.values() for col in row.keys() if col != (None,) and not col[0].startswith("__")))

            results = []
            for row_key in sorted(pivot.keys()):
                row_data = {rows[i]: row_key[i] for i in range(len(rows))}
                for col_key in all_col_keys:
                    col_label = "_".join(str(k) for k in col_key) if col_key != (None,) else "total"
                    row_data[col_label] = pivot[row_key].get(col_key, fill_value)
                results.append(row_data)

            return ActionResult(
                success=True,
                message=f"Pivot table: {len(results)} rows",
                data={"pivot_table": results, "row_count": len(results), "column_count": len(all_col_keys)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"PivotTable error: {e}")


class TimeSeriesAggregationAction(BaseAction):
    """Time-series aggregation."""
    action_type = "time_series_aggregation"
    display_name = "时间序列聚合"
    description = "时间序列数据聚合"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            timestamp_field = params.get("timestamp_field", "timestamp")
            value_field = params.get("value_field", "value")
            interval = params.get("interval", "1h")
            aggregation = params.get("aggregation", "sum")
            timezone_str = params.get("timezone", "UTC")

            if not isinstance(data, list):
                return ActionResult(success=False, message="data must be a list")

            interval_seconds = self._parse_interval(interval)
            buckets: Dict[int, List[float]] = defaultdict(list)

            for item in data:
                if not isinstance(item, dict):
                    continue
                ts = item.get(timestamp_field)
                val = item.get(value_field, 0)

                if isinstance(ts, (int, float)):
                    dt = datetime.fromtimestamp(ts, tz=None)
                elif isinstance(ts, str):
                    try:
                        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    except Exception:
                        continue
                else:
                    continue

                if not isinstance(val, (int, float)):
                    continue

                bucket_key = int(dt.timestamp() // interval_seconds) * interval_seconds
                buckets[bucket_key].append(val)

            sorted_keys = sorted(buckets.keys())
            results = []
            for key in sorted_keys:
                values = buckets[key]
                ts = datetime.fromtimestamp(key)
                result = {"timestamp": ts.isoformat(), "bucket_start": key}

                if aggregation == "sum":
                    result["value"] = sum(values)
                elif aggregation == "avg":
                    result["value"] = sum(values) / len(values) if values else 0
                elif aggregation == "min":
                    result["value"] = min(values) if values else None
                elif aggregation == "max":
                    result["value"] = max(values) if values else None
                elif aggregation == "count":
                    result["value"] = len(values)
                elif aggregation == "first":
                    result["value"] = values[0] if values else None
                elif aggregation == "last":
                    result["value"] = values[-1] if values else None

                results.append(result)

            return ActionResult(
                success=True,
                message=f"Time-series: {len(results)} buckets",
                data={"series": results, "bucket_count": len(results), "interval": interval},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"TimeSeriesAggregation error: {e}")

    def _parse_interval(self, interval: str) -> int:
        units = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
        if interval.endswith(tuple(units.keys())):
            num = int(interval[:-1])
            unit = interval[-1]
            return num * units.get(unit, 1)
        return 3600


class MultiDimensionalAggregationAction(BaseAction):
    """Multi-dimensional data aggregation (OLAP-style)."""
    action_type = "multidimensional_aggregation"
    display_name = "多维聚合"
    description = "多维数据聚合(OLAP风格)"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            dimensions = params.get("dimensions", [])
            measures = params.get("measures", [])
            rollup = params.get("rollup", False)

            if not isinstance(data, list):
                return ActionResult(success=False, message="data must be a list")

            if not dimensions or not measures:
                return ActionResult(success=False, message="dimensions and measures are required")

            def aggregate(group_key: Tuple, items: List[Dict]) -> Dict:
                result = {dim: group_key[i] for i, dim in enumerate(dimensions)}
                for measure in measures:
                    field = measure.get("field")
                    func = measure.get("func", "sum")
                    alias = measure.get("alias", f"{func}_{field}")

                    values = [item.get(field, 0) for item in items if field in item and isinstance(item.get(field), (int, float))]

                    if func == "sum":
                        result[alias] = round(sum(values), 4) if values else 0
                    elif func == "avg":
                        result[alias] = round(sum(values) / len(values), 4) if values else 0
                    elif func == "count":
                        result[alias] = len(values)
                    elif func == "min":
                        result[alias] = min(values) if values else None
                    elif func == "max":
                        result[alias] = max(values) if values else None

                return result

            cells: Dict[Tuple, List[Dict]] = defaultdict(list)
            for item in data:
                if isinstance(item, dict):
                    key = tuple(item.get(d) for d in dimensions)
                    cells[key].append(item)

            results = [aggregate(k, v) for k, v in cells.items()]

            if rollup:
                for i, dim in enumerate(dimensions):
                    subtotal_key = [None] * len(dimensions)
                    subtotal_key[i] = slice(None)
                    subtotal_values = []
                    for key, items in cells.items():
                        if key[i] is not None:
                            subtotal_values.extend(items)
                    if subtotal_values:
                        sub_key = [None] * len(dimensions)
                        sub_key[i] = "__TOTAL__"
                        results.append(aggregate(tuple(sub_key), subtotal_values))

                total_values = []
                for items in cells.values():
                    total_values.extend(items)
                if total_values:
                    total_key = tuple("__TOTAL__" for _ in dimensions)
                    results.append(aggregate(total_key, total_values))

            return ActionResult(
                success=True,
                message=f"Multi-dimensional: {len(results)} cells",
                data={"results": results, "cell_count": len(results), "dimensions": dimensions, "measures": measures},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"MultiDimensionalAggregation error: {e}")
