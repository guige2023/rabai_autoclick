"""Aggregation action module for RabAI AutoClick.

Provides data aggregation operations:
- AggregateSumAction: Sum values
- AggregateAverageAction: Calculate averages
- AggregateCountAction: Count items
- AggregateGroupAction: Group and aggregate
- AggregatePivotAction: Pivot table aggregation
- AggregateWindowAction: Window functions
"""

import statistics
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AggregateSumAction(BaseAction):
    """Sum values."""
    action_type = "aggregate_sum"
    display_name = "求和"
    description = "计算数值求和"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            values = params.get("values", [])
            field = params.get("field", "")
            records = params.get("records", [])

            if records and field:
                values = [r.get(field, 0) for r in records if isinstance(r, dict)]

            if not values:
                return ActionResult(success=False, message="No values to sum")

            numeric_values = []
            for v in values:
                try:
                    numeric_values.append(float(v))
                except (TypeError, ValueError):
                    pass

            total = sum(numeric_values)
            return ActionResult(
                success=True,
                message=f"Sum of {len(numeric_values)} values: {total}",
                data={"sum": total, "count": len(numeric_values), "values": numeric_values}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Sum error: {str(e)}")


class AggregateAverageAction(BaseAction):
    """Calculate averages."""
    action_type = "aggregate_average"
    display_name = "计算平均值"
    description = "计算平均值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            values = params.get("values", [])
            field = params.get("field", "")
            records = params.get("records", [])

            if records and field:
                values = [r.get(field, 0) for r in records if isinstance(r, dict)]

            if not values:
                return ActionResult(success=False, message="No values to average")

            numeric_values = []
            for v in values:
                try:
                    numeric_values.append(float(v))
                except (TypeError, ValueError):
                    pass

            if not numeric_values:
                return ActionResult(success=False, message="No numeric values found")

            mean = statistics.mean(numeric_values)
            median = statistics.median(numeric_values)
            stdev = statistics.stdev(numeric_values) if len(numeric_values) > 1 else 0

            return ActionResult(
                success=True,
                message=f"Average of {len(numeric_values)} values: {mean:.2f}",
                data={
                    "mean": mean,
                    "median": median,
                    "stdev": stdev,
                    "count": len(numeric_values),
                    "min": min(numeric_values),
                    "max": max(numeric_values)
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Average error: {str(e)}")


class AggregateCountAction(BaseAction):
    """Count items."""
    action_type = "aggregate_count"
    display_name = "计数"
    description = "计数统计"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            field = params.get("field", "")
            records = params.get("records", [])

            if records:
                if field:
                    items = [r.get(field) for r in records if isinstance(r, dict)]
                else:
                    items = records

            if not items:
                return ActionResult(success=False, message="No items to count")

            total = len(items)
            unique_values = list(dict.fromkeys(items))
            unique_count = len(unique_values)

            value_counts = {}
            for item in items:
                key = str(item)
                value_counts[key] = value_counts.get(key, 0) + 1

            top_values = sorted(value_counts.items(), key=lambda x: x[1], reverse=True)[:10]

            return ActionResult(
                success=True,
                message=f"Count: {total} total, {unique_count} unique",
                data={
                    "total": total,
                    "unique": unique_count,
                    "value_counts": dict(value_counts),
                    "top_values": dict(top_values)
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Count error: {str(e)}")


class AggregateGroupAction(BaseAction):
    """Group and aggregate."""
    action_type = "aggregate_group"
    display_name = "分组聚合"
    description = "分组并聚合数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            group_by = params.get("group_by", [])
            aggregations = params.get("aggregations", {})

            if not records:
                return ActionResult(success=False, message="No records to aggregate")

            if isinstance(group_by, str):
                group_by = [group_by]

            groups = {}
            for record in records:
                if not isinstance(record, dict):
                    continue
                key = tuple(record.get(col) for col in group_by)
                if key not in groups:
                    groups[key] = []
                groups[key].append(record)

            results = []
            for key, group_records in groups.items():
                result = dict(zip(group_by, key))

                for field, agg_func in aggregations.items():
                    values = []
                    for record in group_records:
                        val = record.get(field)
                        if val is not None:
                            try:
                                values.append(float(val))
                            except (TypeError, ValueError):
                                pass

                    if agg_func == "sum":
                        result[f"{field}_sum"] = round(sum(values), 4) if values else 0
                    elif agg_func == "avg" or agg_func == "mean":
                        result[f"{field}_avg"] = round(sum(values) / len(values), 4) if values else 0
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
                    elif agg_func == "median":
                        result[f"{field}_median"] = statistics.median(values) if values else None
                    elif agg_func == "std":
                        result[f"{field}_std"] = round(statistics.stdev(values), 4) if len(values) > 1 else 0

                results.append(result)

            results.sort(key=lambda x: x.get(group_by[0], "") if group_by else "")

            return ActionResult(
                success=True,
                message=f"Grouped into {len(results)} groups",
                data={"groups": results, "group_count": len(results)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Group error: {str(e)}")


class AggregatePivotAction(BaseAction):
    """Pivot table aggregation."""
    action_type = "aggregate_pivot"
    display_name = "透视聚合"
    description = "透视表聚合"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            index = params.get("index", [])
            columns = params.get("columns", "")
            values = params.get("values", "")
            agg_func = params.get("agg_func", "sum")

            if not records:
                return ActionResult(success=False, message="No records to pivot")

            if isinstance(index, str):
                index = [index]

            pivot = {}
            for record in records:
                if not isinstance(record, dict):
                    continue

                index_val = tuple(record.get(col) for col in index)
                col_val = record.get(columns, "total")
                val = record.get(values, 0)

                try:
                    val = float(val)
                except (TypeError, ValueError):
                    val = 0

                if index_val not in pivot:
                    pivot[index_val] = {}
                if col_val not in pivot[index_val]:
                    pivot[index_val][col_val] = []
                pivot[index_val][col_val].append(val)

            agg_methods = {
                "sum": sum, "avg": lambda x: sum(x) / len(x) if x else 0,
                "count": len, "min": min, "max": max,
                "first": lambda x: x[0] if x else None,
                "last": lambda x: x[-1] if x else None
            }
            agg_fn = agg_methods.get(agg_func, sum)

            all_columns = sorted(set(col for idx_data in pivot.values() for col in idx_data.keys()))
            result_rows = []

            for index_val in sorted(pivot.keys()):
                row = dict(zip(index, index_val))
                for col in all_columns:
                    vals = pivot[index_val].get(col, [])
                    row[col] = round(agg_fn(vals), 4) if vals else 0
                result_rows.append(row)

            return ActionResult(
                success=True,
                message=f"Pivot table: {len(result_rows)} rows x {len(all_columns)} columns",
                data={"rows": result_rows, "columns": all_columns}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Pivot error: {str(e)}")


class AggregateWindowAction(BaseAction):
    """Window functions."""
    action_type = "aggregate_window"
    display_name = "窗口函数"
    description = "窗口函数计算"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            field = params.get("field", "")
            window_size = params.get("window_size", 3)
            operation = params.get("operation", "mean")
            partition_by = params.get("partition_by", None)

            if not records:
                return ActionResult(success=False, message="No records to process")

            if not field:
                return ActionResult(success=False, message="field required")

            result = []
            current_partition = []
            partition_key = None

            for record in records:
                if not isinstance(record, dict):
                    continue

                if partition_by:
                    new_key = record.get(partition_by)
                    if new_key != partition_key:
                        current_partition = []
                        partition_key = new_key

                val = record.get(field, 0)
                try:
                    val = float(val)
                except:
                    val = 0

                current_partition.append(val)

                if len(current_partition) > window_size:
                    current_partition.pop(0)

                window_result = None
                if operation == "mean":
                    window_result = sum(current_partition) / len(current_partition)
                elif operation == "sum":
                    window_result = sum(current_partition)
                elif operation == "min":
                    window_result = min(current_partition)
                elif operation == "max":
                    window_result = max(current_partition)
                elif operation == "count":
                    window_result = len(current_partition)
                elif operation == "first":
                    window_result = current_partition[0]
                elif operation == "last":
                    window_result = current_partition[-1]

                new_record = dict(record)
                new_record[f"{field}_window_{operation}"] = round(window_result, 4) if window_result is not None else None
                result.append(new_record)

            return ActionResult(
                success=True,
                message=f"Applied {operation} window ({window_size}) to {len(result)} records",
                data={"records": result, "operation": operation, "window_size": window_size}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Window error: {str(e)}")
