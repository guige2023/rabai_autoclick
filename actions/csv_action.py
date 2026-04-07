"""CSV data processing action module for RabAI AutoClick.

Provides CSV operations:
- CsvParseAction: Parse CSV string to records
- CsvEncodeAction: Encode records to CSV string
- CsvFilterAction: Filter CSV rows by condition
- CsvSortAction: Sort CSV by column(s)
- CsvJoinAction: Join multiple CSV datasets
- CsvPivotAction: Pivot CSV data
"""

import csv
import io
from typing import Any, Dict, List, Optional, Callable

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CsvParseAction(BaseAction):
    """Parse CSV string to list of records."""
    action_type = "csv_parse"
    display_name = "CSV解析"
    description = "解析CSV字符串为记录列表"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            csv_str = params.get("csv_str", "")
            delimiter = params.get("delimiter", ",")
            quotechar = params.get("quotechar", '"')
            has_header = params.get("has_header", True)
            skip_rows = params.get("skip_rows", 0)

            if not csv_str:
                return ActionResult(success=False, message="csv_str is required")

            lines = csv_str.strip().split("\n")
            if skip_rows > 0:
                lines = lines[skip_rows:]

            reader = csv.reader(lines, delimiter=delimiter, quotechar=quotechar)
            rows = list(reader)

            if not rows:
                return ActionResult(success=True, message="Empty CSV", data={"records": [], "columns": []})

            if has_header:
                columns = rows[0]
                records = [dict(zip(columns, row)) for row in rows[1:] if len(row) == len(columns)]
                return ActionResult(
                    success=True,
                    message=f"Parsed {len(records)} records",
                    data={"records": records, "columns": columns, "row_count": len(records)}
                )
            else:
                return ActionResult(
                    success=True,
                    message=f"Parsed {len(rows)} rows",
                    data={"rows": rows, "row_count": len(rows)}
                )

        except csv.Error as e:
            return ActionResult(success=False, message=f"CSV parse error: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class CsvEncodeAction(BaseAction):
    """Encode records to CSV string."""
    action_type = "csv_encode"
    display_name = "CSV编码"
    description = "将记录编码为CSV字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            columns = params.get("columns", None)
            delimiter = params.get("delimiter", ",")
            quotechar = params.get("quotechar", '"')
            include_header = params.get("include_header", True)

            if not records and not columns:
                return ActionResult(success=False, message="records or columns is required")

            if not columns and records:
                if isinstance(records[0], dict):
                    columns = list(records[0].keys())

            output = io.StringIO()
            writer = csv.writer(output, delimiter=delimiter, quotechar=quotechar, lineterminator="\n")

            if include_header and columns:
                writer.writerow(columns)

            for record in records:
                if isinstance(record, dict):
                    writer.writerow([record.get(col, "") for col in columns])
                elif isinstance(record, (list, tuple)):
                    writer.writerow(record)

            csv_str = output.getvalue()
            return ActionResult(
                success=True,
                message=f"Encoded {len(records)} records",
                data={"csv_str": csv_str, "length": len(csv_str), "row_count": len(records)}
            )

        except csv.Error as e:
            return ActionResult(success=False, message=f"CSV encode error: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class CsvFilterAction(BaseAction):
    """Filter CSV rows by condition."""
    action_type = "csv_filter"
    display_name = "CSV过滤"
    description = "按条件过滤CSV行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            column = params.get("column", "")
            operator = params.get("operator", "eq")
            value = params.get("value", None)
            expression = params.get("expression", "")

            if not records:
                return ActionResult(success=False, message="records list is required")

            filtered = []

            for record in records:
                if isinstance(record, dict):
                    if expression:
                        try:
                            row_globals = {"row": record}
                            if not eval(expression, {"__builtins__": {}}, row_globals):
                                continue
                        except Exception:
                            continue
                    elif column:
                        cell = record.get(column, "")
                        if not self._compare(cell, operator, value):
                            continue

                    filtered.append(record)

            return ActionResult(
                success=True,
                message=f"Filtered to {len(filtered)} records",
                data={"records": filtered, "original_count": len(records), "filtered_count": len(filtered)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Filter error: {str(e)}")

    def _compare(self, cell: Any, operator: str, value: Any) -> bool:
        """Compare cell value with condition."""
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
            "in": lambda a, b: a in b if isinstance(b, (list, tuple)) else a == b,
            "not_in": lambda a, b: a not in b if isinstance(b, (list, tuple)) else a != b,
        }

        op_func = ops.get(operator, ops["eq"])
        try:
            return op_func(cell, value)
        except (TypeError, ValueError):
            return False


class CsvSortAction(BaseAction):
    """Sort CSV by column(s)."""
    action_type = "csv_sort"
    display_name = "CSV排序"
    description = "按列排序CSV"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            sort_by = params.get("sort_by", [])
            ascending = params.get("ascending", True)

            if not records:
                return ActionResult(success=False, message="records list is required")

            if isinstance(sort_by, str):
                sort_by = [sort_by]

            if not sort_by:
                return ActionResult(success=False, message="sort_by column(s) required")

            def sort_key(record):
                values = []
                for col in sort_by:
                    val = record.get(col, "") if isinstance(record, dict) else record[col] if isinstance(record, (list, tuple)) else record
                    try:
                        values.append(float(val))
                    except (TypeError, ValueError):
                        values.append(str(val).lower())
                return values

            sorted_records = sorted(records, key=sort_key, reverse=not ascending)

            return ActionResult(
                success=True,
                message=f"Sorted {len(sorted_records)} records",
                data={"records": sorted_records}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Sort error: {str(e)}")


class CsvJoinAction(BaseAction):
    """Join multiple CSV datasets."""
    action_type = "csv_join"
    display_name = "CSV连接"
    description = "连接多个CSV数据集"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            datasets = params.get("datasets", [])
            join_type = params.get("join_type", "inner")
            left_key = params.get("left_key", "")
            right_key = params.get("right_key", "")
            suffixes = params.get("suffixes", ["_left", "_right"])

            if not datasets or len(datasets) < 2:
                return ActionResult(success=False, message="At least 2 datasets required")

            left = datasets[0]
            right = datasets[1]

            if not isinstance(left, list) or not isinstance(right, list):
                return ActionResult(success=False, message="Datasets must be lists of records")

            left_columns = list(left[0].keys()) if left else []
            right_columns = list(right[0].keys()) if right else []

            if not left_columns or not right_columns:
                return ActionResult(success=False, message="Datasets must have columns")

            results = []
            right_lookup = {row.get(right_key, ""): row for row in right}

            for left_row in left:
                left_val = left_row.get(left_key, "")
                right_row = right_lookup.get(left_val)

                if right_row is None:
                    if join_type in ("left", "outer"):
                        merged = dict(left_row)
                        for col in right_columns:
                            merged[f"{col}{suffixes[1]}"] = None
                        results.append(merged)
                else:
                    if join_type in ("inner", "left", "outer"):
                        merged = dict(left_row)
                        for col in right_columns:
                            if col == right_key:
                                continue
                            new_col = f"{col}{suffixes[1]}" if col in left_columns else col
                            merged[new_col] = right_row.get(col)
                        results.append(merged)

            if join_type == "outer":
                right_used = set()
                for left_row in left:
                    left_val = left_row.get(left_key, "")
                    if left_val in right_lookup:
                        right_used.add(left_val)

                for right_row in right:
                    right_val = right_row.get(right_key, "")
                    if right_val not in right_used:
                        merged = {}
                        for col in left_columns:
                            merged[col] = None
                        for col in right_columns:
                            if col == right_key:
                                continue
                            new_col = f"{col}{suffixes[1]}" if col in left_columns else col
                            merged[new_col] = right_row.get(col)
                        results.append(merged)

            return ActionResult(
                success=True,
                message=f"Joined to {len(results)} records",
                data={"records": results, "row_count": len(results)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Join error: {str(e)}")


class CsvPivotAction(BaseAction):
    """Pivot CSV data."""
    action_type = "csv_pivot"
    display_name = "CSV透视"
    description = "透视CSV数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            index_col = params.get("index_col", "")
            column_col = params.get("column_col", "")
            value_col = params.get("value_col", "")
            agg_func = params.get("agg_func", "sum")

            if not records:
                return ActionResult(success=False, message="records list is required")

            if not all([index_col, column_col, value_col]):
                return ActionResult(success=False, message="index_col, column_col, value_col required")

            pivot = {}
            index_values = set()

            for record in records:
                if not isinstance(record, dict):
                    continue
                index_val = record.get(index_col, "")
                column_val = record.get(column_col, "")
                cell_val = record.get(value_col, 0)

                try:
                    cell_val = float(cell_val)
                except (TypeError, ValueError):
                    cell_val = 0

                index_values.add(index_val)

                if index_val not in pivot:
                    pivot[index_val] = {}
                if column_val not in pivot[index_val]:
                    pivot[index_val][column_val] = []
                pivot[index_val][column_val].append(cell_val)

            aggs = {"sum": sum, "avg": lambda x: sum(x) / len(x) if x else 0, "count": len, "min": min, "max": max}
            agg_fn = aggs.get(agg_func, sum)

            column_values = sorted(set(col for row_data in pivot.values() for col in row_data.keys()))
            result_records = []

            for index_val in sorted(index_values):
                row = {index_col: index_val}
                if index_val in pivot:
                    for col in column_values:
                        values = pivot[index_val].get(col, [])
                        row[col] = agg_fn(values) if values else None
                result_records.append(row)

            return ActionResult(
                success=True,
                message=f"Pivoted to {len(result_records)} rows",
                data={"records": result_records, "columns": [index_col] + column_values}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Pivot error: {str(e)}")
