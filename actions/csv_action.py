"""
CSV utilities - parsing, filtering, aggregation, transformation, join operations.
"""
from typing import Any, Dict, List, Optional, Tuple, TextIO
import csv
import io
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


def _parse_csv(text: str, delimiter: str = ",", has_header: bool = True) -> Tuple[List[str], List[List[str]]]:
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    rows = list(reader)
    if not rows:
        return [], []
    if has_header:
        return rows[0], rows[1:]
    return [], rows


def _to_dicts(headers: List[str], rows: List[List[str]]) -> List[Dict[str, str]]:
    return [dict(zip(headers, row)) for row in rows]


class CSVAction(BaseAction):
    """CSV operations.

    Provides parsing, filtering, aggregation, column operations, sorting, join.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "parse")
        text = params.get("text", "")
        delimiter = params.get("delimiter", ",")
        headers = params.get("headers", [])
        data = params.get("data", [])

        try:
            if operation == "parse":
                if not text:
                    return {"success": False, "error": "text required"}
                hdrs, rows = _parse_csv(text, delimiter)
                return {"success": True, "headers": hdrs, "rows": rows, "count": len(rows)}

            elif operation == "to_dicts":
                if not headers and not data:
                    hdrs, rows = _parse_csv(text, delimiter)
                else:
                    hdrs, rows = headers, data
                dicts = _to_dicts(hdrs, rows)
                return {"success": True, "data": dicts, "count": len(dicts)}

            elif operation == "filter":
                hdrs, rows = _parse_csv(text, delimiter)
                if not hdrs:
                    return {"success": False, "error": "No data"}
                column = params.get("column", "")
                value = params.get("value", "")
                if column not in hdrs:
                    return {"success": False, "error": f"Column not found: {column}"}
                col_idx = hdrs.index(column)
                filtered = [row for row in rows if len(row) > col_idx and row[col_idx] == value]
                return {"success": True, "headers": hdrs, "rows": filtered, "count": len(filtered)}

            elif operation == "sort":
                hdrs, rows = _parse_csv(text, delimiter)
                if not hdrs:
                    return {"success": False, "error": "No data"}
                sort_col = params.get("column", hdrs[0])
                reverse = params.get("reverse", False)
                if sort_col not in hdrs:
                    return {"success": False, "error": f"Column not found: {sort_col}"}
                col_idx = hdrs.index(sort_col)
                sorted_rows = sorted(rows, key=lambda r: r[col_idx] if len(r) > col_idx else "", reverse=reverse)
                return {"success": True, "headers": hdrs, "rows": sorted_rows, "count": len(sorted_rows)}

            elif operation == "select_columns":
                hdrs, rows = _parse_csv(text, delimiter)
                selected = params.get("columns", [])
                if not selected:
                    return {"success": False, "error": "columns required"}
                indices = [hdrs.index(c) for c in selected if c in hdrs]
                new_rows = [[row[i] if i < len(row) else "" for i in indices] for row in rows]
                return {"success": True, "headers": selected, "rows": new_rows, "count": len(new_rows)}

            elif operation == "aggregate":
                hdrs, rows = _parse_csv(text, delimiter)
                if not hdrs:
                    return {"success": False, "error": "No data"}
                group_col = params.get("group_by", "")
                agg_col = params.get("column", "")
                agg_func = params.get("func", "count")
                if not group_col or not agg_col:
                    return {"success": False, "error": "group_by and column required"}
                if group_col not in hdrs or agg_col not in hdrs:
                    return {"success": False, "error": "Column not found"}
                g_idx = hdrs.index(group_col)
                a_idx = hdrs.index(agg_col)
                groups: Dict[str, List[float]] = defaultdict(list)
                for row in rows:
                    if len(row) > max(g_idx, a_idx):
                        try:
                            groups[row[g_idx]].append(float(row[a_idx]))
                        except ValueError:
                            groups[row[g_idx]].append(0)
                results = []
                for key, values in groups.items():
                    if agg_func == "count":
                        val = len(values)
                    elif agg_func == "sum":
                        val = sum(values)
                    elif agg_func == "avg":
                        val = sum(values) / len(values) if values else 0
                    elif agg_func == "min":
                        val = min(values) if values else 0
                    elif agg_func == "max":
                        val = max(values) if values else 0
                    else:
                        val = len(values)
                    results.append({group_col: key, agg_col: val})
                return {"success": True, "data": results, "count": len(results)}

            elif operation == "join":
                left_text = text
                right_text = params.get("right", "")
                left_hdrs, left_rows = _parse_csv(left_text, delimiter)
                right_hdrs, right_rows = _parse_csv(right_text, delimiter)
                left_key = params.get("left_key", left_hdrs[0] if left_hdrs else "")
                right_key = params.get("right_key", right_hdrs[0] if right_hdrs else "")
                join_type = params.get("join_type", "inner")
                if left_key not in left_hdrs or right_key not in right_hdrs:
                    return {"success": False, "error": "Key columns not found"}
                lk_idx, rk_idx = left_hdrs.index(left_key), right_hdrs.index(right_key)
                right_index = {row[rk_idx]: row for row in right_rows if len(row) > rk_idx}
                new_headers = left_hdrs + [h for h in right_hdrs if h != right_key]
                joined = []
                for left_row in left_rows:
                    if len(left_row) > lk_idx and left_row[lk_idx] in right_index:
                        right_row = right_index[left_row[lk_idx]]
                        merged = left_row + [right_row[i] for i in range(len(right_row)) if i != rk_idx]
                        joined.append(merged)
                    elif join_type == "left":
                        merged = left_row + ["" for _ in right_hdrs if _ != right_key]
                        joined.append(merged)
                return {"success": True, "headers": new_headers, "rows": joined, "count": len(joined)}

            elif operation == "deduplicate":
                hdrs, rows = _parse_csv(text, delimiter)
                if not hdrs:
                    return {"success": False, "error": "No data"}
                subset = params.get("subset", hdrs)
                indices = [hdrs.index(c) for c in subset if c in hdrs]
                seen = set()
                unique = []
                removed = 0
                for row in rows:
                    key = tuple(row[i] if i < len(row) else "" for i in indices)
                    if key not in seen:
                        seen.add(key)
                        unique.append(row)
                    else:
                        removed += 1
                return {"success": True, "headers": hdrs, "rows": unique, "count": len(unique), "removed": removed}

            elif operation == "build":
                hdrs = params.get("headers", headers)
                rows = params.get("rows", data)
                output = io.StringIO()
                writer = csv.writer(output, delimiter=delimiter)
                writer.writerow(hdrs)
                writer.writerows(rows)
                return {"success": True, "csv": output.getvalue(), "rows": len(rows), "headers": hdrs}

            elif operation == "column_stats":
                hdrs, rows = _parse_csv(text, delimiter)
                column = params.get("column", hdrs[0] if hdrs else "")
                if column not in hdrs:
                    return {"success": False, "error": f"Column not found: {column}"}
                col_idx = hdrs.index(column)
                values = []
                for row in rows:
                    if len(row) > col_idx and row[col_idx]:
                        values.append(row[col_idx])
                return {"success": True, "column": column, "count": len(values), "unique": len(set(values))}

            elif operation == "export_json":
                hdrs, rows = _parse_csv(text, delimiter)
                dicts = _to_dicts(hdrs, rows)
                import json
                return {"success": True, "json": json.dumps(dicts, indent=2), "count": len(dicts)}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"CSVAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for CSV operations."""
    return CSVAction().execute(context, params)
