"""CSV action module for RabAI AutoClick.

Provides CSV file operations:
- CSVReadAction: Read CSV files with customizable delimiters and encodings
- CSVWriteAction: Write data to CSV files
- CSVFilterAction: Filter CSV rows by conditions
- CSVSortAction: Sort CSV data by columns
- CSVJoinAction: Join two CSV files on a key column
- CSVStatsAction: Compute statistics on CSV columns
"""

from typing import Any, Dict, List, Optional, Union
import csv
import os
import sys

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CSVReadAction(BaseAction):
    """Read CSV file into list of dicts."""
    action_type = "csv_read"
    display_name = "CSV读取"
    description = "读取CSV文件并返回列表数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute CSV read operation.

        Args:
            context: Execution context.
            params: Dict with file_path, delimiter, encoding, has_header, output_var.

        Returns:
            ActionResult with rows data.
        """
        file_path = params.get('file_path', '')
        delimiter = params.get('delimiter', ',')
        encoding = params.get('encoding', 'utf-8')
        has_header = params.get('has_header', True)
        output_var = params.get('output_var', 'csv_data')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_delimiter = context.resolve_value(delimiter)
            resolved_encoding = context.resolve_value(encoding)

            rows = []
            with open(resolved_path, 'r', encoding=resolved_encoding, newline='') as f:
                reader = csv.reader(f, delimiter=resolved_delimiter)
                for row in reader:
                    rows.append(row)

            if has_header and len(rows) > 0:
                header = rows[0]
                data_dicts = []
                for row in rows[1:]:
                    row_dict = {}
                    for i, val in enumerate(row):
                        key = header[i] if i < len(header) else f'col_{i}'
                        row_dict[key] = val
                    data_dicts.append(row_dict)
                context.set(output_var, data_dicts)
                context.set(f'{output_var}_header', header)
                return ActionResult(success=True, data=data_dicts,
                                   message=f"Read {len(data_dicts)} rows with header")
            else:
                context.set(output_var, rows)
                return ActionResult(success=True, data=rows,
                                   message=f"Read {len(rows)} rows")

        except FileNotFoundError:
            return ActionResult(success=False, message=f"File not found: {resolved_path}")
        except Exception as e:
            return ActionResult(success=False, message=f"CSV read error: {str(e)}")


class CSVWriteAction(BaseAction):
    """Write data to CSV file."""
    action_type = "csv_write"
    display_name = "CSV写入"
    description = "将数据写入CSV文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute CSV write operation.

        Args:
            context: Execution context.
            params: Dict with file_path, data, delimiter, encoding, headers.

        Returns:
            ActionResult with write status.
        """
        file_path = params.get('file_path', '')
        data = params.get('data', [])
        delimiter = params.get('delimiter', ',')
        encoding = params.get('encoding', 'utf-8')
        headers = params.get('headers', None)
        output_var = params.get('output_var', 'csv_write_result')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_data = context.resolve_value(data)
            resolved_delimiter = context.resolve_value(delimiter)
            resolved_encoding = context.resolve_value(encoding)

            os.makedirs(os.path.dirname(resolved_path) or '.', exist_ok=True)

            with open(resolved_path, 'w', encoding=resolved_encoding, newline='') as f:
                writer = csv.writer(f, delimiter=resolved_delimiter)

                if headers:
                    resolved_headers = context.resolve_value(headers)
                    if isinstance(resolved_headers, list):
                        writer.writerow(resolved_headers)

                if isinstance(resolved_data, list):
                    for row in resolved_data:
                        if isinstance(row, dict):
                            writer.writerow([row.get(h, '') for h in (resolved_headers or [])])
                        elif isinstance(row, (list, tuple)):
                            writer.writerow(row)
                        else:
                            writer.writerow([row])

            context.set(output_var, True)
            return ActionResult(success=True, data=True,
                               message=f"Wrote CSV to {resolved_path}")

        except Exception as e:
            return ActionResult(success=False, message=f"CSV write error: {str(e)}")


class CSVFilterAction(BaseAction):
    """Filter CSV rows by condition."""
    action_type = "csv_filter"
    display_name = "CSV筛选"
    description = "按条件筛选CSV行数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute CSV filter operation.

        Args:
            context: Execution context.
            params: Dict with data, condition, output_var.

        Returns:
            ActionResult with filtered rows.
        """
        data = params.get('data', [])
        condition = params.get('condition', '')
        output_var = params.get('output_var', 'csv_filtered')

        if not condition:
            return ActionResult(success=False, message="condition is required")

        try:
            resolved_data = context.resolve_value(data)
            resolved_cond = context.resolve_value(condition)

            if not isinstance(resolved_data, list):
                return ActionResult(success=False, message="data must be a list")

            filtered = []
            for idx, row in enumerate(resolved_data):
                context.set('_filter_row', row)
                context.set('_filter_idx', idx)
                try:
                    if context.safe_exec(f"return_value = {resolved_cond}"):
                        filtered.append(row)
                except Exception:
                    continue

            context.set(output_var, filtered)
            return ActionResult(success=True, data=filtered,
                               message=f"Filtered to {len(filtered)} rows")

        except Exception as e:
            return ActionResult(success=False, message=f"CSV filter error: {str(e)}")


class CSVSortAction(BaseAction):
    """Sort CSV data by column."""
    action_type = "csv_sort"
    display_name = "CSV排序"
    description = "按指定列排序CSV数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute CSV sort operation.

        Args:
            context: Execution context.
            params: Dict with data, sort_by, reverse, output_var.

        Returns:
            ActionResult with sorted data.
        """
        data = params.get('data', [])
        sort_by = params.get('sort_by', '')
        reverse = params.get('reverse', False)
        output_var = params.get('output_var', 'csv_sorted')

        if not sort_by:
            return ActionResult(success=False, message="sort_by column is required")

        try:
            resolved_data = context.resolve_value(data)
            resolved_sort_by = context.resolve_value(sort_by)
            resolved_reverse = context.resolve_value(reverse)

            if not isinstance(resolved_data, list):
                return ActionResult(success=False, message="data must be a list")

            def get_sort_key(row):
                if isinstance(row, dict):
                    return row.get(resolved_sort_by, '')
                elif isinstance(row, (list, tuple)):
                    try:
                        return row[0]
                    except (IndexError, ValueError):
                        return ''
                return row

            sorted_data = sorted(resolved_data, key=get_sort_key, reverse=resolved_reverse)

            context.set(output_var, sorted_data)
            return ActionResult(success=True, data=sorted_data,
                               message=f"Sorted {len(sorted_data)} rows by {resolved_sort_by}")

        except Exception as e:
            return ActionResult(success=False, message=f"CSV sort error: {str(e)}")


class CSVJoinAction(BaseAction):
    """Join two CSV files on a key column."""
    action_type = "csv_join"
    display_name = "CSV关联"
    description = "基于键列关联两个CSV数据源"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute CSV join operation.

        Args:
            context: Execution context.
            params: Dict with left_data, right_data, left_key, right_key, join_type, output_var.

        Returns:
            ActionResult with joined data.
        """
        left_data = params.get('left_data', [])
        right_data = params.get('right_data', [])
        left_key = params.get('left_key', 'id')
        right_key = params.get('right_key', 'id')
        join_type = params.get('join_type', 'inner')
        output_var = params.get('output_var', 'csv_joined')

        try:
            resolved_left = context.resolve_value(left_data)
            resolved_right = context.resolve_value(right_data)
            resolved_left_key = context.resolve_value(left_key)
            resolved_right_key = context.resolve_value(right_key)
            resolved_join_type = context.resolve_value(join_type)

            if not isinstance(resolved_left, list) or not isinstance(resolved_right, list):
                return ActionResult(success=False, message="Both data must be lists")

            right_index = {}
            for row in resolved_right:
                if isinstance(row, dict):
                    key_val = row.get(resolved_right_key, '')
                elif isinstance(row, (list, tuple)):
                    key_val = row[0] if row else ''
                else:
                    key_val = row
                right_index[key_val] = row

            joined = []
            for left_row in resolved_left:
                if isinstance(left_row, dict):
                    key_val = left_row.get(resolved_left_key, '')
                elif isinstance(left_row, (list, tuple)):
                    key_val = left_row[0] if left_row else ''
                else:
                    key_val = left_row

                if key_val in right_index:
                    right_row = right_index[key_val]
                    if isinstance(left_row, dict) and isinstance(right_row, dict):
                        merged = {**right_row, **left_row}
                        joined.append(merged)
                    else:
                        joined.append((left_row, right_row))
                elif resolved_join_type == 'left':
                    joined.append(left_row)

            context.set(output_var, joined)
            return ActionResult(success=True, data=joined,
                               message=f"Joined {len(joined)} rows ({resolved_join_type} join)")

        except Exception as e:
            return ActionResult(success=False, message=f"CSV join error: {str(e)}")


class CSVStatsAction(BaseAction):
    """Compute statistics on CSV columns."""
    action_type = "csv_stats"
    display_name = "CSV统计"
    description = "计算CSV列的统计信息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute CSV stats operation.

        Args:
            context: Execution context.
            params: Dict with data, column, stats, output_var.

        Returns:
            ActionResult with statistics.
        """
        data = params.get('data', [])
        column = params.get('column', '')
        stats = params.get('stats', ['count', 'sum', 'avg', 'min', 'max'])
        output_var = params.get('output_var', 'csv_stats')

        try:
            resolved_data = context.resolve_value(data)
            resolved_column = context.resolve_value(column) if column else None
            resolved_stats = context.resolve_value(stats)

            if not isinstance(resolved_data, list):
                return ActionResult(success=False, message="data must be a list")

            values = []
            for row in resolved_data:
                if resolved_column:
                    if isinstance(row, dict):
                        val = row.get(resolved_column, 0)
                    elif isinstance(row, (list, tuple)):
                        val = row[0] if row else 0
                    else:
                        val = 0
                else:
                    val = row
                try:
                    values.append(float(val))
                except (ValueError, TypeError):
                    values.append(0)

            result = {}
            if 'count' in resolved_stats:
                result['count'] = len(values)
            if 'sum' in resolved_stats:
                result['sum'] = sum(values)
            if 'avg' in resolved_stats:
                result['avg'] = sum(values) / len(values) if values else 0
            if 'min' in resolved_stats:
                result['min'] = min(values) if values else 0
            if 'max' in resolved_stats:
                result['max'] = max(values) if values else 0

            context.set(output_var, result)
            return ActionResult(success=True, data=result,
                               message=f"Computed stats for {len(values)} values")

        except Exception as e:
            return ActionResult(success=False, message=f"CSV stats error: {str(e)}")
