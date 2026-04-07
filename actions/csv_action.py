"""CSV data processing action module for RabAI AutoClick.

Provides CSV file operations:
- CsvReadAction: Read CSV file into list of dicts
- CsvWriteAction: Write data to CSV file
- CsvAppendAction: Append rows to existing CSV
- CsvFilterAction: Filter CSV rows by condition
- CsvMergeAction: Merge multiple CSV files
- CsvSortAction: Sort CSV by column
- CsvDedupeAction: Remove duplicate rows
- CsvStatsAction: Compute statistics on CSV data
"""

from __future__ import annotations

import csv
import os
from typing import Any, Dict, List, Optional

import sys
import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CsvReadAction(BaseAction):
    """Read CSV file into list of dictionaries."""
    action_type = "csv_read"
    display_name = "读取CSV文件"
    description = "将CSV文件读取为字典列表"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute CSV read."""
        file_path = params.get('file_path', '')
        encoding = params.get('encoding', 'utf-8')
        delimiter = params.get('delimiter', ',')
        has_header = params.get('has_header', True)
        output_var = params.get('output_var', 'csv_data')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            resolved_path = context.resolve_value(file_path) if context else file_path
            resolved_encoding = context.resolve_value(encoding) if context else encoding
            resolved_delimiter = context.resolve_value(delimiter) if context else delimiter

            rows = []
            with open(resolved_path, 'r', encoding=resolved_encoding, newline='') as f:
                if has_header:
                    reader = csv.DictReader(f, delimiter=resolved_delimiter)
                    rows = list(reader)
                else:
                    reader = csv.reader(f, delimiter=resolved_delimiter)
                    rows = [{f'col_{i}': v for i, v in enumerate(row)} for row in reader]

            result = {'rows': rows, 'count': len(rows)}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Read {len(rows)} rows", data=result)
        except FileNotFoundError:
            return ActionResult(success=False, message=f"File not found: {resolved_path}")
        except Exception as e:
            return ActionResult(success=False, message=f"CSV read error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'delimiter': ',', 'has_header': True, 'output_var': 'csv_data'}


class CsvWriteAction(BaseAction):
    """Write data to CSV file."""
    action_type = "csv_write"
    display_name = "写入CSV文件"
    description = "将数据写入CSV文件"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute CSV write."""
        file_path = params.get('file_path', '')
        data = params.get('data', [])
        fieldnames = params.get('fieldnames', None)
        encoding = params.get('encoding', 'utf-8')
        delimiter = params.get('delimiter', ',')
        write_header = params.get('write_header', True)

        if not file_path:
            return ActionResult(success=False, message="file_path is required")
        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_path = context.resolve_value(file_path) if context else file_path
            resolved_data = context.resolve_value(data) if context else data
            resolved_encoding = context.resolve_value(encoding) if context else encoding

            _os.makedirs(_os.path.dirname(resolved_path) or '.', exist_ok=True)

            fields = fieldnames
            if not fields and resolved_data:
                if isinstance(resolved_data[0], dict):
                    fields = list(resolved_data[0].keys())

            with open(resolved_path, 'w', encoding=resolved_encoding, newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fields, delimiter=delimiter)
                if write_header and fields:
                    writer.writeheader()
                for row in resolved_data:
                    if isinstance(row, dict):
                        writer.writerow(row)
                    elif isinstance(row, (list, tuple)) and fields:
                        writer.writerow(dict(zip(fields, row)))

            return ActionResult(
                success=True,
                message=f"Wrote {len(resolved_data)} rows to {resolved_path}",
                data={'rows_written': len(resolved_data), 'file_path': resolved_path}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"CSV write error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'fieldnames': None, 'encoding': 'utf-8', 'delimiter': ',', 'write_header': True}


class CsvAppendAction(BaseAction):
    """Append rows to existing CSV."""
    action_type = "csv_append"
    display_name = "追加CSV行"
    description = "向现有CSV文件追加行"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute CSV append."""
        file_path = params.get('file_path', '')
        data = params.get('data', [])
        delimiter = params.get('delimiter', ',')
        encoding = params.get('encoding', 'utf-8')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            resolved_path = context.resolve_value(file_path) if context else file_path
            resolved_data = context.resolve_value(data) if context else data

            with open(resolved_path, 'a', encoding=encoding, newline='') as f:
                writer = csv.writer(f, delimiter=delimiter)
                for row in resolved_data:
                    if isinstance(row, dict):
                        writer.writerow(list(row.values()))
                    elif isinstance(row, (list, tuple)):
                        writer.writerow(row)
                    else:
                        writer.writerow([row])

            return ActionResult(
                success=True,
                message=f"Appended {len(resolved_data)} rows",
                data={'rows_appended': len(resolved_data)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"CSV append error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'delimiter': ',', 'encoding': 'utf-8'}


class CsvFilterAction(BaseAction):
    """Filter CSV rows by condition."""
    action_type = "csv_filter"
    display_name = "过滤CSV行"
    description = "按条件过滤CSV行"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute CSV filter."""
        data = params.get('data', [])
        column = params.get('column', '')
        operator = params.get('operator', 'eq')
        value = params.get('value', '')
        output_var = params.get('output_var', 'filtered_data')

        if not data:
            return ActionResult(success=False, message="data is required")
        if not column:
            return ActionResult(success=False, message="column is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_value = context.resolve_value(value) if context else value

            operators = {
                'eq': lambda a, b: str(a) == str(b),
                'ne': lambda a, b: str(a) != str(b),
                'gt': lambda a, b: _try_num(a) > _try_num(b),
                'lt': lambda a, b: _try_num(a) < _try_num(b),
                'ge': lambda a, b: _try_num(a) >= _try_num(b),
                'le': lambda a, b: _try_num(a) <= _try_num(b),
                'contains': lambda a, b: str(b) in str(a),
            }

            op_func = operators.get(operator, operators['eq'])
            filtered = [row for row in resolved_data if isinstance(row, dict) and op_func(row.get(column, ''), resolved_value)]

            result = {'rows': filtered, 'count': len(filtered)}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Filtered to {len(filtered)} rows", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"CSV filter error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data', 'column']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'operator': 'eq', 'value': '', 'output_var': 'filtered_data'}


class CsvMergeAction(BaseAction):
    """Merge multiple CSV files."""
    action_type = "csv_merge"
    display_name = "合并CSV文件"
    description = "合并多个CSV文件"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute CSV merge."""
        file_paths = params.get('file_paths', [])
        output_path = params.get('output_path', '')
        encoding = params.get('encoding', 'utf-8')
        delimiter = params.get('delimiter', ',')
        output_var = params.get('output_var', 'merged_data')

        if not file_paths or not output_path:
            return ActionResult(success=False, message="file_paths and output_path are required")

        try:
            resolved_paths = context.resolve_value(file_paths) if context else file_paths
            resolved_output = context.resolve_value(output_path) if context else output_path

            all_rows = []
            for path in resolved_paths:
                with open(path, 'r', encoding=encoding, newline='') as f:
                    reader = csv.DictReader(f, delimiter=delimiter)
                    all_rows.extend(list(reader))

            _os.makedirs(_os.path.dirname(resolved_output) or '.', exist_ok=True)
            if all_rows:
                fields = list(all_rows[0].keys())
                with open(resolved_output, 'w', encoding=encoding, newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=fields, delimiter=delimiter)
                    writer.writeheader()
                    writer.writerows(all_rows)

            result = {'count': len(all_rows), 'output_path': resolved_output, 'files_merged': len(resolved_paths)}
            if context:
                context.set(output_var, all_rows)
            return ActionResult(success=True, message=f"Merged {len(all_rows)} rows from {len(resolved_paths)} files", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"CSV merge error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_paths', 'output_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'delimiter': ',', 'output_var': 'merged_data'}


class CsvSortAction(BaseAction):
    """Sort CSV data by column."""
    action_type = "csv_sort"
    display_name = "排序CSV"
    description = "按列排序CSV数据"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute CSV sort."""
        data = params.get('data', [])
        sort_by = params.get('sort_by', '')
        reverse = params.get('reverse', False)
        output_var = params.get('output_var', 'sorted_data')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_reverse = context.resolve_value(reverse) if context else reverse

            dict_rows = [r for r in resolved_data if isinstance(r, dict)]
            if sort_by:
                sorted_rows = sorted(dict_rows, key=lambda r: r.get(sort_by, ''), reverse=resolved_reverse)
            else:
                sorted_rows = dict_rows

            result = {'rows': sorted_rows, 'count': len(sorted_rows)}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Sorted {len(sorted_rows)} rows", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"CSV sort error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'sort_by': '', 'reverse': False, 'output_var': 'sorted_data'}


class CsvDedupeAction(BaseAction):
    """Remove duplicate rows from CSV."""
    action_type = "csv_dedupe"
    display_name = "CSV去重"
    description = "删除CSV中的重复行"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute CSV dedupe."""
        data = params.get('data', [])
        columns = params.get('columns', None)
        output_var = params.get('output_var', 'deduped_data')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_columns = context.resolve_value(columns) if context else columns

            seen = set()
            deduped = []
            for row in resolved_data:
                if isinstance(row, dict):
                    if resolved_columns:
                        key = tuple(row.get(c, '') for c in resolved_columns)
                    else:
                        key = tuple(sorted(row.items()))
                    if key not in seen:
                        seen.add(key)
                        deduped.append(row)

            result = {'rows': deduped, 'count': len(deduped), 'removed': len(resolved_data) - len(deduped)}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Deduplication: {len(resolved_data)} -> {len(deduped)} rows", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"CSV dedupe error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'columns': None, 'output_var': 'deduped_data'}


class CsvStatsAction(BaseAction):
    """Compute statistics on CSV data."""
    action_type = "csv_stats"
    display_name = "CSV统计"
    description = "计算CSV数据统计信息"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute CSV stats."""
        data = params.get('data', [])
        column = params.get('column', '')
        output_var = params.get('output_var', 'csv_stats')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data

            if column:
                values = [row.get(column) for row in resolved_data if isinstance(row, dict) and column in row]
                numeric_values = [_try_num(v) for v in values]
                numeric_values = [v for v in numeric_values if v is not None]

                stats = {
                    'count': len(values),
                    'unique': len(set(str(v) for v in values)),
                    'numeric_count': len(numeric_values),
                }
                if numeric_values:
                    stats.update({
                        'min': min(numeric_values),
                        'max': max(numeric_values),
                        'sum': sum(numeric_values),
                        'avg': sum(numeric_values) / len(numeric_values),
                    })
            else:
                stats = {
                    'total_rows': len(resolved_data),
                    'columns': list(resolved_data[0].keys()) if resolved_data else [],
                }

            result = {'stats': stats, 'column': column}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message="CSV stats computed", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"CSV stats error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'column': '', 'output_var': 'csv_stats'}


def _try_num(v: Any) -> Optional[float]:
    """Try to convert value to number."""
    try:
        return float(v)
    except (ValueError, TypeError):
        return None
