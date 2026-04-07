"""Table/data structure action module for RabAI AutoClick.

Provides table operations:
- TableCreateAction: Create table from data
- TableFilterAction: Filter table rows
- TableSortAction: Sort table
- TableJoinAction: Join two tables
- TablePivotAction: Pivot table
- TableExportAction: Export table to CSV/JSON
"""

from __future__ import annotations

import csv
import json
import sys
from io import StringIO
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TableCreateAction(BaseAction):
    """Create table from data."""
    action_type = "table_create"
    display_name = "创建表格"
    description = "从数据创建表格"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute table create."""
        data = params.get('data', [])
        columns = params.get('columns', None)
        output_var = params.get('output_var', 'table')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data

            if not columns:
                if isinstance(resolved_data, list) and len(resolved_data) > 0:
                    if isinstance(resolved_data[0], dict):
                        columns = list(resolved_data[0].keys())
                    else:
                        columns = [f'col_{i}' for i in range(len(resolved_data[0]))]

            table = {'columns': columns, 'data': resolved_data, 'rows': len(resolved_data)}
            if context:
                context.set(output_var, table)
            return ActionResult(success=True, message=f"Table created: {len(resolved_data)} rows, {len(columns)} columns", data=table)
        except Exception as e:
            return ActionResult(success=False, message=f"Table create error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'columns': None, 'output_var': 'table'}


class TableFilterAction(BaseAction):
    """Filter table rows."""
    action_type = "table_filter"
    display_name = "表格过滤"
    description = "过滤表格行"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute table filter."""
        table = params.get('table', {})
        column = params.get('column', '')
        operator = params.get('operator', 'eq')  # eq, ne, gt, lt, ge, le, contains
        value = params.get('value', None)
        output_var = params.get('output_var', 'filtered_table')

        if not table or not column:
            return ActionResult(success=False, message="table and column are required")

        try:
            resolved_table = context.resolve_value(table) if context else table
            resolved_col = context.resolve_value(column) if context else column
            resolved_op = context.resolve_value(operator) if context else operator
            resolved_val = context.resolve_value(value) if context else value

            data = resolved_table.get('data', [])
            filtered = []

            for row in data:
                if not isinstance(row, dict):
                    continue
                cell = row.get(resolved_col)
                matches = False

                if resolved_op == 'eq':
                    matches = cell == resolved_val
                elif resolved_op == 'ne':
                    matches = cell != resolved_val
                elif resolved_op == 'gt':
                    matches = cell is not None and resolved_val is not None and cell > resolved_val
                elif resolved_op == 'lt':
                    matches = cell is not None and resolved_val is not None and cell < resolved_val
                elif resolved_op == 'ge':
                    matches = cell is not None and resolved_val is not None and cell >= resolved_val
                elif resolved_op == 'le':
                    matches = cell is not None and resolved_val is not None and cell <= resolved_val
                elif resolved_op == 'contains':
                    matches = cell is not None and str(resolved_val) in str(cell)

                if matches:
                    filtered.append(row)

            result = {'columns': resolved_table.get('columns', []), 'data': filtered, 'rows': len(filtered)}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Filtered: {len(filtered)} rows", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Table filter error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['table', 'column']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'operator': 'eq', 'value': None, 'output_var': 'filtered_table'}


class TableSortAction(BaseAction):
    """Sort table."""
    action_type = "table_sort"
    display_name = "表格排序"
    description = "排序表格"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute table sort."""
        table = params.get('table', {})
        column = params.get('column', '')
        reverse = params.get('reverse', False)
        output_var = params.get('output_var', 'sorted_table')

        if not table or not column:
            return ActionResult(success=False, message="table and column are required")

        try:
            resolved_table = context.resolve_value(table) if context else table
            resolved_col = context.resolve_value(column) if context else column
            resolved_rev = context.resolve_value(reverse) if context else reverse

            data = resolved_table.get('data', [])
            sorted_data = sorted(data, key=lambda row: row.get(resolved_col, '') if isinstance(row, dict) else '', reverse=resolved_rev)

            result = {'columns': resolved_table.get('columns', []), 'data': sorted_data, 'rows': len(sorted_data)}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Sorted by {resolved_col}: {len(sorted_data)} rows", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Table sort error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['table', 'column']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'reverse': False, 'output_var': 'sorted_table'}


class TableExportAction(BaseAction):
    """Export table to CSV/JSON."""
    action_type = "table_export"
    display_name = "表格导出"
    description = "导出表格为CSV/JSON"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute table export."""
        table = params.get('table', {})
        format_type = params.get('format', 'csv')  # csv, json
        output_var = params.get('output_var', 'exported_data')

        if not table:
            return ActionResult(success=False, message="table is required")

        try:
            resolved_table = context.resolve_value(table) if context else table
            resolved_fmt = context.resolve_value(format_type) if context else format_type

            data = resolved_table.get('data', [])
            columns = resolved_table.get('columns', [])

            if resolved_fmt == 'csv':
                output = StringIO()
                writer = csv.writer(output)
                if columns:
                    writer.writerow(columns)
                for row in data:
                    if isinstance(row, dict):
                        writer.writerow([row.get(c, '') for c in columns])
                    else:
                        writer.writerow(row)
                result = output.getvalue()
            else:
                result = json.dumps({'columns': columns, 'data': data}, indent=2, ensure_ascii=False)

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Exported as {resolved_fmt.upper()}", data={'result': result[:500] if len(result) > 500 else result})
        except Exception as e:
            return ActionResult(success=False, message=f"Table export error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['table']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': 'csv', 'output_var': 'exported_data'}
