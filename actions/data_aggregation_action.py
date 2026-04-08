"""Data Aggregation action module for RabAI AutoClick.

Provides data aggregation operations:
- AggregateSumAction: Sum aggregation
- AggregateGroupAction: Group by aggregation
- AggregateWindowAction: Window functions
- AggregatePivotAction: Pivot table
"""

from __future__ import annotations

import sys
import os
from typing import Any, Dict, List, Optional
from collections import defaultdict

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AggregateSumAction(BaseAction):
    """Sum aggregation."""
    action_type = "aggregate_sum"
    display_name = "求和聚合"
    description = "求和聚合"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute sum aggregation."""
        data = params.get('data', [])
        field = params.get('field', '')
        group_by = params.get('group_by', None)
        output_var = params.get('output_var', 'sum_result')

        if not data or not field:
            return ActionResult(success=False, message="data and field are required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_group_by = context.resolve_value(group_by) if context else group_by

            if resolved_group_by:
                groups = defaultdict(list)
                for record in resolved_data:
                    key = record.get(resolved_group_by, 'unknown')
                    value = record.get(field, 0)
                    if isinstance(value, (int, float)):
                        groups[key].append(value)

                result = {k: sum(v) for k, v in groups.items()}
            else:
                values = [r.get(field, 0) for r in resolved_data if isinstance(r.get(field), (int, float))]
                result = {'total': sum(values)}

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Sum: {result.get('total', sum(result.values())) if isinstance(result, dict) else result}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Sum aggregation error: {e}")


class AggregateGroupAction(BaseAction):
    """Group by aggregation."""
    action_type = "aggregate_group"
    display_name = "分组聚合"
    description = "分组聚合"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute group aggregation."""
        data = params.get('data', [])
        group_by = params.get('group_by', '')
        aggregations = params.get('aggregations', [])
        output_var = params.get('output_var', 'group_result')

        if not data or not group_by:
            return ActionResult(success=False, message="data and group_by are required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_aggregations = context.resolve_value(aggregations) if context else aggregations

            groups = defaultdict(list)
            for record in resolved_data:
                key = record.get(group_by, 'unknown')
                groups[key].append(record)

            results = []
            for group_key, group_records in groups.items():
                result_row = {group_by: group_key, '_count': len(group_records)}

                for agg in resolved_aggregations:
                    field = agg.get('field', '')
                    func = agg.get('function', 'sum')

                    values = [r.get(field, 0) for r in group_records if isinstance(r.get(field), (int, float))]

                    if func == 'sum':
                        result_row[f'{field}_sum'] = sum(values)
                    elif func == 'avg':
                        result_row[f'{field}_avg'] = sum(values) / len(values) if values else 0
                    elif func == 'min':
                        result_row[f'{field}_min'] = min(values) if values else None
                    elif func == 'max':
                        result_row[f'{field}_max'] = max(values) if values else None
                    elif func == 'count':
                        result_row[f'{field}_count'] = len(values)

                results.append(result_row)

            return ActionResult(
                success=True,
                data={output_var: {'groups': results, 'group_count': len(results)}},
                message=f"Grouped into {len(results)} groups"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Group aggregation error: {e}")


class AggregateWindowAction(BaseAction):
    """Window functions."""
    action_type = "aggregate_window"
    display_name = "窗口函数"
    description = "窗口函数聚合"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute window function."""
        data = params.get('data', [])
        field = params.get('field', '')
        window_type = params.get('window_type', 'rolling')
        window_size = params.get('window_size', 3)
        func = params.get('function', 'avg')
        sort_by = params.get('sort_by', None)
        output_var = params.get('output_var', 'window_result')

        if not data or not field:
            return ActionResult(success=False, message="data and field are required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_sort_by = context.resolve_value(sort_by) if context else sort_by

            if resolved_sort_by:
                resolved_data = sorted(resolved_data, key=lambda x: x.get(resolved_sort_by, 0))

            values = [r.get(field, 0) for r in resolved_data if isinstance(r.get(field), (int, float))]

            windowed = []
            for i, record in enumerate(resolved_data):
                if window_type == 'rolling':
                    start = max(0, i - window_size + 1)
                    window_values = values[start:i + 1]
                else:
                    window_values = values[:i + 1]

                if func == 'avg':
                    record[f'{field}_window_avg'] = sum(window_values) / len(window_values) if window_values else 0
                elif func == 'sum':
                    record[f'{field}_window_sum'] = sum(window_values)
                elif func == 'min':
                    record[f'{field}_window_min'] = min(window_values) if window_values else None
                elif func == 'max':
                    record[f'{field}_window_max'] = max(window_values) if window_values else None

                windowed.append(record)

            result = {
                'data': windowed,
                'window_type': window_type,
                'window_size': window_size,
                'function': func,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Window function '{func}' applied"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Window aggregation error: {e}")


class AggregatePivotAction(BaseAction):
    """Pivot table."""
    action_type = "aggregate_pivot"
    display_name = "数据透视"
    description = "数据透视表"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute pivot table."""
        data = params.get('data', [])
        index = params.get('index', '')
        columns = params.get('columns', '')
        values = params.get('values', '')
        aggfunc = params.get('aggfunc', 'sum')
        output_var = params.get('output_var', 'pivot_result')

        if not data or not index or not columns or not values:
            return ActionResult(success=False, message="data, index, columns, and values are required")

        try:
            resolved_data = context.resolve_value(data) if context else data

            pivot = defaultdict(lambda: defaultdict(list))

            for record in resolved_data:
                idx_val = record.get(index, 'unknown')
                col_val = record.get(columns, 'unknown')
                val = record.get(values, 0)
                if isinstance(val, (int, float)):
                    pivot[idx_val][col_val].append(val)

            result_data = []
            all_columns = set()
            for idx_val in pivot:
                all_columns.update(pivot[idx_val].keys())

            for idx_val in sorted(pivot.keys()):
                row = {index: idx_val}
                for col_val in sorted(all_columns):
                    col_values = pivot[idx_val].get(col_val, [])
                    if col_values:
                        if aggfunc == 'sum':
                            row[f'{col_val}'] = sum(col_values)
                        elif aggfunc == 'avg':
                            row[f'{col_val}'] = sum(col_values) / len(col_values)
                        elif aggfunc == 'count':
                            row[f'{col_val}'] = len(col_values)
                        elif aggfunc == 'min':
                            row[f'{col_val}'] = min(col_values)
                        elif aggfunc == 'max':
                            row[f'{col_val}'] = max(col_values)
                    else:
                        row[f'{col_val}'] = None
                result_data.append(row)

            result = {
                'data': result_data,
                'row_count': len(result_data),
                'column_count': len(all_columns),
                'index': index,
                'columns': columns,
                'values': values,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Pivot: {len(result_data)} rows x {len(all_columns)} columns"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pivot aggregation error: {e}")
