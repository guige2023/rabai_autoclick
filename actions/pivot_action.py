"""Pivot action module for RabAI AutoClick.

Provides pivot table operations for data analysis:
group by, aggregate, reshape data.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Union
from collections import defaultdict, Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PivotTableAction(BaseAction):
    """Create a pivot table from list of records.
    
    Supports multiple aggregation functions (sum, avg, count,
    min, max, first, last), multi-level grouping, and
    computed columns.
    """
    action_type = "pivot_table"
    display_name = "数据透视表"
    description = "对列表数据创建透视表，支持多种聚合函数"

    AGG_FUNCS = ['sum', 'avg', 'count', 'min', 'max', 'first', 'last', 'median', 'std']

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Create a pivot table.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of dicts (records)
                - index: str or list (grouping keys)
                - columns: str (column to pivot on)
                - values: str or list (fields to aggregate)
                - aggfunc: str or list (aggregation functions)
                - fill_value: any (value for missing cells)
                - save_to_var: str
        
        Returns:
            ActionResult with pivot table result.
        """
        data = params.get('data', [])
        index = params.get('index', '')
        columns = params.get('columns', '')
        values = params.get('values', '')
        aggfunc = params.get('aggfunc', 'sum')
        fill_value = params.get('fill_value', None)
        save_to_var = params.get('save_to_var', 'pivot_result')

        if not data:
            return ActionResult(success=False, message="No data provided")

        # Normalize index to list
        if isinstance(index, str):
            index = [index] if index else []
        if isinstance(values, str):
            values = [values] if values else []
        if isinstance(aggfunc, str):
            aggfunc = [aggfunc]

        # Ensure same length for values and aggfuncs
        while len(aggfunc) < len(values):
            aggfunc.append(aggfunc[-1])

        # Group data by index
        groups: Dict[tuple, List[Dict]] = defaultdict(list)
        for record in data:
            key = tuple(str(record.get(k, '')) for k in index)
            groups[key].append(record)

        # Build pivot table
        result = {}
        column_values = set()

        for key, records in groups.items():
            row_key = key if len(key) > 1 else key[0]
            result[row_key] = {}

            for vi, val_field in enumerate(values):
                func = aggfunc[min(vi, len(aggfunc) - 1)]
                
                # Get column pivot values
                if columns:
                    for record in records:
                        col_val = str(record.get(columns, ''))
                        column_values.add(col_val)

                # Aggregate
                agg_result = self._aggregate(records, val_field, func)
                result[row_key][val_field] = agg_result

            # Add column-level pivot
            if columns:
                col_groups: Dict[str, List[Dict]] = defaultdict(list)
                for record in records:
                    col_val = str(record.get(columns, ''))
                    col_groups[col_val].append(record)

                for col_val, col_records in col_groups.items():
                    col_key = f"{columns}={col_val}"
                    for vi, val_field in enumerate(values):
                        func = aggfunc[min(vi, len(aggfunc) - 1)]
                        result[row_key][f"{val_field}_{col_key}"] = self._aggregate(
                            col_records, val_field, func
                        )

        # Apply fill_value
        if fill_value is not None:
            all_vals = set()
            for row in result.values():
                all_vals.update(row.keys())
            for row in result.values():
                for val_key in all_vals:
                    if val_key not in row:
                        row[val_key] = fill_value

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data={
                'rows': len(result),
                'columns': values,
                'pivot': result,
            },
            message=f"Pivot table: {len(result)} rows"
        )

    def _aggregate(self, records: List[Dict], field: str, func: str) -> Any:
        """Aggregate a field across records."""
        values = []
        for record in records:
            val = record.get(field)
            if val is not None:
                try:
                    values.append(float(val))
                except (ValueError, TypeError):
                    values.append(val)

        if not values:
            return None

        if func == 'sum':
            try:
                return sum(values)
            except:
                return values[0]
        elif func == 'avg' or func == 'mean':
            try:
                return sum(values) / len(values)
            except:
                return values[0]
        elif func == 'count':
            return len(values)
        elif func == 'min':
            try:
                return min(values)
            except:
                return values[0]
        elif func == 'max':
            try:
                return max(values)
            except:
                return values[0]
        elif func == 'first':
            return values[0]
        elif func == 'last':
            return values[-1]
        elif func == 'median':
            try:
                sorted_vals = sorted(values)
                n = len(sorted_vals)
                mid = n // 2
                if n % 2 == 0:
                    return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2
                return sorted_vals[mid]
            except:
                return values[0]
        elif func == 'std':
            try:
                import statistics
                return statistics.stdev(values)
            except:
                return 0
        else:
            return values[0]


class GroupByAction(BaseAction):
    """Group data by one or more keys and aggregate.
    
    Simpler than pivot - just group and aggregate.
    """
    action_type = "group_by"
    display_name = "数据分组"
    description = "按字段分组数据并聚合，支持多级分组"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Group data by keys.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of dicts
                - group_by: str or list (fields to group by)
                - aggregate: dict {output_field: (source_field, func)}
                - having: dict {field: condition} (filter groups)
                - save_to_var: str
        
        Returns:
            ActionResult with grouped result.
        """
        data = params.get('data', [])
        group_by = params.get('group_by', '')
        aggregate = params.get('aggregate', {})
        having = params.get('having', {})
        save_to_var = params.get('save_to_var', 'group_result')

        if not data:
            return ActionResult(success=False, message="No data provided")

        # Normalize group_by to list
        if isinstance(group_by, str):
            group_by = [group_by] if group_by else []

        # Group data
        groups: Dict[tuple, List[Dict]] = defaultdict(list)
        for record in data:
            key = tuple(str(record.get(k, '')) for k in group_by)
            groups[key].append(record)

        # Aggregate each group
        results = []
        for key, records in groups.items():
            group_key = key if len(key) > 1 else key[0]
            result = {'_group_key': group_key}

            # Add group key fields
            for i, gb_field in enumerate(group_by):
                result[gb_field] = key[i]

            # Compute aggregates
            for out_field, (src_field, func) in aggregate.items():
                values = [r.get(src_field) for r in records if r.get(src_field) is not None]
                result[out_field] = self._compute_agg(values, func)

            # Apply having filter
            if self._passes_having(result, having):
                results.append(result)

        # Sort by group key
        results.sort(key=lambda x: str(x.get('_group_key', '')))

        if context and save_to_var:
            context.variables[save_to_var] = results

        return ActionResult(
            success=True,
            data={'groups': results, 'count': len(results)},
            message=f"Grouped into {len(results)} groups"
        )

    def _compute_agg(self, values: List, func: str) -> Any:
        """Compute aggregation on values."""
        if not values:
            return None

        try:
            nums = [float(v) for v in values]
        except (ValueError, TypeError):
            nums = values

        if func == 'sum':
            return sum(nums) if nums else None
        elif func == 'avg':
            return sum(nums) / len(nums) if nums else None
        elif func == 'count':
            return len(values)
        elif func == 'min':
            return min(nums) if nums else None
        elif func == 'max':
            return max(nums) if nums else None
        elif func == 'first':
            return values[0]
        elif func == 'last':
            return values[-1]
        elif func == 'list':
            return values
        elif func == 'set':
            return list(set(values))
        elif func == 'count_distinct':
            return len(set(values))
        return values[0] if values else None

    def _passes_having(self, result: Dict, having: Dict) -> bool:
        """Check if result passes having filter."""
        for field, condition in having.items():
            val = result.get(field)
            if isinstance(condition, dict):
                op = condition.get('op', '==')
                cmp_val = condition.get('value')
                if op == '==' and val != cmp_val:
                    return False
                elif op == '!=' and val == cmp_val:
                    return False
                elif op == '>' and val <= cmp_val:
                    return False
                elif op == '<' and val >= cmp_val:
                    return False
                elif op == '>=' and val < cmp_val:
                    return False
                elif op == '<=' and val > cmp_val:
                    return False
            else:
                if val != condition:
                    return False
        return True


class UnpivotAction(BaseAction):
    """Unpivot (melt) a wide table into long format.
    
    Transform columns into variable/value pairs.
    """
    action_type = "unpivot"
    display_name = "数据展开"
    description = "将宽表展开为长格式，将列转换为变量值对"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Unpivot a table.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of dicts
                - id_vars: list (columns to keep as identifiers)
                - value_vars: list (columns to unpivot)
                - var_name: str (name for variable column)
                - value_name: str (name for value column)
                - save_to_var: str
        
        Returns:
            ActionResult with unpivoted data.
        """
        data = params.get('data', [])
        id_vars = params.get('id_vars', [])
        value_vars = params.get('value_vars', [])
        var_name = params.get('var_name', 'variable')
        value_name = params.get('value_name', 'value')
        save_to_var = params.get('save_to_var', 'unpivot_result')

        if not data:
            return ActionResult(success=False, message="No data provided")

        # Auto-detect value_vars if not provided
        if not value_vars:
            if id_vars:
                all_keys = set()
                for record in data:
                    all_keys.update(record.keys())
                value_vars = [k for k in all_keys if k not in id_vars]
            else:
                return ActionResult(success=False, message="id_vars or value_vars required")

        results = []
        for record in data:
            id_data = {k: record.get(k) for k in id_vars}
            for v_var in value_vars:
                row = dict(id_data)
                row[var_name] = v_var
                row[value_name] = record.get(v_var)
                results.append(row)

        if context and save_to_var:
            context.variables[save_to_var] = results

        return ActionResult(
            success=True,
            data={'rows': len(results), 'data': results},
            message=f"Unpivoted: {len(data)} rows -> {len(results)} rows"
        )
