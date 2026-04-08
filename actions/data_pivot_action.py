"""Data Pivot Action.

Pivots data with aggregation, supporting row/column pivoting,
multi-level aggregation, and sparse/dense output modes.
"""

import sys
import os
from typing import Any, Dict, List, Optional
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataPivotAction(BaseAction):
    """Pivot data with aggregation.
    
    Transforms data from long to wide format with configurable
    row/column aggregation and fill values.
    """
    action_type = "data_pivot"
    display_name = "数据透视"
    description = "数据透视表，支持行列转换和多级聚合"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Pivot data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Data to pivot.
                - index: Field(s) for row index.
                - columns: Field(s) for column headers.
                - values: Field to aggregate for values.
                - aggfunc: Aggregation function ('sum', 'count', 'avg', 'first', 'last').
                - fill_value: Value to fill for missing cells.
                - margins: Add row/column margins (default: False).
                - save_to_var: Variable name for result.
        
        Returns:
            ActionResult with pivoted data.
        """
        try:
            data = params.get('data')
            index = params.get('index')
            columns = params.get('columns')
            values = params.get('values')
            aggfunc = params.get('aggfunc', 'sum')
            fill_value = params.get('fill_value', None)
            margins = params.get('margins', False)
            save_to_var = params.get('save_to_var', 'pivoted_data')

            if data is None:
                data = context.get_variable(params.get('use_var', 'input_data'))

            if not data:
                return ActionResult(success=False, message="No data provided")

            if not index or not columns or not values:
                return ActionResult(success=False, message="index, columns, and values are required")

            if isinstance(index, str):
                index = [index]
            if isinstance(columns, str):
                columns = [columns]

            # Build pivot table
            pivot = defaultdict(lambda: defaultdict(list))
            row_keys = set()
            col_keys = set()

            for item in data:
                if not isinstance(item, dict):
                    continue
                
                row_key = tuple(item.get(i) for i in index)
                col_key = tuple(item.get(c) for c in columns)
                value = item.get(values)
                
                if value is not None:
                    pivot[row_key][col_key].append(value)
                    row_keys.add(row_key)
                    col_keys.add(col_key)

            # Build output
            sorted_row_keys = sorted(row_keys)
            sorted_col_keys = sorted(col_keys)

            # Header row
            header = list(index) + [self._format_col_key(c) for c in sorted_col_keys]
            if margins:
                header.append('__margin__')

            result = [header]

            # Data rows
            for row_key in sorted_row_keys:
                row = list(row_key)
                
                for col_key in sorted_col_keys:
                    cell_values = pivot[row_key].get(col_key, [])
                    aggregated = self._aggregate(cell_values, aggfunc)
                    row.append(aggregated if aggregated is not None else fill_value)
                
                if margins:
                    # Row margin
                    all_values = []
                    for col_key in sorted_col_keys:
                        all_values.extend(pivot[row_key].get(col_key, []))
                    row.append(self._aggregate(all_values, aggfunc) if all_values else fill_value)
                
                result.append(row)

            # Column margins
            if margins:
                margin_row = ['__margin__']
                for col_key in sorted_col_keys:
                    col_values = []
                    for row_key in sorted_row_keys:
                        col_values.extend(pivot[row_key].get(col_key, []))
                    margin_row.append(self._aggregate(col_values, aggfunc) if col_values else fill_value)
                margin_row.append(None)
                result.append(margin_row)

            summary = {
                'original_rows': len(data),
                'pivoted_rows': len(result) - 1,
                'pivoted_columns': len(sorted_col_keys),
                'index_fields': index,
                'column_fields': columns,
                'value_field': values
            }

            context.set_variable(save_to_var, result)
            return ActionResult(success=True, data=summary,
                             message=f"Pivoted: {len(result)-1} rows x {len(sorted_col_keys)} cols")

        except Exception as e:
            return ActionResult(success=False, message=f"Pivot error: {e}")

    def _format_col_key(self, key: tuple) -> str:
        """Format column key for header."""
        return ' | '.join(str(k) for k in key)

    def _aggregate(self, values: List, func: str) -> Any:
        """Apply aggregation function."""
        if not values:
            return None
        
        numeric = [v for v in values if isinstance(v, (int, float))]
        
        if func == 'sum':
            return sum(numeric) if numeric else None
        elif func == 'count':
            return len(values)
        elif func == 'avg' or func == 'mean':
            return sum(numeric) / len(numeric) if numeric else None
        elif func == 'min':
            return min(values) if values else None
        elif func == 'max':
            return max(values) if values else None
        elif func == 'first':
            return values[0]
        elif func == 'last':
            return values[-1]
        elif func == 'median':
            import statistics
            return statistics.median(values)
        return values[0] if values else None
