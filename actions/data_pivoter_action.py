"""Data pivoter action module for RabAI AutoClick.

Provides data pivoting and unpivoting capabilities for
reshaping data between wide and long formats.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Union
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataPivoterAction(BaseAction):
    """Data pivoter action for reshaping data.
    
    Supports pivot (long to wide) and unpivot (wide to long)
    transformations with aggregation.
    """
    action_type = "data_pivoter"
    display_name = "数据透视"
    description = "数据透视表变换"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pivot operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                operation: pivot|unpivot
                data: Input data
                index: Column(s) to group by (for pivot)
                columns: Column to pivot (for pivot)
                values: Column to aggregate (for pivot)
                aggfunc: Aggregation function (default: sum)
                id_vars: Columns to keep as variables (for unpivot)
                value_vars: Columns to unpivot (for unpivot).
        
        Returns:
            ActionResult with pivoted/unpivoted data.
        """
        operation = params.get('operation', 'pivot')
        
        if operation == 'pivot':
            return self._pivot(params)
        elif operation == 'unpivot':
            return self._unpivot(params)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")
    
    def _pivot(self, params: Dict[str, Any]) -> ActionResult:
        """Pivot data from long to wide format."""
        data = params.get('data', [])
        index = params.get('index')
        columns = params.get('columns')
        values = params.get('values')
        aggfunc = params.get('aggfunc', 'sum')
        
        if not data:
            return ActionResult(success=False, message="No data provided")
        
        if not index or not columns or not values:
            return ActionResult(success=False, message="index, columns, and values are required")
        
        if not isinstance(data[0], dict):
            return ActionResult(success=False, message="Pivot requires list of dicts")
        
        index_fields = index.split(',') if isinstance(index, str) else index
        columns_field = columns
        
        pivot_data: Dict[tuple, Dict[Any, List]] = defaultdict(lambda: defaultdict(list))
        
        for item in data:
            index_values = tuple(item.get(f) for f in index_fields)
            col_value = item.get(columns_field)
            val_value = item.get(values)
            
            if val_value is not None:
                pivot_data[index_values][col_value].append(val_value)
        
        results = []
        all_columns = set()
        
        for index_values, col_data in pivot_data.items():
            all_columns.update(col_data.keys())
            result = dict(zip(index_fields, index_values))
            result.update({
                col: self._aggregate(col_data[col], aggfunc)
                for col in col_data.keys()
            })
            results.append(result)
        
        return ActionResult(
            success=True,
            message=f"Pivoted to {len(results)} rows x {len(all_columns)} columns",
            data={
                'results': results,
                'columns': list(all_columns),
                'index_columns': index_fields,
                'pivot_column': columns_field,
                'value_column': values
            }
        )
    
    def _unpivot(self, params: Dict[str, Any]) -> ActionResult:
        """Unpivot data from wide to long format."""
        data = params.get('data', [])
        id_vars = params.get('id_vars', [])
        value_vars = params.get('value_vars')
        
        if not data:
            return ActionResult(success=False, message="No data provided")
        
        if not isinstance(data[0], dict):
            return ActionResult(success=False, message="Unpivot requires list of dicts")
        
        if isinstance(id_vars, str):
            id_vars = [id_vars]
        
        if value_vars is None:
            if id_vars:
                value_vars = [k for k in data[0].keys() if k not in id_vars]
            else:
                value_vars = list(data[0].keys())
        
        if isinstance(value_vars, str):
            value_vars = [value_vars]
        
        results = []
        
        for item in data:
            base = {k: item.get(k) for k in id_vars if k in item}
            
            for var in value_vars:
                result = dict(base)
                result['variable'] = var
                result['value'] = item.get(var)
                results.append(result)
        
        return ActionResult(
            success=True,
            message=f"Unpivoted to {len(results)} rows",
            data={
                'results': results,
                'id_vars': id_vars,
                'value_vars': value_vars,
                'original_rows': len(data)
            }
        )
    
    def _aggregate(self, values: List[Any], aggfunc: str) -> Any:
        """Aggregate values."""
        if not values:
            return None
        
        numeric_values = [v for v in values if isinstance(v, (int, float))]
        
        if aggfunc == 'sum':
            return sum(numeric_values) if numeric_values else sum(values)
        elif aggfunc == 'avg' or aggfunc == 'mean':
            return sum(numeric_values) / len(numeric_values) if numeric_values else None
        elif aggfunc == 'count':
            return len(values)
        elif aggfunc == 'min':
            return min(values) if values else None
        elif aggfunc == 'max':
            return max(values) if values else None
        elif aggfunc == 'first':
            return values[0] if values else None
        elif aggfunc == 'last':
            return values[-1] if values else None
        elif aggfunc == 'median':
            sorted_vals = sorted(values)
            n = len(sorted_vals)
            return sorted_vals[n // 2] if n > 0 else None
        
        return values[0] if values else None
