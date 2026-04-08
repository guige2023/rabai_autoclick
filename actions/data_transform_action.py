"""Data transform action module for RabAI AutoClick.

Provides data transformation with pivot, unpivot,
transpose, and aggregation operations.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict, Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataTransformAction(BaseAction):
    """Transform data structures with pivot, unpivot, and transpose.
    
    Supports pivot tables, unpivoting, matrix transpose,
    and aggregation-based transformations.
    """
    action_type = "data_transform"
    display_name = "数据变换"
    description = "数据变换：透视/逆透视/转置，支持聚合操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Transform data structures.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str (pivot/unpivot/transpose/aggregate/reshape)
                - data: list of dicts, data to transform
                - pivot_key: str, field to pivot on
                - value_key: str, field with values
                - agg_func: str (sum/avg/count/min/max)
                - save_to_var: str
        
        Returns:
            ActionResult with transformed data.
        """
        operation = params.get('operation', 'pivot')
        data = params.get('data', [])
        save_to_var = params.get('save_to_var', None)

        if not data:
            return ActionResult(success=False, message="No data provided")

        try:
            if operation == 'pivot':
                result = self._pivot(data, params)
            elif operation == 'unpivot':
                result = self._unpivot(data, params)
            elif operation == 'transpose':
                result = self._transpose(data, params)
            elif operation == 'aggregate':
                result = self._aggregate(data, params)
            elif operation == 'reshape':
                result = self._reshape(data, params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

            if save_to_var and hasattr(context, 'vars'):
                context.vars[save_to_var] = result

            return ActionResult(
                success=True,
                message=f"Transformed using {operation}",
                data=result
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Transform error: {e}")

    def _pivot(self, data: List[Dict], params: Dict) -> List[Dict]:
        """Create pivot table from data."""
        pivot_key = params.get('pivot_key', '')
        value_key = params.get('value_key', '')
        agg_func = params.get('agg_func', 'sum')

        if not pivot_key or not value_key:
            return data

        # Group by pivot key
        groups = defaultdict(list)
        for record in data:
            if pivot_key in record:
                groups[record[pivot_key]].append(record[value_key])

        # Aggregate each group
        result = []
        for key, values in groups.items():
            agg_value = self._aggregate_values(values, agg_func)
            result.append({pivot_key: key, value_key: agg_value})

        return result

    def _aggregate_values(self, values: List, func: str) -> Any:
        """Aggregate a list of values."""
        nums = [v for v in values if isinstance(v, (int, float))]
        if not nums:
            return values[0] if values else None

        if func == 'sum':
            return sum(nums)
        elif func == 'avg':
            return sum(nums) / len(nums)
        elif func == 'count':
            return len(values)
        elif func == 'min':
            return min(nums)
        elif func == 'max':
            return max(nums)
        elif func == 'first':
            return values[0]
        elif func == 'last':
            return values[-1]
        return nums[0]

    def _unpivot(self, data: List[Dict], params: Dict) -> List[Dict]:
        """Unpivot (melt) a wide table to long format."""
        id_vars = params.get('id_vars', [])
        value_vars = params.get('value_vars', None)
        var_name = params.get('var_name', 'variable')
        val_name = params.get('val_name', 'value')

        if not data:
            return []

        # Auto-detect value vars (all non-id columns)
        if value_vars is None:
            if id_vars:
                value_vars = [k for k in data[0].keys() if k not in id_vars]
            else:
                # Use first column as id
                id_key = list(data[0].keys())[0]
                id_vars = [id_key]
                value_vars = [k for k in data[0].keys() if k != id_key]

        result = []
        for record in data:
            for var in value_vars:
                if var in record:
                    result.append({
                        **({k: record[k] for k in id_vars if k in record}),
                        var_name: var,
                        val_name: record[var]
                    })

        return result

    def _transpose(self, data: List[Dict], params: Dict) -> List[Dict]:
        """Transpose a list of dicts (swap rows and columns)."""
        if not data:
            return []

        keys = list(data[0].keys())
        transposed = []
        for key in keys:
            new_record = {'_column': key}
            for i, record in enumerate(data):
                if key in record:
                    new_record[f'_row_{i}'] = record[key]
            transposed.append(new_record)

        return transposed

    def _aggregate(self, data: List[Dict], params: Dict) -> List[Dict]:
        """Aggregate data by group."""
        group_by = params.get('group_by', [])
        agg_field = params.get('agg_field', None)
        agg_func = params.get('agg_func', 'sum')

        if not group_by or not agg_field:
            return data

        groups = defaultdict(list)
        for record in data:
            key = tuple(record.get(k) for k in group_by)
            if agg_field in record:
                groups[key].append(record[agg_field])

        result = []
        for key, values in groups.items():
            result_dict = dict(zip(group_by, key))
            result_dict[f'{agg_field}_{agg_func}'] = self._aggregate_values(values, agg_func)
            result_dict['count'] = len(values)
            result.append(result_dict)

        return result

    def _reshape(self, data: List[Dict], params: Dict) -> List[Dict]:
        """Reshape data using melt/pivot operations."""
        shape_type = params.get('shape_type', 'wide_to_long')
        index = params.get('index', [])
        columns = params.get('columns', [])
        values = params.get('values', [])

        if shape_type == 'wide_to_long':
            return self._unpivot(data, {'id_vars': index, 'value_vars': columns})
        elif shape_type == 'long_to_wide':
            return self._pivot(data, {
                'pivot_key': columns[0] if columns else '',
                'value_key': values[0] if values else '',
                'agg_func': 'sum'
            })
        return data

    def get_required_params(self) -> List[str]:
        return ['operation', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'pivot_key': '',
            'value_key': '',
            'agg_func': 'sum',
            'group_by': [],
            'agg_field': '',
            'id_vars': [],
            'value_vars': None,
            'var_name': 'variable',
            'val_name': 'value',
            'index': [],
            'columns': [],
            'values': [],
            'shape_type': 'wide_to_long',
            'save_to_var': None,
        }
