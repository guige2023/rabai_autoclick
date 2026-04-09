"""Data Transform Pipeline Action Module.

Provides chained data transformation operations.
"""

import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataTransformPipelineAction(BaseAction):
    """Build and execute transformation pipelines.
    
    Chains multiple transformations on data.
    """
    action_type = "data_transform_pipeline"
    display_name = "数据转换管道"
    description = "构建和执行数据转换管道"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute transformation pipeline.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, transforms.
        
        Returns:
            ActionResult with transformed data.
        """
        data = params.get('data', [])
        transforms = params.get('transforms', [])
        
        if not data:
            return ActionResult(
                success=False,
                data=None,
                error="No data provided"
            )
        
        if not transforms:
            return ActionResult(
                success=True,
                data={'data': data, 'applied': []},
                error=None
            )
        
        current_data = data
        applied = []
        
        for transform in transforms:
            transform_type = transform.get('type', '')
            
            try:
                if transform_type == 'filter':
                    current_data = self._filter(current_data, transform)
                elif transform_type == 'map':
                    current_data = self._map(current_data, transform)
                elif transform_type == 'flatmap':
                    current_data = self._flatmap(current_data, transform)
                elif transform_type == 'reduce':
                    current_data = self._reduce(current_data, transform)
                elif transform_type == 'sort':
                    current_data = self._sort(current_data, transform)
                elif transform_type == 'group':
                    current_data = self._group(current_data, transform)
                else:
                    continue
                
                applied.append(transform_type)
                
            except Exception as e:
                return ActionResult(
                    success=False,
                    data={'applied': applied, 'error_at': transform_type},
                    error=f"Transform {transform_type} failed: {str(e)}"
                )
        
        return ActionResult(
            success=True,
            data={
                'data': current_data,
                'applied': applied,
                'count': len(current_data)
            },
            error=None
        )
    
    def _filter(self, data: List, transform: Dict) -> List:
        """Filter data."""
        field = transform.get('field', '')
        operator = transform.get('operator', 'eq')
        value = transform.get('value')
        
        result = []
        for item in data:
            if not isinstance(item, dict):
                continue
            
            item_value = item.get(field)
            
            if operator == 'eq' and item_value == value:
                result.append(item)
            elif operator == 'ne' and item_value != value:
                result.append(item)
            elif operator == 'gt' and item_value > value:
                result.append(item)
            elif operator == 'lt' and item_value < value:
                result.append(item)
            elif operator == 'contains' and value in str(item_value):
                result.append(item)
            elif operator == 'in' and item_value in value:
                result.append(item)
        
        return result
    
    def _map(self, data: List, transform: Dict) -> List:
        """Map data to new structure."""
        field = transform.get('field', '')
        new_field = transform.get('new_field', field)
        func = transform.get('func', 'identity')
        
        result = []
        for item in data:
            if not isinstance(item, dict):
                continue
            
            new_item = item.copy()
            value = item.get(field)
            
            if func == 'uppercase':
                new_item[new_field] = str(value).upper()
            elif func == 'lowercase':
                new_item[new_field] = str(value).lower()
            elif func == 'abs':
                new_item[new_field] = abs(value) if isinstance(value, (int, float)) else value
            elif func == 'str':
                new_item[new_field] = str(value)
            elif func == 'int':
                new_item[new_field] = int(value) if value else 0
            else:
                new_item[new_field] = value
            
            result.append(new_item)
        
        return result
    
    def _flatmap(self, data: List, transform: Dict) -> List:
        """Flatmap data."""
        field = transform.get('field', '')
        result = []
        
        for item in data:
            if not isinstance(item, dict):
                continue
            
            value = item.get(field, [])
            if isinstance(value, list):
                for v in value:
                    new_item = item.copy()
                    new_item[field] = v
                    result.append(new_item)
            else:
                result.append(item)
        
        return result
    
    def _reduce(self, data: List, transform: Dict) -> List:
        """Reduce data to aggregated form."""
        group_by = transform.get('group_by', [])
        agg_func = transform.get('agg_func', 'sum')
        agg_field = transform.get('agg_field', '')
        
        groups = {}
        for item in data:
            if not isinstance(item, dict):
                continue
            
            key = tuple(item.get(f) for f in group_by)
            if key not in groups:
                groups[key] = []
            groups[key].append(item)
        
        result = []
        for key, items in groups.items():
            new_item = dict(zip(group_by, key))
            
            if agg_field:
                values = [item.get(agg_field, 0) for item in items]
                if agg_func == 'sum':
                    new_item[f'{agg_field}_sum'] = sum(values)
                elif agg_func == 'avg':
                    new_item[f'{agg_field}_avg'] = sum(values) / len(values)
                elif agg_func == 'min':
                    new_item[f'{agg_field}_min'] = min(values)
                elif agg_func == 'max':
                    new_item[f'{agg_field}_max'] = max(values)
                elif agg_func == 'count':
                    new_item[f'{agg_field}_count'] = len(values)
            
            result.append(new_item)
        
        return result
    
    def _sort(self, data: List, transform: Dict) -> List:
        """Sort data."""
        field = transform.get('field', '')
        order = transform.get('order', 'asc')
        
        return sorted(
            data,
            key=lambda x: x.get(field) if isinstance(x, dict) else x,
            reverse=(order == 'desc')
        )
    
    def _group(self, data: List, transform: Dict) -> List:
        """Group data."""
        group_by = transform.get('group_by', [])
        
        groups = {}
        for item in data:
            if not isinstance(item, dict):
                continue
            
            key = tuple(item.get(f) for f in group_by)
            if key not in groups:
                groups[key] = []
            groups[key].append(item)
        
        result = []
        for key, items in groups.items():
            new_item = dict(zip(group_by, key))
            new_item['_group'] = items
            new_item['_count'] = len(items)
            result.append(new_item)
        
        return result


class DataTransformerAction(BaseAction):
    """Apply individual transformations to data.
    
    Single-step data transformation operations.
    """
    action_type = "data_transformer"
    display_name = "数据转换器"
    description = "应用单项数据转换操作"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute transformation.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, transform_type, options.
        
        Returns:
            ActionResult with transformed data.
        """
        data = params.get('data', [])
        transform_type = params.get('transform_type', 'normalize')
        options = params.get('options', {})
        
        try:
            if transform_type == 'normalize':
                result = self._normalize(data, options)
            elif transform_type == 'standardize':
                result = self._standardize(data, options)
            elif transform_type == 'pivot':
                result = self._pivot(data, options)
            elif transform_type == 'unpivot':
                result = self._unpivot(data, options)
            elif transform_type == 'transpose':
                result = self._transpose(data, options)
            else:
                return ActionResult(
                    success=False,
                    data=None,
                    error=f"Unknown transform: {transform_type}"
                )
            
            return ActionResult(
                success=True,
                data={
                    'result': result,
                    'transform': transform_type
                },
                error=None
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                data=None,
                error=f"Transform failed: {str(e)}"
            )
    
    def _normalize(self, data: List, options: Dict) -> List:
        """Normalize values to 0-1 range."""
        field = options.get('field', 'value')
        min_val = min(d.get(field, 0) for d in data if isinstance(d, dict))
        max_val = max(d.get(field, 1) for d in data if isinstance(d, dict))
        
        if max_val == min_val:
            return data
        
        result = []
        for item in data:
            if isinstance(item, dict):
                item_copy = item.copy()
                item_copy[f'{field}_normalized'] = (
                    (item.get(field, 0) - min_val) / (max_val - min_val)
                )
                result.append(item_copy)
            else:
                result.append(item)
        
        return result
    
    def _standardize(self, data: List, options: Dict) -> List:
        """Standardize values (z-score)."""
        import math
        field = options.get('field', 'value')
        values = [d.get(field, 0) for d in data if isinstance(d, dict)]
        
        mean = sum(values) / len(values) if values else 0
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        stddev = math.sqrt(variance)
        
        if stddev == 0:
            return data
        
        result = []
        for item in data:
            if isinstance(item, dict):
                item_copy = item.copy()
                item_copy[f'{field}_standardized'] = (
                    (item.get(field, 0) - mean) / stddev
                )
                result.append(item_copy)
            else:
                result.append(item)
        
        return result
    
    def _pivot(self, data: List, options: Dict) -> List:
        """Pivot data."""
        index = options.get('index', 'id')
        columns = options.get('columns', 'category')
        values = options.get('values', 'value')
        
        pivot = {}
        for item in data:
            if not isinstance(item, dict):
                continue
            
            idx = item.get(index)
            col = item.get(columns)
            val = item.get(values)
            
            if idx not in pivot:
                pivot[idx] = {}
            pivot[idx][col] = val
        
        return [{index: k, **v} for k, v in pivot.items()]
    
    def _unpivot(self, data: List, options: Dict) -> List:
        """Unpivot data."""
        value_columns = options.get('value_columns', [])
        
        result = []
        for item in data:
            if not isinstance(item, dict):
                continue
            
            non_value_cols = {k: v for k, v in item.items() if k not in value_columns}
            
            for col in value_columns:
                if col in item:
                    new_item = {**non_value_cols, 'column': col, 'value': item[col]}
                    result.append(new_item)
        
        return result
    
    def _transpose(self, data: List, options: Dict) -> List:
        """Transpose data."""
        if not data or not isinstance(data[0], dict):
            return data
        
        keys = list(data[0].keys())
        result = []
        
        for key in keys:
            new_item = {'column': key}
            for i, item in enumerate(data):
                new_item[f'row_{i}'] = item.get(key)
            result.append(new_item)
        
        return result


def register_actions():
    """Register all Data Transform Pipeline actions."""
    return [
        DataTransformPipelineAction,
        DataTransformerAction,
    ]
