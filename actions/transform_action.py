"""Transform action module for RabAI AutoClick.

Provides data transformation operations: mapping,
filtering, pivoting, and custom transformations.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Union, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class MapTransformAction(BaseAction):
    """Apply a mapping transformation to data.
    
    Transform each record by applying a mapping
    of input fields to output fields.
    """
    action_type = "map_transform"
    display_name: "映射变换"
    description = "将输入字段映射到输出字段的变换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Apply map transformation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of dicts
                - mapping: dict {output_field: input_field or expression}
                - drop_nulls: bool
                - save_to_var: str
        
        Returns:
            ActionResult with transformed data.
        """
        data = params.get('data', [])
        mapping = params.get('mapping', {})
        drop_nulls = params.get('drop_nulls', False)
        save_to_var = params.get('save_to_var', 'map_result')

        if not data:
            return ActionResult(success=False, message="No data provided")

        result = []
        for record in data:
            if not isinstance(record, dict):
                record = {'_value': record}

            new_record = {}
            for out_field, in_field in mapping.items():
                if isinstance(in_field, str) and not in_field.startswith('$'):
                    new_record[out_field] = record.get(in_field)
                elif isinstance(in_field, str) and in_field.startswith('$'):
                    # Expression
                    expr = in_field[1:]
                    new_record[out_field] = self._eval_simple(record, expr)

            if drop_nulls:
                new_record = {k: v for k, v in new_record.items() if v is not None}

            result.append(new_record)

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data={'count': len(result), 'data': result},
            message=f"Mapped {len(data)} records to {len(result)}"
        )

    def _eval_simple(self, record: Dict, expr: str) -> Any:
        """Evaluate simple expression on record."""
        import math
        try:
            local_vars = dict(record)
            allowed = {'math': math, '__builtins__': {}}
            return eval(expr, allowed, local_vars)
        except:
            return None


class FlattenTransformAction(BaseAction):
    """Flatten nested data structures.
    
    Convert nested dicts and lists into flat
    key-value pairs with dot-notation keys.
    """
    action_type: "flatten_transform"
    display_name = "扁平化变换"
    description = "将嵌套数据结构扁平化为点号分隔的键"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Flatten nested data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of dicts
                - separator: str (key separator, default '.')
                - max_depth: int (maximum nesting depth)
                - save_to_var: str
        
        Returns:
            ActionResult with flattened data.
        """
        data = params.get('data', [])
        separator = params.get('separator', '.')
        max_depth = params.get('max_depth', 10)
        save_to_var = params.get('save_to_var', 'flatten_result')

        if not data:
            return ActionResult(success=False, message="No data provided")

        result = []
        for record in data:
            if isinstance(record, dict):
                flat = self._flatten_dict(record, separator, max_depth)
                result.append(flat)
            else:
                result.append({'_value': record})

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data={'count': len(result), 'data': result},
            message=f"Flattened {len(data)} records"
        )

    def _flatten_dict(self, d: Dict, sep: str, max_depth: int, parent_key: str = '', depth: int = 0) -> Dict:
        """Recursively flatten a dictionary."""
        if depth >= max_depth:
            return {parent_key: str(d)} if parent_key else {k: str(v) for k, v in d.items()}

        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, sep, max_depth, new_key, depth + 1).items())
            elif isinstance(v, list):
                for i, item in enumerate(v):
                    list_key = f"{new_key}[{i}]"
                    if isinstance(item, dict):
                        items.extend(self._flatten_dict(item, sep, max_depth, list_key, depth + 1).items())
                    else:
                        items.append((list_key, item))
            else:
                items.append((new_key, v))
        return dict(items)


class FoldTransformAction(BaseAction):
    """Fold/accumulate values across records.
    
    Compute running totals, cumulative functions,
    and sequence-dependent transformations.
    """
    action_type = "fold_transform"
    display_name = "折叠变换"
    description = "跨记录折叠/累积值：计算运行总和与序列变换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Apply fold transformation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of dicts
                - field: str (numeric field to fold)
                - fold_type: str (sum/count/min/max/avg/product)
                - output_field: str (output field name)
                - group_by: str (optional grouping field)
                - save_to_var: str
        
        Returns:
            ActionResult with folded data.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        fold_type = params.get('fold_type', 'sum')
        output_field = params.get('output_field', 'cumulative')
        group_by = params.get('group_by', '')
        save_to_var = params.get('save_to_var', 'fold_result')

        if not data:
            return ActionResult(success=False, message="No data provided")

        if group_by:
            groups: Dict[str, List] = {}
            for record in data:
                key = str(record.get(group_by, '__unknown__'))
                if key not in groups:
                    groups[key] = []
                groups[key].append(record)

            result = []
            for group_key, group_data in groups.items():
                folded = self._fold_list(group_data, field, fold_type, output_field)
                for item in folded:
                    item[group_by] = group_key
                    result.append(item)
        else:
            result = self._fold_list(data, field, fold_type, output_field)

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data={'count': len(result), 'data': result},
            message=f"Folded {len(data)} records with {fold_type}"
        )

    def _fold_list(self, data: List, field: str, fold_type: str, output_field: str) -> List:
        """Apply fold to a list of records."""
        result = []
        running = 0
        count = 0
        min_val = None
        max_val = None
        product = 1

        for record in data:
            new_record = dict(record)

            if field:
                val = record.get(field)
                try:
                    num = float(val)
                except (ValueError, TypeError):
                    num = 0
            else:
                num = 1

            if fold_type == 'sum':
                running += num
            elif fold_type == 'count':
                count += 1
                running = count
            elif fold_type == 'min':
                min_val = num if min_val is None else min(min_val, num)
                running = min_val
            elif fold_type == 'max':
                max_val = num if max_val is None else max(max_val, num)
                running = max_val
            elif fold_type == 'avg':
                count += 1
                running = (running * (count - 1) + num) / count
            elif fold_type == 'product':
                product *= num
                running = product

            new_record[output_field] = running
            result.append(new_record)

        return result


class NormalizeTransformAction(BaseAction):
    """Normalize numeric values to a range.
    
    Scale values to [0, 1] range or custom min/max.
    """
    action_type = "normalize_transform"
    display_name = "归一化变换"
    description = "将数值归一化到指定范围(默认0-1)"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Normalize numeric values.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of dicts
                - field: str (numeric field)
                - output_field: str (output field)
                - min_value: float (target min, default computed from data)
                - max_value: float (target max, default computed from data)
                - scale_min: float (source min for scaling)
                - scale_max: float (source max for scaling)
                - save_to_var: str
        
        Returns:
            ActionResult with normalized data.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        output_field = params.get('output_field', 'normalized')
        target_min = params.get('min_value', 0.0)
        target_max = params.get('max_value', 1.0)
        scale_min = params.get('scale_min', None)
        scale_max = params.get('scale_max', None)
        save_to_var = params.get('save_to_var', 'normalize_result')

        if not data:
            return ActionResult(success=False, message="No data provided")

        # Compute source min/max if not provided
        values = []
        for record in data:
            if field and isinstance(record, dict):
                val = record.get(field)
            else:
                val = record
            try:
                values.append(float(val))
            except (ValueError, TypeError):
                pass

        if not values:
            return ActionResult(success=False, message="No valid numeric values")

        src_min = scale_min if scale_min is not None else min(values)
        src_max = scale_max if scale_max is not None else max(values)
        src_range = src_max - src_min

        if src_range == 0:
            src_range = 1

        target_range = target_max - target_min

        result = []
        for record in data:
            new_record = dict(record)

            if field and isinstance(record, dict):
                val = record.get(field)
            else:
                val = record

            try:
                num = float(val)
                normalized = target_min + (num - src_min) / src_range * target_range
                normalized = max(target_min, min(target_max, normalized))
            except (ValueError, TypeError):
                normalized = None

            new_record[output_field] = normalized
            result.append(new_record)

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data={'count': len(result), 'data': result},
            message=f"Normalized {len(data)} values from [{src_min:.2f}, {src_max:.2f}] to [{target_min}, {target_max}]"
        )
