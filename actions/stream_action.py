"""Stream action module for RabAI AutoClick.

Provides stream processing operations: map, filter, reduce,
window, and real-time aggregation over data streams.
"""

import sys
import os
import time
import threading
from typing import Any, Dict, List, Optional, Callable, Union, Iterator
from collections import deque
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class StreamMapAction(BaseAction):
    """Map a function over a stream of items.
    
    Apply a transformation to each element in a stream,
    yielding a new stream of transformed values.
    """
    action_type = "stream_map"
    display_name = "流映射"
    description = "对流数据执行映射转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Map a function over stream items.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of items (stream)
                - map_field: str (field to transform)
                - expression: str (transformation expression)
                - output_field: str (output field name)
                - save_to_var: str
        
        Returns:
            ActionResult with mapped stream.
        """
        data = params.get('data', [])
        map_field = params.get('map_field', '')
        expression = params.get('expression', '')
        output_field = params.get('output_field', 'result')
        save_to_var = params.get('save_to_var', 'stream_result')

        if not data:
            return ActionResult(success=False, message="No data provided")

        result = []
        for item in data:
            if isinstance(item, dict) and map_field:
                val = item.get(map_field)
                transformed = self._apply_transform(expression, val, item)
                new_item = dict(item)
                new_item[output_field] = transformed
            else:
                transformed = self._apply_transform(expression, item, item)
                if isinstance(item, dict):
                    new_item = dict(item)
                    new_item[output_field] = transformed
                else:
                    new_item = {output_field: transformed}
            result.append(new_item)

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data={'count': len(result), 'items': result},
            message=f"Mapped {len(result)} items"
        )

    def _apply_transform(self, expr: str, val: Any, row: Dict) -> Any:
        """Apply transformation expression to value."""
        import math
        if not expr:
            return val
        
        # Replace $value with the current value
        local_expr = expr.replace('$value', repr(val))
        
        # Also replace $field references
        for k, v in row.items():
            local_expr = local_expr.replace(f'${k}', repr(v))
        
        try:
            allowed = {'math': math, '__builtins__': {}}
            return eval(local_expr, allowed, {})
        except:
            return val


class StreamFilterAction(BaseAction):
    """Filter a stream of items by predicate.
    
    Keep only items that match a condition.
    """
    action_type = "stream_filter"
    display_name = "流过滤"
    description = "按条件过滤流数据"

    OPERATORS = ['==', '!=', '>', '<', '>=', '<=', 'in', 'not in', 'contains', 'startswith', 'endswith']

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Filter stream items.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of items
                - field: str (field to check)
                - operator: str (comparison operator)
                - value: any (comparison value)
                - expression: str (complex filter expression)
                - save_to_var: str
        
        Returns:
            ActionResult with filtered stream.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        operator = params.get('operator', '==')
        value = params.get('value')
        expression = params.get('expression', '')
        save_to_var = params.get('save_to_var', 'filtered_stream')

        if not data:
            return ActionResult(success=False, message="No data provided")

        result = []
        for item in data:
            if expression:
                if self._eval_expr(expression, item):
                    result.append(item)
            elif field:
                if operator in self.OPERATORS:
                    item_val = item.get(field) if isinstance(item, dict) else item
                    if self._compare(item_val, operator, value):
                        result.append(item)
            else:
                # No filter specified, include all
                result.append(item)

        removed = len(data) - len(result)

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data={'total': len(data), 'filtered': len(result), 'removed': removed},
            message=f"Filtered: {len(result)}/{len(data)} kept"
        )

    def _compare(self, val: Any, op: str, cmp_val: Any) -> bool:
        """Compare with operator."""
        try:
            if op == '==':
                return str(val) == str(cmp_val)
            elif op == '!=':
                return str(val) != str(cmp_val)
            elif op == '>':
                return float(val) > float(cmp_val)
            elif op == '<':
                return float(val) < float(cmp_val)
            elif op == '>=':
                return float(val) >= float(cmp_val)
            elif op == '<=':
                return float(val) <= float(cmp_val)
            elif op == 'in':
                return val in cmp_val if isinstance(cmp_val, list) else str(val) in str(cmp_val)
            elif op == 'not in':
                return val not in cmp_val if isinstance(cmp_val, list) else str(val) not in str(cmp_val)
            elif op == 'contains':
                return str(cmp_val) in str(val)
            elif op == 'startswith':
                return str(val).startswith(str(cmp_val))
            elif op == 'endswith':
                return str(val).endswith(str(cmp_val))
        except (ValueError, TypeError):
            pass
        return False

    def _eval_expr(self, expr: str, row: Dict) -> bool:
        """Evaluate a boolean expression against a row."""
        import math
        local_expr = expr
        for k, v in row.items():
            local_expr = local_expr.replace(f'${k}', repr(v))
        
        try:
            allowed = {'math': math, '__builtins__': {}}
            return bool(eval(local_expr, allowed, {}))
        except:
            return False


class StreamReduceAction(BaseAction):
    """Reduce a stream to a single value.
    
    Accumulate stream items into a summary using
    a reducer function.
    """
    action_type = "stream_reduce"
    display_name = "流聚合"
    description = "将流数据聚合为单个值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Reduce stream to single value.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of items
                - field: str (field to reduce)
                - reduce_func: str (sum/avg/count/min/max/median/concat/collect)
                - initial: any (initial accumulator value)
                - save_to_var: str
        
        Returns:
            ActionResult with reduced value.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        reduce_func = params.get('reduce_func', 'count')
        initial = params.get('initial', None)
        save_to_var = params.get('save_to_var', 'reduce_result')

        if not data:
            return ActionResult(success=False, message="No data provided")

        values = []
        for item in data:
            if field:
                val = item.get(field) if isinstance(item, dict) else item
            else:
                val = item
            values.append(val)

        result = self._reduce(values, reduce_func, initial)

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data={'value': result, 'count': len(values)},
            message=f"Reduced with {reduce_func}: {result}"
        )

    def _reduce(self, values: List, func: str, initial: Any) -> Any:
        """Perform reduction."""
        if not values:
            return initial

        try:
            nums = [float(v) for v in values]
        except (ValueError, TypeError):
            nums = values

        if func == 'sum':
            return sum(nums) if nums else 0
        elif func == 'avg' or func == 'mean':
            return sum(nums) / len(nums) if nums else 0
        elif func == 'count':
            return len(values)
        elif func == 'min':
            return min(nums) if nums else None
        elif func == 'max':
            return max(nums) if nums else None
        elif func == 'median':
            sorted_vals = sorted(nums)
            n = len(sorted_vals)
            mid = n // 2
            if n % 2 == 0:
                return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2
            return sorted_vals[mid]
        elif func == 'concat':
            return ''.join(str(v) for v in values)
        elif func == 'collect':
            return values
        elif func == 'first':
            return values[0]
        elif func == 'last':
            return values[-1]
        return values[0] if values else None


class StreamBufferAction(BaseAction):
    """Buffer stream items into batches.
    
    Collect stream items into fixed-size or timed batches.
    """
    action_type = "stream_buffer"
    display_name = "流缓冲"
    description = "将流数据缓冲成分批处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Buffer stream items into batches.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of items
                - batch_size: int (items per batch)
                - flush_on_save: bool (flush buffer when saving)
                - save_to_var: str
        
        Returns:
            ActionResult with batched stream.
        """
        data = params.get('data', [])
        batch_size = params.get('batch_size', 10)
        flush_on_save = params.get('flush_on_save', True)
        save_to_var = params.get('save_to_var', 'buffered_stream')

        if not data:
            return ActionResult(success=False, message="No data provided")

        batches = []
        current_batch = []

        for item in data:
            current_batch.append(item)
            if len(current_batch) >= batch_size:
                batches.append({
                    'batch_id': len(batches),
                    'size': len(current_batch),
                    'items': list(current_batch),
                })
                current_batch = []

        # Handle remaining items
        if current_batch:
            batches.append({
                'batch_id': len(batches),
                'size': len(current_batch),
                'items': list(current_batch),
                'incomplete': True,
            })

        result = {
            'batch_count': len(batches),
            'batches': batches,
            'flush_on_save': flush_on_save,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Buffered {len(data)} items into {len(batches)} batches"
        )


class StreamDedupeAction(BaseAction):
    """Deduplicate consecutive items in a stream.
    
    Remove consecutive duplicates, keeping only the first.
    """
    action_type = "stream_dedupe"
    display_name = "流去重"
    description = "去除流中相邻的重复项"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Remove consecutive duplicates.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of items
                - key: str (field to check for duplicates)
                - save_to_var: str
        
        Returns:
            ActionResult with deduplicated stream.
        """
        data = params.get('data', [])
        key = params.get('key', '')
        save_to_var = params.get('save_to_var', 'deduped_stream')

        if not data:
            return ActionResult(success=False, message="No data provided")

        result = []
        prev_key = None
        removed = 0

        for item in data:
            if key:
                curr_key = item.get(key) if isinstance(item, dict) else item
            else:
                curr_key = item

            if curr_key != prev_key:
                result.append(item)
                prev_key = curr_key
            else:
                removed += 1

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data={'original': len(data), 'deduped': len(result), 'removed': removed},
            message=f"Removed {removed} consecutive duplicates"
        )
