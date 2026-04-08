"""Filter action module for RabAI AutoClick.

Provides data filtering operations with conditions and expressions.
"""

import operator
import re
import sys
import os
from typing import Any, Dict, List, Optional, Union, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class FilterAction(BaseAction):
    """Data filtering with condition expressions.
    
    Supports field-based filtering, logical operators (and/or/not),
    comparison operators, pattern matching, and range filters.
    """
    action_type = "filter"
    display_name = "数据过滤器"
    description = "条件过滤：比较、逻辑、正则、范围"
    
    OPS = {
        '==': operator.eq, 'eq': operator.eq,
        '!=': operator.ne, 'ne': operator.ne,
        '>': operator.gt, 'gt': operator.gt,
        '>=': operator.ge, 'ge': operator.ge,
        '<': operator.lt, 'lt': operator.lt,
        '<=': operator.le, 'le': operator.le,
        'in': lambda a, b: a in b,
        'not_in': lambda a, b: a not in b,
        'contains': lambda a, b: b in a,
        'not_contains': lambda a, b: b not in a,
        'starts_with': lambda a, b: str(a).startswith(b),
        'ends_with': lambda a, b: str(a).endswith(b),
        'regex': lambda a, b: bool(re.search(b, str(a))),
        'is_null': lambda a, _: a is None,
        'is_not_null': lambda a, _: a is not None,
        'is_empty': lambda a, _: (a is None or a == '' or a == [] or a == {}),
        'is_not_empty': lambda a, _: not (a is None or a == '' or a == [] or a == {}),
    }
    
    def __init__(self) -> None:
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute filter operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'filter', 'exclude', 'unique'
                - data: List of items to filter
                - where: Filter condition dict or expression string
                - fields: Fields to keep in output (optional)
                - limit: Max results to return
        
        Returns:
            ActionResult with filtered data.
        """
        command = params.get('command', 'filter')
        data = params.get('data', [])
        where = params.get('where')
        fields = params.get('fields')
        limit = params.get('limit')
        
        if not isinstance(data, list):
            return ActionResult(success=False, message="data must be a list")
        
        if command == 'filter':
            return self._filter_data(data, where, fields, limit)
        if command == 'exclude':
            return self._exclude_data(data, where, fields, limit)
        if command == 'unique':
            return self._unique_data(data, params.get('by'))
        
        return ActionResult(success=False, message=f"Unknown command: {command}")
    
    def _filter_data(self, data: List[Any], where: Any, fields: Optional[List[str]], limit: Optional[int]) -> ActionResult:
        """Filter data by condition."""
        if where is None:
            result = data
        elif isinstance(where, str):
            result = [row for row in data if self._eval_expression(row, where)]
        elif isinstance(where, dict):
            result = [row for row in data if self._eval_condition(row, where)]
        else:
            return ActionResult(success=False, message="where must be dict or expression string")
        
        if fields:
            result = [self._select_fields(row, fields) for row in result]
        
        if limit:
            result = result[:limit]
        
        return ActionResult(
            success=True,
            message=f"Filtered: {len(result)}/{len(data)} items",
            data={'results': result, 'filtered_count': len(result), 'total_count': len(data)}
        )
    
    def _exclude_data(self, data: List[Any], where: Any, fields: Optional[List[str]], limit: Optional[int]) -> ActionResult:
        """Exclude data matching condition (inverse filter)."""
        if where is None:
            return ActionResult(success=True, message="No condition, returning all data", data={'results': data})
        
        result = []
        for row in data:
            if isinstance(where, str):
                matches = self._eval_expression(row, where)
            elif isinstance(where, dict):
                matches = self._eval_condition(row, where)
            else:
                matches = False
            if not matches:
                result.append(row)
        
        if fields:
            result = [self._select_fields(row, fields) for row in result]
        
        if limit:
            result = result[:limit]
        
        return ActionResult(
            success=True,
            message=f"Excluded: {len(result)}/{len(data)} items",
            data={'results': result, 'remaining_count': len(result), 'total_count': len(data)}
        )
    
    def _unique_data(self, data: List[Any], by: Optional[str]) -> ActionResult:
        """Get unique values or items."""
        if by is None:
            seen = set()
            unique = []
            for row in data:
                key = tuple(sorted(row.items())) if isinstance(row, dict) else row
                if key not in seen:
                    seen.add(key)
                    unique.append(row)
        else:
            seen = set()
            unique = []
            for row in data:
                val = row.get(by) if isinstance(row, dict) else getattr(row, by, None)
                if val not in seen:
                    seen.add(val)
                    unique.append(row)
        
        return ActionResult(
            success=True,
            message=f"Unique: {len(unique)}/{len(data)} items",
            data={'results': unique, 'unique_count': len(unique), 'total_count': len(data)}
        )
    
    def _eval_condition(self, row: Dict, condition: Dict) -> bool:
        """Evaluate a condition dict against a row."""
        logic = condition.get('_logic', 'and')
        conditions = []
        
        for key, value in condition.items():
            if key.startswith('_'):
                continue
            row_val = row.get(key) if isinstance(row, dict) else getattr(row, key, None)
            if isinstance(value, dict):
                for op, target in value.items():
                    op_func = self.OPS.get(op)
                    if op_func and not op_func(row_val, target):
                        conditions.append(False)
                        break
                else:
                    conditions.append(True)
            else:
                if row_val != value:
                    conditions.append(False)
                else:
                    conditions.append(True)
        
        if not conditions:
            return True
        
        if logic == 'and':
            return all(conditions)
        elif logic == 'or':
            return any(conditions)
        return True
    
    def _eval_expression(self, row: Dict, expr: str) -> bool:
        """Evaluate a simple expression string."""
        try:
            for key, val in row.items():
                expr = expr.replace(key, repr(val))
            return eval(expr)
        except Exception:
            return False
    
    def _select_fields(self, row: Any, fields: List[str]) -> Dict:
        """Select specific fields from a row."""
        if isinstance(row, dict):
            return {f: row.get(f) for f in fields}
        return {f: getattr(row, f, None) for f in fields}
