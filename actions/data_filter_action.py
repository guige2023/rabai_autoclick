"""Data filter action module for RabAI AutoClick.

Provides data filtering with complex condition support,
including comparison operators, pattern matching, and
compound conditions.
"""

import sys
import os
import re
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class FilterOperator(Enum):
    """Filter operators."""
    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GE = "ge"
    LT = "lt"
    LE = "le"
    IN = "in"
    NOT_IN = "not_in"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    REGEX = "regex"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


from enum import Enum


@dataclass
class FilterCondition:
    """A filter condition."""
    field: str
    operator: FilterOperator
    value: Any = None


class DataFilterAction(BaseAction):
    """Data filter action for filtering data with conditions.
    
    Supports various operators including comparison, pattern
    matching, and compound conditions.
    """
    action_type = "data_filter"
    display_name = "数据过滤器"
    description = "数据条件过滤"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute filter operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                data: Data to filter
                conditions: List of filter conditions
                condition: Single condition (alternative)
                logic: 'and' or 'or' for combining conditions
                exclude: If True, exclude matching records.
        
        Returns:
            ActionResult with filtered data.
        """
        data = params.get('data', [])
        conditions = params.get('conditions', [])
        condition = params.get('condition')
        logic = params.get('logic', 'and')
        exclude = params.get('exclude', False)
        
        if not data:
            return ActionResult(success=False, message="No data provided")
        
        if condition:
            conditions = [condition]
        
        parsed_conditions = [self._parse_condition(c) for c in conditions]
        
        filtered = []
        excluded_count = 0
        
        for item in data:
            matches = self._evaluate_conditions(item, parsed_conditions, logic)
            
            if exclude:
                if not matches:
                    filtered.append(item)
                else:
                    excluded_count += 1
            else:
                if matches:
                    filtered.append(item)
        
        return ActionResult(
            success=True,
            message=f"Filtered {len(data)} items to {len(filtered)}",
            data={
                'items': filtered,
                'count': len(filtered),
                'original_count': len(data),
                'excluded_count': excluded_count if exclude else len(data) - len(filtered)
            }
        )
    
    def _parse_condition(self, cond: Union[Dict, str]) -> FilterCondition:
        """Parse condition definition."""
        if isinstance(cond, str):
            parts = cond.split(':', 1)
            return FilterCondition(
                field=parts[0],
                operator=FilterOperator.EQ,
                value=parts[1] if len(parts) > 1 else None
            )
        
        field = cond.get('field', '')
        op = cond.get('operator', 'eq')
        value = cond.get('value')
        
        try:
            operator = FilterOperator(op)
        except ValueError:
            operator = FilterOperator.EQ
        
        return FilterCondition(field=field, operator=operator, value=value)
    
    def _evaluate_conditions(
        self,
        item: Any,
        conditions: List[FilterCondition],
        logic: str
    ) -> bool:
        """Evaluate all conditions against an item."""
        if not conditions:
            return True
        
        results = []
        
        for cond in conditions:
            result = self._evaluate_condition(item, cond)
            results.append(result)
        
        if logic == 'and':
            return all(results)
        else:
            return any(results)
    
    def _evaluate_condition(self, item: Any, cond: FilterCondition) -> bool:
        """Evaluate single condition."""
        if not isinstance(item, dict):
            return False
        
        value = self._get_value(item, cond.field)
        
        if cond.operator == FilterOperator.EQ:
            return value == cond.value
        elif cond.operator == FilterOperator.NE:
            return value != cond.value
        elif cond.operator == FilterOperator.GT:
            try:
                return float(value) > float(cond.value)
            except (TypeError, ValueError):
                return str(value) > str(cond.value)
        elif cond.operator == FilterOperator.GE:
            try:
                return float(value) >= float(cond.value)
            except (TypeError, ValueError):
                return str(value) >= str(cond.value)
        elif cond.operator == FilterOperator.LT:
            try:
                return float(value) < float(cond.value)
            except (TypeError, ValueError):
                return str(value) < str(cond.value)
        elif cond.operator == FilterOperator.LE:
            try:
                return float(value) <= float(cond.value)
            except (TypeError, ValueError):
                return str(value) <= str(cond.value)
        elif cond.operator == FilterOperator.IN:
            return value in cond.value if isinstance(cond.value, (list, tuple, set)) else False
        elif cond.operator == FilterOperator.NOT_IN:
            return value not in cond.value if isinstance(cond.value, (list, tuple, set)) else True
        elif cond.operator == FilterOperator.CONTAINS:
            return str(cond.value) in str(value) if value is not None else False
        elif cond.operator == FilterOperator.STARTS_WITH:
            return str(value).startswith(str(cond.value)) if value is not None else False
        elif cond.operator == FilterOperator.ENDS_WITH:
            return str(value).endswith(str(cond.value)) if value is not None else False
        elif cond.operator == FilterOperator.REGEX:
            try:
                return bool(re.search(str(cond.value), str(value) if value is not None else ''))
            except re.error:
                return False
        elif cond.operator == FilterOperator.IS_NULL:
            return value is None or value == ''
        elif cond.operator == FilterOperator.IS_NOT_NULL:
            return value is not None and value != ''
        
        return False
    
    def _get_value(self, data: Dict, field: str) -> Any:
        """Get value from dict using dot notation."""
        parts = field.split('.')
        current = data
        
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        
        return current
