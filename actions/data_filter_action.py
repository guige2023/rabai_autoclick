"""
Data Filter Action Module.

Filters data based on conditions, expressions, ranges,
and pattern matching with support for complex logic.

Author: RabAi Team
"""

from __future__ import annotations

import re
import sys
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class FilterOperator(Enum):
    """Filter operators."""
    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    IN = "in"
    NOT_IN = "not_in"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    REGEX = "regex"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    BETWEEN = "between"
    CUSTOM = "custom"


class FilterLogic(Enum):
    """Logic combinators for multiple conditions."""
    AND = "and"
    OR = "or"
    NAND = "nand"
    NOR = "nor"


@dataclass
class FilterCondition:
    """A single filter condition."""
    field: str
    operator: FilterOperator
    value: Any = None
    value2: Any = None


class DataFilterAction(BaseAction):
    """Data filter action.
    
    Filters data based on conditions, expressions, and
    pattern matching with complex logical combinations.
    """
    action_type = "data_filter"
    display_name = "数据过滤"
    description = "数据条件过滤"
    
    def __init__(self):
        super().__init__()
        self._custom_filters: Dict[str, Callable] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Filter data.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - data: Data to filter
                - operation: filter/partition/exclude/sample
                - conditions: List of filter conditions
                - logic: AND/OR/NAND/NOR for combining conditions
                - field: Field name for single-condition filtering
                - operator: Filter operator
                - value: Filter value
                - expression: Filter expression string
                - limit: Max records to return
                - offset: Records to skip
                
        Returns:
            ActionResult with filtered data.
        """
        start_time = time.time()
        
        operation = params.get("operation", "filter")
        data = params.get("data", [])
        conditions = params.get("conditions", [])
        logic = params.get("logic", "and")
        field = params.get("field")
        operator_str = params.get("operator", "eq")
        value = params.get("value")
        expression = params.get("expression")
        limit = params.get("limit")
        offset = params.get("offset", 0)
        
        try:
            logic_type = FilterLogic(logic)
        except ValueError:
            logic_type = FilterLogic.AND
        
        try:
            if not conditions and field:
                operator = FilterOperator(operator_str)
                conditions = [FilterCondition(field=field, operator=operator, value=value)]
        except ValueError:
            return ActionResult(
                success=False,
                message=f"Unknown operator: {operator_str}",
                duration=time.time() - start_time
            )
        
        try:
            if operation == "filter":
                result = self._filter_data(data, conditions, logic_type, expression, limit, offset, start_time)
            elif operation == "partition":
                result = self._partition_data(data, conditions, logic_type, start_time)
            elif operation == "exclude":
                result = self._exclude_data(data, conditions, logic_type, expression, start_time)
            elif operation == "sample":
                result = self._sample_data(data, value, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
            
            return result
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Filter failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _evaluate_condition(self, record: Any, condition: FilterCondition) -> bool:
        """Evaluate a single filter condition."""
        field_value = self._get_field_value(record, condition.field)
        
        op = condition.operator
        
        if op == FilterOperator.IS_NULL:
            return field_value is None
        
        if op == FilterOperator.IS_NOT_NULL:
            return field_value is not None
        
        if op == FilterOperator.EQ:
            return self._compare_eq(field_value, condition.value)
        
        if op == FilterOperator.NE:
            return not self._compare_eq(field_value, condition.value)
        
        if op == FilterOperator.GT:
            return self._compare_gt(field_value, condition.value)
        
        if op == FilterOperator.GTE:
            return self._compare_gte(field_value, condition.value)
        
        if op == FilterOperator.LT:
            return self._compare_lt(field_value, condition.value)
        
        if op == FilterOperator.LTE:
            return self._compare_lte(field_value, condition.value)
        
        if op == FilterOperator.IN:
            return field_value in (condition.value if isinstance(condition.value, (list, tuple, set)) else [condition.value])
        
        if op == FilterOperator.NOT_IN:
            return field_value not in (condition.value if isinstance(condition.value, (list, tuple, set)) else [condition.value])
        
        if op == FilterOperator.CONTAINS:
            return self._contains(field_value, condition.value)
        
        if op == FilterOperator.NOT_CONTAINS:
            return not self._contains(field_value, condition.value)
        
        if op == FilterOperator.STARTS_WITH:
            return self._starts_with(field_value, condition.value)
        
        if op == FilterOperator.ENDS_WITH:
            return self._ends_with(field_value, condition.value)
        
        if op == FilterOperator.REGEX:
            return self._regex_match(field_value, condition.value)
        
        if op == FilterOperator.BETWEEN:
            return self._between(field_value, condition.value, condition.value2)
        
        if op == FilterOperator.CUSTOM:
            return self._custom_filter(condition.value, record)
        
        return True
    
    def _get_field_value(self, record: Any, field: str) -> Any:
        """Get value from record by field path."""
        if field is None or field == "":
            return record
        
        if isinstance(record, dict):
            parts = field.split(".")
            value = record
            for part in parts:
                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    return None
            return value
        elif isinstance(record, (list, tuple)) and field.isdigit():
            idx = int(field)
            return record[idx] if 0 <= idx < len(record) else None
        else:
            return getattr(record, field, None) if hasattr(record, field) else None
    
    def _compare_eq(self, a: Any, b: Any) -> bool:
        """Compare equality."""
        if a is None or b is None:
            return a is b
        try:
            return float(a) == float(b) if isinstance(a, (int, float)) and isinstance(b, (int, float)) else str(a) == str(b)
        except (ValueError, TypeError):
            return str(a) == str(b)
    
    def _compare_gt(self, a: Any, b: Any) -> bool:
        """Compare greater than."""
        try:
            return float(a) > float(b)
        except (ValueError, TypeError):
            return str(a) > str(b)
    
    def _compare_gte(self, a: Any, b: Any) -> bool:
        """Compare greater than or equal."""
        try:
            return float(a) >= float(b)
        except (ValueError, TypeError):
            return str(a) >= str(b)
    
    def _compare_lt(self, a: Any, b: Any) -> bool:
        """Compare less than."""
        try:
            return float(a) < float(b)
        except (ValueError, TypeError):
            return str(a) < str(b)
    
    def _compare_lte(self, a: Any, b: Any) -> bool:
        """Compare less than or equal."""
        try:
            return float(a) <= float(b)
        except (ValueError, TypeError):
            return str(a) <= str(b)
    
    def _contains(self, a: Any, b: Any) -> bool:
        """Check if a contains b."""
        if a is None:
            return False
        return str(b) in str(a)
    
    def _starts_with(self, a: Any, b: Any) -> bool:
        """Check if a starts with b."""
        if a is None:
            return False
        return str(a).startswith(str(b))
    
    def _ends_with(self, a: Any, b: Any) -> bool:
        """Check if a ends with b."""
        if a is None:
            return False
        return str(a).endswith(str(b))
    
    def _regex_match(self, a: Any, pattern: str) -> bool:
        """Check regex match."""
        if a is None:
            return False
        try:
            return bool(re.search(pattern, str(a)))
        except re.error:
            return False
    
    def _between(self, a: Any, low: Any, high: Any) -> bool:
        """Check if a is between low and high."""
        try:
            a_f, low_f, high_f = float(a), float(low), float(high)
            return low_f <= a_f <= high_f
        except (ValueError, TypeError):
            return str(a) >= str(low) and str(a) <= str(high)
    
    def _custom_filter(self, filter_name: str, record: Any) -> bool:
        """Apply custom filter function."""
        if filter_name not in self._custom_filters:
            return False
        try:
            return bool(self._custom_filters[filter_name](record))
        except Exception:
            return False
    
    def _evaluate_logical_combination(
        self, record: Any, conditions: List[FilterCondition], logic: FilterLogic
    ) -> bool:
        """Evaluate combination of conditions."""
        if not conditions:
            return True
        
        results = [self._evaluate_condition(record, cond) for cond in conditions]
        
        if logic == FilterLogic.AND:
            return all(results)
        elif logic == FilterLogic.OR:
            return any(results)
        elif logic == FilterLogic.NAND:
            return not all(results)
        elif logic == FilterLogic.NOR:
            return not any(results)
        
        return all(results)
    
    def _evaluate_expression(self, record: Any, expression: str) -> bool:
        """Evaluate a filter expression."""
        import math
        
        def safe_eval(expr: str, context: Dict) -> Any:
            context_safe = {k: v for k, v in context.items() if not k.startswith("_")}
            context_safe["math"] = math
            context_safe["str"] = str
            context_safe["int"] = int
            context_safe["float"] = float
            context_safe["bool"] = bool
            context_safe["len"] = len
            context_safe["abs"] = abs
            context_safe["min"] = min
            context_safe["max"] = max
            context_safe["sum"] = sum
            context_safe["any"] = any
            context_safe["all"] = all
            context_safe["re"] = re
            
            try:
                return eval(expr, {"__builtins__": {}}, context_safe)
            except Exception:
                return False
        
        if isinstance(record, dict):
            context = dict(record)
        else:
            context = {"record": record}
        
        return bool(safe_eval(expression, context))
    
    def _filter_data(
        self, data: List, conditions: List[FilterCondition], logic: FilterLogic,
        expression: Optional[str], limit: Optional[int], offset: int, start_time: float
    ) -> ActionResult:
        """Filter data based on conditions."""
        filtered = []
        matched_count = 0
        skipped_count = 0
        
        for record in data:
            if offset > 0 and skipped_count < offset:
                skipped_count += 1
                continue
            
            matched = False
            
            if expression:
                matched = self._evaluate_expression(record, expression)
            elif conditions:
                matched = self._evaluate_logical_combination(record, conditions, logic)
            else:
                matched = True
            
            if matched:
                filtered.append(record)
                matched_count += 1
            
            if limit and len(filtered) >= limit:
                break
        
        return ActionResult(
            success=True,
            message=f"Filtered {matched_count} records",
            data={
                "filtered": filtered,
                "count": len(filtered),
                "matched": matched_count,
                "skipped": skipped_count
            },
            duration=time.time() - start_time
        )
    
    def _partition_data(
        self, data: List, conditions: List[FilterCondition], logic: FilterLogic, start_time: float
    ) -> ActionResult:
        """Partition data into matching and non-matching."""
        matched = []
        not_matched = []
        
        for record in data:
            if self._evaluate_logical_combination(record, conditions, logic):
                matched.append(record)
            else:
                not_matched.append(record)
        
        return ActionResult(
            success=True,
            message=f"Partitioned: {len(matched)} matched, {len(not_matched)} not matched",
            data={
                "matched": matched,
                "not_matched": not_matched,
                "matched_count": len(matched),
                "not_matched_count": len(not_matched)
            },
            duration=time.time() - start_time
        )
    
    def _exclude_data(
        self, data: List, conditions: List[FilterCondition], logic: FilterLogic,
        expression: Optional[str], start_time: float
    ) -> ActionResult:
        """Exclude records matching conditions."""
        excluded = []
        remaining = []
        
        for record in data:
            if expression:
                matched = self._evaluate_expression(record, expression)
            else:
                matched = self._evaluate_logical_combination(record, conditions, logic)
            
            if matched:
                excluded.append(record)
            else:
                remaining.append(record)
        
        return ActionResult(
            success=True,
            message=f"Excluded {len(excluded)} records",
            data={
                "remaining": remaining,
                "excluded": excluded,
                "remaining_count": len(remaining),
                "excluded_count": len(excluded)
            },
            duration=time.time() - start_time
        )
    
    def _sample_data(self, data: List, sample_size: Any, start_time: float) -> ActionResult:
        """Randomly sample records from data."""
        import random
        
        if isinstance(sample_size, float) and 0 < sample_size <= 1:
            n = int(len(data) * sample_size)
        elif isinstance(sample_size, int):
            n = min(sample_size, len(data))
        else:
            n = max(1, len(data) // 10)
        
        sampled = random.sample(data, n) if n <= len(data) else data
        
        return ActionResult(
            success=True,
            message=f"Sampled {len(sampled)} records",
            data={
                "sampled": sampled,
                "count": len(sampled),
                "original_count": len(data)
            },
            duration=time.time() - start_time
        )
    
    def register_filter(self, name: str, filter_fn: Callable) -> None:
        """Register a custom filter function."""
        self._custom_filters[name] = filter_fn
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate filter parameters."""
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return []
