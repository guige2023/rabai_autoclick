"""Data filter action module for RabAI AutoClick.

Provides data filtering operations:
- FieldFilterAction: Filter by field values
- RangeFilterAction: Filter by numeric/string ranges
- PatternFilterAction: Filter by pattern matching
- CompositeFilterAction: Combine multiple filters
- DeduplicateFilterAction: Remove duplicate entries
"""

from typing import Any, Dict, List, Optional, Callable
import re

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FieldFilterAction(BaseAction):
    """Filter by field values."""
    action_type = "field_filter"
    display_name = "字段过滤"
    description = "按字段值进行数据过滤"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field")
            operator = params.get("operator", "eq")
            value = params.get("value")
            invert = params.get("invert", False)
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not field:
                return ActionResult(success=False, message="field is required")
            
            filtered = []
            
            for item in data:
                if not isinstance(item, dict):
                    continue
                
                item_value = item.get(field)
                matches = self._evaluate_condition(item_value, operator, value)
                
                if invert:
                    matches = not matches
                
                if matches:
                    filtered.append(item)
            
            return ActionResult(
                success=True,
                message=f"Field filter complete",
                data={
                    "original_count": len(data),
                    "filtered_count": len(filtered),
                    "removed_count": len(data) - len(filtered),
                    "field": field,
                    "operator": operator,
                    "value": value,
                    "filtered_data": filtered[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _evaluate_condition(self, item_value: Any, operator: str, value: Any) -> bool:
        if operator == "eq":
            return item_value == value
        elif operator == "ne":
            return item_value != value
        elif operator == "gt":
            return item_value > value if item_value is not None else False
        elif operator == "ge":
            return item_value >= value if item_value is not None else False
        elif operator == "lt":
            return item_value < value if item_value is not None else False
        elif operator == "le":
            return item_value <= value if item_value is not None else False
        elif operator == "in":
            return item_value in value if isinstance(value, (list, tuple)) else False
        elif operator == "not_in":
            return item_value not in value if isinstance(value, (list, tuple)) else True
        elif operator == "is_null":
            return item_value is None
        elif operator == "is_not_null":
            return item_value is not None
        else:
            return False


class RangeFilterAction(BaseAction):
    """Filter by numeric/string ranges."""
    action_type = "range_filter"
    display_name = "范围过滤"
    description = "按数值或字符串范围进行数据过滤"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field")
            min_val = params.get("min")
            max_val = params.get("max")
            inclusive = params.get("inclusive", True)
            invert = params.get("invert", False)
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not field:
                return ActionResult(success=False, message="field is required")
            
            filtered = []
            
            for item in data:
                if not isinstance(item, dict):
                    continue
                
                item_value = item.get(field)
                
                if item_value is None:
                    continue
                
                in_range = self._check_range(item_value, min_val, max_val, inclusive)
                
                if invert:
                    in_range = not in_range
                
                if in_range:
                    filtered.append(item)
            
            return ActionResult(
                success=True,
                message="Range filter complete",
                data={
                    "original_count": len(data),
                    "filtered_count": len(filtered),
                    "removed_count": len(data) - len(filtered),
                    "field": field,
                    "min": min_val,
                    "max": max_val,
                    "inclusive": inclusive,
                    "filtered_data": filtered[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _check_range(self, value: Any, min_val: Any, max_val: Any, inclusive: bool) -> bool:
        if min_val is not None and max_val is not None:
            if inclusive:
                return min_val <= value <= max_val
            else:
                return min_val < value < max_val
        elif min_val is not None:
            if inclusive:
                return value >= min_val
            else:
                return value > min_val
        elif max_val is not None:
            if inclusive:
                return value <= max_val
            else:
                return value < max_val
        else:
            return True


class PatternFilterAction(BaseAction):
    """Filter by pattern matching."""
    action_type = "pattern_filter"
    display_name = "模式过滤"
    description = "按模式匹配进行数据过滤"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field")
            pattern = params.get("pattern")
            pattern_type = params.get("pattern_type", "regex")
            invert = params.get("invert", False)
            case_sensitive = params.get("case_sensitive", False)
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not field or not pattern:
                return ActionResult(success=False, message="field and pattern are required")
            
            compiled_pattern = None
            if pattern_type == "regex":
                flags = 0 if case_sensitive else re.IGNORECASE
                compiled_pattern = re.compile(pattern, flags)
            elif pattern_type == "wildcard":
                regex_pattern = self._wildcard_to_regex(pattern, case_sensitive)
                flags = 0 if case_sensitive else re.IGNORECASE
                compiled_pattern = re.compile(regex_pattern, flags)
            
            filtered = []
            
            for item in data:
                if not isinstance(item, dict):
                    continue
                
                item_value = item.get(field)
                
                if item_value is None:
                    continue
                
                item_value_str = str(item_value)
                matches = bool(compiled_pattern.search(item_value_str))
                
                if invert:
                    matches = not matches
                
                if matches:
                    filtered.append(item)
            
            return ActionResult(
                success=True,
                message="Pattern filter complete",
                data={
                    "original_count": len(data),
                    "filtered_count": len(filtered),
                    "removed_count": len(data) - len(filtered),
                    "field": field,
                    "pattern": pattern,
                    "pattern_type": pattern_type,
                    "filtered_data": filtered[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _wildcard_to_regex(self, pattern: str, case_sensitive: bool) -> str:
        regex = pattern.replace(".", "\\.")
        regex = regex.replace("*", ".*")
        regex = regex.replace("?", ".")
        return f"^{regex}$"


class CompositeFilterAction(BaseAction):
    """Combine multiple filters."""
    action_type = "composite_filter"
    display_name = "组合过滤"
    description = "组合多个过滤条件"
    
    def __init__(self):
        super().__init__()
        self._filters: List[Dict] = []
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            filter_logic = params.get("logic", "and")
            filters = params.get("filters", self._filters)
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not filters:
                return ActionResult(
                    success=True,
                    message="No filters applied",
                    data={
                        "original_count": len(data),
                        "filtered_count": len(data),
                        "filters_applied": 0
                    }
                )
            
            filtered = data
            
            for filter_spec in filters:
                filtered = self._apply_filter(filtered, filter_spec)
            
            return ActionResult(
                success=True,
                message="Composite filter complete",
                data={
                    "original_count": len(data),
                    "filtered_count": len(filtered),
                    "removed_count": len(data) - len(filtered),
                    "filters_applied": len(filters),
                    "logic": filter_logic,
                    "filtered_data": filtered[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _apply_filter(self, data: List, filter_spec: Dict) -> List:
        filter_type = filter_spec.get("type")
        
        if filter_type == "field":
            field_action = FieldFilterAction()
            result = field_action.execute(None, filter_spec)
            return result.data.get("filtered_data", data)
        elif filter_type == "range":
            range_action = RangeFilterAction()
            result = range_action.execute(None, filter_spec)
            return result.data.get("filtered_data", data)
        elif filter_type == "pattern":
            pattern_action = PatternFilterAction()
            result = pattern_action.execute(None, filter_spec)
            return result.data.get("filtered_data", data)
        else:
            return data


class DeduplicateFilterAction(BaseAction):
    """Remove duplicate entries."""
    action_type = "deduplicate_filter"
    display_name = "去重过滤"
    description = "去除重复数据条目"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            key = params.get("key")
            keep = params.get("keep", "first")
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if key:
                seen = set()
                deduplicated = []
                
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    
                    item_key = item.get(key)
                    
                    if item_key is None:
                        deduplicated.append(item)
                        continue
                    
                    if item_key not in seen:
                        seen.add(item_key)
                        deduplicated.append(item)
            else:
                if keep == "first":
                    deduplicated = list(dict.fromkeys(data))
                else:
                    deduplicated = list(dict.fromkeys(reversed(data)))[::-1]
            
            return ActionResult(
                success=True,
                message="Deduplication complete",
                data={
                    "original_count": len(data),
                    "deduplicated_count": len(deduplicated),
                    "removed_count": len(data) - len(deduplicated),
                    "key": key,
                    "keep": keep,
                    "deduplicated_data": deduplicated[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class CustomFilterAction(BaseAction):
    """Filter using custom filter function."""
    action_type = "custom_filter"
    display_name = "自定义过滤"
    description = "使用自定义函数进行数据过滤"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            filter_fn = params.get("filter_fn")
            description = params.get("description", "custom filter")
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not callable(filter_fn):
                return ActionResult(success=False, message="filter_fn must be callable")
            
            filtered = [item for item in data if filter_fn(item)]
            
            return ActionResult(
                success=True,
                message=f"Custom filter '{description}' complete",
                data={
                    "original_count": len(data),
                    "filtered_count": len(filtered),
                    "removed_count": len(data) - len(filtered),
                    "description": description,
                    "filtered_data": filtered[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
