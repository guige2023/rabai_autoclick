"""Data transformer action module for RabAI AutoClick.

Provides data transformation operations including mapping, filtering,
aggregation, and format conversion for ETL pipelines.
"""

import sys
import os
import json
from typing import Any, Dict, List, Optional, Callable, Union, TypeVar
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

T = TypeVar('T')


@dataclass
class TransformRule:
    """A data transformation rule."""
    source_path: str
    target_path: str
    transform_func: Optional[Callable[[Any], Any]] = None
    default: Any = None


class DataTransformerAction(BaseAction):
    """Transform data using mapping rules and functions.
    
    Supports field mapping, nested path access, value transformation,
    and format conversion.
    """
    action_type = "data_transformer"
    display_name = "数据转换"
    description = "数据映射、转换和格式化"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data transformation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data (dict or list)
                - rules: List of TransformRule dicts
                - mapping: Dict of target_key -> source_key or transform function
                - format: Output format ('dict', 'list', 'json')
        
        Returns:
            ActionResult with transformed data.
        """
        data = params.get('data')
        rules = params.get('rules', [])
        mapping = params.get('mapping', {})
        output_format = params.get('format', 'dict')
        
        if data is None:
            return ActionResult(success=False, message="data is required")
        
        # Convert rules to TransformRule objects
        transform_rules = []
        for rule in rules:
            if isinstance(rule, dict):
                transform_rules.append(TransformRule(
                    source_path=rule.get('source', ''),
                    target_path=rule.get('target', ''),
                    transform_func=rule.get('func'),
                    default=rule.get('default')
                ))
        
        # Apply mapping
        if mapping:
            result = self._apply_mapping(data, mapping)
        elif transform_rules:
            result = self._apply_rules(data, transform_rules)
        else:
            result = data
        
        # Format output
        if output_format == 'json':
            result = json.dumps(result, ensure_ascii=False)
        
        return ActionResult(
            success=True,
            message="Data transformed",
            data={'result': result}
        )
    
    def _apply_mapping(
        self,
        data: Union[Dict, List],
        mapping: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Apply field mapping to data."""
        if isinstance(data, dict):
            result = {}
            for target_key, source_spec in mapping.items():
                if callable(source_spec):
                    result[target_key] = source_spec(data)
                elif isinstance(source_spec, str):
                    if '.' in source_spec:
                        result[target_key] = self._get_nested(data, source_spec)
                    else:
                        result[target_key] = data.get(source_spec)
                else:
                    result[target_key] = source_spec
            return result
        elif isinstance(data, list):
            return [self._apply_mapping(item, mapping) for item in data]
        else:
            return data
    
    def _apply_rules(
        self,
        data: Union[Dict, List],
        rules: List[TransformRule]
    ) -> Dict[str, Any]:
        """Apply transformation rules to data."""
        if isinstance(data, dict):
            result = {}
            for rule in rules:
                value = self._get_nested(data, rule.source_path, rule.default)
                if rule.transform_func:
                    value = rule.transform_func(value)
                self._set_nested(result, rule.target_path, value)
            return result
        elif isinstance(data, list):
            return [self._apply_rules(item, rules) for item in data]
        else:
            return data
    
    def _get_nested(self, data: Dict, path: str, default: Any = None) -> Any:
        """Get value from nested dict using dot notation."""
        if not path:
            return data
        parts = path.split('.')
        value = data
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            elif isinstance(value, list) and part.isdigit():
                idx = int(part)
                value = value[idx] if idx < len(value) else None
            else:
                return default
            if value is None:
                return default
        return value
    
    def _set_nested(self, data: Dict, path: str, value: Any) -> None:
        """Set value in nested dict using dot notation."""
        parts = path.split('.')
        current = data
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value


class DataFilterAction(BaseAction):
    """Filter data based on conditions."""
    action_type = "data_filter"
    display_name = "数据过滤"
    description = "根据条件过滤数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data filtering.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data (list of dicts)
                - conditions: Dict of field -> expected value or filter function
                - exclude: If True, exclude matching items (default False)
        
        Returns:
            ActionResult with filtered data.
        """
        data = params.get('data')
        conditions = params.get('conditions', {})
        exclude = params.get('exclude', False)
        
        if data is None:
            return ActionResult(success=False, message="data is required")
        
        if not isinstance(data, list):
            return ActionResult(success=False, message="data must be a list")
        
        filtered = []
        for item in data:
            if not isinstance(item, dict):
                continue
            
            matches = self._check_conditions(item, conditions)
            
            if exclude:
                if not matches:
                    filtered.append(item)
            else:
                if matches:
                    filtered.append(item)
        
        return ActionResult(
            success=True,
            message=f"Filtered to {len(filtered)} items",
            data={'items': filtered, 'count': len(filtered)}
        )
    
    def _check_conditions(self, item: Dict, conditions: Dict) -> bool:
        """Check if item matches all conditions."""
        for field_path, condition in conditions.items():
            value = self._get_nested_value(item, field_path)
            
            if callable(condition):
                if not condition(value):
                    return False
            elif isinstance(condition, (list, tuple)):
                if value not in condition:
                    return False
            elif value != condition:
                return False
        
        return True
    
    def _get_nested_value(self, data: Dict, path: str) -> Any:
        """Get value using dot notation."""
        parts = path.split('.')
        value = data
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
        return value


class DataAggregatorAction(BaseAction):
    """Aggregate data using group-by and aggregation functions."""
    action_type = "data_aggregator"
    display_name = "数据聚合"
    description = "分组聚合计算"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data aggregation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data (list of dicts)
                - group_by: Field to group by
                - aggregations: Dict of result_field -> (agg_func, source_field)
                  Example: {'total': ('sum', 'amount'), 'count': ('count', None)}
        
        Returns:
            ActionResult with aggregated results.
        """
        data = params.get('data')
        group_by = params.get('group_by')
        aggregations = params.get('aggregations', {})
        
        if data is None:
            return ActionResult(success=False, message="data is required")
        
        if not isinstance(data, list):
            return ActionResult(success=False, message="data must be a list")
        
        if not group_by:
            return ActionResult(success=False, message="group_by is required")
        
        # Group data
        groups: Dict[str, List[Dict]] = {}
        for item in data:
            if not isinstance(item, dict):
                continue
            key = str(item.get(group_by, 'null'))
            if key not in groups:
                groups[key] = []
            groups[key].append(item)
        
        # Aggregate each group
        results = []
        for group_key, items in groups.items():
            result = {group_by: group_key}
            
            for agg_name, (agg_func, source_field) in aggregations.items():
                result[agg_name] = self._aggregate(items, agg_func, source_field)
            
            results.append(result)
        
        return ActionResult(
            success=True,
            message=f"Aggregated into {len(results)} groups",
            data={'groups': results, 'group_count': len(results)}
        )
    
    def _aggregate(
        self,
        items: List[Dict],
        func: str,
        field: Optional[str]
    ) -> Any:
        """Apply aggregation function."""
        if func == 'count':
            return len(items)
        
        if func == 'sum':
            return sum(item.get(field, 0) for item in items if item.get(field) is not None)
        
        if func == 'avg':
            values = [item.get(field, 0) for item in items if item.get(field) is not None]
            return sum(values) / len(values) if values else 0
        
        if func == 'min':
            values = [item.get(field) for item in items if item.get(field) is not None]
            return min(values) if values else None
        
        if func == 'max':
            values = [item.get(field) for item in items if item.get(field) is not None]
            return max(values) if values else None
        
        if func == 'first':
            return items[0].get(field) if items else None
        
        if func == 'last':
            return items[-1].get(field) if items else None
        
        return None


class DataSorterAction(BaseAction):
    """Sort data by one or more fields."""
    action_type = "data_sorter"
    display_name = "数据排序"
    description = "多字段排序"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data sorting.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data (list)
                - sort_by: Field to sort by (or list of fields)
                - reverse: Sort descending (default False)
        
        Returns:
            ActionResult with sorted data.
        """
        data = params.get('data')
        sort_by = params.get('sort_by')
        reverse = params.get('reverse', False)
        
        if data is None:
            return ActionResult(success=False, message="data is required")
        
        if not isinstance(data, list):
            return ActionResult(success=False, message="data must be a list")
        
        if not sort_by:
            return ActionResult(success=False, message="sort_by is required")
        
        sort_fields = [sort_by] if isinstance(sort_by, str) else sort_by
        
        def sort_key(item):
            values = []
            for field in sort_fields:
                if isinstance(item, dict):
                    value = item.get(field)
                else:
                    value = getattr(item, field, None)
                values.append(value)
            return tuple(values)
        
        sorted_data = sorted(data, key=sort_key, reverse=reverse)
        
        return ActionResult(
            success=True,
            message=f"Sorted {len(sorted_data)} items",
            data={'items': sorted_data, 'count': len(sorted_data)}
        )
