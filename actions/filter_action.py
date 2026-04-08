"""Filter action module for RabAI AutoClick.

Provides advanced filtering actions for lists, dicts,
and structured data with multiple filter strategies.
"""

import re
import sys
import os
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataFilter:
    """Filter data structures based on predicates."""
    
    @staticmethod
    def filter_list(data: List[Any], predicate: Callable[[Any], bool]) -> List[Any]:
        """Filter list by predicate.
        
        Args:
            data: List to filter.
            predicate: Filter function.
        
        Returns:
            Filtered list.
        """
        return [item for item in data if predicate(item)]
    
    @staticmethod
    def filter_dict(data: Dict[str, Any], predicate: Callable[[str, Any], bool]) -> Dict[str, Any]:
        """Filter dict by predicate.
        
        Args:
            data: Dict to filter.
            predicate: Function(key, value) returning bool.
        
        Returns:
            Filtered dict.
        """
        return {k: v for k, v in data.items() if predicate(k, v)}
    
    @staticmethod
    def filter_by_key(data: List[Dict[str, Any]], key: str, values: List[Any]) -> List[Dict[str, Any]]:
        """Filter list of dicts by key value.
        
        Args:
            data: List of dicts.
            key: Key to filter on.
            values: Allowed values.
        
        Returns:
            Filtered list.
        """
        return [item for item in data if item.get(key) in values]
    
    @staticmethod
    def filter_by_range(data: List[Dict[str, Any]], key: str, min_val: float = None, max_val: float = None) -> List[Dict[str, Any]]:
        """Filter list of dicts by numeric range.
        
        Args:
            data: List of dicts.
            key: Key to filter on.
            min_val: Minimum value (inclusive).
            max_val: Maximum value (inclusive).
        
        Returns:
            Filtered list.
        """
        result = []
        for item in data:
            value = item.get(key)
            if value is None:
                continue
            try:
                num_val = float(value)
                if min_val is not None and num_val < min_val:
                    continue
                if max_val is not None and num_val > max_val:
                    continue
                result.append(item)
            except (TypeError, ValueError):
                continue
        return result
    
    @staticmethod
    def filter_by_pattern(data: List[str], pattern: str, case_sensitive: bool = True) -> List[str]:
        """Filter strings by regex pattern.
        
        Args:
            data: List of strings.
            pattern: Regex pattern.
            case_sensitive: Whether pattern is case sensitive.
        
        Returns:
            Filtered list.
        """
        flags = 0 if case_sensitive else re.IGNORECASE
        compiled = re.compile(pattern, flags)
        return [item for item in data if compiled.search(item)]
    
    @staticmethod
    def filter_unique(data: List[Any], key: Callable[[Any], Any] = None) -> List[Any]:
        """Filter to unique items.
        
        Args:
            data: List to deduplicate.
            key: Optional key function for comparison.
        
        Returns:
            List with duplicates removed.
        """
        if key is None:
            seen = set()
            result = []
            for item in data:
                if item not in seen:
                    seen.add(item)
                    result.append(item)
            return result
        else:
            seen = set()
            result = []
            for item in data:
                k = key(item)
                if k not in seen:
                    seen.add(k)
                    result.append(item)
            return result
    
    @staticmethod
    def filter_by_length(data: List[Any], min_length: int = 0, max_length: int = None) -> List[Any]:
        """Filter by item length.
        
        Args:
            data: List to filter.
            min_length: Minimum length.
            max_length: Maximum length.
        
        Returns:
            Filtered list.
        """
        result = []
        for item in data:
            try:
                length = len(item)
                if length >= min_length:
                    if max_length is None or length <= max_length:
                        result.append(item)
            except TypeError:
                continue
        return result
    
    @staticmethod
    def filter_by_index(data: List[Any], indices: List[int]) -> List[Any]:
        """Filter by specific indices.
        
        Args:
            data: List to filter.
            indices: Indices to keep.
        
        Returns:
            Filtered list.
        """
        return [data[i] for i in indices if 0 <= i < len(data)]


class FilterListAction(BaseAction):
    """Filter list by predicate."""
    action_type = "filter_list"
    display_name = "过滤列表"
    description = "按条件过滤列表"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Filter list.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, predicate.
        
        Returns:
            ActionResult with filtered list.
        """
        data = params.get('data', [])
        predicate = params.get('predicate', 'lambda x: bool(x)')
        
        if not isinstance(data, list):
            return ActionResult(success=False, message="data must be a list")
        
        try:
            if isinstance(predicate, str):
                pred_func = eval(f"lambda x: {predicate}")
            else:
                pred_func = predicate
            
            filtered = DataFilter.filter_list(data, pred_func)
            
            return ActionResult(
                success=True,
                message=f"Filtered {len(data)} items to {len(filtered)}",
                data={"filtered": filtered, "original_count": len(data), "filtered_count": len(filtered)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Filter error: {str(e)}")


class FilterByKeyAction(BaseAction):
    """Filter list of dicts by key values."""
    action_type = "filter_by_key"
    display_name = "按键过滤"
    description = "按字典键值过滤"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Filter by key.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, key, values.
        
        Returns:
            ActionResult with filtered list.
        """
        data = params.get('data', [])
        key = params.get('key', '')
        values = params.get('values', [])
        
        if not isinstance(data, list):
            return ActionResult(success=False, message="data must be a list")
        
        if not key:
            return ActionResult(success=False, message="key is required")
        
        try:
            filtered = DataFilter.filter_by_key(data, key, values)
            
            return ActionResult(
                success=True,
                message=f"Filtered to {len(filtered)} items matching {key} in {values}",
                data={"filtered": filtered, "count": len(filtered)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Filter error: {str(e)}")


class FilterByRangeAction(BaseAction):
    """Filter by numeric range."""
    action_type = "filter_by_range"
    display_name = "按范围过滤"
    description = "按数值范围过滤"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Filter by range.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, key, min, max.
        
        Returns:
            ActionResult with filtered list.
        """
        data = params.get('data', [])
        key = params.get('key', '')
        min_val = params.get('min', None)
        max_val = params.get('max', None)
        
        if not isinstance(data, list):
            return ActionResult(success=False, message="data must be a list")
        
        if not key:
            return ActionResult(success=False, message="key is required")
        
        try:
            filtered = DataFilter.filter_by_range(data, key, min_val, max_val)
            
            return ActionResult(
                success=True,
                message=f"Filtered to {len(filtered)} items in range [{min_val}, {max_val}]",
                data={"filtered": filtered, "count": len(filtered)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Filter error: {str(e)}")


class FilterByPatternAction(BaseAction):
    """Filter strings by regex pattern."""
    action_type = "filter_by_pattern"
    display_name = "按模式过滤"
    description = "按正则模式过滤"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Filter by pattern.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, pattern, case_sensitive.
        
        Returns:
            ActionResult with filtered list.
        """
        data = params.get('data', [])
        pattern = params.get('pattern', '')
        case_sensitive = params.get('case_sensitive', True)
        
        if not isinstance(data, list):
            return ActionResult(success=False, message="data must be a list")
        
        if not pattern:
            return ActionResult(success=False, message="pattern is required")
        
        try:
            filtered = DataFilter.filter_by_pattern(data, pattern, case_sensitive)
            
            return ActionResult(
                success=True,
                message=f"Filtered to {len(filtered)} items matching pattern",
                data={"filtered": filtered, "count": len(filtered)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Filter error: {str(e)}")


class FilterUniqueAction(BaseAction):
    """Remove duplicates from list."""
    action_type = "filter_unique"
    display_name = "去重"
    description = "移除列表重复项"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Filter unique.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, key.
        
        Returns:
            ActionResult with unique items.
        """
        data = params.get('data', [])
        key = params.get('key', None)
        
        if not isinstance(data, list):
            return ActionResult(success=False, message="data must be a list")
        
        try:
            if key:
                key_func = eval(f"lambda x: {key}") if isinstance(key, str) else key
                filtered = DataFilter.filter_unique(data, key_func)
            else:
                filtered = DataFilter.filter_unique(data)
            
            removed = len(data) - len(filtered)
            
            return ActionResult(
                success=True,
                message=f"Removed {removed} duplicates, {len(filtered)} unique items",
                data={"unique": filtered, "count": len(filtered), "removed": removed}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Filter error: {str(e)}")


class FilterByLengthAction(BaseAction):
    """Filter by item length."""
    action_type = "filter_by_length"
    display_name = "按长度过滤"
    description = "按长度过滤"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Filter by length.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, min_length, max_length.
        
        Returns:
            ActionResult with filtered list.
        """
        data = params.get('data', [])
        min_length = params.get('min_length', 0)
        max_length = params.get('max_length', None)
        
        if not isinstance(data, list):
            return ActionResult(success=False, message="data must be a list")
        
        try:
            filtered = DataFilter.filter_by_length(data, min_length, max_length)
            
            return ActionResult(
                success=True,
                message=f"Filtered to {len(filtered)} items with length in range [{min_length}, {max_length}]",
                data={"filtered": filtered, "count": len(filtered)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Filter error: {str(e)}")
